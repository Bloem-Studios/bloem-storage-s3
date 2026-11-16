package s3provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestDescribeDeclaresRawMediaAndDirectoryBrowsing(t *testing.T) {
	p, _ := protocolProvider(t, func(http.ResponseWriter, *http.Request) { t.Error("Describe contacted S3") })
	for _, mode := range []string{"versioned", "checked-etag", ""} {
		p.cfg.RevisionMode = mode
		desc, err := p.Describe(context.Background(), &pb.DescribeRequest{})
		if err != nil {
			t.Fatal(err)
		}
		source := desc.Sources[0]
		if !reflect.DeepEqual(source.MediaKinds, []string{"ebooks", "movies", "audiobooks"}) || !source.DirectoryOnlyListing {
			t.Fatalf("missing raw-file/browse declarations: %v", source)
		}
		if source.RevisionPinnedReads != (mode == "versioned") {
			t.Fatalf("changed pin policy: %v", source)
		}
	}
}

// Filter before file identity registration and HEAD: mutable leaf files must
// not make strict-mode browsing fail or populate an inventory of files.
func TestDirectoryOnlyRootNestedAndEmptyLeaf(t *testing.T) {
	for _, mode := range []string{"versioned", "checked-etag", ""} {
		t.Run(mode, func(t *testing.T) {
			f := newFakeS3()
			for _, key := range []string{"books/", "books/a.epub", "books/book/01.mp3", "books/book/sub/02.flac", "books/empty/", "books/foo", "books/foo/a.mkv", "books/z.pdf"} {
				f.put(key, "data")
			}
			p := f.provider(t, mode)
			request := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), DirectoriesOnly: true}
			root, err := p.List(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if !root.Complete || len(root.Entries) != 3 {
				t.Fatalf("root dirs: %v", root)
			}
			for i, want := range []string{"book/", "empty/", "foo/"} {
				entry := root.Entries[i]
				if entry.Kind != pb.EntryKind_ENTRY_KIND_DIRECTORY || entry.LogicalPath != "books/"+want || entry.Revision != "s3-layout-v1" {
					t.Fatalf("root entry: %v", entry)
				}
			}
			request.DirectoryId = root.Entries[0].Id
			nested, err := p.List(context.Background(), request)
			if err != nil || !nested.GetComplete() || len(nested.GetEntries()) != 1 || nested.Entries[0].LogicalPath != "books/book/sub/" {
				t.Fatalf("nested dirs: %v %v", nested, err)
			}
			for _, id := range []string{root.Entries[1].Id, nested.Entries[0].Id} {
				request.DirectoryId = id
				leaf, err := p.List(context.Background(), request)
				if err != nil || !leaf.GetComplete() || len(leaf.GetEntries()) != 0 || leaf.GetNextCursor() != "" {
					t.Fatalf("selectable leaf: %v %v", leaf, err)
				}
			}
			if n := f.heads.Load() + f.gets.Load() + f.bucketHeads.Load(); n != 0 {
				t.Fatalf("browse made %d HEAD/GetObject calls", n)
			}
			for _, q := range f.listQueries {
				if q.Get("delimiter") != "/" || !strings.HasPrefix(q.Get("prefix"), "books/") {
					t.Fatalf("browse listing was not delimited within the source: %v", q)
				}
			}
			err = p.db.View(func(tx *bolt.Tx) error {
				return tx.Bucket([]byte("ids")).ForEach(func(_, raw []byte) error {
					var item identity
					if err := json.Unmarshal(raw, &item); err != nil {
						return err
					}
					if item.Kind == pb.EntryKind_ENTRY_KIND_FILE {
						t.Errorf("browse registered a file: %s", item.Key)
					}
					return nil
				})
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDirectoryOnlyPagesAreNotLimitedByFileHeadBudget(t *testing.T) {
	f := newFakeS3()
	f.versioned = true
	for i := 0; i < maxRevisionHeadsPerList+12; i++ {
		f.put(fmt.Sprintf("books/%03d/file.mp3", i), "data")
	}
	p := f.provider(t, "versioned")
	page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: maxRevisionHeadsPerList + 12, DirectoriesOnly: true})
	if err != nil || !page.GetComplete() || len(page.GetEntries()) != maxRevisionHeadsPerList+12 || f.heads.Load() != 0 {
		t.Fatalf("directory page incorrectly HEAD-budgeted: %v %v HEADs=%d", page, err, f.heads.Load())
	}
}

func TestDirectoryOnlyFileHeavyPaginationRestartAndRetry(t *testing.T) {
	var calls atomic.Int32
	p, path := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			t.Error("browse hydrated a file")
			w.WriteHeader(403)
			return
		}
		if r.URL.Query().Get("delimiter") != "/" {
			t.Error("browse listing was not delimited")
		}
		calls.Add(1)
		n := 0
		if token := r.URL.Query().Get("continuation-token"); token != "" {
			var err error
			n, err = strconv.Atoi(token)
			if err != nil || n < 1 || n > 10 {
				t.Errorf("bad backend position: %q", token)
				w.WriteHeader(400)
				return
			}
		}
		fmt.Fprintf(w, "<ListBucketResult><IsTruncated>%t</IsTruncated>", n < 10)
		if n < 10 {
			fmt.Fprintf(w, "<NextContinuationToken>%d</NextContinuationToken>", n+1)
		}
		// Eight full file-only pages precede directories. The next page
		// repeats its final prefix, which must appear only once after restart.
		if n < 8 {
			for i := 0; i < 512; i++ {
				fmt.Fprintf(w, "<Contents><Key>books/a%02d%03d.mp3</Key><Size>4</Size></Contents>", n, i)
			}
		}
		if n == 8 || n == 9 {
			io.WriteString(w, "<CommonPrefixes><Prefix>books/dir/</Prefix></CommonPrefixes>")
		}
		if n == 10 {
			io.WriteString(w, "<CommonPrefixes><Prefix>books/empty/</Prefix></CommonPrefixes>")
		}
		io.WriteString(w, "</ListBucketResult>")
	})
	cfg := p.cfg
	cfg.RevisionMode = "versioned"
	p.Close()
	p, err := Open(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	req := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: 1, DirectoriesOnly: true}
	page, err := p.List(context.Background(), req)
	if err != nil || page.GetComplete() || len(page.GetEntries()) != 0 || page.GetNextCursor() == "" || calls.Load() != 8 {
		t.Fatalf("file-only pages lost continuation/fetch bound: %v %v calls=%d", page, err, calls.Load())
	}
	// Reopen the actual durable cursor using the unchanged stored configuration.
	p.Close()
	resumed, err := Open(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	var entries []*pb.Entry
	for n := 0; !page.Complete && n < 4; n++ {
		req.Cursor = page.NextCursor
		page, err = resumed.List(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		retry, err := resumed.List(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		// A new opaque continuation may be issued, while page contents and status
		// must remain identical for retries of the same input cursor.
		retry.NextCursor = page.NextCursor
		if !proto.Equal(page, retry) {
			t.Fatalf("browse retry changed page: %v %v", page, retry)
		}
		entries = append(entries, page.Entries...)
	}
	if !page.Complete || len(entries) != 2 || entries[0].LogicalPath != "books/dir/" || entries[1].LogicalPath != "books/empty/" {
		t.Fatalf("filtered pages duplicated/lost dirs: %v %v", entries, page)
	}
}

func TestListCursorsBindIntentAndRequestedPageSize(t *testing.T) {
	for _, browse := range []bool{false, true} {
		t.Run(fmt.Sprint(browse), func(t *testing.T) {
			f := newFakeS3()
			f.versioned = true
			f.put("books/a/f.mp3", "data")
			f.put("books/b/f.mp3", "data")
			p := f.provider(t, "versioned")
			// Reject changes to enumeration intent or the requested page size.
			req := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: 1, DirectoriesOnly: browse}
			page, err := p.List(context.Background(), req)
			if err != nil || page.GetNextCursor() == "" {
				t.Fatalf("cursor setup: %v %v", page, err)
			}
			req.Cursor = page.NextCursor
			for _, mutate := range []func(*pb.ListRequest){
				func(r *pb.ListRequest) { r.DirectoriesOnly = !browse },
				func(r *pb.ListRequest) { r.MaxEntries = 2 },
			} {
				crossed := proto.Clone(req).(*pb.ListRequest)
				mutate(crossed)
				before := f.lists.Load() + f.heads.Load()
				if _, err := p.List(context.Background(), crossed); status.Code(err) != codes.InvalidArgument {
					t.Fatalf("crossed cursor admitted: %v %v", crossed, err)
				}
				if f.lists.Load()+f.heads.Load() != before {
					t.Error("crossed cursor reached backend")
				}
			}
			resumed, err := p.List(context.Background(), req)
			if err != nil || !resumed.GetComplete() || len(resumed.GetEntries()) != 1 || resumed.Entries[0].LogicalPath != "books/b/" {
				t.Fatalf("rejected requests damaged cursor: %v %v", resumed, err)
			}
		})
	}
}

func TestDirectoryBrowsePreservesScanEntriesAndTokens(t *testing.T) {
	f := newFakeS3()
	f.versioned = true
	f.put("books/a.mp3", "data")
	f.put("books/book/f.mp3", "data")
	p := f.provider(t, "versioned")
	req := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: 1}
	scan, err := p.List(context.Background(), req)
	if err != nil || scan.GetNextCursor() == "" {
		t.Fatalf("scan setup: %v %v", scan, err)
	}
	var savedCursor []byte
	var savedIDs map[string]string
	snapshot := func() map[string]string {
		t.Helper()
		result := map[string]string{}
		if err := p.db.View(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("ids")).ForEach(func(k, v []byte) error { result[string(k)] = string(v); return nil })
		}); err != nil {
			t.Fatal(err)
		}
		return result
	}
	savedIDs = snapshot()
	if err := p.db.View(func(tx *bolt.Tx) error {
		savedCursor = append([]byte{}, tx.Bucket([]byte("cursors")).Get([]byte(scan.NextCursor))...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	browseReq := proto.Clone(req).(*pb.ListRequest)
	browseReq.DirectoriesOnly = true
	browse, err := p.List(context.Background(), browseReq)
	if err != nil || !browse.GetComplete() || len(browse.GetEntries()) != 1 || browse.ChangeToken != "" {
		t.Fatalf("browse page: %v %v", browse, err)
	}
	if !reflect.DeepEqual(savedIDs, snapshot()) {
		t.Fatal("browse changed existing scan identities")
	}
	if err := p.db.View(func(tx *bolt.Tx) error {
		if string(savedCursor) != string(tx.Bucket([]byte("cursors")).Get([]byte(scan.NextCursor))) {
			t.Error("browse changed retained scan cursor")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	req.Cursor = scan.NextCursor
	after, err := p.List(context.Background(), req)
	if err != nil || !after.GetComplete() || len(after.GetEntries()) != 1 || !proto.Equal(after.Entries[0], browse.Entries[0]) || after.ChangeToken != "" {
		t.Fatalf("browse damaged scan continuation: %v %v", after, err)
	}
	req.Cursor = ""
	req.MaxEntries = 0
	full, err := p.List(context.Background(), req)
	if err != nil || !full.GetComplete() || len(full.GetEntries()) != 2 || !proto.Equal(full.Entries[0], scan.Entries[0]) || !proto.Equal(full.Entries[1], after.Entries[0]) {
		t.Fatalf("full scan changed after browse: %v %v", full, err)
	}
	if f.gets.Load() != 0 {
		t.Fatal("unexpected object body read")
	}
}

func TestListCursorBindsRequestedSizeBeforeScanHeadCap(t *testing.T) {
	const requested, total = maxRevisionHeadsPerList + 1, maxRevisionHeadsPerList + 6
	for _, browse := range []bool{false, true} {
		t.Run(fmt.Sprint(browse), func(t *testing.T) {
			f := newFakeS3()
			f.versioned = true
			for i := 0; i < total; i++ {
				key := fmt.Sprintf("books/%03d.mp3", i)
				if browse {
					key = fmt.Sprintf("books/%03d/f.mp3", i)
				}
				f.put(key, "data")
			}
			p := f.provider(t, "versioned")
			req := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: requested, DirectoriesOnly: browse}
			page, err := p.List(context.Background(), req)
			want := requested
			if !browse {
				want = maxRevisionHeadsPerList
			}
			if err != nil || page.GetNextCursor() == "" || len(page.GetEntries()) != want {
				t.Fatalf("cursor setup: entries=%d err=%v", len(page.GetEntries()), err)
			}
			req.Cursor = page.NextCursor
			req.MaxEntries = requested + 1
			if _, err := p.List(context.Background(), req); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("requested size changed above HEAD cap: %v", err)
			}
			req.MaxEntries = requested
			resumed, err := p.List(context.Background(), req)
			if err != nil || !resumed.GetComplete() || len(resumed.GetEntries()) != total-want {
				t.Fatalf("original size cannot resume: %v %v", resumed, err)
			}
		})
	}
}

func TestDirectoryOnlyRejectsMalformedAndLoopingDelimitedPages(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"outside-prefix", `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>other/f.mp3</Key><Size>4</Size></Contents></ListBucketResult>`},
		{"traversal", `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>books/../f.mp3</Key><Size>4</Size></Contents></ListBucketResult>`},
		{"unordered-files", `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>books/b.mp3</Key><Size>4</Size></Contents><Contents><Key>books/a.mp3</Key><Size>4</Size></Contents></ListBucketResult>`},
		{"missing-flag", `<ListBucketResult></ListBucketResult>`},
		{"missing-token", `<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>`},
		{"repeated-token", `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>same</NextContinuationToken></ListBucketResult>`},
		{"ignored-delimiter", `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>books/book/f.mp3</Key><Size>4</Size></Contents></ListBucketResult>`},
		{"nested-prefix", `<ListBucketResult><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>books/book/sub/</Prefix></CommonPrefixes></ListBucketResult>`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, tc.body) })
			page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), DirectoriesOnly: true})
			if page != nil || status.Code(err) != codes.Unavailable {
				t.Fatalf("browse accepted malformed delimited page: %v %v", page, err)
			}
		})
	}
}

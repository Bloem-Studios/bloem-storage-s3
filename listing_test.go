package s3provider

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func decodeRevision(t *testing.T, encoded string) revision {
	t.Helper()
	var rev revision
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || json.Unmarshal(raw, &rev) != nil {
		t.Fatalf("undecodable revision %q: %v", encoded, err)
	}
	return rev
}

func stateWrites(p *Provider) int64 {
	stats := p.db.Stats()
	return stats.TxStats.GetWrite()
}

func logicalPaths(entries []*pb.Entry) []string {
	paths := make([]string, len(entries))
	for i, entry := range entries {
		paths[i] = entry.LogicalPath
	}
	return paths
}

func readAll(t *testing.T, p *Provider, entry *pb.Entry, offset, length int64) ([]byte, int, error) {
	t.Helper()
	var data []byte
	chunks := 0
	err := p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision, Offset: offset, Length: length}, func(c *pb.ReadChunk) error {
		chunks++
		data = append(data, c.Data...)
		return nil
	})
	return data, chunks, err
}

// Each level lists only its immediate children: a large sibling subtree must
// not be walked, and directory pages need no per-file object requests.
func TestListUsesDelimitedImmediateChildren(t *testing.T) {
	for _, mode := range []string{"checked-etag", "versioned", ""} {
		t.Run("mode-"+mode, func(t *testing.T) {
			f := newFakeS3()
			f.versioned = mode == "versioned"
			f.put("books/a.mkv", "data")
			for i := 0; i < 3000; i++ {
				f.put(fmt.Sprintf("books/dir1/sub/x%04d.srt", i), "s")
			}
			f.put("books/dir2/movie.mkv", "movie")
			f.put("books/zz.nfo", "nfo")
			p := f.provider(t, mode)
			req := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: 512}
			page, err := p.List(context.Background(), req)
			if err != nil || !page.Complete || page.NextCursor != "" {
				t.Fatalf("root page: %v %v", page, err)
			}
			if got := strings.Join(logicalPaths(page.Entries), ","); got != "books/a.mkv,books/dir1/,books/dir2/,books/zz.nfo" {
				t.Fatalf("root children = %s", got)
			}
			if f.lists.Load() != 1 || f.listQueries[0].Get("delimiter") != "/" || f.listQueries[0].Get("prefix") != "books/" {
				t.Fatalf("root listing walked the subtree: lists=%d queries=%v", f.lists.Load(), f.listQueries)
			}
			wantHeads := map[string]int32{"versioned": 2, "checked-etag": 1, "": 0}[mode]
			if f.heads.Load() != wantHeads || f.gets.Load() != 0 {
				t.Fatalf("root listing made HEADs=%d GETs=%d, want %d HEADs", f.heads.Load(), f.gets.Load(), wantHeads)
			}
			for _, entry := range page.Entries {
				if mode != "" && entry.Revision == "" {
					t.Fatalf("entry without revision: %v", entry)
				}
			}
			// A rescan of known identities performs no durable write.
			writes := stateWrites(p)
			again, err := p.List(context.Background(), req)
			if err != nil || !proto.Equal(page, again) {
				t.Fatalf("rescan changed page: %v %v", again, err)
			}
			if stateWrites(p) != writes {
				t.Fatal("rescan of known identities wrote to the state DB")
			}
			sub, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: page.Entries[1].Id, MaxEntries: 512})
			if err != nil || !sub.Complete || len(sub.Entries) != 1 || sub.Entries[0].LogicalPath != "books/dir1/sub/" || sub.Entries[0].Kind != pb.EntryKind_ENTRY_KIND_DIRECTORY {
				t.Fatalf("nested directory page: %v %v", sub, err)
			}
		})
	}
}

// Checked ETag listings take ETag, size and modification time from
// ListObjectsV2, so a host page fills to its requested size.
func TestCheckedListingFillsHostPagesFromListMetadata(t *testing.T) {
	f := newFakeS3()
	objects := map[string]*fakeObject{}
	for i := 0; i < 600; i++ {
		key := fmt.Sprintf("books/m%03d.mkv", i)
		objects[key] = f.put(key, fmt.Sprintf("movie-%03d", i))
	}
	p := f.provider(t, "checked-etag")
	req := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: 512}
	first, err := p.List(context.Background(), req)
	if err != nil || first.Complete || len(first.Entries) != 512 {
		t.Fatalf("first page: entries=%d complete=%v err=%v", len(first.GetEntries()), first.GetComplete(), err)
	}
	req.Cursor = first.NextCursor
	second, err := p.List(context.Background(), req)
	if err != nil || !second.Complete || len(second.Entries) != 88 {
		t.Fatalf("second page: entries=%d complete=%v err=%v", len(second.GetEntries()), second.GetComplete(), err)
	}
	if f.heads.Load() > f.lists.Load() || f.gets.Load() != 0 {
		t.Fatalf("listing hydrated files individually: HEADs=%d lists=%d GETs=%d", f.heads.Load(), f.lists.Load(), f.gets.Load())
	}
	for _, entry := range append(first.Entries, second.Entries...) {
		obj := objects[entry.LogicalPath]
		rev := decodeRevision(t, entry.Revision)
		if !rev.Listed || rev.ID != entry.Id || rev.ETag != obj.etag || rev.Version != "" || rev.Size != int64(len(obj.data)) || rev.Modified != obj.modified.UnixNano() {
			t.Fatalf("listed revision %+v for %s", rev, entry.LogicalPath)
		}
		if entry.Size != int64(len(obj.data)) || entry.ModifiedUnixNano != obj.modified.UnixNano() {
			t.Fatalf("entry metadata %v", entry)
		}
	}
}

// The host's scan reader requires Stat(expected) to return exactly the listed
// name, path, size, revision and modification time.
func TestListedRevisionStatMatchesListingAndReadsRanges(t *testing.T) {
	for _, unquoted := range []bool{false, true} {
		t.Run(fmt.Sprintf("unquoted-list-etag-%v", unquoted), func(t *testing.T) {
			f := newFakeS3()
			f.listETagUnquoted = unquoted
			f.put("books/movie.mkv", "0123456789")
			p := f.provider(t, "checked-etag")
			page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
			if err != nil || len(page.GetEntries()) != 1 {
				t.Fatalf("listing: %v %v", page, err)
			}
			entry := page.Entries[0]
			stat, err := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision})
			if err != nil || !proto.Equal(stat.Entry, entry) {
				t.Fatalf("Stat(expected) differs from listing:\n stat=%v\n list=%v\n err=%v", stat.GetEntry(), entry, err)
			}
			headsBefore := f.heads.Load()
			data, chunks, err := readAll(t, p, entry, 2, 4)
			if err != nil || string(data) != "2345" || chunks != 1 || f.gets.Load() != 1 || f.heads.Load()-headsBefore != 2 {
				t.Fatalf("listed range: data=%q chunks=%d GETs=%d HEADs=%d err=%v", data, chunks, f.gets.Load(), f.heads.Load()-headsBefore, err)
			}
			if data, chunks, err = readAll(t, p, entry, 10, 0); err != nil || chunks != 1 || len(data) != 0 {
				t.Fatalf("listed zero range: %d %v", chunks, err)
			}
			f.put("books/movie.mkv", "9876543210")
			if _, err = p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("replaced object Stat: %v", err)
			}
			if data, chunks, err = readAll(t, p, entry, 0, 4); status.Code(err) != codes.FailedPrecondition || chunks != 0 {
				t.Fatalf("replaced object read: data=%q chunks=%d err=%v", data, chunks, err)
			}
		})
	}
}

// One gateway lists a different ETag scheme than HEAD and GET report. Listing
// ETags are then not read validators: the page probe detects the disagreement
// and the page's files take HEAD revisions, reusing the probe's response.
func TestCheckedListingHydratesWhenListETagsDisagreeWithHEAD(t *testing.T) {
	f := newFakeS3()
	f.listETagDiffers = true
	for _, key := range []string{"books/t/movie.mkv", "books/t/movie.nfo", "books/t/movie.srt", "books/t/poster.jpg", "books/t/sample.mkv"} {
		f.put(key, "0123456789")
	}
	p := f.provider(t, "checked-etag")
	root, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
	if err != nil || len(root.GetEntries()) != 1 {
		t.Fatalf("root: %v %v", root, err)
	}
	page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: root.Entries[0].Id})
	if err != nil || !page.GetComplete() || len(page.GetEntries()) != 5 {
		t.Fatalf("title page: %v %v", page, err)
	}
	if f.heads.Load() != 5 {
		t.Fatalf("HEADs=%d, want one per file with the probe reused", f.heads.Load())
	}
	for _, entry := range page.Entries {
		rev := decodeRevision(t, entry.Revision)
		if rev.Listed || rev.ETag != f.objects[entry.LogicalPath].etag {
			t.Fatalf("disagreeing listing ETag became a revision: %+v", rev)
		}
		stat, err := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision})
		if err != nil || !proto.Equal(stat.Entry, entry) {
			t.Fatalf("Stat(expected) differs from listing: %v %v", stat.GetEntry(), err)
		}
	}
	if data, _, err := readAll(t, p, page.Entries[0], 3, 4); err != nil || string(data) != "3456" {
		t.Fatalf("read: %q %v", data, err)
	}
}

// One filesystem-backed gateway reports, for the same unchanged object, the
// listing ETag "c8cd…" in ListObjectsV2 and its hex text "6338…" in HEAD and
// GET. The page probe recognizes that encoding, so the page keeps listed
// revisions without per-file HEADs, and Stat, reads and replacement checks
// use the validator HEAD and GET actually report.
func TestCheckedListingAcceptsHexEncodedHeadETags(t *testing.T) {
	f := newFakeS3()
	f.headETagHex = true
	f.fsMarkers = true
	keys := []string{"books/t/movie.mkv", "books/t/movie.nfo", "books/t/movie.srt", "books/t/poster.jpg", "books/t/sample.mkv"}
	for i, key := range keys {
		f.put(key, fmt.Sprintf("%d-0123456789", i))
	}
	p := f.provider(t, "checked-etag")
	root, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
	if err != nil || len(root.GetEntries()) != 1 || f.heads.Load() != 0 {
		t.Fatalf("root: %v %v HEADs=%d", root, err, f.heads.Load())
	}
	req := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: root.Entries[0].Id, MaxEntries: 512}
	page, err := p.List(context.Background(), req)
	if err != nil || !page.GetComplete() || len(page.GetEntries()) != len(keys) {
		t.Fatalf("title page: %v %v", page, err)
	}
	if f.heads.Load() != 1 || f.gets.Load() != 0 {
		t.Fatalf("title page HEADs=%d GETs=%d, want only the page probe", f.heads.Load(), f.gets.Load())
	}
	for _, entry := range page.Entries {
		obj := f.objects[entry.LogicalPath]
		rev := decodeRevision(t, entry.Revision)
		head := `"` + hex.EncodeToString([]byte(strings.Trim(obj.etag, `"`))) + `"`
		if !rev.Listed || rev.ETag != head || rev.Size != int64(len(obj.data)) || rev.Modified != obj.modified.UnixNano() || entry.ModifiedUnixNano != obj.modified.UnixNano() {
			t.Fatalf("listed revision %+v for %s", rev, entry.LogicalPath)
		}
		stat, err := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision})
		if err != nil || !proto.Equal(stat.Entry, entry) {
			t.Fatalf("Stat(expected) differs from listing:\n stat=%v\n list=%v\n err=%v", stat.GetEntry(), entry, err)
		}
	}
	again, err := p.List(context.Background(), req)
	if err != nil || !proto.Equal(again, page) {
		t.Fatalf("rescan changed listed revisions: %v %v", again, err)
	}
	video := page.Entries[0]
	if data, chunks, err := readAll(t, p, video, 2, 4); err != nil || string(data) != "0123" || chunks != 1 {
		t.Fatalf("hex gateway range: data=%q chunks=%d err=%v", data, chunks, err)
	}
	f.put(keys[0], "replaced-0123456789")
	if _, err = p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: video.Id, ExpectedRevision: video.Revision}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("replaced object Stat: %v", err)
	}
	if data, chunks, err := readAll(t, p, video, 0, 4); status.Code(err) != codes.FailedPrecondition || chunks != 0 {
		t.Fatalf("replaced object read: data=%q chunks=%d err=%v", data, chunks, err)
	}
}

// A checked ETag source on a versioned backend keeps the strict revisions it
// had before: a page whose first file reports a genuine VersionId is hydrated.
func TestCheckedListingHydratesGenuineVersionsForOldVersionReads(t *testing.T) {
	f := newFakeS3()
	f.versioned = true
	for _, key := range []string{"books/a.mkv", "books/b.mkv", "books/c.mkv"} {
		f.put(key, "old!")
	}
	p := f.provider(t, "checked-etag")
	page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
	if err != nil || !page.Complete || len(page.Entries) != 3 {
		t.Fatalf("listing: %v %v", page, err)
	}
	for _, entry := range page.Entries {
		if rev := decodeRevision(t, entry.Revision); rev.Listed || !strings.HasPrefix(rev.Version, "immutable-v") {
			t.Fatalf("versioned object lost its genuine version: %+v", rev)
		}
	}
	if f.heads.Load() != 3 {
		t.Fatalf("hydration HEADs=%d, want one per file with the probe reused", f.heads.Load())
	}
	f.put("books/b.mkv", "new!")
	if data, _, err := readAll(t, p, page.Entries[1], 0, 4); err != nil || string(data) != "old!" {
		t.Fatalf("old genuine version read: %q %v", data, err)
	}
}

// In a mixed bucket a listed revision of an object that later reports a
// genuine VersionId reads that immutable version instead of the current key.
func TestListedRevisionPinsGenuineCurrentVersion(t *testing.T) {
	f := newFakeS3()
	f.put("books/a.mkv", "unversioned")
	f.versioned = true
	b := f.put("books/b.mkv", "versioned!")
	p := f.provider(t, "checked-etag")
	page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
	if err != nil || len(page.GetEntries()) != 2 {
		t.Fatalf("listing: %v %v", page, err)
	}
	entry := page.Entries[1]
	if rev := decodeRevision(t, entry.Revision); !rev.Listed || rev.Version != "" {
		t.Fatalf("mixed page revision: %+v", rev)
	}
	if stat, err := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision}); err != nil || !proto.Equal(stat.Entry, entry) {
		t.Fatalf("Stat of versioned listed object: %v %v", stat, err)
	}
	heads := f.heads.Load()
	data, _, err := readAll(t, p, entry, 0, 4)
	if err != nil || string(data) != "vers" {
		t.Fatalf("read: %q %v", data, err)
	}
	// Pre-read HEAD resolves the version; an immutable GET needs no final HEAD.
	if f.heads.Load()-heads != 1 {
		t.Fatalf("pinned read HEADs=%d", f.heads.Load()-heads)
	}
	if b.version == "" {
		t.Fatal("fixture did not version b")
	}
}

// Strict revisions need VersionIds that ListObjectsV2 cannot supply. HEADs
// run with bounded concurrency and a per-RPC budget well inside the host's
// 30-second List deadline.
func TestVersionedListingHydratesWithinBoundedConcurrentHEADs(t *testing.T) {
	f := newFakeS3()
	f.versioned = true
	f.headDelay = 20 * time.Millisecond
	for i := 0; i < 100; i++ {
		f.put(fmt.Sprintf("books/m%03d.mkv", i), "data")
	}
	p := f.provider(t, "versioned")
	req := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: 512}
	first, err := p.List(context.Background(), req)
	if err != nil || first.Complete || len(first.Entries) != maxRevisionHeadsPerList {
		t.Fatalf("first strict page: entries=%d err=%v", len(first.GetEntries()), err)
	}
	if f.heads.Load() != maxRevisionHeadsPerList || f.maxInflight.Load() < 2 || f.maxInflight.Load() > revisionHeadConcurrency {
		t.Fatalf("HEADs=%d max in flight=%d", f.heads.Load(), f.maxInflight.Load())
	}
	req.Cursor = first.NextCursor
	second, err := p.List(context.Background(), req)
	if err != nil || !second.Complete || len(second.Entries) != 100-maxRevisionHeadsPerList || f.heads.Load() != 100 {
		t.Fatalf("second strict page: entries=%d HEADs=%d err=%v", len(second.GetEntries()), f.heads.Load(), err)
	}
	for i, entry := range append(first.Entries, second.Entries...) {
		if entry.LogicalPath != fmt.Sprintf("books/m%03d.mkv", i) || !strings.HasPrefix(decodeRevision(t, entry.Revision).Version, "immutable-v") {
			t.Fatalf("strict entry %d: %v", i, entry)
		}
	}
}

// Cursors replay identically, survive restarts and never mix listing layouts.
func TestDelimitedCursorsReplayAndSurviveRestart(t *testing.T) {
	f := newFakeS3()
	var want []string
	for i := 0; i < 700; i++ {
		f.put(fmt.Sprintf("books/f%04d.mkv", i*2), "file")
		want = append(want, fmt.Sprintf("books/f%04d.mkv", i*2))
	}
	for i := 0; i < 400; i++ {
		f.put(fmt.Sprintf("books/f%04d/x.mkv", i*2+1), "nested")
		f.put(fmt.Sprintf("books/f%04d/y.srt", i*2+1), "nested")
		want = append(want, fmt.Sprintf("books/f%04d/", i*2+1))
	}
	sort.Strings(want)
	p, path := f.providerAt(t, "checked-etag")
	cfg := p.cfg
	req := &pb.ListRequest{SourceId: cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: 300}
	var got []string
	for n := 0; n < 10; n++ {
		page, err := p.List(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		retry, err := p.List(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(&pb.ListResponse{Entries: page.Entries, Complete: page.Complete}, &pb.ListResponse{Entries: retry.Entries, Complete: retry.Complete}) {
			t.Fatal("retrying a cursor changed its page")
		}
		got = append(got, logicalPaths(page.Entries)...)
		if page.Complete {
			break
		}
		req.Cursor = page.NextCursor
		if n == 1 {
			p.Close()
			if p, err = Open(cfg, path); err != nil {
				t.Fatal(err)
			}
			defer p.Close()
		}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("enumeration lost or duplicated entries: got %d want %d", len(got), len(want))
	}
	// A recursive-layout cursor retained by an earlier release must restart.
	lists := f.lists.Load()
	legacy, _ := json.Marshal(cursor{Layout: "delimited-v1", Directory: p.rootID(), PageSize: 300, Token: "raw-token", Expires: time.Now().Add(time.Hour).Unix()})
	token := strings.Repeat("ab", 24)
	if err := p.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("cursors")).Put([]byte(token), legacy) }); err != nil {
		t.Fatal(err)
	}
	req.Cursor = token
	if _, err := p.List(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("legacy layout cursor accepted: %v", err)
	}
	if f.lists.Load() != lists {
		t.Fatal("legacy cursor reached the backend")
	}
}

func TestDelimitedListingRejectsMalformedPages(t *testing.T) {
	file := func(key string) string {
		return fmt.Sprintf(`<Contents><Key>%s</Key><ETag>"e"</ETag><Size>1</Size></Contents>`, key)
	}
	dir := func(prefix string) string {
		return fmt.Sprintf(`<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>`, prefix)
	}
	page := func(parts ...string) string {
		return `<ListBucketResult><IsTruncated>false</IsTruncated>` + strings.Join(parts, "") + `</ListBucketResult>`
	}
	many := make([]string, 513)
	for i := range many {
		many[i] = file(fmt.Sprintf("books/%04d", i))
	}
	for _, tc := range []struct{ name, body string }{
		{"ignored-delimiter", page(file("books/dir/a.mkv"))},
		{"nested-marker", page(file("books/dir/sub/"))},
		{"empty-segment-marker", page(file("books//"))},
		{"outside-prefix", page(file("other/a.mkv"))},
		{"traversal", page(file("books/../a.mkv"))},
		{"prefix-outside", page(dir("other/"))},
		{"prefix-without-slash", page(dir("books/dir"))},
		{"nested-prefix", page(dir("books/a/b/"))},
		{"empty-segment", page(dir("books//"))},
		{"unordered-files", page(file("books/b"), file("books/a"))},
		{"duplicate-file", page(file("books/a"), file("books/a"))},
		{"unordered-prefixes", page(dir("books/b/"), dir("books/a/"))},
		{"duplicate-prefix", page(dir("books/a/"), dir("books/a/"))},
		{"missing-etag", page(`<Contents><Key>books/a</Key><Size>1</Size></Contents>`)},
		{"missing-size", page(`<Contents><Key>books/a</Key><ETag>"e"</ETag></Contents>`)},
		{"oversized", page(many...)},
		{"missing-flag", `<ListBucketResult></ListBucketResult>`},
		{"missing-token", `<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") != "2" {
					t.Error("malformed page reached object requests")
					w.WriteHeader(http.StatusForbidden)
					return
				}
				io.WriteString(w, tc.body)
			})
			p.cfg.RevisionMode = "checked-etag"
			if page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), DirectoriesOnly: false}); page != nil || status.Code(err) != codes.Unavailable {
				t.Fatalf("malformed delimited page accepted: %v %v", page, err)
			}
		})
	}
	for _, tc := range []struct{ name, second string }{
		{"backwards-across-pages", page(file("books/a"))},
		{"repeated-token", `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>one</NextContinuationToken></ListBucketResult>`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("continuation-token") == "" {
					io.WriteString(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>one</NextContinuationToken>`+dir("books/m/")+`</ListBucketResult>`)
					return
				}
				io.WriteString(w, tc.second)
			})
			if page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), DirectoriesOnly: true}); page != nil || status.Code(err) != codes.Unavailable {
				t.Fatalf("inconsistent continuation accepted: %v %v", page, err)
			}
		})
	}
}

// Filesystem-backed gateways list every subdirectory twice: as a CommonPrefix
// and as a zero-size marker object with the same key. max-keys counts each
// directory once, so a page can hold up to 512 Contents plus 512 prefixes.
func TestDelimitedListingAcceptsFilesystemGatewayDirectoryMarkers(t *testing.T) {
	f := newFakeS3()
	f.fsMarkers = true
	var want []string
	for i := 0; i < 700; i++ {
		f.put(fmt.Sprintf("books/t%04d/movie.mkv", i), "movie")
		f.put(fmt.Sprintf("books/t%04d/movie.srt", i), "subs")
		want = append(want, fmt.Sprintf("books/t%04d/", i))
	}
	for i := 0; i < 5; i++ {
		f.put(fmt.Sprintf("books/f%d.nfo", i), "nfo")
		want = append(want, fmt.Sprintf("books/f%d.nfo", i))
	}
	sort.Strings(want)
	p := f.provider(t, "checked-etag")
	req := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: 512}
	var got []*pb.Entry
	for n := 0; n < 5; n++ {
		page, err := p.List(context.Background(), req)
		if err != nil {
			t.Fatalf("page %d: %v", n, err)
		}
		got = append(got, page.Entries...)
		if page.Complete {
			break
		}
		req.Cursor = page.NextCursor
	}
	if strings.Join(logicalPaths(got), ",") != strings.Join(want, ",") {
		t.Fatalf("marker listing lost or duplicated children: got %d want %d", len(got), len(want))
	}
	for _, entry := range got {
		if strings.HasSuffix(entry.LogicalPath, "/") != (entry.Kind == pb.EntryKind_ENTRY_KIND_DIRECTORY) {
			t.Fatalf("marker listed with the wrong kind: %v", entry)
		}
	}
	title, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: got[len(got)-1].Id})
	if err != nil || !title.Complete || strings.Join(logicalPaths(title.Entries), ",") != "books/t0699/movie.mkv,books/t0699/movie.srt" {
		t.Fatalf("title directory with own marker: %v %v", title, err)
	}
}

// Some gateways repeat the last CommonPrefix at the start of the next page.
func TestDelimitedListingDeduplicatesRepeatedBoundaryPrefix(t *testing.T) {
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("ETag", `"e"`)
			w.Header().Set("Content-Length", "1")
			return
		}
		if r.URL.Query().Get("continuation-token") == "" {
			io.WriteString(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>one</NextContinuationToken><Contents><Key>books/a.mkv</Key><ETag>"e"</ETag><Size>1</Size></Contents><CommonPrefixes><Prefix>books/d/</Prefix></CommonPrefixes></ListBucketResult>`)
			return
		}
		io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>books/d/</Prefix></CommonPrefixes><Contents><Key>books/e.mkv</Key><ETag>"e"</ETag><Size>1</Size></Contents></ListBucketResult>`)
	})
	p.cfg.RevisionMode = "checked-etag"
	page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
	if err != nil || !page.Complete || strings.Join(logicalPaths(page.Entries), ",") != "books/a.mkv,books/d/,books/e.mkv" {
		t.Fatalf("boundary prefix: %v %v", page, err)
	}
}

// Long keys bound a page by the host's 1 MiB reply limit, not its count.
func TestListPagesStayWithinHostResponseBudget(t *testing.T) {
	f := newFakeS3()
	for i := 0; i < 512; i++ {
		f.put(fmt.Sprintf("books/%04d%s.mkv", i, strings.Repeat("x", 1000)), "data")
	}
	p := f.provider(t, "checked-etag")
	req := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: 512}
	total, pages := 0, 0
	for {
		page, err := p.List(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if proto.Size(page) > 1<<20 || (pages == 1 && len(page.Entries) < 128) {
			t.Fatalf("page %d: entries=%d size=%d", pages, len(page.Entries), proto.Size(page))
		}
		total += len(page.Entries)
		if page.Complete {
			break
		}
		req.Cursor = page.NextCursor
	}
	if total != 512 || pages < 2 {
		t.Fatalf("budgeted pages lost entries: total=%d pages=%d", total, pages)
	}
}

// A listed object deleted before its HEAD is a source race, never absence.
func TestListObjectRequestsNeverReportAbsence(t *testing.T) {
	for _, mode := range []string{"checked-etag", "versioned"} {
		t.Run(mode, func(t *testing.T) {
			f := newFakeS3()
			f.versioned = mode == "versioned"
			f.put("books/gone.mkv", "data")
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead && strings.Contains(r.URL.Path, "gone.mkv") {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				f.ServeHTTP(w, r)
			})
			p.cfg.RevisionMode = mode
			if page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()}); page != nil || status.Code(err) != codes.Unavailable {
				t.Fatalf("listing race reported %v %v", page, err)
			}
		})
	}
}

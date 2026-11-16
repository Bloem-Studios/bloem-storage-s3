package s3provider

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func protocolProvider(t *testing.T, handler http.HandlerFunc) (*Provider, string) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	c := fixtureConfig()
	c.Endpoint = server.URL
	path := filepath.Join(t.TempDir(), "private", "db")
	p, e := Open(c, path)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { p.Close() })
	return p, path
}
func TestOverflowPageRestartAndForeignCursor(t *testing.T) {
	p, path := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, "<ListBucketResult><IsTruncated>false</IsTruncated>")
		for i := 0; i < 512; i++ {
			fmt.Fprintf(w, "<Contents><Key>books/%03d%s</Key><Size>1</Size></Contents>", i, strings.Repeat("x", 1015))
		}
		io.WriteString(w, "</ListBucketResult>")
	})
	ctx := context.Background()
	r := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()}
	page, e := p.List(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	if page.Complete || len(page.Entries) >= 512 || proto.Size(page) > 1024*1024 {
		t.Fatal("overflow not bounded", len(page.Entries), proto.Size(page))
	}
	total := len(page.Entries)
	c := p.cfg
	p.Close()
	p2, e := Open(c, path)
	if e != nil {
		t.Fatal(e)
	}
	defer p2.Close()
	r.Cursor = page.NextCursor
	page, e = p2.List(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	total += len(page.Entries)
	if total != 512 || !page.Complete {
		t.Fatal("overflow lost keys", total)
	}
	dir, e := p2.remember("books/else/", pb.EntryKind_ENTRY_KIND_DIRECTORY)
	if e != nil {
		t.Fatal(e)
	}
	r.DirectoryId = dir
	if _, e = p2.List(ctx, r); status.Code(e) != codes.InvalidArgument {
		t.Fatal("foreign cursor accepted", e)
	}
}
func TestMalformedAndLoopingBackend(t *testing.T) {
	for _, body := range []string{"<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>", "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>a</NextContinuationToken></ListBucketResult>", "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>outside/book</Key><Size>1</Size></Contents></ListBucketResult>", "<ListBucketResult></ListBucketResult>"} {
		t.Run(body, func(t *testing.T) {
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) })
			_, e := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
			if status.Code(e) != codes.Unavailable {
				t.Fatal("malformed backend accepted", e)
			}
		})
	}
}
func TestReadRejectsLateBodyErrorAndBadRange(t *testing.T) {
	for _, mode := range []string{"short", "extra", "range", "etag", "denied", "outage"} {
		t.Run(mode, func(t *testing.T) {
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if mode == "denied" {
					w.WriteHeader(403)
					io.WriteString(w, "<Error><Code>AccessDenied</Code></Error>")
					return
				}
				if mode == "outage" {
					w.WriteHeader(503)
					return
				}
				w.Header().Set("ETag", "\"fixture\"")
				w.Header().Set("Content-Length", "4")
				w.Header().Set("Content-Range", "bytes 0-3/4")
				if r.Method == "HEAD" {
					return
				}
				if mode == "range" {
					w.Header().Set("Content-Range", "bytes 0-3/5")
				}
				if mode == "etag" {
					w.Header().Set("ETag", "\"other\"")
				}
				if mode == "extra" {
					w.Header().Del("Content-Length")
					io.WriteString(w, "12345")
					return
				}
				if mode == "short" {
					io.WriteString(w, "12")
					return
				}
				io.WriteString(w, "1234")
			})
			id, e := p.remember("books/f", pb.EntryKind_ENTRY_KIND_FILE)
			if e != nil {
				t.Fatal(e)
			}
			st, e := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
			if mode == "denied" || mode == "outage" {
				want := codes.PermissionDenied
				if mode == "outage" {
					want = codes.Unavailable
				}
				if status.Code(e) != want {
					t.Fatal(e)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			eof := false
			e = p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision, Length: 4}, func(c *pb.ReadChunk) error { eof = c.Eof; return nil })
			if e == nil || eof {
				t.Fatal("bad response completed", e)
			}
		})
	}
}
func TestMixedFileAndPrefixIdentity(t *testing.T) {
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>books/foo</Key><Size>1</Size></Contents><CommonPrefixes><Prefix>books/foo/</Prefix></CommonPrefixes></ListBucketResult>")
	})
	page, e := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
	if e != nil {
		t.Fatal(e)
	}
	if len(page.Entries) != 2 || page.Entries[0].Id == page.Entries[1].Id {
		t.Fatal("file/directory identity collision")
	}
}
func TestListRejectsOversizedWireResponse(t *testing.T) {
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "<ListBucketResult><!--")
		io.WriteString(w, strings.Repeat("x", 9*1024*1024))
		io.WriteString(w, "--><IsTruncated>false</IsTruncated></ListBucketResult>")
	})
	_, e := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
	if status.Code(e) != codes.Unavailable {
		t.Fatal("oversized response accepted", e)
	}
}
func TestLateStreamErrorAfterDeliveredChunk(t *testing.T) {
	const size = 128*1024 + 4
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "\"fixture\"")
		w.Header().Set("Content-Length", fmt.Sprint(size))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", size-1, size))
		if r.Method != "HEAD" {
			w.WriteHeader(http.StatusPartialContent)
			io.WriteString(w, strings.Repeat("x", size-2))
		}
	})
	id, _ := p.remember("books/late", pb.EntryKind_ENTRY_KIND_FILE)
	st, e := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
	if e != nil {
		t.Fatal(e)
	}
	chunks := 0
	eof := false
	e = p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision, Length: size}, func(c *pb.ReadChunk) error { chunks++; eof = eof || c.Eof; return nil })
	if status.Code(e) != codes.Unavailable || chunks != 1 || eof {
		t.Fatal("late error was not propagated", chunks, eof, e)
	}
}
func TestSparseLongTokenAndEscapedDisplayName(t *testing.T) {
	token := strings.Repeat("t", 65536)
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("continuation-token") == "" {
			fmt.Fprintf(w, "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>%s</NextContinuationToken></ListBucketResult>", token)
			return
		}
		io.WriteString(w, "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>books/a&amp;&lt;b&gt;.epub</Key><Size>1</Size></Contents></ListBucketResult>")
	})
	page, e := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
	if e != nil {
		t.Fatal(e)
	}
	if len(page.Entries) != 1 || page.Entries[0].Name != "a&<b>.epub" || !page.Complete {
		t.Fatal("sparse page or escaped display name lost", page)
	}
}
func TestVersionPinPassedThroughAndValidated(t *testing.T) {
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "\"v1-etag\"")
		w.Header().Set("x-amz-version-id", "immutable-v1")
		w.Header().Set("Content-Length", "4")
		if r.Method == "HEAD" {
			return
		}
		if r.URL.Query().Get("versionId") != "immutable-v1" || r.Header.Get("If-Match") != "\"v1-etag\"" {
			t.Error("version/conditional request missing")
		}
		w.Header().Set("Content-Range", "bytes 0-3/4")
		w.WriteHeader(http.StatusPartialContent)
		io.WriteString(w, "old!")
	})
	id, _ := p.remember("books/versioned", pb.EntryKind_ENTRY_KIND_FILE)
	st, e := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
	if e != nil {
		t.Fatal(e)
	}
	var data string
	e = p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision, Length: 4}, func(c *pb.ReadChunk) error { data += string(c.Data); return nil })
	if e != nil || data != "old!" {
		t.Fatal(data, e)
	}
}
func TestSparseDirectoryStatIsNotAuthoritativeAbsence(t *testing.T) {
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken></ListBucketResult>")
	})
	id, _ := p.remember("books/dir/", pb.EntryKind_ENTRY_KIND_DIRECTORY)
	_, e := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
	if status.Code(e) != codes.Unavailable {
		t.Fatal("sparse directory lookup treated as absence", e)
	}
}

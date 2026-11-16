package s3provider

import (
	"context"
	"fmt"
	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

func TestCredentialRotationPreservesIdentityInvalidatesCursors(t *testing.T) {
	p, path := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>books/a</Key><Size>1</Size></Contents><Contents><Key>books/b</Key><Size>1</Size></Contents></ListBucketResult>")
	})
	ctx := context.Background()
	page, e := p.List(ctx, &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: 1})
	if e != nil || page.Complete {
		t.Fatal(page, e)
	}
	root, id, c := p.rootID(), page.Entries[0].Id, p.cfg
	p.Close()
	c.AccessKey = "rotated-key"
	c.SecretKey = "rotated-secret"
	c.SessionToken = "rotated-token"
	c.Region = "other-region"
	p2, e := Open(c, path)
	if e != nil {
		t.Fatal("rotation rejected existing state", e)
	}
	defer p2.Close()
	if p2.rootID() != root || p2.id("books/a", pb.EntryKind_ENTRY_KIND_FILE) != id {
		t.Fatal("rotation changed IDs")
	}
	if v, e := p2.lookup(id); e != nil || v.Key != "books/a" {
		t.Fatal("rotation lost identity", v, e)
	}
	if _, e = p2.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: root, Cursor: page.NextCursor}); status.Code(e) != codes.InvalidArgument {
		t.Fatal("rotation accepted old cursor", e)
	}
	p2.Close()
	// Rotating back must never revive an invalidated cursor.
	p3, e := Open(p.cfg, path)
	if e != nil {
		t.Fatal(e)
	}
	defer p3.Close()
	if _, e = p3.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: root, Cursor: page.NextCursor}); status.Code(e) != codes.InvalidArgument {
		t.Fatal("old cursor revived", e)
	}
	for _, change := range []func(*Config){func(c *Config) { c.SourceID = "foreign" }, func(c *Config) { c.Bucket = "foreign" }, func(c *Config) { c.Prefix = "other/" }, func(c *Config) { c.Endpoint = "http://127.0.0.1:19341" }} {
		foreign := c
		change(&foreign)
		other, e := Open(foreign, filepath.Join(t.TempDir(), "private", "db"))
		if e != nil {
			t.Fatal(e)
		}
		if other.rootID() == root {
			t.Error("foreign namespace collision")
		}
		other.Close()
	}
}

func TestDelimitedProjectionAcrossBackendPagesAndRestart(t *testing.T) {
	calls := 0
	p, path := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("delimiter") != "/" {
			t.Error("must request immediate children")
		}
		calls++
		switch r.URL.Query().Get("continuation-token") {
		case "":
			io.WriteString(w, "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>one</NextContinuationToken><Contents><Key>books/foo</Key><Size>1</Size></Contents><CommonPrefixes><Prefix>books/foo/</Prefix></CommonPrefixes></ListBucketResult>")
		case "one":
			io.WriteString(w, "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>two</NextContinuationToken></ListBucketResult>")
		case "two":
			io.WriteString(w, "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>books/foo0</Key><Size>1</Size></Contents><CommonPrefixes><Prefix>books/marker/</Prefix></CommonPrefixes></ListBucketResult>")
		default:
			t.Error("unexpected token")
		}
	})
	ctx := context.Background()
	root, c := p.rootID(), p.cfg
	page, e := p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: root, MaxEntries: 1})
	if e != nil {
		t.Fatal(e)
	}
	entries := append([]*pb.Entry{}, page.Entries...)
	p.Close()
	p2, e := Open(c, path)
	if e != nil {
		t.Fatal(e)
	}
	defer p2.Close()
	cursor := page.NextCursor
	for n := 0; !page.Complete && n < 8; n++ {
		page, e = p2.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: root, MaxEntries: 1, Cursor: cursor})
		if e != nil {
			t.Fatal(e)
		}
		// Retried page is identical and does not mutate its original cursor.
		retry, e := p2.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: root, MaxEntries: 1, Cursor: cursor})
		if e != nil || fmt.Sprint(retry.Entries) != fmt.Sprint(page.Entries) {
			t.Fatal("retry changed page", e)
		}
		entries = append(entries, page.Entries...)
		cursor = page.NextCursor
	}
	if !page.Complete || len(entries) != 4 {
		t.Fatalf("lost/duplicate projection: %v calls=%d", entries, calls)
	}
	for i, want := range []string{"foo", "foo/", "foo0", "marker/"} {
		if entries[i].LogicalPath != "books/"+want {
			t.Fatalf("entry %d=%s want %s", i, entries[i].LogicalPath, want)
		}
	}
	if entries[0].Id == entries[1].Id {
		t.Fatal("file and prefix collision")
	}
}

func TestRejectUnorderedDelimitedListing(t *testing.T) {
	for _, keys := range [][]string{{"books/b", "books/a"}, {"books/a", "books/a"}, {"books/dir/" + strings.Repeat("x", 1024)}, {"outside/a"}} {
		t.Run(fmt.Sprint(keys), func(t *testing.T) {
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, "<ListBucketResult><IsTruncated>false</IsTruncated>")
				for _, key := range keys {
					fmt.Fprintf(w, "<Contents><Key>%s</Key><Size>1</Size></Contents>", key)
				}
				io.WriteString(w, "</ListBucketResult>")
			})
			if _, e := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()}); status.Code(e) != codes.Unavailable {
				t.Fatal("malformed delimited listing accepted", e)
			}
		})
	}
}

func TestForeignNamespaceCannotReuseDurableState(t *testing.T) {
	c := fixtureConfig()
	path := filepath.Join(t.TempDir(), "private", "db")
	p, e := Open(c, path)
	if e != nil {
		t.Fatal(e)
	}
	root := p.rootID()
	p.Close()
	for _, change := range []func(*Config){
		func(c *Config) { c.SourceID = "foreign" },
		func(c *Config) { c.Bucket = "foreign" },
		func(c *Config) { c.Prefix = "foreign/" },
		func(c *Config) { c.Endpoint = "http://127.0.0.1:19342" },
	} {
		altered := c
		change(&altered)
		if other, e := Open(altered, path); e == nil {
			other.Close()
			t.Fatal("foreign configuration reused DB")
		}
	}
	p, e = Open(c, path)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	if p.rootID() != root {
		t.Fatal("failed foreign opens damaged state")
	}
}

func TestForgedRequestsAndMissingConditionsFailClosed(t *testing.T) {
	for _, mode := range []string{"missing-etag", "missing-range", "missing-revision"} {
		t.Run(mode, func(t *testing.T) {
			gets := 0
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", "\"fixture\"")
				w.Header().Set("Content-Length", "4")
				if r.Method == "HEAD" {
					return
				}
				gets++
				if r.Header.Get("If-Match") != "\"fixture\"" || r.Header.Get("Range") != "bytes=0-3" {
					t.Error("read omitted mandatory conditions")
				}
				w.Header().Set("Content-Range", "bytes 0-3/4")
				if mode == "missing-etag" {
					w.Header().Del("ETag")
				}
				if mode == "missing-range" {
					w.Header().Del("Content-Range")
				}
				io.WriteString(w, "data")
			})
			id, _ := p.remember("books/f", pb.EntryKind_ENTRY_KIND_FILE)
			st, e := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
			if e != nil {
				t.Fatal(e)
			}
			rev := st.Entry.Revision
			if mode == "missing-revision" {
				rev = ""
			}
			sends := 0
			e = p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: rev, Length: 4}, func(*pb.ReadChunk) error { sends++; return nil })
			if e == nil || sends != 0 {
				t.Fatal("missing condition completed read", e, sends)
			}
			if mode == "missing-revision" && gets != 0 {
				t.Fatal("missing revision reached backend")
			}
			if _, e = p.List(context.Background(), &pb.ListRequest{SourceId: "forged", DirectoryId: p.rootID()}); status.Code(e) != codes.InvalidArgument {
				t.Fatal("forged source accepted", e)
			}
			if _, e = p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: strings.Repeat("x", 1025)}); status.Code(e) != codes.InvalidArgument {
				t.Fatal("oversized identity accepted", e)
			}
		})
	}
}

func TestProjectionRejectsUnexpectedPrefixes(t *testing.T) {
	for _, prefix := range []string{"books/foo/bar/", "other/foo/", "books/foo", "books//"} {
		t.Run(prefix, func(t *testing.T) {
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, "<ListBucketResult><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes></ListBucketResult>", prefix)
			})
			if _, e := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()}); status.Code(e) != codes.Unavailable {
				t.Fatal("non-child prefix accepted", e)
			}
		})
	}
}

package s3provider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeBuckets serves several fakeS3 buckets behind one endpoint, with
// ListBuckets at the root. pageSize > 0 paginates ListBuckets.
type fakeBuckets struct {
	buckets     map[string]*fakeS3
	names       []string
	pageSize    int
	listBuckets atomic.Int32
}

func newFakeBuckets(names ...string) *fakeBuckets {
	m := &fakeBuckets{buckets: map[string]*fakeS3{}, names: names}
	for _, name := range names {
		f := newFakeS3()
		f.bucket = name
		m.buckets[name] = f
	}
	return m
}

func (m *fakeBuckets) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		m.listBuckets.Add(1)
		names := m.names
		start := 0
		if token := r.URL.Query().Get("continuation-token"); token != "" {
			fmt.Sscanf(token, "page-%d", &start)
		}
		next := ""
		if m.pageSize > 0 && start+m.pageSize < len(names) {
			next = fmt.Sprintf("page-%d", start+m.pageSize)
			names = names[start : start+m.pageSize]
		} else {
			names = names[start:]
		}
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, `<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>fixture</ID></Owner><Buckets>`)
		for _, name := range names {
			fmt.Fprintf(w, "<Bucket><Name>%s</Name><CreationDate>2026-10-01T12:00:00.000Z</CreationDate></Bucket>", name)
		}
		io.WriteString(w, "</Buckets>")
		if next != "" {
			fmt.Fprintf(w, "<ContinuationToken>%s</ContinuationToken>", next)
		}
		io.WriteString(w, "</ListAllMyBucketsResult>")
		return
	}
	name, _, _ := strings.Cut(path, "/")
	if f := m.buckets[name]; f != nil {
		f.ServeHTTP(w, r)
		return
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	fakeError(w, "NoSuchBucket", http.StatusNotFound)
}

func allBucketsConfig(endpoint, mode string) Config {
	return Config{SourceID: "everything", Region: "us-east-1", Endpoint: endpoint, AllowLoopbackHTTP: true, AccessKey: "fixture-reader", SecretKey: "synthetic-reader-secret", RevisionMode: mode}
}

func (m *fakeBuckets) provider(t *testing.T, mode string) *Provider {
	t.Helper()
	server := httptest.NewServer(m)
	t.Cleanup(server.Close)
	p, err := Open(allBucketsConfig(server.URL, mode), filepath.Join(t.TempDir(), "private", "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func listAll(t *testing.T, p *Provider, dirID string, directoriesOnly bool) []*pb.Entry {
	t.Helper()
	var entries []*pb.Entry
	request := &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: dirID, DirectoriesOnly: directoriesOnly, MaxEntries: 2}
	for {
		page, err := p.List(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, page.Entries...)
		if page.Complete {
			return entries
		}
		request.Cursor = page.NextCursor
	}
}

func TestConfigBucketOptionalWithoutPrefix(t *testing.T) {
	c := allBucketsConfig("https://s3.example", "")
	if err := c.Validate(); err != nil {
		t.Fatalf("bucket-less source refused: %v", err)
	}
	c.Prefix = "movies/"
	if c.Validate() == nil {
		t.Fatal("a prefix without a bucket was accepted")
	}
	c.Prefix, c.Bucket = "", "Not_A_Bucket"
	if c.Validate() == nil {
		t.Fatal("invalid bucket accepted")
	}
}

// A bucket-bound source keeps the identities it had before buckets became
// optional: its namespace, root and file IDs are fixed values.
func TestBucketBoundIdentityUnchanged(t *testing.T) {
	c := Config{SourceID: "books", Bucket: "fixture-books", Prefix: "books/", Region: "us-east-1", Endpoint: "http://127.0.0.1:19340", AllowLoopbackHTTP: true, AccessKey: "fixture-reader", SecretKey: "synthetic-reader-secret"}
	p, err := Open(c, filepath.Join(t.TempDir(), "private", "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.namespace != "a6088119bd06ca8bd9c2a1d300d11b2fe836a37a05c9e833cae037aeacada1b7" ||
		p.rootID() != "2d452be47b7d8173387636bf4a7db887d6bef93959e95ef82aad0fcece4a4124" ||
		p.id("books/a/b.mkv", pb.EntryKind_ENTRY_KIND_FILE) != "d1624717621f799a494a83bed317960ba49ae22fcf2292ed844cbe75f52b7788" {
		t.Fatalf("bucket-bound identity changed: namespace %s root %s", p.namespace, p.rootID())
	}
}

func TestAllBucketsNamespaceIsDistinct(t *testing.T) {
	bound := Config{SourceID: "everything", Bucket: "alpha-media", Region: "us-east-1", Endpoint: "https://s3.example", AccessKey: "a", SecretKey: "b"}
	path := filepath.Join(t.TempDir(), "private", "db")
	p, err := Open(bound, path)
	if err != nil {
		t.Fatal(err)
	}
	boundID := p.id("movies/a.mkv", pb.EntryKind_ENTRY_KIND_FILE)
	p.Close()
	all := allBucketsConfig("https://s3.example", "")
	all.AllowLoopbackHTTP = false
	if _, err := Open(all, path); err == nil || !strings.Contains(err.Error(), "another source namespace") {
		t.Fatalf("bucket-bound state reused by an all-buckets source: %v", err)
	}
	q, err := Open(all, filepath.Join(t.TempDir(), "private", "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if q.namespace == p.namespace || q.id("alpha-media/movies/a.mkv", pb.EntryKind_ENTRY_KIND_FILE) == boundID {
		t.Fatal("all-buckets identities collide with a bucket-bound source")
	}
}

func TestAllBucketsListsBucketsAsFolders(t *testing.T) {
	for _, mode := range []string{"versioned", "checked-etag", ""} {
		t.Run(mode, func(t *testing.T) {
			m := newFakeBuckets("alpha-media", "Legacy_Bucket", "beta-media", "gamma-media")
			m.pageSize = 2
			m.buckets["alpha-media"].put("movies/Alien (1979)/alien.mkv", "alien")
			m.buckets["beta-media"].put("shows/x.mkv", "x")
			p := m.provider(t, mode)
			for _, directoriesOnly := range []bool{true, false} {
				root := listAll(t, p, p.rootID(), directoriesOnly)
				var got []string
				for _, entry := range root {
					if entry.Kind != pb.EntryKind_ENTRY_KIND_DIRECTORY || entry.Revision != directoryRevision || entry.Name+"/" != entry.LogicalPath {
						t.Fatalf("bucket entry: %v", entry)
					}
					got = append(got, entry.LogicalPath)
				}
				if strings.Join(got, ",") != "alpha-media/,beta-media/,gamma-media/" {
					t.Fatalf("root folders: %v", got)
				}
			}
			for _, f := range m.buckets {
				if n := f.lists.Load() + f.heads.Load() + f.gets.Load() + f.bucketHeads.Load(); n != 0 {
					t.Fatalf("root listing touched bucket %s %d times", f.bucket, n)
				}
			}
		})
	}
}

func TestAllBucketsBrowseStatAndRead(t *testing.T) {
	m := newFakeBuckets("alpha-media", "beta-media")
	m.buckets["alpha-media"].put("movies/Alien (1979)/alien.mkv", "alien bytes")
	m.buckets["beta-media"].put("movies/Heat (1995)/heat.mkv", "heat bytes")
	p := m.provider(t, "checked-etag")
	ctx := context.Background()
	root := listAll(t, p, p.rootID(), true)
	if len(root) != 2 {
		t.Fatalf("root: %v", root)
	}
	for i, bucket := range []string{"alpha-media", "beta-media"} {
		movies := listAll(t, p, root[i].Id, true)
		if len(movies) != 1 || movies[0].LogicalPath != bucket+"/movies/" || movies[0].Name != "movies" {
			t.Fatalf("%s folders: %v", bucket, movies)
		}
		titles := listAll(t, p, movies[0].Id, true)
		if len(titles) != 1 {
			t.Fatalf("%s titles: %v", bucket, titles)
		}
		files := listAll(t, p, titles[0].Id, false)
		if len(files) != 1 || files[0].Kind != pb.EntryKind_ENTRY_KIND_FILE || !strings.HasPrefix(files[0].LogicalPath, bucket+"/movies/") {
			t.Fatalf("%s files: %v", bucket, files)
		}
		stat, err := p.Stat(ctx, &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: files[0].Id, ExpectedRevision: files[0].Revision})
		if err != nil || stat.Entry.LogicalPath != files[0].LogicalPath {
			t.Fatalf("%s stat: %v %v", bucket, stat, err)
		}
		var got []byte
		err = p.read(ctx, &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: files[0].Id, ExpectedRevision: files[0].Revision, Length: files[0].Size}, func(c *pb.ReadChunk) error {
			got = append(got, c.Data...)
			return nil
		})
		want := map[string]string{"alpha-media": "alien bytes", "beta-media": "heat bytes"}[bucket]
		if err != nil || string(got) != want {
			t.Fatalf("%s read: %q %v", bucket, got, err)
		}
		dir, err := p.Stat(ctx, &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: root[i].Id})
		if err != nil || dir.Entry.LogicalPath != bucket+"/" {
			t.Fatalf("%s bucket stat: %v %v", bucket, dir, err)
		}
		if m.buckets[bucket].bucketHeads.Load() != 1 {
			t.Fatalf("%s bucket stat did not HEAD its bucket", bucket)
		}
	}
	listed := m.listBuckets.Load()
	if _, err := p.Stat(ctx, &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: p.rootID()}); err != nil {
		t.Fatalf("root stat: %v", err)
	}
	if m.listBuckets.Load() != listed+1 {
		t.Fatal("root stat did not prove access with ListBuckets")
	}
}

// Object absence is established against the object's own bucket.
func TestAllBucketsObjectAbsenceChecksItsBucket(t *testing.T) {
	m := newFakeBuckets("alpha-media", "beta-media")
	m.buckets["beta-media"].put("gone.mkv", "soon gone")
	p := m.provider(t, "checked-etag")
	ctx := context.Background()
	root := listAll(t, p, p.rootID(), true)
	files := listAll(t, p, root[1].Id, false)
	if len(files) != 1 {
		t.Fatalf("files: %v", files)
	}
	f := m.buckets["beta-media"]
	f.mu.Lock()
	delete(f.objects, "gone.mkv")
	f.mu.Unlock()
	_, err := p.Stat(ctx, &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: files[0].Id})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("absent object: %v", err)
	}
	if f.bucketHeads.Load() != 1 || m.buckets["alpha-media"].bucketHeads.Load() != 0 {
		t.Fatal("absence was not checked against the object's bucket")
	}
	delete(m.buckets, "beta-media")
	_, err = p.Stat(ctx, &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: files[0].Id})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("missing bucket reported as %v", err)
	}
}

func TestBoundedTransportLimitsListBuckets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "<ListAllMyBucketsResult><!--")
		io.WriteString(w, strings.Repeat("x", 9*1024*1024))
		io.WriteString(w, "--><Buckets><Bucket><Name>alpha-media</Name></Bucket></Buckets></ListAllMyBucketsResult>")
	}))
	t.Cleanup(server.Close)
	p, err := Open(allBucketsConfig(server.URL, "checked-etag"), filepath.Join(t.TempDir(), "private", "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	_, err = p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("oversized bucket listing accepted: %v", err)
	}
}

// Buckets ascend by name; a name that is a prefix of the next must not read as
// out of order once the delimiter is appended.
func TestAllBucketsOrdersPrefixNames(t *testing.T) {
	m := newFakeBuckets("media", "media-2", "media.old")
	m.pageSize = 1
	p := m.provider(t, "checked-etag")
	var got []string
	for _, entry := range listAll(t, p, p.rootID(), true) {
		got = append(got, entry.Name)
	}
	if strings.Join(got, ",") != "media,media-2,media.old" {
		t.Fatalf("bucket order: %v", got)
	}
}

// Hosts offer an all-buckets source only when the schema lets bucket be empty.
func TestManifestDeclaresOptionalBucket(t *testing.T) {
	var manifest struct {
		Version string `json:"version"`
		Schemas []struct {
			JSONSchema string `json:"json_schema"`
			Form       struct {
				Fields []struct {
					Key         string `json:"key"`
					Label       string `json:"label"`
					Description string `json:"description"`
				} `json:"fields"`
			} `json:"admin_form"`
		} `json:"global_config_schema"`
	}
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != "0.4.3" {
		t.Fatalf("version %s", manifest.Version)
	}
	var schema struct {
		Properties map[string]map[string]any `json:"properties"`
	}
	if err := json.Unmarshal([]byte(manifest.Schemas[0].JSONSchema), &schema); err != nil {
		t.Fatal(err)
	}
	if _, required := schema.Properties["bucket"]["minLength"]; required {
		t.Fatal("bucket still requires a value")
	}
	labels := map[string]string{}
	for _, field := range manifest.Schemas[0].Form.Fields {
		labels[field.Key] = field.Label
		if field.Key == "prefix" && !strings.Contains(field.Description, "Leave empty to use the whole bucket (recommended). You choose folders for each library later.") {
			t.Fatalf("prefix hint: %q", field.Description)
		}
	}
	for key, want := range map[string]string{"bucket": "Bucket (optional)", "prefix": "Folder (optional)", "session_token": "Session token (optional)"} {
		if labels[key] != want {
			t.Errorf("%s label %q, want %q", key, labels[key], want)
		}
	}
}

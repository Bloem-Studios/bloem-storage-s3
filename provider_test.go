package s3provider

import (
	"context"
	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixtureConfig() Config {
	c := Config{SourceID: "books", Bucket: "fixture-books", Prefix: "books/", Region: "us-east-1", Endpoint: "http://127.0.0.1:19340", AllowLoopbackHTTP: true, AccessKey: "fixture-reader", SecretKey: "synthetic-reader-secret"}
	if os.Getenv("S3_BACKEND") == "garage" {
		c.Endpoint = "http://127.0.0.1:19341"
		c.Region = "garage"
		c.AccessKey = "GK" + strings.Repeat("1", 32)
		c.SecretKey = strings.Repeat("1", 64)
	}
	return c
}
func TestDurableIdentityAndConfigBinding(t *testing.T) {
	c := fixtureConfig()
	path := filepath.Join(t.TempDir(), "private", "index.db")
	p, e := Open(c, path)
	if e != nil {
		t.Fatal(e)
	}
	key := "books/" + strings.Repeat("x", 1018)
	id, e := p.remember(key, pb.EntryKind_ENTRY_KIND_FILE)
	if e != nil {
		t.Fatal(e)
	}
	dir, e := p.remember(key, pb.EntryKind_ENTRY_KIND_DIRECTORY)
	if e != nil || dir == id || len(id) > 1024 {
		t.Fatal("identity collision/budget", e)
	}
	p.Close()
	p, e = Open(c, path)
	if e != nil {
		t.Fatal(e)
	}
	got, e := p.lookup(id)
	if e != nil || got.Key != key {
		t.Fatal("restart lost key", e)
	}
	p.Close()
	c.Prefix = "other/"
	if p, e = Open(c, path); e == nil {
		p.Close()
		t.Fatal("accepted different source config")
	}
	st, e := os.Stat(path)
	if e != nil || st.Mode().Perm() != 0600 {
		t.Fatal("DB permissions", e)
	}
}
func TestBackendConditionalAndProvider(t *testing.T) {
	if os.Getenv("S3_FIXTURE") != "1" {
		t.Skip("requires task-owned loopback SeaweedFS fixture")
	}
	ctx := context.Background()
	c := fixtureConfig()
	c.Prefix = "books/ordinary/"
	writer := s3.New(s3.Options{Region: c.Region, BaseEndpoint: aws.String(c.Endpoint), UsePathStyle: true, Credentials: fixtureWriterCredentials()})
	_, e := writer.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(c.Bucket)})
	if e != nil && !strings.Contains(e.Error(), "BucketAlready") {
		t.Fatal(e)
	}
	put := func(key, value string) {
		t.Helper()
		_, e := writer.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), Body: strings.NewReader(value)})
		if e != nil {
			t.Fatal(e)
		}
	}
	_, _ = writer.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String("books/ordinary/foo/book.pdf")})
	put("books/ordinary/foo", "EPUB-fixture-content")
	put("books/ordinary/docs/book.pdf", syntheticPDF())
	put("books/ordinary/z.epub", syntheticEPUB(t))
	p, e := Open(c, filepath.Join(t.TempDir(), "private", "index.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	// Raw backend contract is evidence independent of provider checks.
	h, e := p.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String("books/ordinary/foo")})
	if e != nil {
		t.Fatal(e)
	}
	r, e := p.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String("books/ordinary/foo"), Range: aws.String("bytes=2-5"), IfMatch: h.ETag})
	if e != nil {
		t.Fatal(e)
	}
	r.Body.Close()
	if aws.ToString(r.ContentRange) != "bytes 2-5/20" || aws.ToInt64(r.ContentLength) != 4 {
		t.Fatalf("RAW range mismatch: %v %v", r.ContentRange, r.ContentLength)
	}
	_, e = p.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String("books/ordinary/foo"), IfMatch: aws.String("\"invalid\"")})
	if e == nil {
		t.Fatal("RAW backend ignored If-Match")
	}
	t.Logf("RAW version=%q; exact range and If-Match rejection verified", aws.ToString(h.VersionId))
	page, e := p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: p.rootID(), MaxEntries: 1})
	if e != nil {
		t.Fatal(e)
	}
	for _, entry := range page.Entries {
		t.Logf("root entry %s kind=%v", entry.LogicalPath, entry.Kind)
	}
	count := len(page.Entries)
	seen := map[string]bool{}
	for !page.Complete {
		if seen[page.NextCursor] {
			t.Fatal("cursor loop")
		}
		seen[page.NextCursor] = true
		page, e = p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: p.rootID(), MaxEntries: 1, Cursor: page.NextCursor})
		if e != nil {
			t.Fatal(e)
		}
		for _, entry := range page.Entries {
			t.Logf("root entry %s kind=%v", entry.LogicalPath, entry.Kind)
		}
		count += len(page.Entries)
	}
	if count != 3 {
		t.Fatalf("root count=%d want 3", count)
	}
	id, e := p.remember("books/ordinary/foo", pb.EntryKind_ENTRY_KIND_FILE)
	if e != nil {
		t.Fatal(e)
	}
	st, e := p.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: id})
	if e != nil {
		t.Fatal(e)
	}
	var data []byte
	e = p.read(ctx, &pb.ReadRequest{SourceId: c.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision, Offset: 2, Length: 4}, func(c *pb.ReadChunk) error { data = append(data, c.Data...); return nil })
	if e != nil || string(data) != "UB-f" {
		t.Fatalf("read=%q err=%v", data, e)
	}
	put("books/ordinary/foo", "replacement-content")
	e = p.read(ctx, &pb.ReadRequest{SourceId: c.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision, Length: 4}, func(*pb.ReadChunk) error { return nil })
	if status.Code(e) != codes.FailedPrecondition {
		t.Fatalf("replacement status: %v", e)
	}
	_, e = writer.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String("books/ordinary/foo")})
	if e != nil {
		t.Fatal(e)
	}
	_, e = p.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: id})
	if status.Code(e) != codes.NotFound {
		t.Fatal("delete status", e)
	}
	_, e = p.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String("books/ordinary/denied"), Body: strings.NewReader("deny")})
	if e == nil {
		t.Fatal("reader could write")
	}
}

// This is an explicit backend acceptance gate, separate from provider unit tests.
// Delimited listings must keep colliding file and prefix keys; the provider
// depends on it since 0.3.1.
func TestBackendRawDelimiterCompatibility(t *testing.T) {
	if os.Getenv("S3_RAW_DELIMITER") != "1" {
		t.Skip("delimited mixed-key gate; known SeaweedFS 4.17 failure")
	}
	c := fixtureConfig()
	ctx := context.Background()
	writer := s3.New(s3.Options{Region: c.Region, BaseEndpoint: aws.String(c.Endpoint), UsePathStyle: true, Credentials: fixtureWriterCredentials()})
	for _, key := range []string{"books/collision/foo", "books/collision/foo/book.pdf"} {
		_, e := writer.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), Body: strings.NewReader("synthetic-fixture")})
		if e != nil {
			t.Fatal(e)
		}
	}
	out, e := writer.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(c.Bucket), Prefix: aws.String("books/collision/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(512)})
	if e != nil {
		t.Fatal(e)
	}
	file, dir := false, false
	for _, o := range out.Contents {
		file = file || aws.ToString(o.Key) == "books/collision/foo"
	}
	for _, o := range out.CommonPrefixes {
		dir = dir || aws.ToString(o.Prefix) == "books/collision/foo/"
	}
	if !file || !dir {
		t.Fatalf("RAW backend mixed-key gate failed: file=%v directory=%v", file, dir)
	}
}
func TestBackendOutage(t *testing.T) {
	if os.Getenv("S3_OUTAGE") != "1" {
		t.Skip("requires task fixture intentionally stopped")
	}
	c := fixtureConfig()
	p, e := Open(c, filepath.Join(t.TempDir(), "private", "db"))
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	id, e := p.remember("books/outage", pb.EntryKind_ENTRY_KIND_FILE)
	if e != nil {
		t.Fatal(e)
	}
	_, e = p.Stat(context.Background(), &pb.StatRequest{SourceId: c.SourceID, EntryId: id})
	if status.Code(e) != codes.Unavailable {
		t.Fatal("outage treated as absence", e)
	}
}

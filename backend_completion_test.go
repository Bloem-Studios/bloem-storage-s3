package s3provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	runtimepb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	sdk "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestBackendMixedFilePrefixConformance(t *testing.T) {
	if os.Getenv("S3_CONFORMANCE") != "1" {
		t.Skip("requires disposable pinned backend")
	}
	ctx := context.Background()
	c := seedConformanceFixture(t)
	writer := s3.New(s3.Options{Region: c.Region, BaseEndpoint: aws.String(c.Endpoint), UsePathStyle: true, Credentials: fixtureWriterCredentials()})

	// Independent raw pagination evidence; the provider's delimited listing
	// must project the same immediate children below.
	raw, e := writer.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(c.Bucket), Prefix: aws.String(c.Prefix), MaxKeys: aws.Int32(512)})
	if e != nil || !aws.ToBool(raw.IsTruncated) || len(raw.Contents) != 512 || len(raw.CommonPrefixes) != 0 {
		t.Fatal("raw first-page contract", raw, e)
	}
	t.Logf("raw listing: 512 keys truncated, no delimiter, next token present=%v", aws.ToString(raw.NextContinuationToken) != "")
	path := filepath.Join(t.TempDir(), "private", "db")
	p, e := Open(c, path)
	if e != nil {
		t.Fatal(e)
	}
	root := p.rootID()
	page, e := p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: root, MaxEntries: 1})
	if e != nil {
		p.Close()
		t.Fatal(e)
	}
	entries := append([]*pb.Entry{}, page.Entries...)
	p.Close()
	p, e = Open(c, path)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	for n := 0; !page.Complete && n < 10; n++ {
		page, e = p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: root, MaxEntries: 1, Cursor: page.NextCursor})
		if e != nil {
			t.Fatal(e)
		}
		entries = append(entries, page.Entries...)
	}
	if !page.Complete || len(entries) != 4 {
		t.Fatal("projection dropped/duplicated entries", entries)
	}
	for i, want := range []string{"foo", "foo/", "marker/", "z.epub"} {
		if entries[i].LogicalPath != "books/"+want {
			t.Fatalf("%d got %q want %q", i, entries[i].LogicalPath, want)
		}
	}
	if entries[0].Id == entries[1].Id {
		t.Fatal("file/directory identity collision")
	}
	nested, e := p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: entries[1].Id})
	if e != nil || len(nested.Entries) != 512 || nested.Complete {
		t.Fatal("nested raw pagination", e)
	}
	last, e := p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: entries[1].Id, Cursor: nested.NextCursor})
	if e != nil || len(last.Entries) != 8 || !last.Complete {
		t.Fatal("nested second page", e)
	}
	for _, entry := range entries[1:3] {
		if _, e := p.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: entry.Id}); e != nil {
			t.Fatal("directory stat", e)
		}
	}
	// A synthetically invalid identity must map to real provider denial, not absence.
	denied := c
	denied.AccessKey = "fixture-invalid"
	denied.SecretKey = "synthetic-invalid"
	d, e := Open(denied, filepath.Join(t.TempDir(), "private", "db"))
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	id, e := d.remember(c.Prefix+"foo", pb.EntryKind_ENTRY_KIND_FILE)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = d.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: id}); status.Code(e) != codes.PermissionDenied {
		t.Fatal("real denied Stat", e)
	}
	if _, e = d.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: d.rootID()}); status.Code(e) != codes.PermissionDenied {
		t.Fatal("real denied List", e)
	}
	good, e := p.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: entries[0].Id})
	if e != nil {
		t.Fatal(e)
	}
	if e = d.read(ctx, &pb.ReadRequest{SourceId: c.SourceID, EntryId: id, ExpectedRevision: good.Entry.Revision, Length: 1}, func(*pb.ReadChunk) error { return nil }); status.Code(e) != codes.PermissionDenied {
		t.Fatal("real denied Read", e)
	}
}

// Uses independent OS processes, the private go-plugin transport and real S3.
// Restart recovers an outstanding page; rotation preserves IDs while deleting it.
func TestBackendExecutableRestartPageAndRotation(t *testing.T) {
	if os.Getenv("S3_CONFORMANCE") != "1" {
		t.Skip("requires disposable pinned backend")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "provider")
	if out, e := exec.Command("go", "build", "-o", bin, "./cmd/bloem-storage-s3").CombinedOutput(); e != nil {
		t.Fatalf("build %v %s", e, out)
	}
	c := seedConformanceFixture(t)
	ctx := context.Background()
	var root, id, cursor, revision string
	for round := 0; round < 3; round++ {
		client := plugin.NewClient(&plugin.ClientConfig{HandshakeConfig: sdk.HandshakeConfig(), Plugins: sdk.DefaultPluginSet(sdk.CapabilityServers{}), Cmd: exec.Command(bin), AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC}, Logger: hclog.NewNullLogger()})
		func() {
			defer client.Kill()
			protocol, e := client.Client()
			if e != nil {
				t.Fatal(e)
			}
			dispensed, e := protocol.Dispense(sdk.PluginSetName)
			if e != nil {
				t.Fatal(e)
			}
			rpc := dispensed.(*sdk.Client)
			config := c
			if round == 2 {
				config.AccessKey, config.SecretKey = fixtureWriterKeys()
			}
			raw, _ := json.Marshal(config)
			fields, e := structpb.NewStruct(map[string]any{"json": string(raw), "state_path": filepath.Join(dir, "private", "db")})
			if e != nil {
				t.Fatal(e)
			}
			if _, e = rpc.Runtime().Configure(ctx, &runtimepb.ConfigureRequest{Config: []*runtimepb.ConfigEntry{{Key: "source_json", Value: fields}}}); e != nil {
				t.Fatal("configure restart/rotation", e)
			}
			storage := pb.NewStorageProviderClient(rpc.Conn())
			desc, e := storage.Describe(ctx, &pb.DescribeRequest{})
			if e != nil {
				t.Fatal(e)
			}
			if round == 0 {
				root = desc.Sources[0].RootEntryId
			} else if root != desc.Sources[0].RootEntryId {
				t.Fatal("process rotation changed root")
			}
			if round == 0 {
				page, e := storage.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: root, MaxEntries: 1})
				if e != nil || len(page.Entries) != 1 || page.Complete {
					t.Fatal(page, e)
				}
				id, cursor = page.Entries[0].Id, page.NextCursor
				st, e := storage.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: id})
				if e != nil {
					t.Fatal(e)
				}
				revision = st.Entry.Revision
			} else {
				st, e := storage.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: id, ExpectedRevision: revision})
				if e != nil || st.Entry.Id != id {
					t.Fatal("old file identity lost", e)
				}
				page, e := storage.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: root, MaxEntries: 1, Cursor: cursor})
				if round == 1 {
					if e != nil || len(page.Entries) != 1 || page.Entries[0].LogicalPath != "books/foo/" {
						t.Fatal("process lost pending page", page, e)
					}
				} else if status.Code(e) != codes.InvalidArgument {
					t.Fatal("rotated process accepted old cursor", e)
				}
				stream, e := storage.Read(ctx, &pb.ReadRequest{SourceId: c.SourceID, EntryId: id, ExpectedRevision: revision, Length: 4})
				if e != nil {
					t.Fatal(e)
				}
				chunk, e := stream.Recv()
				if e != nil || string(chunk.Data) != "synt" || !chunk.Eof {
					t.Fatal("private RPC read", chunk, e)
				}
				if _, e = stream.Recv(); e != io.EOF {
					t.Fatal("missing successful terminal EOF", e)
				}
			}
		}()
	}
}

func seedConformanceFixture(t *testing.T) Config {
	t.Helper()
	ctx := context.Background()
	c := fixtureConfig()
	c.Prefix = "books/flat-conformance/"
	writer := s3.New(s3.Options{Region: c.Region, BaseEndpoint: aws.String(c.Endpoint), UsePathStyle: true, Credentials: fixtureWriterCredentials()})
	keys := []string{c.Prefix + "foo", c.Prefix + "marker/", c.Prefix + "z.epub"}
	for n := 0; n < 520; n++ {
		keys = append(keys, fmt.Sprintf("%sfoo/%04d.pdf", c.Prefix, n))
	}
	jobs := make(chan string)
	errs := make(chan error, len(keys))
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				_, e := writer.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), Body: strings.NewReader("synthetic-fixture")})
				if e != nil {
					errs <- e
				}
			}
		}()
	}
	for _, key := range keys {
		jobs <- key
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	return c
}

func TestBackendRangesAndDeletedPinnedRead(t *testing.T) {
	if os.Getenv("S3_FIXTURE") != "1" {
		t.Skip("requires disposable pinned backend")
	}
	ctx := context.Background()
	c := fixtureConfig()
	writer := s3.New(s3.Options{Region: c.Region, BaseEndpoint: aws.String(c.Endpoint), UsePathStyle: true, Credentials: fixtureWriterCredentials()})
	key := c.Prefix + "range-conformance.bin"
	data := strings.Repeat("0123456789", 27000)
	if _, e := writer.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), Body: strings.NewReader(data)}); e != nil {
		t.Fatal(e)
	}
	p, e := Open(c, filepath.Join(t.TempDir(), "private", "db"))
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	id, e := p.remember(key, pb.EntryKind_ENTRY_KIND_FILE)
	if e != nil {
		t.Fatal(e)
	}
	st, e := p.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: id})
	if e != nil {
		t.Fatal(e)
	}
	for _, span := range [][2]int64{{0, 0}, {0, 1}, {2, 4}, {0, 270000}, {269996, 4}, {270000, 0}} {
		var got []byte
		next := span[0]
		eofs := 0
		e = p.read(ctx, &pb.ReadRequest{SourceId: c.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision, Offset: span[0], Length: span[1]}, func(chunk *pb.ReadChunk) error {
			if chunk.Offset != next || len(chunk.Data) > 128*1024 {
				t.Error("bad chunk offset/bound")
			}
			next += int64(len(chunk.Data))
			got = append(got, chunk.Data...)
			if chunk.Eof {
				eofs++
			}
			return nil
		})
		if e != nil || string(got) != data[span[0]:span[0]+span[1]] || eofs != 1 {
			t.Fatal("actual range/body/EOF mismatch", span, e, eofs)
		}
	}
	if _, e = writer.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), Body: strings.NewReader("new-content")}); e != nil {
		t.Fatal(e)
	}
	for _, length := range []int64{0, 4} {
		if e = p.read(ctx, &pb.ReadRequest{SourceId: c.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision, Length: length}, func(*pb.ReadChunk) error { t.Error("stale read delivered chunk"); return nil }); status.Code(e) != codes.FailedPrecondition {
			t.Fatal("overwrite did not fail pinned read", e)
		}
	}
	if _, e = writer.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key)}); e != nil {
		t.Fatal(e)
	}
	if e = p.read(ctx, &pb.ReadRequest{SourceId: c.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision, Length: 4}, func(*pb.ReadChunk) error { t.Error("deleted read delivered chunk"); return nil }); status.Code(e) != codes.NotFound {
		t.Fatal("deleted pinned read", e)
	}
}

func TestBackendOutageListAndRead(t *testing.T) {
	if os.Getenv("S3_OUTAGE") != "1" {
		t.Skip("requires intentionally stopped task fixture")
	}
	c := fixtureConfig()
	p, e := Open(c, filepath.Join(t.TempDir(), "private", "db"))
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	ctx := context.Background()
	id, e := p.remember(c.Prefix+"outage", pb.EntryKind_ENTRY_KIND_FILE)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: p.rootID()}); status.Code(e) != codes.Unavailable {
		t.Fatal("outage List treated as absence", e)
	}
	raw, _ := json.Marshal(revision{ID: id, ETag: "\"fixture\"", Size: 4})
	if e = p.read(ctx, &pb.ReadRequest{SourceId: c.SourceID, EntryId: id, ExpectedRevision: base64.RawURLEncoding.EncodeToString(raw), Length: 4}, func(*pb.ReadChunk) error { t.Error("outage read delivered chunk"); return nil }); status.Code(e) != codes.Unavailable {
		t.Fatal("outage Read treated as absence", e)
	}
}

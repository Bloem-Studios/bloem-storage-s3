package s3provider

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reads only synthetic, task-owned private configuration. Never log this struct.
type versionedFixture struct {
	Source         string `json:"source_id"`
	Bucket         string `json:"bucket"`
	NegativeBucket string `json:"negative_bucket"`
	Region         string `json:"region"`
	Endpoint       string `json:"endpoint"`
	Reader         string `json:"reader_key"`
	ReaderSecret   string `json:"reader_secret"`
	Writer         string `json:"writer_key"`
	WriterSecret   string `json:"writer_secret"`
}

func loadVersionedFixture(t *testing.T) (versionedFixture, Config, *s3.Client) {
	t.Helper()
	if os.Getenv("S3_VERSIONED_FIXTURE") != "1" {
		t.Skip("requires new task-owned official-source versioned fixture")
	}
	dir := os.Getenv("S3_VERSIONED_DIR")
	absolute, e := filepath.Abs(".superpowers/sdd/versioned-proof")
	if e != nil || !strings.HasPrefix(dir, absolute+"/run.") {
		t.Fatal("invalid task-owned fixture directory")
	}
	for _, path := range []string{dir, filepath.Join(dir, "fixture.json")} {
		st, e := os.Lstat(path)
		if e != nil || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
			t.Fatal("fixture must be private, regular and task-owned")
		}
	}
	raw, e := os.ReadFile(filepath.Join(dir, "fixture.json"))
	if e != nil {
		t.Fatal("cannot read private fixture")
	}
	var f versionedFixture
	if json.Unmarshal(raw, &f) != nil || f.Endpoint != "http://127.0.0.1:19342" || !strings.HasPrefix(f.Bucket, "versioned-proof-") || f.Writer == f.Reader {
		t.Fatal("invalid isolated fixture")
	}
	if os.Getenv("S3_VERSIONED_RED") == "1" {
		f.Bucket = f.NegativeBucket
	}
	c := Config{SourceID: f.Source, Bucket: f.Bucket, Prefix: "books/" + t.Name() + "/", Region: f.Region, Endpoint: f.Endpoint, AccessKey: f.Reader, SecretKey: f.ReaderSecret, AllowLoopbackHTTP: true}
	writer := s3.New(s3.Options{Region: f.Region, BaseEndpoint: aws.String(f.Endpoint), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider(f.Writer, f.WriterSecret, ""), RetryMaxAttempts: 1, HTTPClient: &http.Client{Timeout: 10 * time.Second}})
	return f, c, writer
}

func versionedProvider(t *testing.T, c Config) *Provider {
	t.Helper()
	p, e := Open(c, filepath.Join(t.TempDir(), "private", "db"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if p != nil {
			p.Close()
		}
	})
	return p
}
func requireNonNullVersion(t *testing.T, version string) {
	t.Helper()
	if version == "" || version == "null" {
		t.Fatalf("immutable version gate: got %q; disabled/suspended versioning cannot support admission", version)
	}
}
func versionedPut(t *testing.T, ctx context.Context, w *s3.Client, c Config, key string, data []byte) *s3.PutObjectOutput {
	t.Helper()
	out, e := w.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), Body: bytes.NewReader(data)})
	if e != nil {
		t.Fatal("synthetic upload failed", e)
	}
	return out
}
func decodeVersionedRevision(t *testing.T, encoded string) revision {
	t.Helper()
	raw, e := base64.RawURLEncoding.DecodeString(encoded)
	var r revision
	if e != nil || json.Unmarshal(raw, &r) != nil {
		t.Fatal("invalid provider revision")
	}
	return r
}
func encodeVersionedRevision(t *testing.T, r revision) string {
	t.Helper()
	raw, e := json.Marshal(r)
	if e != nil {
		t.Fatal(e)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}
func versionedStat(t *testing.T, ctx context.Context, p *Provider, id string) *pb.Entry {
	t.Helper()
	out, e := p.Stat(ctx, &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
	if e != nil {
		t.Fatal("current Stat", e)
	}
	return out.Entry
}
func versionedRead(t *testing.T, ctx context.Context, p *Provider, id, rev string, data []byte, span [2]int64) {
	t.Helper()
	var got []byte
	next := span[0]
	eofs := 0
	terminal := false
	chunks := 0
	e := p.read(ctx, &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: rev, Offset: span[0], Length: span[1]}, func(c *pb.ReadChunk) error {
		chunks++
		if terminal || c.Offset != next || len(c.Data) > 128*1024 {
			t.Error("Read chunk ordering/offset/bound violation")
		}
		next += int64(len(c.Data))
		got = append(got, c.Data...)
		if c.Eof {
			eofs++
			terminal = true
		}
		return nil
	})
	if e != nil || !bytes.Equal(got, data[span[0]:span[0]+span[1]]) || eofs != 1 || !terminal || chunks < 1 || next != span[0]+span[1] {
		t.Fatalf("pinned Read bytes/range/terminal mismatch span=%v error=%v eof=%d", span, e, eofs)
	}
}
func versionedReadFailure(t *testing.T, ctx context.Context, p *Provider, id, rev string, length int64, want codes.Code) {
	t.Helper()
	sent := 0
	e := p.read(ctx, &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: rev, Length: length}, func(*pb.ReadChunk) error { sent++; return nil })
	if status.Code(e) != want || sent != 0 {
		t.Fatalf("failed Read got %v chunks=%d want=%v", status.Code(e), sent, want)
	}
}

// Raw AWS-v2 requests independently establish validators, exact range and terminal
// body semantics before the provider's own checks can conceal backend behavior.
func versionedRaw(t *testing.T, ctx context.Context, reader *s3.Client, c Config, key string, r revision, data []byte, span [2]int64) {
	t.Helper()
	h, e := reader.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), VersionId: aws.String(r.Version), IfMatch: aws.String(r.ETag)})
	if e != nil || h.ContentLength == nil || *h.ContentLength != r.Size || aws.ToString(h.VersionId) != r.Version || aws.ToString(h.ETag) != r.ETag {
		t.Fatal("raw versioned HEAD validators", e)
	}
	input := &s3.GetObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), VersionId: aws.String(r.Version), IfMatch: aws.String(r.ETag)}
	if span[1] > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", span[0], span[0]+span[1]-1))
	}
	out, e := reader.GetObject(ctx, input)
	if e != nil {
		t.Fatal("raw versioned GET", e)
	}
	defer out.Body.Close()
	want := data
	if span[1] > 0 {
		want = data[span[0] : span[0]+span[1]]
	}
	if out.ContentLength == nil || *out.ContentLength != int64(len(want)) || aws.ToString(out.ETag) != r.ETag || aws.ToString(out.VersionId) != r.Version {
		t.Fatal("raw version/ETag/content-length mismatch")
	}
	if span[1] > 0 && aws.ToString(out.ContentRange) != fmt.Sprintf("bytes %d-%d/%d", span[0], span[0]+span[1]-1, r.Size) {
		t.Fatal("raw Content-Range mismatch")
	}
	got, e := io.ReadAll(io.LimitReader(out.Body, int64(len(want))+1))
	if e != nil || !bytes.Equal(got, want) {
		t.Fatal("raw body length/content mismatch", e)
	}
	var extra [1]byte
	n, e := out.Body.Read(extra[:])
	if n != 0 || e != io.EOF {
		t.Fatal("raw body missing terminal EOF", n, e)
	}
}
func versionedCommand(t *testing.T, action string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, e := exec.CommandContext(ctx, "bash", "scripts/start-versioned-fixture.sh", action).CombinedOutput()
	if e != nil {
		t.Fatalf("label-checked fixture %s failed: %v %s", action, e, out)
	}
}
func versionedReady(t *testing.T, ctx context.Context, w *s3.Client, c Config) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, e := w.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(c.Bucket)}); e == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("bucket readiness timeout")
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestBackendImmutableVersionsAndMarkers(t *testing.T) {
	f, c, writer := loadVersionedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	p := versionedProvider(t, c)
	// Versioned writes never pass through the read-only provider principal.
	versioning, e := p.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(c.Bucket)})
	if e != nil {
		t.Fatal("reader cannot inspect bucket versioning", e)
	}
	if os.Getenv("S3_VERSIONED_RED") != "1" && string(versioning.Status) != "Enabled" {
		t.Fatal("versioning must be enabled before uploads")
	}
	for _, tc := range []struct {
		name   string
		v1, v2 []byte
	}{
		{"range.bin", []byte(strings.Repeat("0123456789", 27000)), []byte(strings.Repeat("abcdefghij", 27000))},
		{"empty.bin", []byte{}, []byte("replacement")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := c.Prefix + tc.name
			one := versionedPut(t, ctx, writer, c, key, tc.v1)
			requireNonNullVersion(t, aws.ToString(one.VersionId))
			id, e := p.remember(key, pb.EntryKind_ENTRY_KIND_FILE)
			if e != nil {
				t.Fatal(e)
			}
			v1 := versionedStat(t, ctx, p, id)
			r1 := decodeVersionedRevision(t, v1.Revision)
			if r1.Version != aws.ToString(one.VersionId) || r1.ETag != aws.ToString(one.ETag) || r1.Size != int64(len(tc.v1)) {
				t.Fatal("Stat v1 validators mismatch")
			}
			two := versionedPut(t, ctx, writer, c, key, tc.v2)
			requireNonNullVersion(t, aws.ToString(two.VersionId))
			v2 := versionedStat(t, ctx, p, id)
			r2 := decodeVersionedRevision(t, v2.Revision)
			if r1.Version == r2.Version || v1.Id != v2.Id || v1.Revision == v2.Revision || r2.Version != aws.ToString(two.VersionId) || r2.ETag != aws.ToString(two.ETag) || r2.Size != int64(len(tc.v2)) {
				t.Fatal("overwrite changed identity or failed immutable revision")
			}
			if _, e := p.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: id, ExpectedRevision: v1.Revision}); status.Code(e) != codes.FailedPrecondition {
				t.Fatal("Stat expected v1 must check CURRENT v2", e)
			}
			spans := [][2]int64{{0, 0}, {int64(len(tc.v1)), 0}}
			if len(tc.v1) > 0 {
				spans = append(spans, [2]int64{0, int64(len(tc.v1))}, [2]int64{0, 1}, [2]int64{2, 4}, [2]int64{int64(len(tc.v1)) - 4, 4}, [2]int64{int64(len(tc.v1)) - 1, 1})
			}
			checkOld := func() {
				for _, span := range spans {
					versionedRead(t, ctx, p, id, v1.Revision, tc.v1, span)
					if span[1] > 0 || len(tc.v1) == 0 {
						versionedRaw(t, ctx, p.client, c, key, r1, tc.v1, span)
					}
				}
			}
			checkOld()
			// Valid VersionID does not override an incorrect conditional validator.
			bad := r1
			bad.ETag = "\"deliberately-invalid\""
			for _, length := range []int64{0, int64(len(tc.v1))} {
				versionedReadFailure(t, ctx, p, id, encodeVersionedRevision(t, bad), length, codes.FailedPrecondition)
			}
			if _, e := p.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), VersionId: aws.String(r1.Version), IfMatch: aws.String(bad.ETag)}); status.Code(backendError(e)) != codes.FailedPrecondition {
				t.Fatal("raw HEAD ignored invalid If-Match", e)
			}
			badGet, e := p.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), VersionId: aws.String(r1.Version), IfMatch: aws.String(bad.ETag)})
			if badGet != nil && badGet.Body != nil {
				badGet.Body.Close()
			}
			if status.Code(backendError(e)) != codes.FailedPrecondition {
				t.Fatal("raw GET ignored invalid If-Match", e)
			}
			bad = r1
			bad.Size++
			versionedReadFailure(t, ctx, p, id, encodeVersionedRevision(t, bad), 0, codes.FailedPrecondition)
			if len(tc.v1) > 0 {
				versionedReadFailure(t, ctx, p, id, encodeVersionedRevision(t, bad), 1, codes.FailedPrecondition)
			}
			marker, e := writer.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key)})
			if e != nil || !aws.ToBool(marker.DeleteMarker) {
				t.Fatal("backend did not create current delete marker", e)
			}
			requireNonNullVersion(t, aws.ToString(marker.VersionId))
			versions, e := p.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(c.Bucket), Prefix: aws.String(key)})
			if e != nil {
				t.Fatal("reader version listing denied", e)
			}
			currentMarker := false
			oldFound := false
			for _, m := range versions.DeleteMarkers {
				if aws.ToString(m.Key) == key && aws.ToString(m.VersionId) == aws.ToString(marker.VersionId) && aws.ToBool(m.IsLatest) {
					currentMarker = true
				}
			}
			for _, v := range versions.Versions {
				if aws.ToString(v.Key) == key && aws.ToString(v.VersionId) == r1.Version {
					oldFound = true
				}
			}
			if !currentMarker || !oldFound {
				t.Fatal("version list missing current marker or retained v1")
			}
			for _, expected := range []string{"", v1.Revision} {
				if _, e := p.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: id, ExpectedRevision: expected}); status.Code(e) != codes.NotFound {
					t.Fatal("CURRENT delete marker must be Stat NotFound", e)
				}
			}
			checkOld()
			versionedCommand(t, "restart")
			versionedReady(t, ctx, writer, c)
			checkOld()
			deleted, e := writer.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), VersionId: aws.String(r1.Version)})
			if e != nil || aws.ToString(deleted.VersionId) != r1.Version {
				t.Fatal("explicit version deletion failed", e)
			}
			for _, length := range []int64{0, int64(len(tc.v1))} {
				versionedReadFailure(t, ctx, p, id, v1.Revision, length, codes.NotFound)
			}
			if _, e := p.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), VersionId: aws.String(r1.Version)}); e == nil {
				t.Fatal("explicitly deleted v1 still HEADs")
			}
			gone, e := p.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), VersionId: aws.String(r1.Version)})
			if gone != nil && gone.Body != nil {
				gone.Body.Close()
			}
			if status.Code(backendError(e)) != codes.NotFound {
				t.Fatal("raw deleted version must be NoSuchVersion", e)
			}
			t.Logf("%s v1=%s v2=%s marker=%s size1=%d sha256v1=%x size2=%d sha256v2=%x; old reads passed after overwrite/marker/restart then NotFound after explicit deletion", tc.name, r1.Version, r2.Version, aws.ToString(marker.VersionId), len(tc.v1), sha256.Sum256(tc.v1), len(tc.v2), sha256.Sum256(tc.v2))
			// Distinguish authoritative object absence from missing/denied source.
			for _, phase := range []string{"missing-bucket", "denied"} {
				config := c
				want := codes.Unavailable
				if phase == "missing-bucket" {
					config.Bucket += "-does-not-exist"
				} else {
					config.AccessKey = "synthetic-invalid"
					config.SecretKey = "synthetic-invalid"
					want = codes.PermissionDenied
				}
				other := versionedProvider(t, config)
				oid, e := other.remember(key, pb.EntryKind_ENTRY_KIND_FILE)
				if e != nil {
					t.Fatal(e)
				}
				otherRev := r1
				otherRev.ID = oid
				for _, length := range []int64{0, int64(len(tc.v1))} {
					versionedReadFailure(t, ctx, other, oid, encodeVersionedRevision(t, otherRev), length, want)
				}
				for _, entry := range []string{other.rootID(), oid} {
					if _, e := other.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: entry}); status.Code(e) != want {
						t.Fatal(phase, "Stat", e)
					}
				}
				if _, e := other.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: other.rootID()}); status.Code(e) != want {
					t.Fatal(phase, "List", e)
				}
			}
		})
	}
	if os.Getenv("S3_VERSIONED_RED") == "1" {
		return
	}
	t.Run("deterministic-media-fixtures", func(t *testing.T) {
		media := versionedMedia(t)
		dir := filepath.Join(os.Getenv("S3_VERSIONED_DIR"), "media")
		if e := os.Mkdir(dir, 0700); e != nil && !os.IsExist(e) {
			t.Fatal(e)
		}
		for _, name := range []string{"proof.epub", "proof.pdf"} {
			data := media[name]
			if e := os.WriteFile(filepath.Join(dir, name), data, 0600); e != nil {
				t.Fatal(e)
			}
			key := c.Prefix + name
			out := versionedPut(t, ctx, writer, c, key, data)
			requireNonNullVersion(t, aws.ToString(out.VersionId))
			id, e := p.remember(key, pb.EntryKind_ENTRY_KIND_FILE)
			if e != nil {
				t.Fatal(e)
			}
			entry := versionedStat(t, ctx, p, id)
			r := decodeVersionedRevision(t, entry.Revision)
			versionedRead(t, ctx, p, id, entry.Revision, data, [2]int64{0, int64(len(data))})
			versionedRaw(t, ctx, p.client, c, key, r, data, [2]int64{0, int64(len(data))})
			t.Logf("%s size=%d sha256=%x version=%s; expected title=Synthetic fixture author=Fixture Author; EPUB embedded PNG cover/nav/chapter spine; PDF Info dictionary; parser acceptance NOT tested", name, len(data), sha256.Sum256(data), r.Version)
		}
	})
	t.Run("read-only-permissions", func(t *testing.T) {
		denied, e := p.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(c.Prefix + "reader-cannot-write"), Body: strings.NewReader("forbidden")})
		if denied != nil || status.Code(backendError(e)) != codes.PermissionDenied {
			t.Fatal("reader obtained write access", e)
		}
	})
	t.Run("suspended-null-version-negative", func(t *testing.T) {
		negative := c
		negative.Bucket = f.NegativeBucket
		np := versionedProvider(t, negative)
		ver, e := np.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(negative.Bucket)})
		if e != nil || string(ver.Status) != "Suspended" {
			t.Fatal("negative bucket must be suspended", e)
		}
		key := negative.Prefix + "mutable-null.bin"
		one := versionedPut(t, ctx, writer, negative, key, []byte("AAAA"))
		if version := aws.ToString(one.VersionId); version != "" && version != "null" {
			t.Fatal("suspended upload unexpectedly has immutable version")
		}
		versions, e := np.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(negative.Bucket), Prefix: aws.String(key)})
		if e != nil || len(versions.Versions) != 1 || aws.ToString(versions.Versions[0].Key) != key || aws.ToString(versions.Versions[0].VersionId) != "null" || !aws.ToBool(versions.Versions[0].IsLatest) {
			t.Fatal("suspended object must list one current literal null version", e)
		}
		id, e := np.remember(key, pb.EntryKind_ENTRY_KIND_FILE)
		if e != nil {
			t.Fatal(e)
		}
		old := versionedStat(t, ctx, np, id)
		r := decodeVersionedRevision(t, old.Revision)
		if r.Version != "" {
			t.Fatal("null version must normalize to mutable")
		}
		versionedPut(t, ctx, writer, negative, key, []byte("BBBB"))
		for _, length := range []int64{0, 4} {
			versionedReadFailure(t, ctx, np, id, old.Revision, length, codes.FailedPrecondition)
		}
		current := versionedStat(t, ctx, np, id)
		null, e := np.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(negative.Bucket), Key: aws.String(key), VersionId: aws.String("null")})
		if e != nil {
			t.Fatal("explicit null GET", e)
		}
		mutable, e := io.ReadAll(null.Body)
		null.Body.Close()
		if e != nil || string(mutable) != "BBBB" {
			t.Fatal("explicit null version did not mutate after overwrite", e)
		}
		versionedRead(t, ctx, np, id, current.Revision, []byte("BBBB"), [2]int64{0, 4})
		r.Version = "null"
		versionedReadFailure(t, ctx, np, id, encodeVersionedRevision(t, r), 0, codes.InvalidArgument)
		for _, provider := range []*Provider{p, np} {
			d, e := provider.Describe(ctx, &pb.DescribeRequest{})
			if e != nil || len(d.Sources) != 1 || d.Sources[0].RevisionPinnedReads {
				t.Fatal("test proof must not enable admission", e)
			}
		}
		t.Log("suspended null upload overwritten: old zero/full reads conflict; forged null rejected; both configurations Describe false")
	})
	t.Run("outage-restored", func(t *testing.T) {
		key := c.Prefix + "outage.bin"
		versionedPut(t, ctx, writer, c, key, []byte("live"))
		id, e := p.remember(key, pb.EntryKind_ENTRY_KIND_FILE)
		if e != nil {
			t.Fatal(e)
		}
		entry := versionedStat(t, ctx, p, id)
		// Registered BEFORE stop, so fatal/failed assertions always restore this fixture.
		t.Cleanup(func() { versionedCommand(t, "restart"); versionedReady(t, context.Background(), writer, c) })
		versionedCommand(t, "stop")
		if _, e := p.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: id}); status.Code(e) != codes.Unavailable {
			t.Fatal("outage Stat", e)
		}
		if _, e := p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: p.rootID()}); status.Code(e) != codes.Unavailable {
			t.Fatal("outage List", e)
		}
		for _, length := range []int64{0, 4} {
			versionedReadFailure(t, ctx, p, id, entry.Revision, length, codes.Unavailable)
		}
		t.Log("real task-only outage returned Unavailable without chunks; cleanup restores backend")
	})
}

func TestBackendVersionedOrderedProjection(t *testing.T) {
	_, c, writer := loadVersionedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for _, key := range []string{"foo", "marker/", "z.epub"} {
		versionedPut(t, ctx, writer, c, c.Prefix+key, []byte("synthetic"))
	}
	// Cross the provider's real 512-key backend boundary, including a contiguous
	// directory spanning that boundary; only this new backend is exercised.
	for i := 0; i < 520; i++ {
		out := versionedPut(t, ctx, writer, c, fmt.Sprintf("%sfoo/%04d.pdf", c.Prefix, i), []byte("synthetic"))
		requireNonNullVersion(t, aws.ToString(out.VersionId))
	}

	wantKeys := []string{c.Prefix + "foo"}
	for i := 0; i < 520; i++ {
		wantKeys = append(wantKeys, fmt.Sprintf("%sfoo/%04d.pdf", c.Prefix, i))
	}
	wantKeys = append(wantKeys, c.Prefix+"marker/", c.Prefix+"z.epub")
	rawKeys, e := versionedOrderedKeys(ctx, func(ctx context.Context, in *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
		return writer.ListObjectsV2(ctx, in)
	}, s3.ListObjectsV2Input{Bucket: aws.String(c.Bucket), Prefix: aws.String(c.Prefix)}, wantKeys)
	if e != nil {
		// Retain genuine omission diagnostics without dereferencing a nil output.
		t.Logf("root raw returned keys=%q", rawKeys)
		for _, n := range []int{0, 519} {
			key := fmt.Sprintf("%sfoo/%04d.pdf", c.Prefix, n)
			h, err := writer.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key)})
			if err != nil || h == nil {
				t.Logf("descendant %04d HEAD failed=%v", n, err)
			} else {
				t.Logf("descendant %04d exists size=%d version=%s", n, aws.ToInt64(h.ContentLength), aws.ToString(h.VersionId))
			}
		}
		children, err := writer.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(c.Bucket), Prefix: aws.String(c.Prefix + "foo/"), MaxKeys: aws.Int32(512)})
		if err == nil && children != nil {
			t.Logf("explicit foo/ prefix listing contents=%d truncated=%v tokenpresent=%v", len(children.Contents), aws.ToBool(children.IsTruncated), aws.ToString(children.NextContinuationToken) != "")
			if aws.ToBool(children.IsTruncated) {
				end, err := writer.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(c.Bucket), Prefix: aws.String(c.Prefix + "foo/"), MaxKeys: aws.Int32(512), ContinuationToken: children.NextContinuationToken})
				if err == nil && end != nil {
					t.Logf("explicit foo/ second page contents=%d truncated=%v", len(end.Contents), aws.ToBool(end.IsTruncated))
				} else {
					t.Logf("explicit foo/ second page error=%v", err)
				}
			}
		} else {
			t.Logf("explicit foo/ listing error=%v", err)
		}
		provider := versionedProvider(t, c)
		projected, err := provider.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: provider.rootID(), MaxEntries: 512})
		if err == nil && projected != nil {
			paths := []string{}
			for _, entry := range projected.Entries {
				paths = append(paths, entry.LogicalPath)
			}
			t.Logf("actual provider root paths=%q complete=%v", paths, projected.Complete)
		} else {
			t.Logf("actual provider root error=%v", err)
		}
		t.Fatalf("raw ordered backend pagination: %v", e)
	}
	path := filepath.Join(t.TempDir(), "private", "db")
	p, e := Open(c, path)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if p != nil {
			p.Close()
		}
	})
	page, e := p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: p.rootID(), MaxEntries: 1})
	if e != nil {
		t.Fatal(e)
	}
	entries := append([]*pb.Entry{}, page.Entries...)
	if page.Complete || len(entries) != 1 {
		t.Fatal("expected bounded pending page")
	}
	p.Close()
	p, e = Open(c, path)
	if e != nil {
		t.Fatal(e)
	}
	for n := 0; !page.Complete && n < 10; n++ {
		page, e = p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: p.rootID(), MaxEntries: 1, Cursor: page.NextCursor})
		if e != nil {
			t.Fatal(e)
		}
		entries = append(entries, page.Entries...)
	}
	if !page.Complete || len(entries) != 4 {
		t.Fatal("dropped/duplicate projected entries")
	}
	for i, want := range []string{"foo", "foo/", "marker/", "z.epub"} {
		if entries[i].LogicalPath != "books/"+want {
			t.Fatalf("projection[%d]=%q want=%q", i, entries[i].LogicalPath, want)
		}
	}
	if entries[0].Id == entries[1].Id {
		t.Fatal("file/prefix identity collision")
	}
	for _, entry := range entries {
		if _, e := p.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: entry.Id}); e != nil {
			t.Fatal("mixed file/marker Stat", e)
		}
	}
	nested, e := p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: entries[1].Id})
	if e != nil || len(nested.Entries) != 512 || nested.Complete {
		t.Fatal("nested first page", e)
	}
	tail, e := p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: entries[1].Id, Cursor: nested.NextCursor})
	if e != nil || len(tail.Entries) != 8 || !tail.Complete {
		t.Fatal("nested terminal page", e)
	}
	all := append(nested.Entries, tail.Entries...)
	for i, entry := range all {
		if entry.LogicalPath != fmt.Sprintf("books/foo/%04d.pdf", i) {
			t.Fatalf("nested ordering/loss at %d", i)
		}
	}
	t.Log("raw exact 523 ordered keys with bounded sparse-page pagination; root foo/foo//marker//z.epub exactly once across durable provider reopen; nested 512+8 exact ordered entries")
}

// Extend the existing valid EPUB helper with a deterministic embedded cover;
// generate a PDF Info dictionary with correct offsets rather than claiming that
// the ordinary byte-only PDF fixture proves useful metadata.
func versionedMedia(t *testing.T) map[string][]byte {
	t.Helper()
	im := image.NewRGBA(image.Rect(0, 0, 2, 2))
	for y := 0; y < 2; y++ {
		for x := 0; x < 2; x++ {
			im.Set(x, y, color.RGBA{R: 20, G: 80, B: 140, A: 255})
		}
	}
	var cover bytes.Buffer
	if e := png.Encode(&cover, im); e != nil {
		t.Fatal(e)
	}
	original := syntheticEPUB(t)
	in, e := zip.NewReader(strings.NewReader(original), int64(len(original)))
	if e != nil {
		t.Fatal(e)
	}
	var epub bytes.Buffer
	z := zip.NewWriter(&epub)
	for _, entry := range in.File {
		reader, e := entry.Open()
		if e != nil {
			t.Fatal(e)
		}
		data, e := io.ReadAll(reader)
		reader.Close()
		if e != nil {
			t.Fatal(e)
		}
		if entry.Name == "OEBPS/package.opf" {
			text := strings.Replace(strings.Replace(string(data), "Synthetic EPUB fixture", "Synthetic fixture", 1), "</metadata>", `<meta name="cover" content="cover"/></metadata>`, 1)
			text = strings.Replace(text, "</manifest>", `<item id="cover" href="cover.png" media-type="image/png" properties="cover-image"/></manifest>`, 1)
			data = []byte(text)
		}
		w, e := z.CreateHeader(&zip.FileHeader{Name: entry.Name, Method: entry.Method})
		if e != nil {
			t.Fatal(e)
		}
		if _, e = w.Write(data); e != nil {
			t.Fatal(e)
		}
	}
	w, e := z.CreateHeader(&zip.FileHeader{Name: "OEBPS/cover.png", Method: zip.Store})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = w.Write(cover.Bytes()); e != nil {
		t.Fatal(e)
	}
	if e = z.Close(); e != nil {
		t.Fatal(e)
	}
	stream := "BT /F1 12 Tf 72 720 Td (Synthetic fixture) Tj ET\n"
	objects := []string{`<< /Type /Catalog /Pages 2 0 R >>`, `<< /Type /Pages /Kids [3 0 R] /Count 1 >>`, `<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>`, `<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>`, fmt.Sprintf("<< /Length %d >>\nstream\n%sendstream", len(stream), stream), `<< /Title (Synthetic fixture) /Author (Fixture Author) /Subject (Immutable storage proof) /Creator (Bloem synthetic fixture) >>`}
	var pdf bytes.Buffer
	pdf.WriteString("%PDF-1.4\n")
	offsets := []int{0}
	for i, obj := range objects {
		offsets = append(offsets, pdf.Len())
		fmt.Fprintf(&pdf, "%d 0 obj\n%s\nendobj\n", i+1, obj)
	}
	xref := pdf.Len()
	fmt.Fprintf(&pdf, "xref\n0 %d\n0000000000 65535 f \n", len(offsets))
	for _, offset := range offsets[1:] {
		fmt.Fprintf(&pdf, "%010d 00000 n \n", offset)
	}
	fmt.Fprintf(&pdf, "trailer\n<< /Size %d /Root 1 0 R /Info 6 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(offsets), xref)
	return map[string][]byte{"proof.epub": epub.Bytes(), "proof.pdf": pdf.Bytes()}
}

// Test-only raw gate: MaxKeys is an upper bound, not a page-fill guarantee.
// It accepts sparse pages only with advancing nonrepeating opaque tokens,
// and requires terminal completeness of the exact strictly ordered key set.
func versionedOrderedKeys(ctx context.Context, list func(context.Context, *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error), input s3.ListObjectsV2Input, want []string) ([]string, error) {
	input.MaxKeys = aws.Int32(512)
	input.ContinuationToken = nil
	keys := []string{}
	seen := map[string]bool{}
	// Permit one object per page and one additional empty terminal page,
	// but never allow a faulty backend to keep this proof running indefinitely.
	for page := 0; page < len(want)+1; page++ {
		raw, err := list(ctx, &input)
		if err != nil {
			return keys, fmt.Errorf("listing request failed on page %d (details withheld)", page+1)
		}
		if raw == nil {
			return keys, fmt.Errorf("nil listing output on page %d", page+1)
		}
		if len(raw.Contents) > 512 {
			return keys, fmt.Errorf("listing exceeds MaxKeys on page %d", page+1)
		}
		if len(raw.CommonPrefixes) != 0 {
			return keys, fmt.Errorf("unexpected common prefixes on delimiter-free page %d", page+1)
		}
		for _, object := range raw.Contents {
			key := aws.ToString(object.Key)
			if len(keys) > 0 && key <= keys[len(keys)-1] {
				return keys, fmt.Errorf("raw keys not strictly ascending on page %d", page+1)
			}
			keys = append(keys, key)
		}
		token := aws.ToString(raw.NextContinuationToken)
		if !aws.ToBool(raw.IsTruncated) {
			if token != "" {
				return keys, fmt.Errorf("terminal listing has continuation token")
			}
			if len(keys) != len(want) {
				return keys, fmt.Errorf("raw ordered key set incomplete: got %d want %d", len(keys), len(want))
			}
			for i, key := range keys {
				if key != want[i] {
					return keys, fmt.Errorf("raw ordered key set mismatch at index %d", i)
				}
			}
			return keys, nil
		}
		if token == "" || token == aws.ToString(input.ContinuationToken) || seen[token] {
			return keys, fmt.Errorf("missing or nonprogressing continuation token on page %d", page+1)
		}
		seen[token] = true
		input.ContinuationToken = aws.String(token)
	}
	return keys, fmt.Errorf("raw listing exceeded page bound %d", len(want)+1)
}

func TestVersionedOrderedKeysHarness(t *testing.T) {
	want := []string{"books/foo"}
	for i := 0; i < 520; i++ {
		want = append(want, fmt.Sprintf("books/foo/%04d.pdf", i))
	}
	want = append(want, "books/marker/", "books/z.epub")
	page := func(keys []string, token string) *s3.ListObjectsV2Output {
		out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(token != ""), MaxKeys: aws.Int32(512), KeyCount: aws.Int32(int32(len(keys)))}
		if token != "" {
			out.NextContinuationToken = aws.String(token)
		}
		for _, key := range keys {
			out.Contents = append(out.Contents, types.Object{Key: aws.String(key)})
		}
		return out
	}
	type fixture struct {
		name      string
		pages     []*s3.ListObjectsV2Output
		err       error
		wantError string
	}
	wrong := append([]string{}, want[512:]...)
	wrong[len(wrong)-1] = "books/zz.epub"
	cases := []fixture{
		{name: "sparse-complete", pages: []*s3.ListObjectsV2Output{page(want[:2], "a"), page(want[2:300], "b"), page(want[300:522], "c"), page(want[522:], "")}},
		{name: "sparse-empty-page", pages: []*s3.ListObjectsV2Output{page(want[:2], "a"), page(nil, "b"), page(want[2:512], "c"), page(want[512:], "")}},
		{name: "full-complete", pages: []*s3.ListObjectsV2Output{page(want[:512], "a"), page(want[512:], "")}},
		{name: "genuine-three-of-523-omission", pages: []*s3.ListObjectsV2Output{page([]string{want[0], want[521], want[522]}, "")}, wantError: "key set"},
		{name: "terminal-short", pages: []*s3.ListObjectsV2Output{page(want[:512], "a"), page(want[512:522], "")}, wantError: "key set"},
		{name: "duplicate-key", pages: []*s3.ListObjectsV2Output{page(want[:512], "a"), page(append([]string{want[511]}, want[512:]...), "")}, wantError: "ascending"},
		{name: "unordered", pages: []*s3.ListObjectsV2Output{page([]string{want[1], want[0]}, "")}, wantError: "ascending"},
		{name: "wrong-key", pages: []*s3.ListObjectsV2Output{page(want[:512], "a"), page(wrong, "")}, wantError: "key set"},
		{name: "over-limit", pages: []*s3.ListObjectsV2Output{page(want, "")}, wantError: "MaxKeys"},
		{name: "repeated-token", pages: []*s3.ListObjectsV2Output{page(want[:1], "a"), page(want[1:2], "a")}, wantError: "token"},
		{name: "cycled-token", pages: []*s3.ListObjectsV2Output{page(want[:1], "a"), page(want[1:2], "b"), page(want[2:3], "a")}, wantError: "token"},
		{name: "missing-token", pages: []*s3.ListObjectsV2Output{{IsTruncated: aws.Bool(true)}}, wantError: "token"},
		{name: "terminal-token", pages: []*s3.ListObjectsV2Output{{IsTruncated: aws.Bool(false), NextContinuationToken: aws.String("a")}}, wantError: "token"},
		{name: "nil-output", pages: []*s3.ListObjectsV2Output{nil}, wantError: "nil"},
		{name: "nil-output-error", err: errors.New("synthetic listing error"), wantError: "listing request failed"},
		{name: "bounded-empty-pages", wantError: "page bound"},
	}
	prefixes := page(want[:512], "a")
	prefixes.CommonPrefixes = []types.CommonPrefix{{Prefix: aws.String("books/foo/")}}
	cases = append(cases, fixture{name: "unexpected-prefix", pages: []*s3.ListObjectsV2Output{prefixes}, wantError: "prefix"})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("listing diagnostic panicked: %v", r)
				}
			}()
			calls := 0
			previousToken := ""
			list := func(_ context.Context, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
				if aws.ToInt32(input.MaxKeys) != 512 || aws.ToString(input.ContinuationToken) != previousToken || input.Delimiter != nil {
					t.Fatal("wrong bounded continuation request")
				}
				calls++
				if calls > 525 {
					t.Fatal("unbounded listing")
				}
				if tc.err != nil {
					return nil, tc.err
				}
				if tc.name == "bounded-empty-pages" {
					previousToken = fmt.Sprint(calls)
					return page(nil, previousToken), nil
				}
				if calls > len(tc.pages) {
					t.Fatal("unexpected listing request")
				}
				out := tc.pages[calls-1]
				if out != nil {
					previousToken = aws.ToString(out.NextContinuationToken)
				}
				return out, nil
			}
			keys, err := versionedOrderedKeys(context.Background(), list, s3.ListObjectsV2Input{Bucket: aws.String("synthetic"), Prefix: aws.String("books/")}, want)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("got %v, want error containing %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(keys) != len(want) {
				t.Fatalf("got %d keys want %d", len(keys), len(want))
			}
			for i := range want {
				if keys[i] != want[i] {
					t.Fatalf("key %d mismatch", i)
				}
			}
		})
	}
}

package s3provider

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	runtimepb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	sdk "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Verify SigV4 independently of the SDK client. Fixture credentials are supplied
// literals; the fixture never loads ambient AWS configuration or real secrets.
func fixtureSignatureValid(r *http.Request, access, secret, token string) bool {
	auth, ok := strings.CutPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=")
	if !ok || r.Header.Get("X-Amz-Security-Token") != token {
		return false
	}
	credential, rest, ok := strings.Cut(auth, ", SignedHeaders=")
	if !ok {
		return false
	}
	signedHeaders, signature, ok := strings.Cut(rest, ", Signature=")
	scope := strings.Split(credential, "/")
	if !ok || len(scope) != 5 || scope[0] != access || scope[2] != "us-east-1" || scope[3] != "s3" || scope[4] != "aws4_request" {
		return false
	}
	stamp := r.Header.Get("X-Amz-Date")
	if len(stamp) != 16 || stamp[:8] != scope[1] {
		return false
	}
	var headers strings.Builder
	for _, key := range strings.Split(signedHeaders, ";") {
		value := r.Header.Get(key)
		if key == "host" {
			value = r.Host
		}
		if value == "" {
			return false
		}
		fmt.Fprintf(&headers, "%s:%s\n", key, strings.Join(strings.Fields(value), " "))
	}
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		return false
	}
	canonical := strings.Join([]string{r.Method, r.URL.EscapedPath(), r.URL.Query().Encode(), headers.String(), signedHeaders, payloadHash}, "\n")
	digest := sha256.Sum256([]byte(canonical))
	toSign := "AWS4-HMAC-SHA256\n" + stamp + "\n" + strings.Join(scope[1:], "/") + "\n" + hex.EncodeToString(digest[:])
	sign := func(key []byte, text string) []byte {
		h := hmac.New(sha256.New, key)
		h.Write([]byte(text))
		return h.Sum(nil)
	}
	key := sign([]byte("AWS4"+secret), scope[1])
	key = sign(key, scope[2])
	key = sign(key, scope[3])
	key = sign(key, scope[4])
	return hmac.Equal([]byte(hex.EncodeToString(sign(key, toSign))), []byte(signature))
}

func TestNativeExecutableIndexesAllKindsAndReadsImmutableObjects(t *testing.T) {
	testNativeExecutableImmutableObjects(t, "versioned")
}

func TestNativeCheckedETagExecutableKeepsGenuineOldVersions(t *testing.T) {
	testNativeExecutableImmutableObjects(t, "checked-etag")
}

func testNativeExecutableImmutableObjects(t *testing.T, mode string) {
	t.Helper()
	cfg := fixtureConfig()
	cfg.RevisionMode = mode
	cfg.SessionToken = "fixture-session-only"
	var changed atomic.Bool
	var verifiedRequests atomic.Int32
	keys := []string{"books/audio.m4b", "books/book.epub", "books/comic.cbz", "books/movie.mkv", "books/music.flac", "books/series/episode.mp4"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !fixtureSignatureValid(r, cfg.AccessKey, cfg.SecretKey, cfg.SessionToken) {
			t.Error("fixture rejected S3 request signature")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		verifiedRequests.Add(1)
		if r.URL.Query().Get("list-type") == "2" {
			io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`)
			limit := 512
			if r.URL.Query().Get("max-keys") == "1" {
				limit = 1
			}
			prefix, delimited := r.URL.Query().Get("prefix"), r.URL.Query().Get("delimiter") == "/"
			for _, key := range keys {
				if !strings.HasPrefix(key, prefix) || limit == 0 {
					continue
				}
				if i := strings.Index(key[len(prefix):], "/"); delimited && i >= 0 {
					io.WriteString(w, "<CommonPrefixes><Prefix>")
					xml.EscapeText(w, []byte(key[:len(prefix)+i+1]))
					io.WriteString(w, "</Prefix></CommonPrefixes>")
				} else {
					io.WriteString(w, "<Contents><Key>")
					xml.EscapeText(w, []byte(key))
					io.WriteString(w, `</Key><ETag>"immutable-v1"</ETag><Size>4</Size></Contents>`)
				}
				limit--
			}
			io.WriteString(w, `</ListBucketResult>`)
			return
		}
		if r.Method == http.MethodHead && strings.Trim(r.URL.Path, "/") == cfg.Bucket {
			return
		}
		version := r.URL.Query().Get("versionId")
		if version == "" {
			version = "immutable-v1"
			if changed.Load() {
				version = "immutable-v2"
			}
		}
		w.Header().Set("X-Amz-Version-Id", version)
		w.Header().Set("ETag", `"`+version+`"`)
		w.Header().Set("Content-Length", "4")
		if r.Method == http.MethodHead {
			return
		}
		if version != "immutable-v1" || r.Header.Get("If-Match") != `"immutable-v1"` || r.Header.Get("Range") != "bytes=1-2" {
			t.Error("read did not request exact immutable version and range")
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("Content-Length", "2")
		w.Header().Set("Content-Range", "bytes 1-2/4")
		w.WriteHeader(http.StatusPartialContent)
		io.WriteString(w, "ld")
	}))
	defer server.Close()
	cfg.Endpoint = server.URL
	dir := t.TempDir()
	bin := filepath.Join(dir, "plugin")
	cmd := exec.Command("go", "build", "-mod=readonly", "-o", bin, "./cmd/bloem-storage-s3")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	client := plugin.NewClient(&plugin.ClientConfig{HandshakeConfig: sdk.HandshakeConfig(), Plugins: sdk.DefaultPluginSet(sdk.CapabilityServers{}), Cmd: exec.Command(bin), AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC}, Logger: hclog.NewNullLogger()})
	defer client.Kill()
	protocol, err := client.Client()
	if err != nil {
		t.Fatal(err)
	}
	dispensed, err := protocol.Dispense(sdk.PluginSetName)
	if err != nil {
		t.Fatal(err)
	}
	rpc := dispensed.(*sdk.Client)
	ctx := context.Background()
	if _, err := rpc.Runtime().Configure(ctx, &runtimepb.ConfigureRequest{}); err != nil {
		t.Fatal(err)
	}
	fields := nativeFields(t, cfg, filepath.Join(dir, "private", "state.db"))
	if _, err := rpc.Runtime().Configure(ctx, &runtimepb.ConfigureRequest{Config: []*runtimepb.ConfigEntry{{Key: "connection", Value: fields}}}); err != nil {
		t.Fatal(err)
	}
	storage := pb.NewStorageProviderClient(rpc.Conn())
	desc, err := storage.Describe(ctx, &pb.DescribeRequest{})
	if err != nil || len(desc.GetSources()) != 1 || desc.Sources[0].RevisionPinnedReads != (mode == "versioned") {
		t.Fatalf("Describe: %v %v", desc, err)
	}
	page, err := storage.List(ctx, &pb.ListRequest{SourceId: cfg.SourceID, DirectoryId: desc.Sources[0].RootEntryId})
	if err != nil || !page.GetComplete() || len(page.GetEntries()) != 6 {
		t.Fatalf("List: %v %v", page, err)
	}
	for _, response := range []proto.Message{desc, page} {
		raw, _ := proto.Marshal(response)
		for _, secret := range []string{cfg.AccessKey, cfg.SecretKey, cfg.SessionToken} {
			if strings.Contains(string(raw), secret) {
				t.Fatal("credentials exposed in source metadata")
			}
		}
	}
	for i, want := range []string{"audio.m4b", "book.epub", "comic.cbz", "movie.mkv", "music.flac", "series/"} {
		entry := page.Entries[i]
		if entry.LogicalPath != "books/"+want || entry.Revision == "" {
			t.Fatalf("entry %d: %v", i, entry)
		}
	}
	entry := page.Entries[3]
	stat, err := storage.Stat(ctx, &pb.StatRequest{SourceId: cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision})
	if err != nil || stat.GetEntry().GetRevision() != entry.Revision {
		t.Fatalf("Stat: %v %v", stat, err)
	}
	changed.Store(true)
	if _, err := storage.Stat(ctx, &pb.StatRequest{SourceId: cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("replacement Stat: %v", err)
	}
	stream, err := storage.Read(ctx, &pb.ReadRequest{SourceId: cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision, Offset: 1, Length: 2})
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := stream.Recv()
	if err != nil || chunk.Offset != 1 || !chunk.Eof || string(chunk.Data) != "ld" {
		t.Fatalf("immutable range: %v %v", chunk, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("terminal RPC status: %v", err)
	}
	empty, err := storage.Read(ctx, &pb.ReadRequest{SourceId: cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision})
	if err != nil {
		t.Fatal(err)
	}
	chunk, err = empty.Recv()
	if err != nil || !chunk.Eof || len(chunk.Data) != 0 {
		t.Fatalf("immutable zero range: %v %v", chunk, err)
	}
	if _, err = empty.Recv(); err != io.EOF {
		t.Fatalf("zero range terminal RPC status: %v", err)
	}
	if verifiedRequests.Load() < 9 {
		t.Fatal("fixture did not authenticate complete source operations")
	}
}

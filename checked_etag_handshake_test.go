package s3provider

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	runtimepb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	sdk "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNativeCheckedETagExecutableUnversionedRangesRequireFinalSuccess(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "plugin")
	if output, err := exec.Command("go", "build", "-mod=readonly", "-o", bin, "./cmd/bloem-storage-s3").CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, output)
	}
	for _, drift := range []bool{false, true} {
		name := "stable"
		if drift {
			name = "changed-after-body"
		}
		t.Run(name, func(t *testing.T) {
			cfg := fixtureConfig()
			cfg.RevisionMode = "checked-etag"
			var served atomic.Bool
			var verified atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !fixtureSignatureValid(r, cfg.AccessKey, cfg.SecretKey, cfg.SessionToken) {
					t.Error("fixture rejected S3 request signature")
					w.WriteHeader(http.StatusForbidden)
					return
				}
				verified.Add(1)
				if r.URL.Query().Get("list-type") == "2" {
					io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>books/movie.mkv</Key><ETag>"old"</ETag><Size>4</Size></Contents></ListBucketResult>`)
					return
				}
				w.Header().Set("ETag", `"old"`)
				w.Header().Set("Content-Length", "4")
				w.Header().Set("X-Amz-Version-Id", "null")
				if r.Method == http.MethodHead {
					if served.Load() && drift {
						// Simulate a gateway that ignores the old If-Match value.
						w.Header().Set("ETag", `"new"`)
					}
					return
				}
				if r.URL.Query().Has("versionId") || r.Header.Get("If-Match") != `"old"` || r.Header.Get("Range") != "bytes=1-2" {
					t.Error("unversioned GET did not request expected validator and range")
				}
				w.Header().Set("Content-Range", "bytes 1-2/4")
				w.Header().Set("Content-Length", "2")
				w.WriteHeader(http.StatusPartialContent)
				io.WriteString(w, "ld")
				served.Store(true)
			}))
			defer server.Close()
			cfg.Endpoint = server.URL
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
			_, err = rpc.Runtime().Configure(ctx, &runtimepb.ConfigureRequest{Config: []*runtimepb.ConfigEntry{{Key: "connection", Value: nativeFields(t, cfg, filepath.Join(t.TempDir(), "private", "db"))}}})
			if err != nil {
				t.Fatal(err)
			}
			storage := pb.NewStorageProviderClient(rpc.Conn())
			desc, err := storage.Describe(ctx, &pb.DescribeRequest{})
			if err != nil || len(desc.GetSources()) != 1 || desc.Sources[0].RevisionPinnedReads {
				t.Fatalf("checked Describe claimed immutable revisions: %v %v", desc, err)
			}
			page, err := storage.List(ctx, &pb.ListRequest{SourceId: cfg.SourceID, DirectoryId: desc.Sources[0].RootEntryId})
			if err != nil || !page.GetComplete() || len(page.GetEntries()) != 1 || page.Entries[0].Revision == "" {
				t.Fatalf("checked List: %v %v", page, err)
			}
			entry := page.Entries[0]
			stream, err := storage.Read(ctx, &pb.ReadRequest{SourceId: cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision, Offset: 1, Length: 2})
			if err != nil {
				t.Fatal(err)
			}
			chunk, err := stream.Recv()
			if drift {
				if status.Code(err) != codes.FailedPrecondition || chunk != nil {
					t.Fatalf("late drift emitted final bytes/success: %v %v", chunk, err)
				}
			} else {
				if err != nil || chunk.Offset != 1 || string(chunk.Data) != "ld" || !chunk.Eof {
					t.Fatalf("checked range: %v %v", chunk, err)
				}
				if _, err = stream.Recv(); err != io.EOF {
					t.Fatalf("checked range terminal RPC status: %v", err)
				}
			}
			if verified.Load() != 5 {
				t.Fatalf("expected authenticated list, page version probe HEAD, pre-read HEAD, GET, post-read HEAD; got %d requests", verified.Load())
			}
		})
	}
}

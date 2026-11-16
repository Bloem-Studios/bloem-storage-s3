package s3provider

import (
	"context"
	"encoding/json"
	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	runtimepb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	sdk "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginsdk/runtime"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"google.golang.org/protobuf/types/known/structpb"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestExecutablePrivateHandshakeConfigureAndRestart(t *testing.T) {
	var declared struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(manifestJSON, &declared); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "provider")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/bloem-storage-s3")
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("build: %v %s", e, out)
	}
	c := fixtureConfig()
	raw, _ := json.Marshal(c)
	fields, e := structpb.NewStruct(map[string]any{"json": string(raw), "state_path": filepath.Join(dir, "state", "index.db")})
	if e != nil {
		t.Fatal(e)
	}
	var root string
	for i := 0; i < 2; i++ {
		client := plugin.NewClient(&plugin.ClientConfig{HandshakeConfig: sdk.HandshakeConfig(), Plugins: sdk.DefaultPluginSet(sdk.CapabilityServers{}), Cmd: exec.Command(bin), AllowedProtocols: []plugin.Protocol{plugin.ProtocolGRPC}, Logger: hclog.NewNullLogger()})
		protocol, e := client.Client()
		if e != nil {
			client.Kill()
			t.Fatal(e)
		}
		dispensed, e := protocol.Dispense(sdk.PluginSetName)
		if e != nil {
			client.Kill()
			t.Fatal(e)
		}
		rpc := dispensed.(*sdk.Client)
		ctx := context.Background()
		manifest, e := rpc.Runtime().GetManifest(ctx, &runtimepb.GetManifestRequest{})
		if e != nil || manifest.Manifest.PluginId != "bloem.storage.s3" {
			client.Kill()
			t.Fatal("manifest", e)
		}
		if manifest.Manifest.Version != declared.Version {
			client.Kill()
			t.Fatalf("executable version = %q, manifest version = %q", manifest.Manifest.Version, declared.Version)
		}
		_, e = rpc.Runtime().Configure(ctx, &runtimepb.ConfigureRequest{Config: []*runtimepb.ConfigEntry{{Key: "source_json", Value: fields}}})
		if e != nil {
			client.Kill()
			t.Fatal(e)
		}
		storage := pb.NewStorageProviderClient(rpc.Conn())
		desc, e := storage.Describe(ctx, &pb.DescribeRequest{})
		if e != nil || len(desc.Sources) != 1 {
			client.Kill()
			t.Fatal(e)
		}
		if i == 0 {
			root = desc.Sources[0].RootEntryId
		} else if root != desc.Sources[0].RootEntryId {
			client.Kill()
			t.Fatal("restart identity changed")
		}
		if desc.Sources[0].RevisionPinnedReads {
			client.Kill()
			t.Fatal("unverified capability advertised")
		}
		client.Kill()
	}
}

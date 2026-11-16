package s3provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	runtimepb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func nativeFields(t *testing.T, c Config, statePath string) *structpb.Struct {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	fields["state_path"] = statePath
	value, err := structpb.NewStruct(fields)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func strictConfig(t *testing.T, c Config) Config {
	t.Helper()
	c.RevisionMode = "versioned"
	return c
}

func TestNativeEmptyConfigurationAllowsHandshakeWithoutAmbientCredentials(t *testing.T) {
	s := new(Service)
	ctx := context.Background()
	t.Setenv("AWS_ACCESS_KEY_ID", "ambient-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")
	if _, err := s.Configure(ctx, &runtimepb.ConfigureRequest{}); err != nil {
		t.Fatalf("empty startup configuration refused: %v", err)
	}
	if _, err := s.Describe(ctx, &pb.DescribeRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ambient credentials activated a source: %v", err)
	}
}

func TestNativeManifestDeclaresSecretConnectionFields(t *testing.T) {
	var manifest struct {
		Schemas []struct {
			Key  string `json:"key"`
			Form struct {
				Fields []struct {
					Key    string `json:"key"`
					Secret bool   `json:"secret"`
				} `json:"fields"`
			} `json:"admin_form"`
		} `json:"global_config_schema"`
	}
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Schemas) != 1 || manifest.Schemas[0].Key != "connection" {
		t.Fatal("native connection schema missing")
	}
	secrets := map[string]bool{}
	for _, field := range manifest.Schemas[0].Form.Fields {
		secrets[field.Key] = field.Secret
	}
	for _, key := range []string{"access_key", "secret_key", "session_token"} {
		if !secrets[key] {
			t.Errorf("credential field %s is not declared secret", key)
		}
	}
}

func TestNativeConnectionDefaultsToImmutableVersions(t *testing.T) {
	s := new(Service)
	ctx := context.Background()
	fields := nativeFields(t, fixtureConfig(), filepath.Join(t.TempDir(), "private", "index.db"))
	if _, err := s.Configure(ctx, &runtimepb.ConfigureRequest{Config: []*runtimepb.ConfigEntry{{Key: "connection", Value: fields}}}); err != nil {
		t.Fatalf("native connection refused: %v", err)
	}
	defer s.provider.Close()
	desc, err := s.Describe(ctx, &pb.DescribeRequest{})
	if err != nil || len(desc.GetSources()) != 1 || !desc.Sources[0].RevisionPinnedReads {
		t.Fatalf("native immutable source not advertised: %v %v", desc, err)
	}
}

func TestStrictSourceRejectsMutableVersions(t *testing.T) {
	for _, version := range []string{"", "null"} {
		t.Run(version, func(t *testing.T) {
			p, path := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", "\"object\"")
				w.Header().Set("Content-Length", "4")
				if version != "" {
					w.Header().Set("X-Amz-Version-Id", version)
				}
			})
			cfg := strictConfig(t, p.cfg)
			p.Close()
			p, err := Open(cfg, path)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			id, err := p.remember("books/movie.mkv", pb.EntryKind_ENTRY_KIND_FILE)
			if err != nil {
				t.Fatal(err)
			}
			_, err = p.Stat(context.Background(), &pb.StatRequest{SourceId: cfg.SourceID, EntryId: id})
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("mutable version %q admitted: %v", version, err)
			}
		})
	}
}

func TestStrictListingSuppliesRevisionsForFilesAndDirectories(t *testing.T) {
	f := newFakeS3()
	f.versioned = true
	f.put("books/movie.mkv", "data")
	f.put("books/series/episode.mp4", "data")
	p := f.provider(t, "versioned")
	page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
	if err != nil || len(page.GetEntries()) != 2 || !page.Complete {
		t.Fatalf("listing: %v %v", page, err)
	}
	for _, entry := range page.Entries {
		if entry.Revision == "" {
			t.Fatalf("listing emitted entry without revision: %v", entry)
		}
		stat, err := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision})
		if err != nil || stat.GetEntry().GetRevision() != entry.Revision {
			t.Fatalf("listed revision not stat-able: %v %v", stat, err)
		}
	}
}

func TestPinnedReadRejectsIgnoredConditionsBeforeEmittingBytes(t *testing.T) {
	for _, mode := range []string{"wrong-etag", "missing-etag", "wrong-version", "missing-version", "full-response"} {
		t.Run(mode, func(t *testing.T) {
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("If-Match") != `"v1-etag"` || r.URL.Query().Get("versionId") != "immutable-v1" {
					t.Error("requested pin was not sent to S3")
				}
				// Deliberately ignore the requested condition, like the observed gateway.
				w.Header().Set("ETag", `"v1-etag"`)
				w.Header().Set("X-Amz-Version-Id", "immutable-v1")
				w.Header().Set("Content-Range", "bytes 0-3/4")
				w.Header().Set("Content-Length", "4")
				switch mode {
				case "wrong-etag":
					w.Header().Set("ETag", `"v2-etag"`)
				case "missing-etag":
					w.Header().Del("ETag")
				case "wrong-version":
					w.Header().Set("X-Amz-Version-Id", "immutable-v2")
				case "missing-version":
					w.Header().Del("X-Amz-Version-Id")
				}
				if mode != "full-response" {
					w.WriteHeader(http.StatusPartialContent)
				}
				io.WriteString(w, "new!")
			})
			id, err := p.remember("books/movie.mkv", pb.EntryKind_ENTRY_KIND_FILE)
			if err != nil {
				t.Fatal(err)
			}
			chunks := 0
			err = p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: reviewRevision(t, id, "immutable-v1"), Length: 4}, func(*pb.ReadChunk) error { chunks++; return nil })
			if err == nil || chunks != 0 {
				t.Fatalf("invalid %s exposed bytes: chunks=%d err=%v", mode, chunks, err)
			}
		})
	}
}

func TestEndpointAndIdentityValidation(t *testing.T) {
	for _, endpoint := range []string{"https://example.test?", "https://example.test#", "https://example.test:0", "https://example.test:65536", "https://example.test/%2e%2e"} {
		cfg := fixtureConfig()
		cfg.Endpoint = endpoint
		if err := cfg.Validate(); err == nil {
			t.Errorf("ambiguous endpoint admitted: %s", endpoint)
		}
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Bucket = "evil:bucket" },
		func(c *Config) { c.SourceID = "source\nname" },
		func(c *Config) { c.Prefix = "books/../outside/" },
		func(c *Config) { c.SecretKey = "secret\nvalue" },
	} {
		cfg := fixtureConfig()
		mutate(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Error("invalid source settings admitted")
		}
	}
}

func TestTraversalListingKeysFailClosed(t *testing.T) {
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>books/../outside.mkv</Key><Size>4</Size></Contents></ListBucketResult>`)
	})
	page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
	if page != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("traversal key published: %v %v", page, err)
	}
}

func TestObjectValidatorsCannotExceedHostRevisionBound(t *testing.T) {
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"object"`)
		w.Header().Set("X-Amz-Version-Id", strings.Repeat("v", 4096))
		w.Header().Set("Content-Length", "4")
	})
	id, err := p.remember("books/movie.mkv", pb.EntryKind_ENTRY_KIND_FILE)
	if err != nil {
		t.Fatal(err)
	}
	stat, err := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
	if stat != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("oversized validator published: %v", err)
	}
}

func TestUnsupportedChangeTokenIsRefused(t *testing.T) {
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("unsupported change token reached backend")
	})
	_, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), ChangesSince: "foreign-token"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("change token admitted: %v", err)
	}
}

func TestReadRangeIsBoundedBeforeBackendIO(t *testing.T) {
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) { t.Error("oversized range reached backend") })
	id, err := p.remember("books/movie.mkv", pb.EntryKind_ENTRY_KIND_FILE)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(revision{ID: id, ETag: "\"object\"", Version: "immutable-v1", Size: 16 << 20})
	// The native host caps each range at 8 MiB; plugin callers must obey it too.
	err = p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: base64.RawURLEncoding.EncodeToString(raw), Length: 8<<20 + 1}, func(*pb.ReadChunk) error { t.Error("oversized range emitted data"); return nil })
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized range admitted: %v", err)
	}
}

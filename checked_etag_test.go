package s3provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	runtimepb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func checkedProvider(t *testing.T, handler http.HandlerFunc) *Provider {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	cfg := fixtureConfig()
	cfg.Endpoint = server.URL
	cfg.RevisionMode = "checked-etag"
	s := new(Service)
	_, err := s.Configure(context.Background(), &runtimepb.ConfigureRequest{Config: []*runtimepb.ConfigEntry{{Key: "connection", Value: nativeFields(t, cfg, filepath.Join(t.TempDir(), "private", "db"))}}})
	if err != nil {
		t.Fatalf("explicit checked connection refused: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	if s.provider == nil {
		t.Fatal("complete checked connection did not activate a source")
	}
	return s.provider
}

func TestNativeCheckedETagRequiresExplicitMode(t *testing.T) {
	p := checkedProvider(t, func(w http.ResponseWriter, r *http.Request) { t.Error("configuration performed backend I/O") })
	desc, err := p.Describe(context.Background(), &pb.DescribeRequest{})
	if err != nil || len(desc.GetSources()) != 1 || desc.Sources[0].RevisionPinnedReads {
		t.Fatalf("checked source claimed immutable pinning: %v %v", desc, err)
	}
	for _, mode := range []string{"", "versioned"} {
		t.Run("strict-"+mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"old"`)
				w.Header().Set("Content-Length", "4")
			}))
			defer server.Close()
			cfg := fixtureConfig()
			cfg.Endpoint, cfg.RevisionMode = server.URL, mode
			s := new(Service)
			defer s.Close()
			_, err := s.Configure(context.Background(), &runtimepb.ConfigureRequest{Config: []*runtimepb.ConfigEntry{{Key: "connection", Value: nativeFields(t, cfg, filepath.Join(t.TempDir(), "private", "db"))}}})
			if err != nil || s.provider == nil {
				t.Fatalf("native strict configure: %v", err)
			}
			id, err := s.provider.remember("books/movie.mkv", pb.EntryKind_ENTRY_KIND_FILE)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Stat(context.Background(), &pb.StatRequest{SourceId: cfg.SourceID, EntryId: id}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("native mode %q admitted unversioned object: %v", mode, err)
			}
		})
	}
}

func TestNativeUnknownRevisionModesFailClosed(t *testing.T) {
	for _, mode := range []string{"etag", "CHECKED-ETAG", " checked-etag", "future-mode"} {
		t.Run(mode, func(t *testing.T) {
			cfg := fixtureConfig()
			cfg.RevisionMode = mode
			if cfg.Validate() == nil {
				t.Fatal("unknown revision mode accepted by Config")
			}
			s := new(Service)
			defer s.Close()
			_, err := s.Configure(context.Background(), &runtimepb.ConfigureRequest{Config: []*runtimepb.ConfigEntry{{Key: "connection", Value: nativeFields(t, cfg, filepath.Join(t.TempDir(), "private", "db"))}}})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("unknown revision mode activated source: %v", err)
			}
		})
	}
}

func checkedEntry(t *testing.T, p *Provider) *pb.Entry {
	t.Helper()
	id, err := p.remember("books/movie.mkv", pb.EntryKind_ENTRY_KIND_FILE)
	if err != nil {
		t.Fatal(err)
	}
	stat, err := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
	if err != nil {
		t.Fatalf("checked Stat: %v", err)
	}
	return stat.Entry
}

func TestCheckedETagUnversionedReadValidatesBeforeAndAfterBody(t *testing.T) {
	for _, version := range []string{"", "null"} {
		t.Run("version-"+version, func(t *testing.T) {
			var heads, gets atomic.Int32
			p := checkedProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Has("versionId") {
					t.Error("unversioned read requested a VersionId")
				}
				w.Header().Set("ETag", `"old"`)
				w.Header().Set("Content-Length", "4")
				if version != "" {
					w.Header().Set("X-Amz-Version-Id", version)
				}
				if r.Method == http.MethodHead {
					if heads.Add(1) > 1 && r.Header.Get("If-Match") != `"old"` {
						t.Error("checked HEAD omitted expected ETag")
					}
					return
				}
				gets.Add(1)
				if heads.Load() != 2 || r.Header.Get("If-Match") != `"old"` || r.Header.Get("Range") != "bytes=1-2" {
					t.Error("GET did not follow a conditional revision HEAD with exact range")
				}
				w.Header().Set("Content-Length", "2")
				w.Header().Set("Content-Range", "bytes 1-2/4")
				w.WriteHeader(http.StatusPartialContent)
				io.WriteString(w, "ld")
			})
			entry := checkedEntry(t, p)
			chunks := 0
			err := p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision, Offset: 1, Length: 2}, func(chunk *pb.ReadChunk) error {
				chunks++
				if heads.Load() != 3 || chunk.Offset != 1 || string(chunk.Data) != "ld" || !chunk.Eof {
					t.Error("final EOF preceded the post-body HEAD or range bytes differ")
				}
				return nil
			})
			if err != nil || chunks != 1 || gets.Load() != 1 {
				t.Fatalf("valid checked range: chunks=%d GETs=%d err=%v", chunks, gets.Load(), err)
			}
		})
	}
}

func TestCheckedETagDriftNeverCompletesRead(t *testing.T) {
	const size = 128*1024 + 4
	for _, phase := range []string{"before", "after"} {
		for _, drift := range []string{"etag", "size", "version", "missing-etag", "missing-size"} {
			t.Run(phase+"-"+drift, func(t *testing.T) {
				var heads, gets atomic.Int32
				p := checkedProvider(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("ETag", `"old"`)
					w.Header().Set("Content-Length", fmt.Sprint(size))
					if r.Method == http.MethodHead {
						n := heads.Add(1)
						if (phase == "before" && n == 2) || (phase == "after" && n == 3) {
							// Ignore If-Match intentionally; returned validators still must fail.
							switch drift {
							case "etag":
								w.Header().Set("ETag", `"new"`)
							case "size":
								w.Header().Set("Content-Length", fmt.Sprint(size+1))
							case "version":
								w.Header().Set("X-Amz-Version-Id", "new-version")
							case "missing-etag":
								w.Header().Del("ETag")
							case "missing-size":
								w.Header().Del("Content-Length")
							}
						}
						return
					}
					gets.Add(1)
					w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", size-1, size))
					w.WriteHeader(http.StatusPartialContent)
					io.WriteString(w, strings.Repeat("a", size))
				})
				entry := checkedEntry(t, p)
				bytes, eofs := 0, 0
				err := p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision, Length: size}, func(chunk *pb.ReadChunk) error {
					bytes += len(chunk.Data)
					if chunk.Eof {
						eofs++
					}
					return nil
				})
				want := codes.FailedPrecondition
				if strings.HasPrefix(drift, "missing-") {
					want = codes.Unavailable
				}
				wantBytes, wantGets := 0, int32(0)
				if phase == "after" {
					wantBytes, wantGets = 128*1024, 1
				}
				if status.Code(err) != want || bytes != wantBytes || gets.Load() != wantGets || eofs != 0 {
					t.Fatalf("observed drift completed or exposed final chunk: bytes=%d GETs=%d EOFs=%d err=%v", bytes, gets.Load(), eofs, err)
				}
			})
		}
	}
}

func TestCheckedETagListingsPreferGenuineVersionsAndRequireValidators(t *testing.T) {
	for _, version := range []string{"", "null", "immutable-v1"} {
		t.Run("version-"+version, func(t *testing.T) {
			p := checkedProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.Header().Set("ETag", `"old"`)
					w.Header().Set("Content-Length", "4")
					if version != "" {
						w.Header().Set("X-Amz-Version-Id", version)
					}
					return
				}
				io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>books/movie.mkv</Key><ETag>"old"</ETag><Size>4</Size></Contents></ListBucketResult>`)
			})
			page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
			if err != nil || !page.GetComplete() || len(page.GetEntries()) != 1 {
				t.Fatalf("checked listing: %v %v", page, err)
			}
			// Unversioned pages take validators from the listing; a page whose
			// probed object has a genuine version keeps HEAD-pinned revisions.
			entry := page.Entries[0]
			var rev revision
			raw, err := base64.RawURLEncoding.DecodeString(entry.Revision)
			if err != nil || json.Unmarshal(raw, &rev) != nil || rev.ID != entry.Id || rev.ETag != `"old"` || rev.Size != 4 {
				t.Fatalf("checked listing omitted validators: %v %v", entry, err)
			}
			wantVersion := version
			if version == "null" {
				wantVersion = ""
			}
			if rev.Version != wantVersion || rev.Listed != (wantVersion == "") {
				t.Fatalf("checked listing version=%q listed=%v, want %q", rev.Version, rev.Listed, wantVersion)
			}
		})
	}
}

func TestCheckedETagRejectsIgnoredGETConditions(t *testing.T) {
	for _, invalid := range []string{"etag", "missing-etag", "version", "range", "length", "full-response", "short"} {
		t.Run(invalid, func(t *testing.T) {
			p := checkedProvider(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"old"`)
				w.Header().Set("Content-Length", "4")
				if r.Method == http.MethodHead {
					return
				}
				if r.Header.Get("If-Match") != `"old"` {
					t.Error("GET omitted expected ETag")
				}
				w.Header().Set("Content-Range", "bytes 0-3/4")
				data := "new!"
				switch invalid {
				case "etag":
					w.Header().Set("ETag", `"new"`)
				case "missing-etag":
					w.Header().Del("ETag")
				case "version":
					w.Header().Set("X-Amz-Version-Id", "unexpected-version")
				case "range":
					w.Header().Set("Content-Range", "bytes 0-3/5")
				case "length":
					w.Header().Set("Content-Length", "5")
					data = "new!!"
				case "short":
					data = "ne"
				}
				if invalid != "full-response" {
					w.WriteHeader(http.StatusPartialContent)
				}
				io.WriteString(w, data)
			})
			entry := checkedEntry(t, p)
			chunks := 0
			err := p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision, Length: 4}, func(*pb.ReadChunk) error { chunks++; return nil })
			if err == nil || chunks != 0 {
				t.Fatalf("ignored/malformed GET exposed bytes: chunks=%d err=%v", chunks, err)
			}
		})
	}
}

type checkedFaultBody struct {
	io.ReadCloser
	fault string
}

func (b checkedFaultBody) Read(data []byte) (int, error) {
	n, err := b.ReadCloser.Read(data)
	if err == io.EOF {
		if b.fault == "terminal" {
			return n, errors.New("synthetic terminal failure")
		}
		if b.fault == "extra" && n == 0 {
			data[0] = 'x'
			return 1, nil
		}
	}
	return n, err
}

type checkedFaultTransport struct {
	base  http.RoundTripper
	fault string
}

func (tr checkedFaultTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := tr.base.RoundTrip(r)
	if err == nil && r.Method == http.MethodGet && r.Header.Get("Range") != "" {
		response.Body = checkedFaultBody{ReadCloser: response.Body, fault: tr.fault}
	}
	return response, err
}

func TestCheckedETagExtraBodyAndTerminalErrorsNeverEmitEOF(t *testing.T) {
	for _, fault := range []string{"extra", "terminal"} {
		t.Run(fault, func(t *testing.T) {
			var heads atomic.Int32
			p := checkedProvider(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"old"`)
				w.Header().Set("Content-Length", "4")
				if r.Method == http.MethodHead {
					heads.Add(1)
					return
				}
				w.Header().Set("Content-Range", "bytes 0-3/4")
				w.WriteHeader(http.StatusPartialContent)
				io.WriteString(w, "old!")
			})
			// Keep real HTTP/SigV4/SDK decoding and inject a faulty response body
			// after transport framing. Production needs no test-only hooks.
			p.client = s3.New(s3.Options{Region: p.cfg.Region, BaseEndpoint: aws.String(p.cfg.Endpoint), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider(p.cfg.AccessKey, p.cfg.SecretKey, ""), HTTPClient: &http.Client{Transport: checkedFaultTransport{base: boundedTransport{base: p.transport}, fault: fault}}})
			entry := checkedEntry(t, p)
			chunks := 0
			err := p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision, Length: 4}, func(*pb.ReadChunk) error { chunks++; return nil })
			if status.Code(err) != codes.Unavailable || chunks != 0 || heads.Load() != 2 {
				t.Fatalf("invalid body proceeded to successful completion: HEADs=%d chunks=%d err=%v", heads.Load(), chunks, err)
			}
		})
	}
}

func TestCheckedETagNullRevisionCannotForgeVersionPin(t *testing.T) {
	p := checkedProvider(t, func(w http.ResponseWriter, r *http.Request) { t.Error("forged null version reached backend") })
	id, err := p.remember("books/movie.mkv", pb.EntryKind_ENTRY_KIND_FILE)
	if err != nil {
		t.Fatal(err)
	}
	for _, length := range []int64{0, 4} {
		err := p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: reviewRevision(t, id, "null"), Length: length}, func(*pb.ReadChunk) error { t.Error("forged null version emitted bytes"); return nil })
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("forged null version length=%d: %v", length, err)
		}
	}
}

func TestCheckedETagAuthorityFailureAfterBodyCannotCompleteOrExposeBackendDetails(t *testing.T) {
	for _, failure := range []struct {
		name string
		code string
		http int
		want codes.Code
	}{
		{"denied", "AccessDenied", http.StatusForbidden, codes.PermissionDenied},
		{"expired", "ExpiredToken", http.StatusForbidden, codes.PermissionDenied},
		{"outage", "ServiceUnavailable", http.StatusServiceUnavailable, codes.Unavailable},
	} {
		t.Run(failure.name, func(t *testing.T) {
			var heads atomic.Int32
			p := checkedProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead && heads.Add(1) >= 3 {
					w.Header().Set("X-Amz-Request-Id", "private-backend-detail")
					w.WriteHeader(failure.http)
					fmt.Fprintf(w, "<Error><Code>%s</Code><Message>private-backend-detail</Message></Error>", failure.code)
					return
				}
				w.Header().Set("ETag", `"old"`)
				w.Header().Set("Content-Length", "4")
				if r.Method == http.MethodGet {
					w.Header().Set("Content-Range", "bytes 0-3/4")
					w.WriteHeader(http.StatusPartialContent)
					io.WriteString(w, "old!")
				}
			})
			entry := checkedEntry(t, p)
			chunks := 0
			err := p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision, Length: 4}, func(*pb.ReadChunk) error { chunks++; return nil })
			if status.Code(err) != failure.want || chunks != 0 || strings.Contains(fmt.Sprint(err), "private-backend-detail") || strings.Contains(fmt.Sprint(err), p.cfg.Endpoint) {
				t.Fatalf("authority failure leaked/completed: chunks=%d err=%v", chunks, err)
			}
		})
	}
}

func TestCheckedETagCanceledPostBodyHEADCannotCompleteRead(t *testing.T) {
	var heads atomic.Int32
	checking := make(chan struct{})
	p := checkedProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && heads.Add(1) == 3 {
			close(checking)
			<-r.Context().Done()
			return
		}
		w.Header().Set("ETag", `"old"`)
		w.Header().Set("Content-Length", "4")
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Range", "bytes 0-3/4")
			w.WriteHeader(http.StatusPartialContent)
			io.WriteString(w, "old!")
		}
	})
	entry := checkedEntry(t, p)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		select {
		case <-checking:
			cancel()
		case <-ctx.Done():
		}
	}()
	err := p.read(ctx, &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision, Length: 4}, func(*pb.ReadChunk) error { t.Error("canceled check emitted final chunk"); return nil })
	if status.Code(err) != codes.Canceled {
		t.Fatalf("canceled post-body HEAD: %v", err)
	}
}

func TestCheckedETagZeroReadValidatesCurrentRevisionWithoutGET(t *testing.T) {
	for _, drift := range []string{"none", "etag", "size"} {
		t.Run(drift, func(t *testing.T) {
			var heads atomic.Int32
			p := checkedProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodHead {
					t.Error("zero read issued GET")
				}
				w.Header().Set("ETag", `"old"`)
				w.Header().Set("Content-Length", "4")
				if heads.Add(1) == 2 {
					if r.Header.Get("If-Match") != `"old"` {
						t.Error("zero read omitted conditional ETag")
					}
					if drift == "etag" {
						w.Header().Set("ETag", `"new"`)
					}
					if drift == "size" {
						w.Header().Set("Content-Length", "5")
					}
				}
			})
			entry := checkedEntry(t, p)
			chunks := 0
			err := p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: entry.Id, ExpectedRevision: entry.Revision, Offset: 4}, func(chunk *pb.ReadChunk) error {
				chunks++
				if !chunk.Eof || chunk.Offset != 4 || len(chunk.Data) != 0 {
					t.Error("incorrect empty EOF")
				}
				return nil
			})
			want, wantChunks := codes.FailedPrecondition, 0
			if drift == "none" {
				want, wantChunks = codes.OK, 1
			}
			if status.Code(err) != want || chunks != wantChunks || heads.Load() != 2 {
				t.Fatalf("zero read: HEADs=%d chunks=%d err=%v", heads.Load(), chunks, err)
			}
		})
	}
}

func TestCheckedETagListingsFailWithoutObjectValidators(t *testing.T) {
	for _, missing := range []string{"ETag", "Content-Length", "listed-ETag", "versioned-ETag", "versioned-Content-Length"} {
		t.Run(missing, func(t *testing.T) {
			var heads atomic.Int32
			p := checkedProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.Header().Set("ETag", `"old"`)
					w.Header().Set("Content-Length", "4")
					header := missing
					if versioned := strings.TrimPrefix(missing, "versioned-"); versioned != missing {
						// The page probe sees a genuine version and its response is
						// reused; hydrating the page's next file then fails.
						w.Header().Set("X-Amz-Version-Id", "immutable-v1")
						header = versioned
						if heads.Add(1) == 1 {
							return
						}
					}
					w.Header().Del(header)
					return
				}
				etag := `<ETag>"old"</ETag>`
				if missing == "listed-ETag" {
					etag = ""
				}
				probed := ""
				if strings.HasPrefix(missing, "versioned-") {
					probed = `<Contents><Key>books/a.mkv</Key>` + etag + `<Size>4</Size></Contents>`
				}
				io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`+probed+`<Contents><Key>books/movie.mkv</Key>`+etag+`<Size>4</Size></Contents></ListBucketResult>`)
			})
			page, err := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
			if page != nil || status.Code(err) != codes.Unavailable {
				t.Fatalf("listing without validators claimed authority: %v %v", page, err)
			}
			if strings.HasPrefix(missing, "versioned-") && heads.Load() != 2 {
				t.Fatalf("HEADs=%d, want the probe and one hydration", heads.Load())
			}
		})
	}
}

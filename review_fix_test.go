package s3provider

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func reviewRevision(t *testing.T, id, version string) string {
	t.Helper()
	raw, e := json.Marshal(revision{ID: id, ETag: "\"v1-etag\"", Version: version, Size: 4})
	if e != nil {
		t.Fatal(e)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func reviewS3Error(w http.ResponseWriter, code string, httpStatus int) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(httpStatus)
	if code != "" {
		fmt.Fprintf(w, "<Error><Code>%s</Code><Message>private-backend-message secret-credential</Message><BucketName>private-bucket</BucketName></Error>", code)
	}
}

func TestSourceFailureClassification(t *testing.T) {
	e := backendError(&smithy.GenericAPIError{Code: "NoSuchBucket", Message: "private-backend-message secret-credential"})
	if status.Code(e) != codes.Unavailable {
		t.Fatalf("explicit NoSuchBucket = %v, want Unavailable", e)
	}
	for _, code := range []string{"NoSuchKey", "NoSuchVersion"} {
		if e := backendError(&smithy.GenericAPIError{Code: code}); status.Code(e) != codes.NotFound {
			t.Errorf("explicit object absence %s = %v", code, e)
		}
	}
}

func TestSource404ListStatRead(t *testing.T) {
	for _, backendCode := range []string{"NoSuchBucket", "NotFound", ""} {
		for _, operation := range []string{"list", "root-stat", "directory-stat", "file-stat", "read-zero", "read-positive", "read-version-zero", "read-version-positive"} {
			t.Run(backendCode+"/"+operation, func(t *testing.T) {
				p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
					reviewS3Error(w, backendCode, http.StatusNotFound)
				})
				ctx := context.Background()
				id, e := p.remember("books/file", pb.EntryKind_ENTRY_KIND_FILE)
				if e != nil {
					t.Fatal(e)
				}
				switch operation {
				case "list":
					_, e = p.List(ctx, &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()})
				case "root-stat":
					_, e = p.Stat(ctx, &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: p.rootID()})
				case "directory-stat":
					dir, err := p.remember("books/dir/", pb.EntryKind_ENTRY_KIND_DIRECTORY)
					if err != nil {
						t.Fatal(err)
					}
					_, e = p.Stat(ctx, &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: dir})
				case "file-stat":
					_, e = p.Stat(ctx, &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
				default:
					length := int64(0)
					if strings.HasSuffix(operation, "positive") {
						length = 1
					}
					version := ""
					if strings.Contains(operation, "version") {
						version = "immutable-v1"
					}
					e = p.read(ctx, &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: reviewRevision(t, id, version), Length: length}, func(*pb.ReadChunk) error {
						t.Error("source failure delivered a chunk/EOF")
						return nil
					})
				}
				if status.Code(e) != codes.Unavailable {
					t.Fatalf("source 404 = %v, want Unavailable", e)
				}
				for _, private := range []string{"private-backend-message", "secret-credential", "private-bucket", p.cfg.Endpoint, p.cfg.AccessKey, p.cfg.SecretKey} {
					if strings.Contains(status.Convert(e).Message(), private) {
						t.Error("client status leaked backend details")
					}
				}
			})
		}
	}
}

func TestList404NeverObjectAbsence(t *testing.T) {
	for _, code := range []string{"NoSuchKey", "NoSuchVersion"} {
		t.Run(code, func(t *testing.T) {
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				reviewS3Error(w, code, http.StatusNotFound)
			})
			if _, e := p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID()}); status.Code(e) != codes.Unavailable {
				t.Fatalf("listing object error classified as absence: %v", e)
			}
		})
	}
}

func TestAmbiguousObject404RequiresCurrentBucketAccess(t *testing.T) {
	for _, bucketStatus := range []int{200, 403, 404, 503} {
		for _, operation := range []string{"file-stat", "read-zero", "read-positive", "read-version-zero", "read-version-positive"} {
			t.Run(fmt.Sprintf("%d/%s", bucketStatus, operation), func(t *testing.T) {
				var probes atomic.Int32
				p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodHead && !strings.Contains(strings.TrimPrefix(r.URL.Path, "/"), "/") {
						probes.Add(1)
						w.WriteHeader(bucketStatus)
						return
					}
					reviewS3Error(w, "", http.StatusNotFound)
				})
				ctx := context.Background()
				id, e := p.remember("books/file", pb.EntryKind_ENTRY_KIND_FILE)
				if e != nil {
					t.Fatal(e)
				}
				// Each request must establish access afresh, never reuse an earlier proof.
				for n := int32(1); n <= 2; n++ {
					if operation == "file-stat" {
						_, e = p.Stat(ctx, &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
					} else {
						length := int64(0)
						if strings.HasSuffix(operation, "positive") {
							length = 1
						}
						version := ""
						if strings.Contains(operation, "version") {
							version = "immutable-v1"
						}
						e = p.read(ctx, &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: reviewRevision(t, id, version), Length: length}, func(*pb.ReadChunk) error {
							t.Error("absent object delivered a chunk/EOF")
							return nil
						})
					}
					want := codes.Unavailable
					if bucketStatus == 200 {
						want = codes.NotFound
					} else if bucketStatus == 403 {
						want = codes.PermissionDenied
					}
					if status.Code(e) != want {
						t.Errorf("ambiguous object 404 with bucket status %d = %v, want %v", bucketStatus, e, want)
					}
					// Retries may add probes for 503, but every operation must probe.
					if probes.Load() < n {
						t.Errorf("object absence accepted without current authorized bucket probe: probes=%d", probes.Load())
					}
				}
			})
		}
	}
}

func TestExplicitObjectAbsenceWithoutBucketProbe(t *testing.T) {
	for _, code := range []string{"NoSuchKey", "NoSuchVersion"} {
		t.Run(code, func(t *testing.T) {
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					t.Error("explicit GET object absence unexpectedly probed bucket")
					w.WriteHeader(http.StatusForbidden)
					return
				}
				reviewS3Error(w, code, http.StatusNotFound)
			})
			id, e := p.remember("books/file", pb.EntryKind_ENTRY_KIND_FILE)
			if e != nil {
				t.Fatal(e)
			}
			e = p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: reviewRevision(t, id, ""), Length: 1}, func(*pb.ReadChunk) error {
				t.Error("absent object delivered a chunk")
				return nil
			})
			if status.Code(e) != codes.NotFound {
				t.Fatalf("explicit object absence = %v", e)
			}
		})
	}
}

func TestBackendMissingBucketIsSourceFailure(t *testing.T) {
	if os.Getenv("S3_BACKEND") != "garage" || os.Getenv("S3_FIXTURE") != "1" {
		t.Skip("requires disposable pinned Garage fixture")
	}
	ctx := context.Background()
	c := fixtureConfig()
	c.AccessKey, c.SecretKey = fixtureWriterKeys()
	c.Bucket = fmt.Sprintf("bloem-s3-review-missing-%d", time.Now().UnixNano())
	p, e := Open(c, filepath.Join(t.TempDir(), "private", "db"))
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	id, e := p.remember(c.Prefix+"file", pb.EntryKind_ENTRY_KIND_FILE)
	if e != nil {
		t.Fatal(e)
	}
	// No create/delete operation: verify raw 404 on a unique synthetic nonexistent bucket.
	_, rawErr := p.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(c.Bucket)})
	var response *smithyhttp.ResponseError
	if !errors.As(rawErr, &response) || response.HTTPStatusCode() != 404 {
		t.Fatal("raw missing bucket List did not return HTTP 404")
	}
	var api smithy.APIError
	if !errors.As(rawErr, &api) || api.ErrorCode() != "NoSuchBucket" {
		t.Fatal("raw missing bucket List did not return NoSuchBucket")
	}
	t.Log("raw Garage missing bucket List: HTTP 404 / NoSuchBucket")
	for _, operation := range []string{"list", "root-stat", "directory-stat", "file-stat", "read-zero", "read-positive", "read-version-zero", "read-version-positive"} {
		t.Run(operation, func(t *testing.T) {
			var err error
			switch operation {
			case "list":
				_, err = p.List(ctx, &pb.ListRequest{SourceId: c.SourceID, DirectoryId: p.rootID()})
			case "root-stat":
				_, err = p.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: p.rootID()})
			case "directory-stat":
				dir, e := p.remember(c.Prefix+"dir/", pb.EntryKind_ENTRY_KIND_DIRECTORY)
				if e != nil {
					t.Fatal(e)
				}
				_, err = p.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: dir})
			case "file-stat":
				_, err = p.Stat(ctx, &pb.StatRequest{SourceId: c.SourceID, EntryId: id})
			default:
				length := int64(0)
				if strings.HasSuffix(operation, "positive") {
					length = 1
				}
				version := ""
				if strings.Contains(operation, "version") {
					version = "immutable-v1"
				}
				err = p.read(ctx, &pb.ReadRequest{SourceId: c.SourceID, EntryId: id, ExpectedRevision: reviewRevision(t, id, version), Length: length}, func(*pb.ReadChunk) error {
					t.Error("missing bucket delivered a chunk/EOF")
					return nil
				})
			}
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("missing bucket = %v, want Unavailable and never NotFound", err)
			}
		})
	}
}

func TestImmutableVersionReadAfterCurrentChanges(t *testing.T) {
	for _, current := range []string{"v2", "delete-marker"} {
		t.Run(current, func(t *testing.T) {
			var changed atomic.Bool
			var versionHeads, versionGets atomic.Int32
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead && !strings.Contains(strings.TrimPrefix(r.URL.Path, "/"), "/") {
					w.WriteHeader(http.StatusOK)
					return
				}
				version := r.URL.Query().Get("versionId")
				if version != "" {
					if version != "immutable-v1" || r.Header.Get("If-Match") != "\"v1-etag\"" {
						t.Error("immutable request missing requested VersionId/conditional ETag")
						w.WriteHeader(http.StatusPreconditionFailed)
						return
					}
					if r.Method == http.MethodHead {
						versionHeads.Add(1)
					} else {
						versionGets.Add(1)
					}
				} else if changed.Load() {
					if current == "delete-marker" {
						w.Header().Set("x-amz-delete-marker", "true")
						reviewS3Error(w, "", http.StatusNotFound)
						return
					}
					w.Header().Set("ETag", "\"v2-etag\"")
					w.Header().Set("x-amz-version-id", "immutable-v2")
					w.Header().Set("Content-Length", "5")
					return
				}
				w.Header().Set("ETag", "\"v1-etag\"")
				w.Header().Set("x-amz-version-id", "immutable-v1")
				w.Header().Set("Content-Length", "4")
				if r.Method != http.MethodHead {
					if r.Header.Get("Range") != "bytes=0-3" {
						t.Error("immutable GET range missing")
					}
					w.Header().Set("Content-Range", "bytes 0-3/4")
					w.WriteHeader(http.StatusPartialContent)
					io.WriteString(w, "old!")
				}
			})
			ctx := context.Background()
			id, e := p.remember("books/versioned", pb.EntryKind_ENTRY_KIND_FILE)
			if e != nil {
				t.Fatal(e)
			}
			st, e := p.Stat(ctx, &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
			if e != nil {
				t.Fatal(e)
			}
			changed.Store(true)
			// Current Stat must diverge even though explicit immutable v1 is accessible.
			_, e = p.Stat(ctx, &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision})
			want := codes.FailedPrecondition
			if current == "delete-marker" {
				want = codes.NotFound
			}
			if status.Code(e) != want {
				t.Fatalf("current Stat = %v, want %v", e, want)
			}
			for _, length := range []int64{0, 4} {
				t.Run(fmt.Sprint(length), func(t *testing.T) {
					data, chunks, eofs := "", 0, 0
					e := p.read(ctx, &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision, Length: length}, func(c *pb.ReadChunk) error {
						chunks++
						if c.Offset != 0 {
							t.Error("unexpected chunk offset")
						}
						data += string(c.Data)
						if c.Eof {
							eofs++
						}
						return nil
					})
					wantData := ""
					if length > 0 {
						wantData = "old!"
					}
					if e != nil || data != wantData || chunks != 1 || eofs != 1 {
						t.Fatalf("immutable read length %d = data %q chunks %d EOFs %d err %v", length, data, chunks, eofs, e)
					}
				})
			}
			if versionHeads.Load() != 1 || versionGets.Load() != 1 {
				t.Errorf("expected conditional v1 HEAD and GET, got HEAD %d GET %d", versionHeads.Load(), versionGets.Load())
			}
		})
	}
}

func TestImmutableEmptyObjectReadAfterOverwrite(t *testing.T) {
	var changed atomic.Bool
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Error("zero-length immutable object requires HEAD")
		}
		if changed.Load() && r.URL.Query().Get("versionId") != "immutable-empty-v1" {
			w.Header().Set("ETag", "\"new\"")
			w.Header().Set("x-amz-version-id", "immutable-v2")
			w.Header().Set("Content-Length", "4")
			return
		}
		if changed.Load() && r.Header.Get("If-Match") != "\"empty-v1\"" {
			t.Error("empty version HEAD omitted conditional ETag")
		}
		w.Header().Set("ETag", "\"empty-v1\"")
		w.Header().Set("x-amz-version-id", "immutable-empty-v1")
		w.Header().Set("Content-Length", "0")
	})
	id, e := p.remember("books/empty", pb.EntryKind_ENTRY_KIND_FILE)
	if e != nil {
		t.Fatal(e)
	}
	st, e := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
	if e != nil {
		t.Fatal(e)
	}
	changed.Store(true)
	eofs := 0
	e = p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision}, func(c *pb.ReadChunk) error {
		if !c.Eof || len(c.Data) != 0 || c.Offset != 0 {
			t.Error("invalid empty read EOF")
		}
		eofs++
		return nil
	})
	if e != nil || eofs != 1 {
		t.Fatalf("immutable empty object = %v EOFs=%d", e, eofs)
	}
}

func TestImmutableVersionReadValidatorsFailClosed(t *testing.T) {
	for _, mode := range []string{"omit-etag", "omit-version", "omit-length", "wrong-etag", "wrong-version", "wrong-size"} {
		for _, length := range []int64{0, 4} {
			t.Run(fmt.Sprintf("%s/%d", mode, length), func(t *testing.T) {
				p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Query().Get("versionId") != "immutable-v1" || r.Header.Get("If-Match") != "\"v1-etag\"" {
						t.Error("version validator request was not conditional/pinned")
					}
					w.Header().Set("ETag", "\"v1-etag\"")
					w.Header().Set("x-amz-version-id", "immutable-v1")
					w.Header().Set("Content-Length", "4")
					if r.Method != http.MethodHead {
						w.Header().Set("Content-Range", "bytes 0-3/4")
					}
					switch mode {
					case "omit-etag":
						w.Header().Del("ETag")
					case "omit-version":
						w.Header().Del("x-amz-version-id")
					case "omit-length":
						w.Header().Del("Content-Length")
						// Prevent HTTP server automatic length inference on GET.
						w.Header().Set("Transfer-Encoding", "chunked")
					case "wrong-etag":
						w.Header().Set("ETag", "\"wrong\"")
					case "wrong-version":
						w.Header().Set("x-amz-version-id", "immutable-v2")
					case "wrong-size":
						if r.Method == http.MethodHead {
							w.Header().Set("Content-Length", "5")
						} else {
							w.Header().Set("Content-Range", "bytes 0-3/5")
						}
					}
					if r.Method != http.MethodHead {
						io.WriteString(w, "old!")
					}
				})
				id, e := p.remember("books/versioned", pb.EntryKind_ENTRY_KIND_FILE)
				if e != nil {
					t.Fatal(e)
				}
				e = p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: reviewRevision(t, id, "immutable-v1"), Length: length}, func(*pb.ReadChunk) error {
					t.Error("invalid version validators delivered chunk/EOF")
					return nil
				})
				if e == nil {
					t.Fatal("missing/mismatched immutable validators accepted")
				}
			})
		}
	}
}

func TestMutableNullVersionRevisionRejected(t *testing.T) {
	for _, length := range []int64{0, 4} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			var calls atomic.Int32
			p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("ETag", "\"v1-etag\"")
				w.Header().Set("x-amz-version-id", "null")
				w.Header().Set("Content-Length", "4")
				if r.Method != http.MethodHead {
					w.Header().Set("Content-Range", "bytes 0-3/4")
					io.WriteString(w, "old!")
				}
			})
			id, e := p.remember("books/mutable", pb.EntryKind_ENTRY_KIND_FILE)
			if e != nil {
				t.Fatal(e)
			}
			e = p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: reviewRevision(t, id, "null"), Length: length}, func(*pb.ReadChunk) error {
				t.Error("forged mutable null version delivered chunk")
				return nil
			})
			if status.Code(e) != codes.InvalidArgument || calls.Load() != 0 {
				t.Fatalf("mutable null revision = %v, backend calls=%d; want InvalidArgument before backend", e, calls.Load())
			}
		})
	}
}

func TestBackendNullVersionUsesCurrentRevisionSemantics(t *testing.T) {
	var changed atomic.Bool
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if _, present := r.URL.Query()["versionId"]; present {
			t.Error("backend null version treated as immutable")
		}
		etag := "\"v1-etag\""
		if changed.Load() {
			etag = "\"v2-etag\""
		}
		if r.Method != http.MethodHead && r.Header.Get("If-Match") != etag {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("x-amz-version-id", "null")
		w.Header().Set("Content-Length", "4")
		if r.Method != http.MethodHead {
			w.Header().Set("Content-Range", "bytes 0-3/4")
			w.WriteHeader(http.StatusPartialContent)
			io.WriteString(w, "old!")
		}
	})
	id, e := p.remember("books/mutable", pb.EntryKind_ENTRY_KIND_FILE)
	if e != nil {
		t.Fatal(e)
	}
	st, e := p.Stat(context.Background(), &pb.StatRequest{SourceId: p.cfg.SourceID, EntryId: id})
	if e != nil {
		t.Fatal(e)
	}
	for _, length := range []int64{0, 4} {
		if e = p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision, Length: length}, func(*pb.ReadChunk) error { return nil }); e != nil {
			t.Fatal("current null revision read", e)
		}
	}
	changed.Store(true)
	for _, length := range []int64{0, 4} {
		e = p.read(context.Background(), &pb.ReadRequest{SourceId: p.cfg.SourceID, EntryId: id, ExpectedRevision: st.Entry.Revision, Length: length}, func(*pb.ReadChunk) error {
			t.Error("stale mutable revision delivered chunk")
			return nil
		})
		if status.Code(e) != codes.FailedPrecondition {
			t.Fatalf("stale null version length=%d = %v", length, e)
		}
	}
}

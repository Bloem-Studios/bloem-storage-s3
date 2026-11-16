package s3provider

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Hard limits fail closed rather than evicting identities needed by the catalog.
const MaxIdentities = 4000000
const MaxCursors = 4096
const MaxCursorHistory = 4096
const CursorLifetime = 24 * time.Hour
const pageBudget = 1024*1024 - 256
const directoryRevision = "s3-layout-v1"

// Each List RPC reads delimited ListObjectsV2 pages of one directory's
// immediate children. Cursors record the layout; cursors retained from the
// earlier recursive or root-relative projection restart enumeration.
const cursorLayout = "delimited-root-v2"
const listDelimiter = "/"
const backendPageKeys = 512
const maxBackendFetchesPerList = 8

// An all-buckets root reads ListBuckets pages of at most this many buckets,
// the default bucket quota of an AWS account.
const maxBucketsPerPage = 10000

// Strict revisions need a VersionId, which ListObjectsV2 omits. Outgoing files
// are HEADed with bounded concurrency and a per-RPC budget that stays well
// inside the host's 30-second List deadline.
const maxRevisionHeadsPerList = 64
const revisionHeadConcurrency = 8

type Provider struct {
	pb.UnimplementedStorageProviderServer
	cfg       Config
	binding   string
	namespace string
	client    *s3.Client
	db        *bolt.DB
	transport *http.Transport
}
type identity struct {
	Key  string
	Kind pb.EntryKind
}
type cursor struct {
	Layout          string `json:",omitempty"`
	Directory       string
	PageSize        int
	DirectoriesOnly bool
	// LastKey is the greatest key or CommonPrefix consumed so far.
	LastKey string
	Token   string
	Pending []*pb.Entry
	Seen    []string
	Done    bool
	Expires int64
}
type revision struct {
	ID      string
	ETag    string
	Version string
	Size    int64
	// Listed revisions carry ListObjectsV2 metadata in checked ETag mode. Stat
	// echoes them, including Modified, while the current ETag and size match.
	Listed   bool  `json:",omitempty"`
	Modified int64 `json:",omitempty"`
}

// child is one validated immediate child from a delimited listing page.
type child struct {
	key      string
	kind     pb.EntryKind
	size     int64
	modified int64
	etag     string
}

func Open(c Config, path string) (*Provider, error) {
	if e := c.Validate(); e != nil {
		return nil, e
	}
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	for _, v := range []string{dir, path} {
		st, e := os.Lstat(v)
		if os.IsNotExist(e) && v == path {
			continue
		}
		if e != nil {
			return nil, e
		}
		if st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm()&0077 != 0 {
			return nil, errors.New("state must be private and not symlinked")
		}
	}
	db, e := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if e != nil {
		return nil, e
	}
	raw, _ := json.Marshal(c)
	sum := sha256.Sum256(raw)
	// Credentials bind cursors, never durable entry identity. Endpoint is part
	// of the namespace so the same source ID cannot alias a different backend.
	namespaceRaw, _ := json.Marshal(struct{ Source, Endpoint, Bucket, Prefix string }{c.SourceID, c.Endpoint, c.Bucket, c.Prefix})
	namespaceSum := sha256.Sum256(namespaceRaw)
	p := &Provider{cfg: c, binding: hex.EncodeToString(sum[:]), namespace: hex.EncodeToString(namespaceSum[:]), db: db}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	p.transport = transport
	pinnedOrigin := ""
	if c.Endpoint != "" {
		u, _ := url.Parse(c.Endpoint)
		pinnedOrigin = u.Scheme + "://" + u.Host
	}
	p.client = s3.New(s3.Options{Region: c.Region, Credentials: credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, c.SessionToken), UsePathStyle: c.Endpoint != "", BaseEndpoint: nil, RetryMaxAttempts: 2, HTTPClient: &http.Client{Transport: boundedTransport{base: transport, origin: pinnedOrigin}, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}}, func(o *s3.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	})
	e = db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"meta", "ids", "cursors"} {
			if _, e := tx.CreateBucketIfNotExists([]byte(name)); e != nil {
				return e
			}
		}
		b := tx.Bucket([]byte("meta"))
		oldNamespace := b.Get([]byte("namespace_v2"))
		if oldNamespace == nil && b.Get([]byte("binding")) != nil {
			return errors.New("legacy state requires explicit identity migration")
		}
		if oldNamespace != nil && string(oldNamespace) != p.namespace {
			return errors.New("state belongs to another source namespace")
		}
		if old := b.Get([]byte("binding")); old != nil && string(old) != p.binding {
			if e := tx.DeleteBucket([]byte("cursors")); e != nil {
				return e
			}
			if _, e := tx.CreateBucket([]byte("cursors")); e != nil {
				return e
			}
		}
		if e := b.Put([]byte("namespace_v2"), []byte(p.namespace)); e != nil {
			return e
		}
		return b.Put([]byte("binding"), []byte(p.binding))
	})
	if e == nil {
		_, e = p.remember(c.Prefix, pb.EntryKind_ENTRY_KIND_DIRECTORY)
	}
	if e != nil {
		db.Close()
		return nil, e
	}
	return p, nil
}
func (p *Provider) Close() error {
	if p.transport != nil {
		p.transport.CloseIdleConnections()
	}
	return p.db.Close()
}
func (p *Provider) id(key string, kind pb.EntryKind) string {
	h := sha256.Sum256([]byte(p.namespace + "\x00" + fmt.Sprint(kind) + "\x00" + key))
	return hex.EncodeToString(h[:])
}

// allBuckets reports a source without a bucket: its root lists every bucket
// the keys can see and its identity keys are bucket/key.
func (p *Provider) allBuckets() bool { return p.cfg.Bucket == "" }

// location splits an identity key into the bucket and object key it names.
// The all-buckets root has no bucket.
func (p *Provider) location(key string) (bucket, object string) {
	if !p.allBuckets() {
		return p.cfg.Bucket, key
	}
	bucket, object, _ = strings.Cut(key, "/")
	return bucket, object
}

// validKey reports whether an identity key belongs to this source: inside the
// prefix, or in all-buckets mode the root or a key inside a named bucket.
func (p *Provider) validKey(key string) bool {
	if !p.allBuckets() {
		return safeKey(key) && strings.HasPrefix(key, p.cfg.Prefix)
	}
	bucket, object, found := strings.Cut(key, "/")
	return key == "" || (found && bucketPattern.MatchString(bucket) && safeKey(object))
}
func (p *Provider) rootID() string { return p.id(p.cfg.Prefix, pb.EntryKind_ENTRY_KIND_DIRECTORY) }
func (p *Provider) remember(key string, kind pb.EntryKind) (string, error) {
	ids, e := p.rememberAll([]identity{{key, kind}})
	if e != nil {
		return "", e
	}
	return ids[0], nil
}

// rememberAll registers a page of identities in one transaction. Known
// identities need no write, so rescans neither rewrite nor sync the DB.
func (p *Provider) rememberAll(items []identity) ([]string, error) {
	ids := make([]string, len(items))
	for i, item := range items {
		if !p.validKey(item.Key) {
			return nil, status.Error(codes.Unavailable, "malformed backend key")
		}
		ids[i] = p.id(item.Key, item.Kind)
	}
	known := true
	e := p.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("ids"))
		for _, id := range ids {
			if b.Get([]byte(id)) == nil {
				known = false
				return nil
			}
		}
		return nil
	})
	if e != nil || known {
		return ids, e
	}
	e = p.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("ids"))
		meta := tx.Bucket([]byte("meta"))
		count := uint64(0)
		if counter := meta.Get([]byte("identity_count")); len(counter) == 8 {
			count = binary.BigEndian.Uint64(counter)
		} else {
			count = uint64(b.Stats().KeyN)
		}
		added := false
		for i, item := range items {
			if b.Get([]byte(ids[i])) != nil {
				continue
			}
			if count >= MaxIdentities {
				return status.Error(codes.ResourceExhausted, "identity retention limit reached")
			}
			raw, _ := json.Marshal(item)
			if e := b.Put([]byte(ids[i]), raw); e != nil {
				return e
			}
			count++
			added = true
		}
		if !added {
			return nil
		}
		next := make([]byte, 8)
		binary.BigEndian.PutUint64(next, count)
		return meta.Put([]byte("identity_count"), next)
	})
	if e != nil {
		return nil, e
	}
	return ids, nil
}
func (p *Provider) lookup(id string) (identity, error) {
	var v identity
	if len(id) != 64 {
		return v, status.Error(codes.InvalidArgument, "invalid entry identity")
	}
	e := p.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte("ids")).Get([]byte(id))
		if raw == nil {
			return status.Error(codes.InvalidArgument, "unknown entry identity")
		}
		return json.Unmarshal(raw, &v)
	})
	return v, e
}
func (p *Provider) source(id string) error {
	if id != p.cfg.SourceID {
		return status.Error(codes.InvalidArgument, "unknown source")
	}
	return nil
}
func (p *Provider) Describe(context.Context, *pb.DescribeRequest) (*pb.DescribeResponse, error) {
	name := p.cfg.SourceName
	if name == "" {
		name = p.cfg.SourceID
	}
	// Strict mode refuses any object without an immutable VersionId. This
	// advertises an enforced protocol policy, not certification of an endpoint.
	// These are raw file feeds. Audiobook probing and playback readiness remain
	// host responsibilities; the declaration alone does not grant admission.
	return &pb.DescribeResponse{Revision: 1, Sources: []*pb.Source{{Id: p.cfg.SourceID, Name: name, RootEntryId: p.rootID(), RevisionPinnedReads: p.cfg.RevisionMode == "versioned", MediaKinds: []string{"ebooks", "movies", "audiobooks"}, DirectoryOnlyListing: true}}}, nil
}
func backendError(e error) error {
	if e == nil {
		return nil
	}
	if errors.Is(e, context.Canceled) {
		return status.Error(codes.Canceled, "canceled")
	}
	if errors.Is(e, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "deadline")
	}
	var api smithy.APIError
	if errors.As(e, &api) {
		switch api.ErrorCode() {
		case "NoSuchBucket":
			return status.Error(codes.Unavailable, "S3 source unavailable")
		case "NoSuchKey", "NoSuchVersion":
			return status.Error(codes.NotFound, "object absent")
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
			return status.Error(codes.PermissionDenied, "backend denied")
		case "PreconditionFailed":
			return status.Error(codes.FailedPrecondition, "revision changed")
		}
	}
	var response *smithyhttp.ResponseError
	if errors.As(e, &response) {
		switch response.HTTPStatusCode() {
		case 403:
			return status.Error(codes.PermissionDenied, "backend denied")
		case 404:
			return status.Error(codes.Unavailable, "S3 source unavailable")
		case 412:
			return status.Error(codes.FailedPrecondition, "revision changed")
		}
	}
	return status.Error(codes.Unavailable, "S3 backend failure")
}

// Source operations never establish absence of a catalog entry.
func sourceBackendError(e error) error {
	mapped := backendError(e)
	if status.Code(mapped) == codes.NotFound {
		return status.Error(codes.Unavailable, "S3 source unavailable")
	}
	return mapped
}

// HEAD commonly reports only NotFound/404 for both bucket and key absence.
// Establish current authorized access to the object's bucket before treating
// that as object absence. Never cache this proof across requests or credential
// changes.
func (p *Provider) objectBackendError(ctx context.Context, bucket string, e error) error {
	var api smithy.APIError
	ambiguous := false
	if errors.As(e, &api) {
		if api.ErrorCode() != "NotFound" && api.ErrorCode() != "" {
			return backendError(e)
		}
		ambiguous = api.ErrorCode() == "NotFound"
	}
	var response *smithyhttp.ResponseError
	if errors.As(e, &response) && response.HTTPStatusCode() == http.StatusNotFound {
		ambiguous = true
	}
	if !ambiguous {
		return backendError(e)
	}
	if _, checkErr := p.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); checkErr != nil {
		return sourceBackendError(checkErr)
	}
	return status.Error(codes.NotFound, "object absent")
}

func (p *Provider) entry(key string, kind pb.EntryKind, size int64, modified int64) (*pb.Entry, error) {
	id, e := p.remember(key, kind)
	if e != nil {
		return nil, e
	}
	return p.entryWithID(id, key, kind, size, modified), nil
}
func (p *Provider) entryWithID(id, key string, kind pb.EntryKind, size int64, modified int64) *pb.Entry {
	logical := strings.TrimPrefix(key, p.cfg.Prefix)
	// Keep the selected root folder as display metadata. The opaque entry ID
	// and revision still bind the full backend key; this path is never authority.
	rootName := strings.TrimSuffix(p.cfg.Prefix, "/")
	if i := strings.LastIndex(rootName, "/"); i >= 0 {
		rootName = rootName[i+1:]
	}
	if rootName != "" {
		logical = rootName + "/" + logical
	}
	name := strings.TrimSuffix(logical, "/")
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	if key == p.cfg.Prefix && kind == pb.EntryKind_ENTRY_KIND_DIRECTORY {
		name = rootName
		if name == "" {
			name = p.cfg.SourceID
		}
	}
	entry := &pb.Entry{Id: id, Name: name, LogicalPath: logical, Kind: kind, Size: size, ModifiedUnixNano: modified}
	if kind == pb.EntryKind_ENTRY_KIND_DIRECTORY {
		entry.Revision = directoryRevision
	}
	return entry
}

func encodeRevision(rev revision) string {
	raw, _ := json.Marshal(rev)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// listedRevision accepts only canonical listed revisions in checked ETag mode.
func (p *Provider) listedRevision(encoded, id string) (revision, bool) {
	var rev revision
	if p.cfg.RevisionMode != "checked-etag" || encoded == "" || len(encoded) > 4096 {
		return revision{}, false
	}
	raw, e := base64.RawURLEncoding.DecodeString(encoded)
	if e != nil || json.Unmarshal(raw, &rev) != nil || !rev.Listed || rev.ID != id || rev.ETag == "" || !safeText(rev.ETag, 1024) || rev.Version != "" || rev.Size < 0 || encodeRevision(rev) != encoded {
		return revision{}, false
	}
	return rev, true
}

// Listed ETags come from ListObjectsV2 XML, where some gateways omit the
// quotes that HEAD and GET headers carry. Other revisions compare exactly.
func etagMatches(rev revision, etag string) bool {
	if etag == rev.ETag {
		return true
	}
	bare := strings.Trim(etag, `"`)
	return rev.Listed && bare != "" && bare == strings.Trim(rev.ETag, `"`)
}

func ifMatch(rev revision) string {
	if rev.Listed && !strings.HasPrefix(rev.ETag, `"`) {
		return `"` + rev.ETag + `"`
	}
	return rev.ETag
}
func (p *Provider) Stat(ctx context.Context, r *pb.StatRequest) (*pb.StatResponse, error) {
	if e := p.source(r.SourceId); e != nil {
		return nil, e
	}
	v, e := p.lookup(r.EntryId)
	if e != nil {
		return nil, e
	}
	if v.Kind == pb.EntryKind_ENTRY_KIND_DIRECTORY {
		if r.ExpectedRevision != "" && r.ExpectedRevision != directoryRevision {
			return nil, status.Error(codes.FailedPrecondition, "directory revision changed")
		}
		bucket, prefix := p.location(v.Key)
		switch {
		case v.Key == p.cfg.Prefix && p.allBuckets():
			// The all-buckets root: the keys must still list buckets.
			if _, e := p.client.ListBuckets(ctx, &s3.ListBucketsInput{}); e != nil {
				return nil, sourceBackendError(e)
			}
		case v.Key == p.cfg.Prefix || prefix == "":
			if _, e := p.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); e != nil {
				return nil, sourceBackendError(e)
			}
		default:
			out, e := p.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), MaxKeys: aws.Int32(1)})
			if e != nil {
				return nil, sourceBackendError(e)
			}
			if out.IsTruncated == nil || len(out.Contents)+len(out.CommonPrefixes) > 1 {
				return nil, status.Error(codes.Unavailable, "malformed directory lookup")
			}
			for _, obj := range out.Contents {
				key := aws.ToString(obj.Key)
				if !utf8.ValidString(key) || len(key) > 1024 || !strings.HasPrefix(key, prefix) || obj.Size == nil || *obj.Size < 0 {
					return nil, status.Error(codes.Unavailable, "malformed directory object")
				}
			}
			for _, obj := range out.CommonPrefixes {
				key := aws.ToString(obj.Prefix)
				if !utf8.ValidString(key) || len(key) > 1024 || !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, "/") {
					return nil, status.Error(codes.Unavailable, "malformed directory prefix")
				}
			}
			if len(out.Contents) == 0 && len(out.CommonPrefixes) == 0 {
				if aws.ToBool(out.IsTruncated) {
					return nil, status.Error(codes.Unavailable, "incomplete directory lookup")
				}
				return nil, status.Error(codes.NotFound, "directory absent")
			}
		}
		entry, e := p.entry(v.Key, v.Kind, 0, 0)
		return &pb.StatResponse{Entry: entry}, e
	}
	bucket, key := p.location(v.Key)
	h, e := p.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if e != nil {
		return nil, p.objectBackendError(ctx, bucket, e)
	}
	etag, _, e := headValidators(h)
	if e != nil {
		return nil, e
	}
	if listed, ok := p.listedRevision(r.ExpectedRevision, r.EntryId); ok {
		// The listing named this object by ETag and size. Echo the listed
		// metadata so Stat matches the listed entry exactly.
		if *h.ContentLength != listed.Size || !etagMatches(listed, etag) {
			return nil, status.Error(codes.FailedPrecondition, "revision changed")
		}
		entry := p.entryWithID(r.EntryId, v.Key, v.Kind, listed.Size, listed.Modified)
		entry.Revision = r.ExpectedRevision
		return &pb.StatResponse{Entry: entry}, nil
	}
	entry, e := p.headEntry(r.EntryId, v.Key, h)
	if e != nil {
		return nil, e
	}
	if r.ExpectedRevision != "" && r.ExpectedRevision != entry.Revision {
		return nil, status.Error(codes.FailedPrecondition, "revision changed")
	}
	return &pb.StatResponse{Entry: entry}, nil
}

// headValidators checks the validators of a HEAD response and normalizes the
// "null" VersionId of unversioned objects.
func headValidators(h *s3.HeadObjectOutput) (etag, version string, err error) {
	etag, version = aws.ToString(h.ETag), aws.ToString(h.VersionId)
	if h.ContentLength == nil || *h.ContentLength < 0 || etag == "" || !safeText(etag, 1024) || !safeText(version, 1024) {
		return "", "", status.Error(codes.Unavailable, "backend omitted object validators")
	}
	if version == "null" {
		version = ""
	}
	return etag, version, nil
}

// headEntry is the file entry and HEAD-derived revision that Stat reports for
// a known identity.
func (p *Provider) headEntry(id, key string, h *s3.HeadObjectOutput) (*pb.Entry, error) {
	etag, version, e := headValidators(h)
	if e != nil {
		return nil, e
	}
	if p.cfg.RevisionMode == "versioned" && version == "" {
		return nil, status.Error(codes.FailedPrecondition, "immutable S3 object version required")
	}
	encoded := encodeRevision(revision{ID: id, ETag: etag, Version: version, Size: *h.ContentLength})
	if len(encoded) > 4096 {
		return nil, status.Error(codes.Unavailable, "backend object validators exceed revision limit")
	}
	modified := int64(0)
	if h.LastModified != nil {
		modified = h.LastModified.UnixNano()
	}
	entry := p.entryWithID(id, key, pb.EntryKind_ENTRY_KIND_FILE, *h.ContentLength, modified)
	entry.Revision = encoded
	return entry, nil
}
func (p *Provider) List(ctx context.Context, r *pb.ListRequest) (*pb.ListResponse, error) {
	if e := p.source(r.SourceId); e != nil {
		return nil, e
	}
	if r.GetChangesSince() != "" {
		return nil, status.Error(codes.FailedPrecondition, "S3 change feed unavailable; list fully")
	}
	dir, e := p.lookup(r.DirectoryId)
	if e != nil {
		return nil, e
	}
	if dir.Kind != pb.EntryKind_ENTRY_KIND_DIRECTORY {
		return nil, status.Error(codes.InvalidArgument, "not directory")
	}
	limit := int(r.MaxEntries)
	if limit == 0 {
		limit = 512
	}
	if limit > 512 {
		return nil, status.Error(codes.InvalidArgument, "page limit exceeds 512")
	}
	state := cursor{Layout: cursorLayout, Directory: r.DirectoryId, PageSize: limit, DirectoriesOnly: r.DirectoriesOnly, Expires: time.Now().Add(CursorLifetime).Unix()}
	if r.Cursor != "" {
		if len(r.Cursor) != 48 {
			return nil, status.Error(codes.InvalidArgument, "invalid cursor")
		}
		e = p.db.View(func(tx *bolt.Tx) error {
			raw := tx.Bucket([]byte("cursors")).Get([]byte(r.Cursor))
			if raw == nil {
				return status.Error(codes.InvalidArgument, "unknown cursor")
			}
			// Decode into a zero value: absent bindings must not inherit
			// this request's values.
			state = cursor{}
			return json.Unmarshal(raw, &state)
		})
		if e != nil {
			return nil, e
		}
		if state.Layout != cursorLayout || state.Directory != r.DirectoryId || state.PageSize != limit || state.DirectoriesOnly != r.DirectoriesOnly || state.Expires < time.Now().Unix() {
			return nil, status.Error(codes.InvalidArgument, "expired or foreign cursor")
		}
	}
	result := &pb.ListResponse{}
	size, heads := 0, 0
	for fetches := 0; len(result.Entries) < limit; {
		if len(state.Pending) == 0 {
			if state.Done || fetches >= maxBackendFetchesPerList {
				break
			}
			fetches++
			if e := p.fetchPage(ctx, dir.Key, r.DirectoriesOnly, &state); e != nil {
				return nil, e
			}
			continue
		}
		want := min(len(state.Pending), limit-len(result.Entries))
		batch, e := p.hydrate(ctx, r.SourceId, state.Pending[:want], &heads)
		if e != nil {
			return nil, e
		}
		full := false
		for _, entry := range batch {
			n := protowire.SizeTag(1) + protowire.SizeBytes(proto.Size(entry))
			if size+n > pageBudget {
				full = true
				break
			}
			size += n
			result.Entries = append(result.Entries, entry)
			state.Pending = state.Pending[1:]
		}
		if full && len(result.Entries) == 0 {
			return nil, status.Error(codes.Unavailable, "backend entry exceeds response budget")
		}
		if full || len(batch) < want {
			break
		}
	}
	result.Complete = state.Done && len(state.Pending) == 0
	if !result.Complete {
		nonce := make([]byte, 24)
		if _, e := rand.Read(nonce); e != nil {
			return nil, e
		}
		token := hex.EncodeToString(nonce)
		raw, _ := json.Marshal(state)
		e = p.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("cursors"))
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				// Decode only the expiry: retained cursors can each hold a
				// backend page of pending entries.
				var old struct{ Expires int64 }
				if json.Unmarshal(v, &old) != nil || old.Expires < time.Now().Unix() {
					if e := c.Delete(); e != nil {
						return e
					}
				}
			}
			if b.Stats().KeyN >= MaxCursors {
				return status.Error(codes.ResourceExhausted, "cursor retention limit reached")
			}
			return b.Put([]byte(token), raw)
		})
		if e != nil {
			return nil, e
		}
		result.NextCursor = token
	}
	return result, nil
}

// fetchPage reads one delimited ListObjectsV2 page: Contents are the
// directory's files and CommonPrefixes its subdirectories. Validation runs
// before any object request or identity write.
func (p *Provider) fetchPage(ctx context.Context, dirKey string, directoriesOnly bool, state *cursor) error {
	if p.allBuckets() && dirKey == "" {
		return p.fetchBuckets(ctx, state)
	}
	// Listings run on object keys; identities add back the bucket in
	// all-buckets mode.
	bucket, prefix := p.location(dirKey)
	base := dirKey[:len(dirKey)-len(prefix)]
	input := &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix), Delimiter: aws.String(listDelimiter), MaxKeys: aws.Int32(backendPageKeys)}
	if state.Token != "" {
		input.ContinuationToken = aws.String(state.Token)
	}
	out, e := p.client.ListObjectsV2(ctx, input)
	if e != nil {
		return sourceBackendError(e)
	}
	if out.IsTruncated == nil {
		return status.Error(codes.Unavailable, "missing pagination flag")
	}
	// Filesystem-backed gateways count max-keys against Contents and repeat
	// each directory as a marker object, so each list is bounded separately.
	if len(out.Contents) > backendPageKeys || len(out.CommonPrefixes) > backendPageKeys {
		return status.Error(codes.Unavailable, "oversized backend page")
	}
	children, e := p.delimitedChildren(prefix, directoriesOnly, out, state)
	if e != nil {
		return e
	}
	for i := range children {
		children[i].key = base + children[i].key
	}
	done := !aws.ToBool(out.IsTruncated)
	next := aws.ToString(out.NextContinuationToken)
	if !done {
		if e := continueFrom(state, next); e != nil {
			return e
		}
	}
	// ListObjectsV2 omits VersionIds. Strict mode HEADs outgoing files later.
	// Checked mode HEADs the first file of each backend page. Without a genuine
	// version, and with an ETag form the probe explains, the page's revisions
	// come from the listing; otherwise its files keep HEAD revisions and the
	// probe's response is reused for its own file.
	probe := -1
	var probeHead *s3.HeadObjectOutput
	var listedForm func(string) string
	if p.cfg.RevisionMode == "checked-etag" {
		for i, c := range children {
			if c.kind != pb.EntryKind_ENTRY_KIND_FILE {
				continue
			}
			h, e := p.probeObject(ctx, c.key)
			if e != nil {
				return e
			}
			etag, version, _ := headValidators(h)
			if version == "" && *h.ContentLength == c.size {
				listedForm = listedETagForm(c.etag, etag)
			}
			if listedForm == nil {
				probe, probeHead = i, h
			}
			break
		}
	}
	items := make([]identity, len(children))
	for i, c := range children {
		items[i] = identity{c.key, c.kind}
	}
	ids, e := p.rememberAll(items)
	if e != nil {
		return e
	}
	for i, c := range children {
		entry := p.entryWithID(ids[i], c.key, c.kind, c.size, c.modified)
		switch {
		case i == probe:
			if entry, e = p.headEntry(ids[i], c.key, probeHead); e != nil {
				return e
			}
		case c.kind == pb.EntryKind_ENTRY_KIND_FILE && listedForm != nil:
			etag := listedForm(c.etag)
			entry.Revision = encodeRevision(revision{ID: ids[i], ETag: etag, Size: c.size, Listed: true, Modified: c.modified})
			if !safeText(etag, 1024) || len(entry.Revision) > 4096 {
				return status.Error(codes.Unavailable, "backend object validators exceed revision limit")
			}
		}
		state.Pending = append(state.Pending, entry)
	}
	state.Done, state.Token = done, next
	return nil
}

// continueFrom validates a backend continuation token and refuses one already
// seen by this enumeration.
func continueFrom(state *cursor, next string) error {
	if next == "" || len(next) > 65536 {
		return status.Error(codes.Unavailable, "invalid continuation token")
	}
	hash := sha256.Sum256([]byte(next))
	tokenHash := hex.EncodeToString(hash[:])
	for _, seen := range state.Seen {
		if seen == tokenHash {
			return status.Error(codes.Unavailable, "backend cursor loop")
		}
	}
	if len(state.Seen) >= MaxCursorHistory {
		return status.Error(codes.ResourceExhausted, "enumeration token history limit reached")
	}
	state.Seen = append(state.Seen, tokenHash)
	return nil
}

// fetchBuckets reads one ListBuckets page of the all-buckets root: each bucket
// is a directory named for it. Names that are not ordinary bucket names, such
// as legacy names with capitals or underscores, are skipped. Names must ascend
// across pages.
func (p *Provider) fetchBuckets(ctx context.Context, state *cursor) error {
	input := &s3.ListBucketsInput{}
	if state.Token != "" {
		input.ContinuationToken = aws.String(state.Token)
	}
	out, e := p.client.ListBuckets(ctx, input)
	if e != nil {
		return sourceBackendError(e)
	}
	if len(out.Buckets) > maxBucketsPerPage {
		return status.Error(codes.Unavailable, "oversized backend page")
	}
	names := make([]string, 0, len(out.Buckets))
	for _, bucket := range out.Buckets {
		if name := aws.ToString(bucket.Name); bucketPattern.MatchString(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	items := make([]identity, 0, len(names))
	for _, name := range names {
		if name <= strings.TrimSuffix(state.LastKey, "/") {
			return status.Error(codes.Unavailable, "malformed or unordered bucket listing")
		}
		key := name + "/"
		state.LastKey = key
		items = append(items, identity{key, pb.EntryKind_ENTRY_KIND_DIRECTORY})
	}
	next := aws.ToString(out.ContinuationToken)
	if next != "" {
		if e := continueFrom(state, next); e != nil {
			return e
		}
	}
	ids, e := p.rememberAll(items)
	if e != nil {
		return e
	}
	for i, item := range items {
		state.Pending = append(state.Pending, p.entryWithID(ids[i], item.Key, item.Kind, 0, 0))
	}
	state.Done, state.Token = next == "", next
	return nil
}

// listedETagForm maps this page's listing ETags to the validator HEAD and GET
// report for the same object, as observed on the page's probe. Most backends
// report one ETag, with or without quotes in listing XML. One filesystem-backed
// gateway reports the hex encoding of the listing ETag's text in HEAD and GET.
// An unexplained relation returns nil and the page keeps HEAD revisions.
func listedETagForm(listed, head string) func(string) string {
	bareListed, bareHead := strings.Trim(listed, `"`), strings.Trim(head, `"`)
	switch {
	case bareListed == "":
		return nil
	case bareHead == bareListed:
		return func(etag string) string { return etag }
	case bareHead == hex.EncodeToString([]byte(bareListed)):
		quoted := strings.HasPrefix(head, `"`)
		return func(etag string) string {
			encoded := hex.EncodeToString([]byte(strings.Trim(etag, `"`)))
			if quoted {
				return `"` + encoded + `"`
			}
			return encoded
		}
	}
	return nil
}

// delimitedChildren validates one page and merges its files and directories
// in key order. A directory may arrive as a CommonPrefix, as an immediate
// marker object with the same key (filesystem-backed gateways send both), or
// both; it is listed once. Keys must ascend across pages; a backend may repeat
// the previous page's final directory, which is skipped.
func (p *Provider) delimitedChildren(dirKey string, directoriesOnly bool, out *s3.ListObjectsV2Output, state *cursor) ([]child, error) {
	malformed := status.Error(codes.Unavailable, "malformed or unordered delimited listing")
	files := make([]child, 0, len(out.Contents))
	var markers []string
	for i, obj := range out.Contents {
		key := aws.ToString(obj.Key)
		if key == "" || !safeKey(key) || !strings.HasPrefix(key, dirKey) || obj.Size == nil || *obj.Size < 0 || (i > 0 && key <= aws.ToString(out.Contents[i-1].Key)) {
			return nil, malformed
		}
		name := key[len(dirKey):]
		if name == "" {
			continue // the listed directory's own marker object
		}
		if slash := strings.Index(name, listDelimiter); slash >= 0 {
			// Only an immediate child's marker may contain the delimiter; a
			// deeper key means the backend ignored it.
			if slash == 0 || slash != len(name)-1 {
				return nil, malformed
			}
			markers = append(markers, key)
			continue
		}
		c := child{key: key, kind: pb.EntryKind_ENTRY_KIND_FILE, size: *obj.Size, etag: aws.ToString(obj.ETag)}
		if p.cfg.RevisionMode == "checked-etag" && (c.etag == "" || !safeText(c.etag, 1024)) {
			return nil, status.Error(codes.Unavailable, "backend omitted object validators")
		}
		if obj.LastModified != nil {
			c.modified = obj.LastModified.UnixNano()
		}
		files = append(files, c)
	}
	prefixes := make([]string, 0, len(out.CommonPrefixes))
	for i, prefix := range out.CommonPrefixes {
		key := aws.ToString(prefix.Prefix)
		if !safeKey(key) || !strings.HasPrefix(key, dirKey) {
			return nil, malformed
		}
		name := key[len(dirKey):]
		if len(name) < 2 || !strings.HasSuffix(name, listDelimiter) || strings.Contains(name[:len(name)-1], listDelimiter) || (i > 0 && key <= prefixes[i-1]) {
			return nil, malformed
		}
		prefixes = append(prefixes, key)
	}
	dirs := make([]string, 0, len(prefixes)+len(markers))
	for m, d := 0, 0; m < len(markers) || d < len(prefixes); {
		switch {
		case d == len(prefixes) || (m < len(markers) && markers[m] < prefixes[d]):
			dirs, m = append(dirs, markers[m]), m+1
		case m == len(markers) || prefixes[d] < markers[m]:
			dirs, d = append(dirs, prefixes[d]), d+1
		default:
			dirs, m, d = append(dirs, prefixes[d]), m+1, d+1
		}
	}
	children := make([]child, 0, len(files)+len(dirs))
	for i, f, d := 0, 0, 0; f < len(files) || d < len(dirs); i++ {
		var c child
		if d == len(dirs) || (f < len(files) && files[f].key < dirs[d]) {
			c, f = files[f], f+1
		} else {
			c, d = child{key: dirs[d], kind: pb.EntryKind_ENTRY_KIND_DIRECTORY}, d+1
		}
		if c.key <= state.LastKey {
			if i == 0 && c.kind == pb.EntryKind_ENTRY_KIND_DIRECTORY && c.key == state.LastKey {
				continue
			}
			return nil, malformed
		}
		state.LastKey = c.key
		if directoriesOnly && c.kind == pb.EntryKind_ENTRY_KIND_FILE {
			// Browsing skips files before identity registration or any
			// object request.
			continue
		}
		children = append(children, c)
	}
	return children, nil
}

// probeObject HEADs a listed object and checks its validators. A listed object
// that HEAD cannot find is a listing race, never absence.
func (p *Provider) probeObject(ctx context.Context, key string) (*s3.HeadObjectOutput, error) {
	bucket, object := p.location(key)
	h, e := p.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(object)})
	if e != nil {
		return nil, listingObjectError(p.objectBackendError(ctx, bucket, e))
	}
	if _, _, e := headValidators(h); e != nil {
		return nil, e
	}
	return h, nil
}

func listingObjectError(e error) error {
	if status.Code(e) == codes.NotFound {
		return status.Error(codes.Unavailable, "S3 listing changed during enumeration")
	}
	return e
}

// hydrate attaches HEAD-derived revisions to outgoing files the listing could
// not pin. It returns a prefix of entries when the per-RPC HEAD budget ends.
func (p *Provider) hydrate(ctx context.Context, sourceID string, entries []*pb.Entry, heads *int) ([]*pb.Entry, error) {
	out := append([]*pb.Entry(nil), entries...)
	if p.cfg.RevisionMode == "" {
		return out, nil
	}
	var pending []int
	for i, entry := range out {
		if entry.Kind != pb.EntryKind_ENTRY_KIND_FILE || entry.Revision != "" {
			continue
		}
		if *heads >= maxRevisionHeadsPerList {
			out = out[:i]
			break
		}
		*heads++
		pending = append(pending, i)
	}
	if len(pending) == 0 {
		return out, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var once sync.Once
	var failure error
	slots := make(chan struct{}, revisionHeadConcurrency)
	for _, i := range pending {
		slots <- struct{}{}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-slots }()
			stat, err := p.Stat(ctx, &pb.StatRequest{SourceId: sourceID, EntryId: out[i].Id})
			if err != nil {
				once.Do(func() {
					failure = err
					cancel()
				})
				return
			}
			out[i] = stat.Entry
		}(i)
	}
	wg.Wait()
	if failure != nil {
		return nil, listingObjectError(failure)
	}
	if e := ctx.Err(); e != nil {
		return nil, backendError(e)
	}
	return out, nil
}
func (p *Provider) Read(r *pb.ReadRequest, s grpc.ServerStreamingServer[pb.ReadChunk]) error {
	return p.read(s.Context(), r, s.Send)
}

// Checked ETags detect observed drift; they do not make mutable objects immutable.
// A conditional HEAD is checked independently because gateways may ignore If-Match.
// It returns a genuine current VersionId, which only a listed revision admits:
// the listing could not observe versions, so the read pins that version.
func (p *Provider) checkUnversionedRevision(ctx context.Context, key string, rev revision) (string, error) {
	bucket, object := p.location(key)
	h, err := p.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(object), IfMatch: aws.String(ifMatch(rev))})
	if err != nil {
		return "", p.objectBackendError(ctx, bucket, err)
	}
	etag, version, err := headValidators(h)
	if err != nil {
		return "", err
	}
	if *h.ContentLength != rev.Size || !etagMatches(rev, etag) || (version != "" && !rev.Listed) {
		return "", status.Error(codes.FailedPrecondition, "revision changed")
	}
	return version, nil
}

func (p *Provider) read(ctx context.Context, r *pb.ReadRequest, send func(*pb.ReadChunk) error) error {
	if e := p.source(r.SourceId); e != nil {
		return e
	}
	v, e := p.lookup(r.EntryId)
	if e != nil {
		return e
	}
	if v.Kind != pb.EntryKind_ENTRY_KIND_FILE {
		return status.Error(codes.InvalidArgument, "not file")
	}
	var rev revision
	if len(r.ExpectedRevision) > 10924 {
		return status.Error(codes.InvalidArgument, "invalid revision")
	}
	raw, e := base64.RawURLEncoding.DecodeString(r.ExpectedRevision)
	if e != nil || len(raw) > 8192 || json.Unmarshal(raw, &rev) != nil || rev.ID != r.EntryId || rev.ETag == "" || !safeText(rev.ETag, 1024) || !safeText(rev.Version, 1024) || rev.Size < 0 || rev.Version == "null" ||
		(rev.Listed && (rev.Version != "" || p.cfg.RevisionMode == "")) || (!rev.Listed && rev.Modified != 0) {
		return status.Error(codes.InvalidArgument, "invalid revision")
	}
	if p.cfg.RevisionMode == "versioned" && rev.Version == "" {
		return status.Error(codes.FailedPrecondition, "immutable S3 object version required")
	}
	if r.Offset < 0 || r.Length < 0 || r.Length > 8<<20 || r.Offset > rev.Size || r.Length > rev.Size-r.Offset {
		return status.Error(codes.InvalidArgument, "invalid range")
	}
	bucket, object := p.location(v.Key)
	checkedUnversioned := p.cfg.RevisionMode == "checked-etag" && rev.Version == ""
	pin := rev.Version
	if checkedUnversioned {
		current, err := p.checkUnversionedRevision(ctx, v.Key, rev)
		if err != nil {
			return err
		}
		if current != "" {
			// Read exactly the immutable version that matched; no post-body check.
			pin, checkedUnversioned = current, false
		}
	}
	if r.Length == 0 {
		if rev.Version != "" {
			// Empty ranges still read the requested immutable revision, not the
			// current object (which may have changed or be a delete marker).
			h, e := p.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(object), VersionId: aws.String(rev.Version), IfMatch: aws.String(rev.ETag)})
			if e != nil {
				return p.objectBackendError(ctx, bucket, e)
			}
			if h.ContentLength == nil || *h.ContentLength != rev.Size || aws.ToString(h.ETag) != rev.ETag || aws.ToString(h.VersionId) != rev.Version {
				return status.Error(codes.FailedPrecondition, "backend response violates pinned revision")
			}
		} else if !checkedUnversioned && pin == "" {
			_, e := p.Stat(ctx, &pb.StatRequest{SourceId: r.SourceId, EntryId: r.EntryId, ExpectedRevision: r.ExpectedRevision})
			if e != nil {
				return e
			}
		}
		return send(&pb.ReadChunk{Offset: r.Offset, Eof: true})
	}
	input := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(object), Range: aws.String(fmt.Sprintf("bytes=%d-%d", r.Offset, r.Offset+r.Length-1)), IfMatch: aws.String(ifMatch(rev))}
	if pin != "" {
		input.VersionId = aws.String(pin)
	}
	out, e := p.client.GetObject(ctx, input)
	if e != nil {
		return p.objectBackendError(ctx, bucket, e)
	}
	if out.Body == nil {
		return status.Error(codes.Unavailable, "missing body")
	}
	defer out.Body.Close()
	if !etagMatches(rev, aws.ToString(out.ETag)) || aws.ToInt64(out.ContentLength) != r.Length || out.ContentLength == nil || aws.ToString(out.ContentRange) != fmt.Sprintf("bytes %d-%d/%d", r.Offset, r.Offset+r.Length-1, rev.Size) || (pin != "" && aws.ToString(out.VersionId) != pin) || (checkedUnversioned && aws.ToString(out.VersionId) != "" && aws.ToString(out.VersionId) != "null") {
		return status.Error(codes.FailedPrecondition, "backend response violates pinned revision or range")
	}
	remaining := r.Length
	offset := r.Offset
	for remaining > 0 {
		size := remaining
		if size > 128*1024 {
			size = 128 * 1024
		}
		buf := make([]byte, size)
		if _, e := io.ReadFull(out.Body, buf); e != nil {
			if ctx.Err() != nil {
				return backendError(ctx.Err())
			}
			return status.Error(codes.Unavailable, "short or failed backend stream")
		}
		remaining -= size
		if remaining == 0 {
			var extra [1]byte
			n, e := out.Body.Read(extra[:])
			if n != 0 || e != io.EOF {
				if ctx.Err() != nil {
					return backendError(ctx.Err())
				}
				return status.Error(codes.Unavailable, "backend stream size or terminal error")
			}
			if checkedUnversioned {
				// Hold the final chunk and successful EOF until the complete body
				// has been read and current ETag/size still match the request.
				current, err := p.checkUnversionedRevision(ctx, v.Key, rev)
				if err != nil {
					return err
				}
				if current != "" {
					return status.Error(codes.FailedPrecondition, "revision changed")
				}
			}
		}
		if e := send(&pb.ReadChunk{Offset: offset, Data: buf, Eof: remaining == 0}); e != nil {
			return e
		}
		offset += size
	}
	return nil
}

// Limit XML before SDK decoding; MaxKeys alone does not bound hostile responses.
type boundedTransport struct {
	base   http.RoundTripper
	origin string
}

func (t boundedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.origin != "" && r.URL.Scheme+"://"+r.URL.Host != t.origin {
		return nil, errors.New("S3 endpoint change refused")
	}
	response, e := t.base.RoundTrip(r)
	if e == nil && r.Method == http.MethodGet && r.Header.Get("Range") != "" && response.StatusCode >= 200 && response.StatusCode < 300 && response.StatusCode != http.StatusPartialContent {
		response.Body.Close()
		return nil, errors.New("S3 backend did not return a partial response")
	}
	// ListObjectsV2 carries list-type=2; path-style ListBuckets is GET /.
	listing := r.URL.Query().Get("list-type") == "2" || (r.URL.Path == "/" || r.URL.Path == "") && r.Header.Get("Range") == ""
	if e == nil && r.Method == http.MethodGet && listing && response.Body != nil {
		response.Body = http.MaxBytesReader(nil, response.Body, 8*1024*1024)
	}
	return response, e
}

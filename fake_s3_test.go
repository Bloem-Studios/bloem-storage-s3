package s3provider

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeS3 is an ordered, path-style S3 fixture. ListObjectsV2 honors prefix,
// delimiter, max-keys and opaque continuation tokens like AWS: Contents and
// CommonPrefixes share one lexicographic sequence and count against max-keys.
type fakeS3 struct {
	bucket string
	mu     sync.Mutex
	// Current objects by key and every stored version by version ID.
	objects  map[string]*fakeObject
	versions map[string]*fakeObject
	// versioned assigns genuine VersionIds; nullVersions reports "null".
	versioned    bool
	nullVersions bool
	// listETagUnquoted lists ETags without the quotes HEAD/GET return.
	listETagUnquoted bool
	// listETagDiffers lists a different ETag scheme than HEAD/GET report for
	// the same unchanged object, as one filesystem-backed gateway does.
	listETagDiffers bool
	// headETagHex models a gateway whose HEAD and GET report the hex encoding
	// of the listing's ETag text for the same unchanged object, and evaluate
	// If-Match against that form.
	headETagHex bool
	// fsMarkers models a filesystem-backed gateway: a delimited listing also
	// returns each subdirectory as a zero-size Contents marker with the same
	// key as its CommonPrefix, plus the listed directory's own marker, and
	// max-keys counts directories once.
	fsMarkers   bool
	headDelay   time.Duration
	nextVersion int
	clock       time.Time

	lists, heads, gets, bucketHeads atomic.Int32
	inflight, maxInflight           atomic.Int32
	listQueries                     []url.Values
}

type fakeObject struct {
	key      string
	data     []byte
	etag     string
	version  string
	modified time.Time
}

func newFakeS3() *fakeS3 {
	return &fakeS3{bucket: fixtureConfig().Bucket, objects: map[string]*fakeObject{}, versions: map[string]*fakeObject{}, clock: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)}
}

func (f *fakeS3) put(key, data string) *fakeObject {
	f.mu.Lock()
	defer f.mu.Unlock()
	sum := md5.Sum([]byte(data))
	// Millisecond list precision deliberately differs from HTTP-date HEADs.
	f.clock = f.clock.Add(1500*time.Millisecond + 123*time.Millisecond)
	obj := &fakeObject{key: key, data: []byte(data), etag: `"` + hex.EncodeToString(sum[:]) + `"`, modified: f.clock}
	if f.versioned {
		f.nextVersion++
		obj.version = fmt.Sprintf("immutable-v%d", f.nextVersion)
		f.versions[obj.version] = obj
	}
	f.objects[key] = obj
	return obj
}

func (f *fakeS3) provider(t *testing.T, mode string) *Provider {
	t.Helper()
	p, _ := f.providerAt(t, mode)
	return p
}

func (f *fakeS3) providerAt(t *testing.T, mode string) (*Provider, string) {
	t.Helper()
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	c := fixtureConfig()
	c.Endpoint = server.URL
	c.RevisionMode = mode
	path := filepath.Join(t.TempDir(), "private", "db")
	p, err := Open(c, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p, path
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == f.bucket || path == f.bucket+"/" {
		if r.Method == http.MethodHead {
			f.bucketHeads.Add(1)
			return
		}
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			f.list(w, r.URL.Query())
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !strings.HasPrefix(path, f.bucket+"/") {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	key := strings.TrimPrefix(path, f.bucket+"/")
	switch r.Method {
	case http.MethodHead:
		f.heads.Add(1)
		n := f.inflight.Add(1)
		for {
			max := f.maxInflight.Load()
			if n <= max || f.maxInflight.CompareAndSwap(max, n) {
				break
			}
		}
		defer f.inflight.Add(-1)
		if f.headDelay > 0 {
			time.Sleep(f.headDelay)
		}
		f.object(w, r, key, false)
	case http.MethodGet:
		f.gets.Add(1)
		f.object(w, r, key, true)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func fakeError(w http.ResponseWriter, code string, status int) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, "<Error><Code>%s</Code><Message>fixture</Message></Error>", code)
}

func (f *fakeS3) object(w http.ResponseWriter, r *http.Request, key string, body bool) {
	f.mu.Lock()
	obj := f.objects[key]
	if version := r.URL.Query().Get("versionId"); version != "" {
		obj = f.versions[version]
		if obj != nil && obj.key != key {
			obj = nil
		}
	}
	f.mu.Unlock()
	if obj == nil {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		fakeError(w, "NoSuchKey", http.StatusNotFound)
		return
	}
	etag := obj.etag
	if f.headETagHex {
		etag = `"` + hex.EncodeToString([]byte(strings.Trim(obj.etag, `"`))) + `"`
	}
	if match := r.Header.Get("If-Match"); match != "" && match != etag {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		fakeError(w, "PreconditionFailed", http.StatusPreconditionFailed)
		return
	}
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", obj.modified.Format(http.TimeFormat))
	switch {
	case obj.version != "":
		w.Header().Set("X-Amz-Version-Id", obj.version)
	case f.nullVersions:
		w.Header().Set("X-Amz-Version-Id", "null")
	}
	size := int64(len(obj.data))
	if !body {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		return
	}
	var start, end int64
	if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end < start || end >= size {
		fakeError(w, "InvalidRange", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
	w.WriteHeader(http.StatusPartialContent)
	w.Write(obj.data[start : end+1])
}

func (f *fakeS3) list(w http.ResponseWriter, q url.Values) {
	f.lists.Add(1)
	f.mu.Lock()
	f.listQueries = append(f.listQueries, q)
	prefix, delimiter := q.Get("prefix"), q.Get("delimiter")
	maxKeys := 1000
	if raw := q.Get("max-keys"); raw != "" {
		maxKeys, _ = strconv.Atoi(raw)
	}
	type item struct {
		name string
		obj  *fakeObject
	}
	var items []item
	seen := map[string]bool{}
	for key, obj := range f.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if delimiter != "" {
			if i := strings.Index(key[len(prefix):], delimiter); i >= 0 {
				common := key[:len(prefix)+i+len(delimiter)]
				if !seen[common] {
					seen[common] = true
					items = append(items, item{name: common})
				}
				continue
			}
		}
		items = append(items, item{name: key, obj: obj})
	}
	if f.fsMarkers && delimiter != "" && prefix != "" && !seen[prefix] {
		if _, exists := f.objects[prefix]; !exists {
			items = append(items, item{name: prefix, obj: &fakeObject{key: prefix, etag: `"d41d8cd98f00b204e9800998ecf8427e"`, modified: f.clock}})
		}
	}
	f.mu.Unlock()
	sort.Slice(items, func(i, j int) bool { return items[i].name < items[j].name })
	if token := q.Get("continuation-token"); token != "" {
		after, err := hex.DecodeString(token)
		if err != nil {
			fakeError(w, "InvalidArgument", http.StatusBadRequest)
			return
		}
		n := sort.Search(len(items), func(i int) bool { return items[i].name > string(after) })
		items = items[n:]
	}
	truncated := len(items) > maxKeys
	if truncated {
		items = items[:maxKeys]
	}
	w.Header().Set("Content-Type", "application/xml")
	io.WriteString(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>`+f.bucket+`</Name>`)
	fmt.Fprintf(w, "<KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys><IsTruncated>%t</IsTruncated>", len(items), maxKeys, truncated)
	if truncated {
		fmt.Fprintf(w, "<NextContinuationToken>%s</NextContinuationToken>", hex.EncodeToString([]byte(items[len(items)-1].name)))
	}
	for _, it := range items {
		if it.obj == nil {
			io.WriteString(w, "<CommonPrefixes><Prefix>")
			xml.EscapeText(w, []byte(it.name))
			io.WriteString(w, "</Prefix></CommonPrefixes>")
			if !f.fsMarkers {
				continue
			}
			it.obj = &fakeObject{key: it.name, etag: `"d41d8cd98f00b204e9800998ecf8427e"`, modified: f.clock}
		}
		etag := it.obj.etag
		if f.listETagUnquoted {
			etag = strings.Trim(etag, `"`)
		}
		if f.listETagDiffers {
			etag = fmt.Sprintf(`"%x-%d"`, it.obj.modified.Unix(), len(it.obj.data))
		}
		io.WriteString(w, "<Contents><Key>")
		xml.EscapeText(w, []byte(it.name))
		io.WriteString(w, "</Key><ETag>")
		xml.EscapeText(w, []byte(etag))
		fmt.Fprintf(w, "</ETag><LastModified>%s</LastModified><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>", it.obj.modified.Format("2006-01-02T15:04:05.000Z"), len(it.obj.data))
	}
	io.WriteString(w, "</ListBucketResult>")
}

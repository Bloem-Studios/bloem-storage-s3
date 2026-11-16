package s3provider

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	pb "github.com/Bloem-Studios/bloem-plugin-sdk/pkg/pluginproto/bloem/plugin/v1"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestIdentityAndCursorRetentionFailClosed(t *testing.T) {
	p, _ := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>books/a</Key><Size>1</Size></Contents><Contents><Key>books/b</Key><Size>1</Size></Contents></ListBucketResult>")
	})
	a, e := p.remember("books/a", pb.EntryKind_ENTRY_KIND_FILE)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = p.remember("books/b", pb.EntryKind_ENTRY_KIND_FILE); e != nil {
		t.Fatal(e)
	}
	e = p.db.Update(func(tx *bolt.Tx) error {
		count := make([]byte, 8)
		binary.BigEndian.PutUint64(count, MaxIdentities)
		return tx.Bucket([]byte("meta")).Put([]byte("identity_count"), count)
	})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = p.remember("books/new", pb.EntryKind_ENTRY_KIND_FILE); status.Code(e) != codes.ResourceExhausted {
		t.Fatal("identity cap not enforced", e)
	}
	if v, e := p.lookup(a); e != nil || v.Key != "books/a" {
		t.Fatal("cap evicted existing identity", e)
	}
	e = p.db.Update(func(tx *bolt.Tx) error {
		raw, _ := json.Marshal(cursor{Directory: p.rootID(), Expires: time.Now().Add(CursorLifetime).Unix()})
		for n := 0; n < MaxCursors; n++ {
			if e := tx.Bucket([]byte("cursors")).Put([]byte(fmt.Sprintf("%048x", n)), raw); e != nil {
				return e
			}
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = p.List(context.Background(), &pb.ListRequest{SourceId: p.cfg.SourceID, DirectoryId: p.rootID(), MaxEntries: 1}); status.Code(e) != codes.ResourceExhausted {
		t.Fatal("cursor cap not enforced", e)
	}
}

func TestLegacyStateRejectedWithoutErasingIdentities(t *testing.T) {
	p, path := protocolProvider(t, func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected backend call") })
	id, e := p.remember("books/legacy", pb.EntryKind_ENTRY_KIND_FILE)
	if e != nil {
		t.Fatal(e)
	}
	c := p.cfg
	if e = p.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("meta")).Delete([]byte("namespace_v2")) }); e != nil {
		t.Fatal(e)
	}
	p.Close()
	if other, e := Open(c, path); e == nil {
		other.Close()
		t.Fatal("legacy schema silently reused")
	}
	db, e := bolt.Open(path, 0600, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if e = db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("ids")).Get([]byte(id)) == nil {
			t.Error("legacy rejection erased identity")
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}

package zenbun

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestSnapshotMirrorRetainsPublishedGeneration(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	index, err := Open(store, "index/")
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := NewSnapshotMirror(index, store, "copies/")
	if err != nil {
		t.Fatal(err)
	}
	if err := mirror.Apply(ctx, []Change{{Key: "a", Data: []byte("old term")}, {Key: "b", Data: []byte("old term")}}); err != nil {
		t.Fatal(err)
	}
	before, err := mirror.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := mirror.Apply(ctx, []Change{{Key: "a", Data: []byte("new term")}, {Key: "b", Delete: true}}); err != nil {
		t.Fatal(err)
	}
	after, err := mirror.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.Generation() == after.Generation() {
		t.Fatal("generation did not change")
	}
	q := Query{All: [][]string{{"old"}}}
	keys, err := before.SearchQuery(ctx, q)
	if err != nil || !reflect.DeepEqual(keys, []string{"a", "b"}) {
		t.Fatalf("old candidates: %v %v", keys, err)
	}
	body, err := before.Get(ctx, "a")
	if err != nil || string(body) != "old term" {
		t.Fatalf("old document: %s %v", body, err)
	}
	keys, err = after.SearchQuery(ctx, q)
	if err != nil || len(keys) != 0 {
		t.Fatalf("new candidates: %v %v", keys, err)
	}
	body, err = after.Get(ctx, "a")
	if err != nil || string(body) != "new term" {
		t.Fatalf("new document: %s %v", body, err)
	}
	if _, err = after.Get(ctx, "b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted: %v", err)
	}
}

type failManifestStore struct {
	*MemoryStore
	fail bool
}

func (s *failManifestStore) Put(ctx context.Context, key string, data []byte, options PutOptions) (string, error) {
	if s.fail && key == "index/manifest" {
		return "", ErrConflict
	}
	return s.MemoryStore.Put(ctx, key, data, options)
}
func TestSnapshotMirrorFailedPublicationKeepsPreviousVersion(t *testing.T) {
	ctx := context.Background()
	store := &failManifestStore{MemoryStore: &MemoryStore{}}
	idx, _ := Open(store, "index/")
	m, _ := NewSnapshotMirror(idx, store, "copies/")
	if err := m.Apply(ctx, []Change{{Key: "a", Data: []byte("before")}}); err != nil {
		t.Fatal(err)
	}
	store.fail = true
	if err := m.Apply(ctx, []Change{{Key: "a", Data: []byte("after")}}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	snapshot, err := m.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	body, err := snapshot.Get(ctx, "a")
	if err != nil || string(body) != "before" {
		t.Fatalf("body: %s %v", body, err)
	}
}

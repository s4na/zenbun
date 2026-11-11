package zenbun

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestStageKeepsBaseAvailableWithoutPublishing(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	idx, _ := Open(store, "idx/")
	m, _ := NewSnapshotMirror(idx, store, "copies/")
	first, err := m.Stage(ctx, "", []SnapshotChange{{Key: "a", Text: "old", Data: []byte("old")}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.Stage(ctx, first.Generation(), []SnapshotChange{{Key: "a", Text: "new", Data: []byte("new")}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ id, want string }{{first.Generation(), "old"}, {second.Generation(), "new"}} {
		s, err := m.OpenSnapshot(ctx, tc.id)
		if err != nil {
			t.Fatal(err)
		}
		b, err := s.Get(ctx, "a")
		if err != nil || string(b) != tc.want {
			t.Fatalf("%s: %s %v", tc.id, b, err)
		}
	}
	current, err := m.Snapshot(ctx)
	if err != nil || current.Generation() != "" {
		t.Fatalf("stage published current manifest: %v %v", current, err)
	}
}

func TestStageCompactsUpdatesAndDeletes(t *testing.T) {
	ctx := t.Context()
	store := &MemoryStore{}
	idx, err := Open(store, "idx/")
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewSnapshotMirror(idx, store, "copies/")
	if err != nil {
		t.Fatal(err)
	}
	base := ""
	var first *Snapshot
	for n := 0; n < 8; n++ {
		changes := []SnapshotChange{{Key: "a", Text: "needle", Data: []byte("needle")}, {Key: fmt.Sprintf("doc-%d", n), Text: "needle", Data: []byte("needle")}}
		snapshot, err := m.Stage(ctx, base, changes)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			first = snapshot
		}
		base = snapshot.Generation()
	}
	latest, err := m.Stage(ctx, base, []SnapshotChange{{Key: "a", Delete: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(latest.manifest.Segments) >= 9 {
		t.Fatalf("expected compaction after 9 updates; segments=%d", len(latest.manifest.Segments))
	}
	old, err := m.OpenSnapshot(ctx, first.Generation())
	if err != nil {
		t.Fatal(err)
	}
	if body, err := old.Get(ctx, "a"); err != nil || string(body) != "needle" {
		t.Fatalf("old: %s %v", body, err)
	}
	if _, err = latest.Get(ctx, "a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted: %v", err)
	}
	query := Query{All: [][]string{{"needle"}}}
	result, err := latest.FindRanked(ctx, query, RankedOptions{Limit: 20}, func(_ string, data []byte) (RankedFields, bool, error) {
		return RankedFields{Text: string(data)}, true, nil
	})
	if err != nil || len(result.Documents) != 8 {
		t.Fatalf("ranked result: %+v %v", result, err)
	}
}

func TestStageRejectsInvalidOrNonStagedBase(t *testing.T) {
	ctx := t.Context()
	store := &MemoryStore{}
	idx, err := Open(store, "idx/")
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewSnapshotMirror(idx, store, "copies/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.OpenSnapshot(ctx, "invalid"); err == nil {
		t.Fatal("invalid ID accepted")
	}
	if err := m.Apply(ctx, []Change{{Key: "a", Data: []byte("text")}}); err != nil {
		t.Fatal(err)
	}
	published, err := m.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Stage(ctx, published.Generation(), nil); !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "staged base") {
		t.Fatalf("published base: %v", err)
	}
}

type unavailableIndexStore struct {
	*MemoryStore
	reads int
}

func (s *unavailableIndexStore) Get(context.Context, string, *ByteRange) (Object, error) {
	s.reads++
	return Object{}, errors.New("storage unavailable")
}
func TestApplyDocumentsValidatesBeforeReadingIndex(t *testing.T) {
	store := &unavailableIndexStore{MemoryStore: &MemoryStore{}}
	idx, err := Open(store, "idx/")
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewSnapshotMirror(idx, store, "copies/")
	if err != nil {
		t.Fatal(err)
	}
	err = m.ApplyDocuments(t.Context(), []SnapshotChange{{Key: "a", Text: "one"}, {Key: "a", Text: "two"}})
	if err == nil || !strings.Contains(err.Error(), "duplicate") || store.reads != 0 {
		t.Fatalf("validation: %v, reads=%d", err, store.reads)
	}
}

func TestStageValidatesBeforeReadingBase(t *testing.T) {
	store := &unavailableIndexStore{MemoryStore: &MemoryStore{}}
	idx, err := Open(store, "idx/")
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewSnapshotMirror(idx, store, "copies/")
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Stage(t.Context(), strings.Repeat("a", 32), []SnapshotChange{{Key: "a", Text: "one"}, {Key: "a", Text: "two"}})
	if err == nil || !strings.Contains(err.Error(), "duplicate") || store.reads != 0 {
		t.Fatalf("validation: %v, reads=%d", err, store.reads)
	}
}

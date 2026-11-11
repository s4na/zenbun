package zenbun

import (
	"context"
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

package zenbun

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestRankedSearchPaginationAndGeneration(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	idx, _ := Open(store, "index/")
	mirror, _ := NewSnapshotMirror(idx, store, "copies/")
	fields := map[string]RankedFields{"a": {Title: "other", Text: "needle"}, "b": {Title: "needle guide"}, "c": {Title: "needle"}, "d": {Title: "other", Tags: []string{"needle"}}}
	changes := []SnapshotChange{}
	for key, f := range fields {
		data, _ := json.Marshal(f)
		changes = append(changes, SnapshotChange{Key: key, Data: data, Text: f.SearchText()})
	}
	if err := mirror.ApplyDocuments(ctx, changes); err != nil {
		t.Fatal(err)
	}
	snap, err := mirror.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	decode := func(_ string, data []byte) (RankedFields, bool, error) {
		var f RankedFields
		err := json.Unmarshal(data, &f)
		return f, true, err
	}
	q := Query{All: [][]string{{"needle"}}}
	first, err := snap.FindRanked(ctx, q, RankedOptions{Limit: 2}, decode)
	if err != nil {
		t.Fatal(err)
	}
	second, err := snap.FindRanked(ctx, q, RankedOptions{Limit: 2, After: first.Next}, decode)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for _, doc := range append(first.Documents, second.Documents...) {
		got = append(got, doc.Key)
	}
	if !reflect.DeepEqual(got, []string{"c", "b", "d", "a"}) || second.Next != "" {
		t.Fatalf("ranked pages: %v %q", got, second.Next)
	}
	if _, err := snap.FindRanked(ctx, q, RankedOptions{After: first.Next, Scope: "other"}, decode); !errors.Is(err, ErrSnapshotChanged) {
		t.Fatalf("scope: %v", err)
	}
	if err := mirror.Apply(ctx, []Change{{Key: "a", Data: []byte("new")}}); err != nil {
		t.Fatal(err)
	}
	newer, _ := mirror.Snapshot(ctx)
	if _, err := newer.FindRanked(ctx, q, RankedOptions{After: first.Next}, decode); !errors.Is(err, ErrSnapshotChanged) {
		t.Fatalf("generation: %v", err)
	}
}

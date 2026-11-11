package zenbun

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
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
	if _, err := snap.FindRanked(ctx, q, RankedOptions{MaxCandidates: 3}, decode); !errors.Is(err, ErrRankedLimit) {
		t.Fatalf("candidate limit: %v", err)
	}
	if _, err := snap.FindRanked(ctx, q, RankedOptions{MaxBytes: 1}, decode); !errors.Is(err, ErrRankedLimit) {
		t.Fatalf("byte limit: %v", err)
	}
	if _, err := snap.FindRanked(ctx, q, RankedOptions{After: "invalid!"}, decode); !errors.Is(err, ErrInvalidRankedCursor) {
		t.Fatalf("invalid cursor: %v", err)
	}
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

func TestRankedCursorAcceptsLongKeys(t *testing.T) {
	ctx := context.Background()
	store := &MemoryStore{}
	idx, _ := Open(store, "index/")
	m, _ := NewSnapshotMirror(idx, store, "copies/")
	long := strings.Repeat("a", 5000)
	f := RankedFields{Text: "needle"}
	data, _ := json.Marshal(f)
	if err := m.ApplyDocuments(ctx, []SnapshotChange{{Key: long, Data: data, Text: f.SearchText()}, {Key: "z", Data: data, Text: f.SearchText()}}); err != nil {
		t.Fatal(err)
	}
	s, err := m.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	decode := func(_ string, data []byte) (RankedFields, bool, error) {
		var fields RankedFields
		err := json.Unmarshal(data, &fields)
		return fields, true, err
	}
	q := Query{All: [][]string{{"needle"}}}
	first, err := s.FindRanked(ctx, q, RankedOptions{Limit: 1}, decode)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.FindRanked(ctx, q, RankedOptions{Limit: 1, After: first.Next}, decode)
	if err != nil || len(second.Documents) != 1 || second.Documents[0].Key != "z" {
		t.Fatalf("next page: %+v %v", second, err)
	}
}

func TestRankedSearchOrdersMatchingAlternativesAndAppliesFilters(t *testing.T) {
	ctx := t.Context()
	store := &MemoryStore{}
	index, err := Open(store, "index/")
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := NewSnapshotMirror(index, store, "copies/")
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]RankedFields{
		"a": {Title: "other", Text: "alpha"},
		"b": {Title: "other", Text: "alpha beta"},
		"c": {Title: "other", Text: "alpha beta"},
		"d": {Title: "other", Text: "alpha beta forbidden"},
		"e": {Title: "other", Tags: []string{"alpha beta"}},
		"f": {Title: "alpha beta"},
	}
	var changes []SnapshotChange
	for key, f := range fields {
		data, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, SnapshotChange{Key: key, Text: f.SearchText(), Data: data})
	}
	if err := mirror.ApplyDocuments(ctx, changes); err != nil {
		t.Fatal(err)
	}
	snapshot, err := mirror.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	decode := func(key string, data []byte) (RankedFields, bool, error) {
		var f RankedFields
		err := json.Unmarshal(data, &f)
		return f, key != "c", err
	}
	query := Query{All: [][]string{{"alpha", "beta"}}, None: []string{"forbidden"}}
	first, err := snapshot.FindRanked(ctx, query, RankedOptions{Limit: 2}, decode)
	if err != nil {
		t.Fatal(err)
	}
	second, err := snapshot.FindRanked(ctx, query, RankedOptions{Limit: 2, After: first.Next}, decode)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for _, item := range append(first.Documents, second.Documents...) {
		got = append(got, item.Key)
	}
	if !reflect.DeepEqual(got, []string{"f", "e", "b", "a"}) || second.Next != "" {
		t.Fatalf("order/filters: %v; next=%q", got, second.Next)
	}
}

func TestExactRankedTitleAlternatives(t *testing.T) {
	q := Query{All: [][]string{{"cat", "feline"}, {"toy", "plaything"}}}
	for _, tc := range []struct {
		title string
		want  bool
	}{
		{"cat toy", true}, {"feline plaything", true},
		{"cat feline toy plaything", true}, {"cat guide toy", false},
		{"cat", false},
	} {
		if got := exactRankedTitle(tc.title, q); got != tc.want {
			t.Errorf("title %q: got %v, want %v", tc.title, got, tc.want)
		}
	}
}

func TestRankedSearchExactAlternativePrecedesTaggedTitle(t *testing.T) {
	ctx := t.Context()
	store := &MemoryStore{}
	index, err := Open(store, "index/")
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := NewSnapshotMirror(index, store, "copies/")
	if err != nil {
		t.Fatal(err)
	}
	var changes []SnapshotChange
	for key, fields := range map[string]RankedFields{
		"a": {Title: "cat guide", Tags: []string{"cat"}},
		"z": {Title: "cat"},
	} {
		data, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, SnapshotChange{Key: key, Text: fields.SearchText(), Data: data})
	}
	if err := mirror.ApplyDocuments(ctx, changes); err != nil {
		t.Fatal(err)
	}
	snapshot, err := mirror.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result, err := snapshot.FindRanked(ctx, Query{All: [][]string{{"cat", "feline"}}}, RankedOptions{}, func(_ string, data []byte) (RankedFields, bool, error) {
		var fields RankedFields
		err := json.Unmarshal(data, &fields)
		return fields, true, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Documents) != 2 || result.Documents[0].Key != "z" {
		t.Fatalf("unexpected order: %+v", result.Documents)
	}
}

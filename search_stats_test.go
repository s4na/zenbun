package zenbun

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type searchStatsTestStore struct {
	Store
	mu    sync.Mutex
	reads SearchStats
}

func (s *searchStatsTestStore) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	obj, err := s.Store.Get(ctx, key, r)
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.HasPrefix(key, "copies/") {
		s.reads.PayloadGets++
		s.reads.PayloadBytes += int64(len(obj.Data))
	} else {
		s.reads.IndexGets++
		s.reads.IndexBytes += int64(len(obj.Data))
	}
	return obj, err
}

func TestSearchStatsMatchStorageReads(t *testing.T) {
	ctx := t.Context()
	backing := &MemoryStore{}
	index, err := Open(backing, "index/")
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := NewSnapshotMirror(index, backing, "copies/")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := mirror.Stage(ctx, "", []SnapshotChange{
		{Key: "a", Text: "needle", Data: []byte("needle")},
		{Key: "b", Text: "needle", Data: []byte("needle twice")},
		{Key: "c", Text: "ne ee ed dl le", Data: []byte("false positive")},
		{Key: "d", Text: "unrelated", Data: []byte("unrelated")},
	})
	if err != nil {
		t.Fatal(err)
	}
	q := Query{All: [][]string{{"needle"}}}
	decode := func(_ string, data []byte) (RankedFields, bool, error) {
		return RankedFields{Text: string(data)}, true, nil
	}

	for _, tc := range []struct {
		name         string
		options      RankedOptions
		wantPayloads int64
		wantBytes    int64
		wantMatches  int64
		wantErr      error
	}{
		{name: "keys", wantPayloads: 3, wantBytes: 32, wantMatches: 2},
		{name: "page", options: RankedOptions{Limit: 1}, wantPayloads: 4, wantBytes: 38, wantMatches: 2},
		{name: "candidate limit", options: RankedOptions{MaxCandidates: 2}, wantErr: ErrRankedLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := func(ctx context.Context) ([]string, SearchStats) {
				store := &searchStatsTestStore{Store: backing}
				index, err := Open(store, "index/")
				if err != nil {
					t.Fatal(err)
				}
				mirror, err := NewSnapshotMirror(index, store, "copies/")
				if err != nil {
					t.Fatal(err)
				}
				snap, err := mirror.OpenSnapshot(ctx, snapshot.Generation())
				if err != nil {
					t.Fatal(err)
				}
				var keys []string
				if tc.options.Limit == 0 {
					keys, err = snap.RankedKeys(ctx, q, tc.options, decode)
				} else {
					page, pageErr := snap.FindRanked(ctx, q, tc.options, decode)
					err = pageErr
					for _, doc := range page.Documents {
						keys = append(keys, doc.Key)
					}
				}
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("search error = %v, want %v", err, tc.wantErr)
				}
				return keys, store.reads
			}
			unmeasuredKeys, unmeasuredReads := run(ctx)
			measuredCtx, stats := WithSearchStats(ctx)
			measuredKeys, measuredReads := run(measuredCtx)
			if !reflect.DeepEqual(unmeasuredKeys, measuredKeys) || unmeasuredReads != measuredReads {
				t.Fatalf("measurement changed results or I/O: %v %+v, want %v %+v", measuredKeys, measuredReads, unmeasuredKeys, unmeasuredReads)
			}
			if measuredReads.IndexGets < 2 || measuredReads.IndexBytes == 0 || measuredReads.PayloadGets != tc.wantPayloads || measuredReads.PayloadBytes != tc.wantBytes {
				t.Fatalf("unexpected reads: %+v", measuredReads)
			}
			want := measuredReads
			want.Candidates = 3
			want.Matches = tc.wantMatches
			if got := stats(); got != want {
				t.Fatalf("stats = %+v, want %+v", got, want)
			}
		})
	}
}

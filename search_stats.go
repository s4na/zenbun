package zenbun

import (
	"context"
	"sync/atomic"
)

// SearchStats describes the reads and ranking work of a search. GET counts
// include failed calls and repeated reads. Byte counts use the data returned
// by Store.Get, excluding transport headers and internal store retries.
type SearchStats struct {
	IndexGets    int64
	IndexBytes   int64
	PayloadGets  int64
	PayloadBytes int64
	// Candidates is the number of keys selected by the index.
	Candidates int64
	// Matches is the number of verified matches before pagination. It is set
	// once ranked verification completes.
	Matches int64
}

type searchStatsKey struct{}

type searchReadKind int

const (
	indexRead searchReadKind = iota
	payloadRead
)

type searchReadStats struct {
	gets  atomic.Int64
	bytes atomic.Int64
}

type searchStatsRecorder struct {
	reads      [2]searchReadStats
	candidates atomic.Int64
	matches    atomic.Int64
}

// WithSearchStats starts collecting stats in a context for a single search.
// Pass it to SnapshotMirror.OpenSnapshot or SnapshotMirror.Snapshot and to
// Snapshot.RankedKeys or Snapshot.FindRanked. Index reads include the manifest
// and any segment data fetched from storage. A cache hit adds no storage reads.
// The returned function can be called after the search, including on error.
func WithSearchStats(ctx context.Context) (context.Context, func() SearchStats) {
	recorder := &searchStatsRecorder{}
	return context.WithValue(ctx, searchStatsKey{}, recorder), recorder.snapshot
}

func (s *searchStatsRecorder) snapshot() SearchStats {
	return SearchStats{
		IndexGets:    s.reads[indexRead].gets.Load(),
		IndexBytes:   s.reads[indexRead].bytes.Load(),
		PayloadGets:  s.reads[payloadRead].gets.Load(),
		PayloadBytes: s.reads[payloadRead].bytes.Load(),
		Candidates:   s.candidates.Load(),
		Matches:      s.matches.Load(),
	}
}

func searchStatsFrom(ctx context.Context) *searchStatsRecorder {
	stats, _ := ctx.Value(searchStatsKey{}).(*searchStatsRecorder)
	return stats
}

func measuredGet(ctx context.Context, store Store, key string, r *ByteRange, kind searchReadKind) (Object, error) {
	obj, err := store.Get(ctx, key, r)
	if stats := searchStatsFrom(ctx); stats != nil {
		stats.reads[kind].gets.Add(1)
		stats.reads[kind].bytes.Add(int64(len(obj.Data)))
	}
	return obj, err
}

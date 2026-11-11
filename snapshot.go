package zenbun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// SnapshotMirror stores immutable document copies and publishes their references
// together with the index. The index must be dedicated to this mirror; do not
// mix its updates with Index.Apply, Sync or Mirror. Old copies are retained so
// readers holding an older snapshot can finish.
type SnapshotMirror struct {
	index  *Index
	store  Store
	prefix string
}

// NewSnapshotMirror associates a dedicated index with immutable copies under
// a separate, non-overlapping prefix.
func NewSnapshotMirror(index *Index, store Store, prefix string) (*SnapshotMirror, error) {
	m, err := NewMirror(index, store, prefix)
	if err != nil {
		return nil, err
	}
	return &SnapshotMirror{index: index, store: store, prefix: m.prefix}, nil
}

// Apply publishes all changes as one index generation. A failed or conflicting
// publication leaves the previous generation intact. Unreferenced copies and
// segments can remain after failures. ETag on Change is ignored.
func (m *SnapshotMirror) Apply(ctx context.Context, changes []Change) error {
	documents := make([]SnapshotChange, len(changes))
	for i, c := range changes {
		documents[i] = SnapshotChange{Key: c.Key, Text: string(c.Data), Data: c.Data, Delete: c.Delete}
	}
	return m.ApplyDocuments(ctx, documents)
}

// SnapshotChange separates searchable text from the stored document payload.
type SnapshotChange struct {
	Key    string
	Text   string
	Data   []byte
	Delete bool
}

// ApplyDocuments atomically publishes searchable text and immutable payloads.
func (m *SnapshotMirror) ApplyDocuments(ctx context.Context, changes []SnapshotChange) error {
	if len(changes) == 0 {
		return nil
	}
	docs, err := snapshotDocuments(changes)
	if err != nil {
		return err
	}
	state, etag, err := m.index.loadManifest(ctx)
	if err != nil {
		return err
	}
	ref, err := m.writeDocuments(ctx, changes, docs)
	if err != nil {
		return err
	}
	state.Segments = append(state.Segments, ref)
	return m.index.publish(ctx, state, etag)
}

func (m *SnapshotMirror) prepareDocuments(ctx context.Context, changes []SnapshotChange) (segmentRef, error) {
	docs, err := snapshotDocuments(changes)
	if err != nil {
		return segmentRef{}, err
	}
	return m.writeDocuments(ctx, changes, docs)
}

func snapshotDocuments(changes []SnapshotChange) (map[string]indexedDocument, error) {
	docs := make(map[string]indexedDocument, len(changes))
	for _, c := range changes {
		if !validMirrorKey(c.Key) || (!c.Delete && !utf8.ValidString(c.Text)) {
			return nil, errors.New("zenbun: invalid document key or UTF-8 content")
		}
		if _, ok := docs[c.Key]; ok {
			return nil, errors.New("zenbun: duplicate key in batch")
		}
		doc := indexedDocument{Deleted: c.Delete}
		if !c.Delete {
			hash := sha256.Sum256(c.Data)
			doc.ETag = hex.EncodeToString(hash[:])
			doc.Terms = terms(c.Text)
		}
		docs[c.Key] = doc
	}
	return docs, nil
}

func (m *SnapshotMirror) writeDocuments(ctx context.Context, changes []SnapshotChange, docs map[string]indexedDocument) (segmentRef, error) {
	for _, c := range changes {
		if c.Delete {
			continue
		}
		hash := docs[c.Key].ETag
		_, err := m.store.Put(ctx, m.prefix+hash, c.Data, PutOptions{IfNoneMatch: true})
		if errors.Is(err, ErrConflict) {
			obj, readErr := m.store.Get(ctx, m.prefix+hash, nil)
			if readErr != nil {
				return segmentRef{}, readErr
			}
			sum := sha256.Sum256(obj.Data)
			if hex.EncodeToString(sum[:]) != hash {
				return segmentRef{}, errors.New("zenbun: invalid immutable document copy")
			}
		} else if err != nil {
			return segmentRef{}, fmt.Errorf("zenbun: save document copy: %w", err)
		}
	}
	return m.index.writeSegment(ctx, docs)
}

// Snapshot holds a consistent index generation. Keep its backing segments and
// copies available for the lifetime of the snapshot.
type Snapshot struct {
	mirror   *SnapshotMirror
	manifest manifest
}

// Snapshot captures the currently published generation. A snapshot of an
// empty index has an empty generation and no documents.
func (m *SnapshotMirror) Snapshot(ctx context.Context) (*Snapshot, error) {
	state, _, err := m.index.loadManifest(ctx)
	if err != nil {
		return nil, err
	}
	return &Snapshot{mirror: m, manifest: state}, nil
}

// Generation changes when the index is updated or compacted.
func (s *Snapshot) Generation() string { return s.manifest.Generation }

// SearchQuery returns candidate keys from this snapshot in key order.
func (s *Snapshot) SearchQuery(ctx context.Context, q Query) ([]string, error) {
	if err := q.validate(); err != nil {
		return nil, err
	}
	return s.mirror.index.searchManifest(ctx, s.manifest, q)
}

// Get reads the document version referenced by the snapshot.
func (s *Snapshot) Get(ctx context.Context, key string) ([]byte, error) {
	if !validMirrorKey(key) {
		return nil, errors.New("zenbun: invalid document key")
	}
	for n := len(s.manifest.Segments) - 1; n >= 0; n-- {
		r, err := s.mirror.index.segment(ctx, s.manifest.Segments[n])
		if err != nil {
			return nil, err
		}
		if !bloomContains(r.bloom, key) {
			continue
		}
		block := sort.Search(len(r.docFirst), func(i int) bool { return r.docFirst[i] > key }) - 1
		if block < 0 {
			continue
		}
		records, err := r.documentBlock(ctx, block)
		if err != nil {
			return nil, err
		}
		j := sort.Search(len(records), func(i int) bool { return records[i].Key >= key })
		if j == len(records) || records[j].Key != key {
			continue
		}
		if records[j].Deleted {
			return nil, ErrNotFound
		}
		hash := records[j].ETag
		if len(hash) != 64 || strings.Trim(hash, "0123456789abcdef") != "" {
			return nil, errors.New("zenbun: invalid copy reference")
		}
		obj, err := s.mirror.store.Get(ctx, s.mirror.prefix+hash, nil)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(obj.Data)
		if hex.EncodeToString(sum[:]) != hash {
			return nil, errors.New("zenbun: invalid immutable document copy")
		}
		return obj.Data, nil
	}
	return nil, ErrNotFound
}

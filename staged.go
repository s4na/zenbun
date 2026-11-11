package zenbun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Stage creates an immutable snapshot based on base. An empty base starts a
// new snapshot. Publishing its ID and serializing competing writers are the
// caller's responsibility. The index must be dedicated to staged snapshots;
// Index.Reclaim cannot be used on it because it only sees the current manifest.
func (m *SnapshotMirror) Stage(ctx context.Context, base string, changes []SnapshotChange) (*Snapshot, error) {
	var state manifest
	if base == "" {
		state = manifest{Version: manifestVersion}
	} else {
		s, err := m.OpenSnapshot(ctx, base)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, fmt.Errorf("zenbun: staged base not found; published snapshots are not staged generations: %w", err)
			}
			return nil, fmt.Errorf("zenbun: open staged base: %w", err)
		}
		state = s.manifest
	}
	if len(changes) > 0 {
		ref, err := m.prepareDocuments(ctx, changes)
		if err != nil {
			return nil, err
		}
		state.Segments = append(state.Segments, ref)
		if _, err := m.index.compactManifest(ctx, &state); err != nil {
			return nil, err
		}
	}
	id, err := randomID()
	if err != nil {
		return nil, err
	}
	state.Generation = id
	data, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	if len(data) > maxMetadataSize {
		return nil, errors.New("zenbun: manifest exceeds size limit")
	}
	if _, err = m.index.store.Put(ctx, m.index.prefix+"snapshots/"+id, data, PutOptions{IfNoneMatch: true}); err != nil {
		return nil, err
	}
	return &Snapshot{mirror: m, manifest: state}, nil
}

// OpenSnapshot opens a previously staged generation by its Generation value.
func (m *SnapshotMirror) OpenSnapshot(ctx context.Context, generation string) (*Snapshot, error) {
	if len(generation) != 32 || strings.Trim(generation, "0123456789abcdef") != "" {
		return nil, errors.New("zenbun: invalid snapshot generation")
	}
	obj, err := m.index.store.Get(ctx, m.index.prefix+"snapshots/"+generation, nil)
	if err != nil {
		return nil, err
	}
	state, err := m.index.decodeManifest(obj.Data)
	if err != nil {
		return nil, err
	}
	if state.Generation != generation {
		return nil, errors.New("zenbun: inconsistent snapshot generation")
	}
	return &Snapshot{mirror: m, manifest: state}, nil
}

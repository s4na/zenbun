package zenbun

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
)

// compactStore coalesces nearby dictionary, document-table and posting reads
// for one sequential merge. The view is private to one input and goroutine.
type compactStore struct {
	Store
	ref     segmentRef
	windows [2]compactWindow
	next    int
}

type compactWindow struct {
	start int64
	data  []byte
}

const (
	compactWindowSize       = 1 << 20
	maxCompactBytes         = 128 << 20
	maxBufferedCompactBytes = 8 << 20
	maxCompactDocuments     = 1 << 17
)

var errCompactSize = errors.New("zenbun: compaction exceeds segment size limit")

func (s *compactStore) Get(ctx context.Context, key string, r *ByteRange) (Object, error) {
	if key != s.ref.Key || r == nil || !validExtent(r.Start, r.End-r.Start, s.ref.Size) {
		return Object{}, ErrInvalidRange
	}
	if err := ctx.Err(); err != nil {
		return Object{}, err
	}
	if r.End-r.Start > compactWindowSize {
		return s.Store.Get(ctx, key, r)
	}

	first, err := s.window(ctx, r.Start)
	if err != nil {
		return Object{}, err
	}
	offset := r.Start - first.start
	if r.End-first.start <= int64(len(first.data)) {
		return Object{Data: first.data[offset : r.End-first.start]}, nil
	}
	// A posting or dictionary can straddle two windows. Fetch only the
	// missing window and join the two pieces of this entry.
	second, err := s.window(ctx, r.End-1)
	if err != nil {
		return Object{}, err
	}
	data := make([]byte, r.End-r.Start)
	n := copy(data, first.data[offset:])
	copy(data[n:], second.data[:r.End-second.start])
	return Object{Data: data}, nil
}

func (s *compactStore) window(ctx context.Context, pos int64) (compactWindow, error) {
	start := pos - pos%compactWindowSize
	for _, cached := range s.windows {
		if cached.start == start && cached.data != nil {
			return cached, nil
		}
	}
	size := min(int64(compactWindowSize), s.ref.Size-start)
	data, err := readRange(ctx, s.Store, s.ref.Key, start, size)
	if err != nil {
		return compactWindow{}, err
	}
	w := compactWindow{start: start, data: data}
	s.windows[s.next] = w
	s.next = (s.next + 1) % len(s.windows)
	return w, nil
}

// compactInput walks an immutable segment's posting entries in term order.
// newIDs maps each input document ID to its output ID, or zero if suppressed.
type compactInput struct {
	reader  *segmentReader
	newIDs  []uint32
	block   int
	entries []dictionaryEntry
	entry   dictionaryEntry
	ids     []uint64
}

func (i *Index) compactInput(ctx context.Context, ref segmentRef) (*compactInput, error) {
	reader, err := openSegment(ctx, i.store, ref)
	if err != nil {
		return nil, err
	}
	reader.store = &compactStore{Store: i.store, ref: ref}
	return &compactInput{reader: reader, newIDs: make([]uint32, ref.Documents+1)}, nil
}

// docCursor pins only the current document block. Each pass constructs new
// cursors so a full output table is never retained between passes.
type docCursor struct {
	input   *compactInput
	records []documentRecord
	id      int
}

func (d *docCursor) peek(ctx context.Context) (*documentRecord, error) {
	if d.id == d.input.reader.ref.Documents {
		return nil, nil
	}
	if len(d.records) == 0 {
		records, err := d.input.reader.documentBlock(ctx, d.id/documentBlockSize)
		if err != nil {
			return nil, err
		}
		d.records = records
	}
	return &d.records[0], nil
}

func (d *docCursor) pop() { d.id++; d.records = d.records[1:] }

func mergeDocuments(ctx context.Context, old, newer *compactInput, oldest bool, w *segmentWriter) (int, error) {
	a, b := docCursor{input: old}, docCursor{input: newer}
	count := 0
	for {
		left, err := a.peek(ctx)
		if err != nil {
			return 0, err
		}
		right, err := b.peek(ctx)
		if err != nil {
			return 0, err
		}
		if left == nil && right == nil {
			return count, nil
		}
		cursor, record := &b, right
		if right == nil || left != nil && left.Key < right.Key {
			cursor, record = &a, left
		} else if left != nil && left.Key == right.Key {
			a.pop()
		}
		cursor.pop()
		if oldest && record.Deleted {
			continue
		}
		count++
		if w != nil {
			if err := w.addDocument(*record); err != nil {
				return 0, err
			}
		} else if !record.Deleted {
			cursor.input.newIDs[cursor.id] = uint32(count)
		}
	}
}

func (in *compactInput) advance(ctx context.Context) error {
	for {
		if len(in.entries) == 0 {
			if in.block == len(in.reader.blocks) {
				in.entry = dictionaryEntry{}
				return nil
			}
			entries, err := in.reader.dictionary(ctx, in.block)
			if err != nil {
				return err
			}
			in.block++
			in.entries = entries
		}
		entry := in.entries[0]
		in.entries = in.entries[1:]
		if !strings.HasPrefix(entry.Term, "t") {
			continue
		}
		if entry.Term <= in.entry.Term || len(entry.Term) == 1 {
			return errors.New("zenbun: invalid posting order")
		}
		in.entry = entry
		return nil
	}
}

func (in *compactInput) mappedPosting(ctx context.Context) ([]uint64, error) {
	entry := in.entry
	in.ids = in.ids[:0]
	var id uint64
	for offset, end := entry.Offset, entry.Offset+entry.Length; offset < end; {
		length := min(int64(64<<10), end-offset)
		data, err := readRange(ctx, in.reader.store, in.reader.ref.Key, offset, length)
		if err != nil {
			return nil, err
		}
		for len(data) > 0 {
			delta, n := binary.Uvarint(data)
			// Retry a split varint from its first byte in the next range.
			if n == 0 && offset+int64(len(data)) < end {
				break
			}
			if n <= 0 || delta == 0 || delta > uint64(in.reader.ref.Documents)-id {
				return nil, errors.New("zenbun: invalid compact posting")
			}
			id += delta
			if mapped := in.newIDs[id]; mapped != 0 {
				in.ids = append(in.ids, uint64(mapped))
			}
			offset += int64(n)
			data = data[n:]
		}
	}
	return in.ids, nil
}

func mergePostingIDs(dst, a, b []uint64) []uint64 {
	dst = dst[:0]
	for len(a) > 0 && len(b) > 0 {
		if a[0] < b[0] {
			dst = append(dst, a[0])
			a = a[1:]
		} else {
			dst = append(dst, b[0])
			b = b[1:]
		}
	}
	dst = append(dst, a...)
	return append(dst, b...)
}

func mergePostings(ctx context.Context, w *segmentWriter, a, b *compactInput) error {
	if err := a.advance(ctx); err != nil {
		return err
	}
	if err := b.advance(ctx); err != nil {
		return err
	}
	var merged []uint64
	for a.entry.Term != "" || b.entry.Term != "" {
		term := a.entry.Term
		if term == "" || b.entry.Term != "" && b.entry.Term < term {
			term = b.entry.Term
		}
		var left, right []uint64
		for _, in := range []*compactInput{a, b} {
			if in.entry.Term != term {
				continue
			}
			ids, err := in.mappedPosting(ctx)
			if err != nil {
				return err
			}
			if in == a {
				left = ids
			} else {
				right = ids
			}
			if err := in.advance(ctx); err != nil {
				return err
			}
		}
		merged = mergePostingIDs(merged, left, right)
		if len(merged) > 0 {
			if err := w.addPosting(term[1:], merged); err != nil {
				return err
			}
		}
	}
	return nil
}

func (i *Index) mergeSegments(ctx context.Context, older, newer segmentRef, oldest bool) (segmentRef, error) {
	a, err := i.compactInput(ctx, older)
	if err != nil {
		return segmentRef{}, err
	}
	b, err := i.compactInput(ctx, newer)
	if err != nil {
		return segmentRef{}, err
	}
	count, err := mergeDocuments(ctx, a, b, oldest, nil)
	if err != nil || count == 0 {
		return segmentRef{}, err
	}
	return i.createSegment(ctx, count, i.compactLimit(), func(w *segmentWriter) error {
		if _, err := mergeDocuments(ctx, a, b, oldest, w); err != nil {
			return err
		}
		return mergePostings(ctx, w, a, b)
	})
}

func (i *Index) compactLimit() int64 {
	if _, ok := i.store.(StreamCreator); ok {
		return maxCompactBytes
	}
	return maxBufferedCompactBytes
}

package zenbun

import (
	"bytes"
	"context"
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

const compactWindowSize = 8 << 20

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
	records []documentRecord
	newIDs  []uint64
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
	var records []documentRecord
	for first := 0; first < len(reader.docFirst); first += documentBatchBlocks {
		last := min(first+documentBatchBlocks, len(reader.docFirst)) - 1
		blocks, err := reader.documentBlockBatch(ctx, first, last)
		if err != nil {
			return nil, err
		}
		for _, block := range blocks {
			records = append(records, block...)
		}
	}
	return &compactInput{reader: reader, records: records, newIDs: make([]uint64, len(records)+1)}, nil
}

func mergeDocumentTables(old, newer *compactInput, oldest bool) []documentRecord {
	var records []documentRecord
	for a, b := 0, 0; a < len(old.records) || b < len(newer.records); {
		input, pos := newer, b
		if b == len(newer.records) || a < len(old.records) && old.records[a].Key < newer.records[b].Key {
			input, pos = old, a
			a++
		} else {
			if a < len(old.records) && old.records[a].Key == newer.records[b].Key {
				a++
			}
			b++
		}
		record := input.records[pos]
		if oldest && record.Deleted {
			continue
		}
		records = append(records, record)
		if !record.Deleted {
			input.newIDs[pos+1] = uint64(len(records))
		}
	}
	old.records, newer.records = nil, nil
	return records
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
	data, err := readRange(ctx, in.reader.store, in.reader.ref.Key, entry.Offset, entry.Length)
	if err != nil {
		return nil, err
	}
	in.ids = in.ids[:0]
	err = validatePosting(data, in.reader.ref.Documents, func(id uint64) {
		if mapped := in.newIDs[id]; mapped != 0 {
			in.ids = append(in.ids, mapped)
		}
	})
	return in.ids, err
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
	records := mergeDocumentTables(a, b, oldest)
	if len(records) == 0 {
		return segmentRef{}, nil
	}
	var out bytes.Buffer
	w := newSegmentWriter(&out, len(records))
	for _, record := range records {
		if err := w.addDocument(record); err != nil {
			return segmentRef{}, err
		}
	}
	if err := mergePostings(ctx, w, a, b); err != nil {
		return segmentRef{}, err
	}
	if err := w.finish(); err != nil {
		return segmentRef{}, err
	}
	return i.storeSegment(ctx, out.Bytes(), len(records))
}

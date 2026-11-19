package zenbun

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// segmentContents rebuilds each v3 document's terms from the postings, since
// v3 records do not store them. The result is as
// large as the segment's postings. Tests use this as an independent decoder.
func segmentContents(ctx context.Context, reader *segmentReader) (map[string]indexedDocument, error) {
	data, err := readRange(ctx, reader.store, reader.ref.Key, 0, reader.ref.Size)
	if err != nil {
		return nil, err
	}
	records := make([]documentRecord, 0, reader.ref.Documents)
	for block := range reader.docFirst {
		part, err := reader.documentBlock(ctx, block)
		if err != nil {
			return nil, err
		}
		records = append(records, part...)
	}
	if len(records) != reader.ref.Documents {
		return nil, errors.New("zenbun: invalid segment document count")
	}
	termsByID := make([][]string, len(records))
	for block := range reader.blocks {
		entries, err := reader.dictionary(ctx, block)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Term, "t") {
				continue
			}
			term := entry.Term[1:]
			err := validatePosting(data[entry.Offset:entry.Offset+entry.Length], len(records), func(id uint64) {
				termsByID[id-1] = append(termsByID[id-1], term)
			})
			if err != nil {
				return nil, err
			}
		}
	}
	docs := make(map[string]indexedDocument, len(records))
	for n, record := range records {
		docs[record.Key] = indexedDocument{Deleted: record.Deleted, ETag: record.ETag, Terms: termsByID[n]}
	}
	return docs, nil
}

func TestMergePostingBlocksPreservesRecordsAndTerms(t *testing.T) {
	ctx := t.Context()
	idx, err := Open(&MemoryStore{}, "index")
	if err != nil {
		t.Fatal(err)
	}
	old, newer, expected := map[string]indexedDocument{}, map[string]indexedDocument{}, map[string]indexedDocument{}
	for n := 0; n < 800; n++ {
		key := fmt.Sprintf("k%04d", n)
		if n%3 != 1 {
			row := indexedDocument{Terms: terms("old" + string(rune(0x4e00+n))), ETag: "v1"}
			old[key] = row
			expected[key] = row
		}
		if n%3 != 0 {
			row := indexedDocument{Terms: terms("new" + string(rune(0x5400+n))), ETag: "v2"}
			if n%5 == 0 {
				row = indexedDocument{Deleted: true}
			}
			newer[key] = row
			expected[key] = row
		}
	}
	a, err := idx.writeSegment(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	b, err := idx.writeSegment(ctx, newer)
	if err != nil {
		t.Fatal(err)
	}
	for _, oldest := range []bool{false, true} {
		if oldest {
			for k, v := range expected {
				if v.Deleted {
					delete(expected, k)
				}
			}
		}
		ref, err := idx.mergeSegments(ctx, a, b, oldest)
		if err != nil {
			t.Fatal(err)
		}
		r, err := idx.segment(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		got, err := segmentContents(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("merged records or postings differ (oldest=%v)", oldest)
		}
	}
}

func TestCompactCrossWindowReadReusesBothRanges(t *testing.T) {
	ctx := t.Context()
	key := "index/segments/one"
	data := make([]byte, compactWindowSize+16)
	copy(data[compactWindowSize-2:], "abcdefgh")
	memory := &MemoryStore{}
	if _, err := memory.Put(ctx, key, data, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	meter := &countingSegmentStore{Store: memory}
	store := compactStore{Store: meter, ref: segmentRef{Key: key, Size: int64(len(data))}}
	for _, tc := range []struct {
		start, end int64
		want       string
	}{
		{compactWindowSize - 2, compactWindowSize, "ab"},
		{compactWindowSize - 1, compactWindowSize + 3, "bcde"},
		{compactWindowSize + 2, compactWindowSize + 6, "efgh"},
		{compactWindowSize - 2, compactWindowSize + 1, "abc"},
	} {
		obj, err := store.Get(ctx, key, &ByteRange{Start: tc.start, End: tc.end})
		if err != nil || string(obj.Data) != tc.want {
			t.Fatalf("range [%d,%d): %q, %v", tc.start, tc.end, obj.Data, err)
		}
	}
	if meter.segmentGets != 2 || meter.segmentBytes != int64(len(data)) {
		t.Fatalf("GETs %d, bytes %d", meter.segmentGets, meter.segmentBytes)
	}
}

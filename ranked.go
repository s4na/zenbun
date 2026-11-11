package zenbun

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrSnapshotChanged means a cursor belongs to another generation or query.
var ErrSnapshotChanged = errors.New("zenbun: search snapshot changed")

// ErrInvalidRankedCursor means a ranked cursor cannot be decoded.
var ErrInvalidRankedCursor = errors.New("zenbun: invalid ranked cursor")

// ErrRankedLimit means a ranked search exceeded its configured work limit.
var ErrRankedLimit = errors.New("zenbun: ranked search work limit exceeded")

// RankedFields describes the searchable fields extracted from a payload.
// Text should contain the body, excluding title and tags.
type RankedFields struct {
	Title string
	Tags  []string
	Text  string
}

// SearchText returns the text to index when using ApplyDocuments.
func (f RankedFields) SearchText() string {
	return f.Title + "\n" + strings.Join(f.Tags, "\n") + "\n" + f.Text
}

// RankedDocument is a verified matching key and its immutable payload.
type RankedDocument struct {
	Key  string
	Data []byte
}

// RankedResult contains one page and, when applicable, a continuation cursor.
type RankedResult struct {
	Documents []RankedDocument
	Next      string
}

// RankedOptions bounds the result and work for a ranked search. Zero limits
// select defaults for Limit and Concurrency, and no work limit otherwise.
type RankedOptions struct {
	Limit         int
	After         string
	Scope         string
	Concurrency   int
	MaxCandidates int
	MaxBytes      int64
}
type rankedCursor struct {
	Generation string `json:"g"`
	Query      string `json:"q"`
	Key        string `json:"k"`
}
type rankedHit struct {
	RankedDocument
	priority, tag, words int
}

// FindRanked verifies all candidates and orders matches by exact title,
// matching title, matching tags, number of distinct matching words, then key. Decode
// extracts fields from the immutable payload and may exclude a document.
// Scope must distinguish application filters used by Decode. Changing a query,
// scope or index generation invalidates the cursor. Every page reads all
// candidate payloads, so common queries can be expensive.
func (s *Snapshot) FindRanked(ctx context.Context, q Query, options RankedOptions, decode func(string, []byte) (RankedFields, bool, error)) (RankedResult, error) {
	if decode == nil || options.Limit < 0 || options.Limit > 1000 || options.Concurrency < 0 || options.Concurrency > maxFindReads || options.MaxCandidates < 0 || options.MaxBytes < 0 {
		return RankedResult{}, errors.New("zenbun: invalid ranked search options")
	}
	if options.Limit == 0 {
		options.Limit = 20
	}
	if options.Concurrency == 0 {
		options.Concurrency = defaultFindReads
	}
	if err := q.validate(); err != nil {
		return RankedResult{}, err
	}
	queryBytes, err := json.Marshal(struct {
		Query Query
		Scope string
	}{q, options.Scope})
	if err != nil {
		return RankedResult{}, err
	}
	hash := sha256.Sum256(queryBytes)
	fingerprint := hex.EncodeToString(hash[:])
	cursor := rankedCursor{}
	if options.After != "" {
		raw, err := base64.RawURLEncoding.DecodeString(options.After)
		if err != nil || json.Unmarshal(raw, &cursor) != nil || cursor.Key == "" {
			return RankedResult{}, ErrInvalidRankedCursor
		}
		if cursor.Generation != s.Generation() || cursor.Query != fingerprint {
			return RankedResult{}, ErrSnapshotChanged
		}
	}
	keys, err := s.SearchQuery(ctx, q)
	if err != nil {
		return RankedResult{}, err
	}
	if options.MaxCandidates > 0 && len(keys) > options.MaxCandidates {
		return RankedResult{}, ErrRankedLimit
	}
	hits := make([]rankedHit, 0, len(keys))
	bodies := readAhead(ctx, keys, options.Concurrency, func(ctx context.Context, key string) ([]byte, bool, error) {
		data, err := s.Get(ctx, key)
		return data, true, err
	})
	defer bodies.stop()
	var bytesRead int64
	for _, key := range keys {
		body := bodies.next()
		if body.err != nil {
			return RankedResult{}, body.err
		}
		bytesRead += int64(len(body.data))
		if options.MaxBytes > 0 && bytesRead > options.MaxBytes {
			return RankedResult{}, ErrRankedLimit
		}
		fields, include, err := decode(key, body.data)
		if err != nil {
			return RankedResult{}, err
		}
		if !include {
			continue
		}
		if !rankMatches(fields.SearchText(), q) {
			continue
		}
		hit := rankedHit{RankedDocument: RankedDocument{Key: key}}
		if rankMatches(fields.Title, q) {
			hit.priority = 2
		}
		words := make([]string, 0, len(q.All))
		for _, group := range q.All {
			words = append(words, group...)
		}
		if len(words) > 0 && normalize(fields.Title) == normalize(strings.Join(words, " ")) {
			hit.priority = 3
		}
		if rankMatches(strings.Join(fields.Tags, "\n"), q) {
			hit.tag = 1
			if hit.priority == 0 {
				hit.priority = 1
			}
		}
		text := normalize(fields.SearchText())
		seen := make(map[string]bool)
		for _, group := range q.All {
			for _, word := range group {
				word = normalize(word)
				if !seen[word] && strings.Contains(text, word) {
					hit.words++
					seen[word] = true
				}
			}
		}
		hits = append(hits, hit)
	}
	sort.Slice(hits, func(i, j int) bool {
		a, b := hits[i], hits[j]
		if a.priority != b.priority {
			return a.priority > b.priority
		}
		if a.tag != b.tag {
			return a.tag > b.tag
		}
		if a.words != b.words {
			return a.words > b.words
		}
		return a.Key < b.Key
	})
	start := 0
	if cursor.Key != "" {
		found := false
		for i, h := range hits {
			if h.Key == cursor.Key {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return RankedResult{}, ErrSnapshotChanged
		}
	}
	end := min(start+options.Limit, len(hits))
	result := RankedResult{Documents: make([]RankedDocument, 0, end-start)}
	for _, h := range hits[start:end] {
		data, err := s.Get(ctx, h.Key)
		if err != nil {
			return RankedResult{}, err
		}
		bytesRead += int64(len(data))
		if options.MaxBytes > 0 && bytesRead > options.MaxBytes {
			return RankedResult{}, ErrRankedLimit
		}
		result.Documents = append(result.Documents, RankedDocument{Key: h.Key, Data: data})
	}
	if end < len(hits) {
		raw, err := json.Marshal(rankedCursor{Generation: s.Generation(), Query: fingerprint, Key: hits[end-1].Key})
		if err != nil {
			return RankedResult{}, fmt.Errorf("zenbun: encode cursor: %w", err)
		}
		result.Next = base64.RawURLEncoding.EncodeToString(raw)
	}
	return result, nil
}
func rankMatches(text string, q Query) bool {
	text = normalize(text)
	for _, group := range q.All {
		matched := false
		for _, word := range group {
			if strings.Contains(text, normalize(word)) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	for _, word := range q.None {
		if strings.Contains(text, normalize(word)) {
			return false
		}
	}
	return true
}

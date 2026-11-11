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

var ErrSnapshotChanged = errors.New("zenbun: search snapshot changed")
var ErrInvalidRankedCursor = errors.New("zenbun: invalid ranked cursor")

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

type RankedDocument struct {
	Key  string
	Data []byte
}
type RankedResult struct {
	Documents []RankedDocument
	Next      string
}
type RankedOptions struct {
	Limit       int
	After       string
	Scope       string
	Concurrency int
}
type rankedCursor struct {
	Generation string `json:"g"`
	Query      string `json:"q"`
	Key        string `json:"k"`
}
type rankedHit struct {
	RankedDocument
	priority, tag, groups int
}

// FindRanked verifies all candidates and orders matches by exact title,
// matching title, matching tags, number of matching groups, then key. Decode
// extracts fields from the immutable payload and may exclude a document.
// Scope must distinguish application filters used by Decode. Changing a query,
// scope or index generation invalidates the cursor. Every page reads all
// candidate payloads, so common queries can be expensive.
func (s *Snapshot) FindRanked(ctx context.Context, q Query, options RankedOptions, decode func(string, []byte) (RankedFields, bool, error)) (RankedResult, error) {
	if decode == nil || options.Limit < 0 || options.Limit > 1000 || options.Concurrency < 0 || options.Concurrency > maxFindReads {
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
	hits := make([]rankedHit, 0, len(keys))
	bodies := readAhead(ctx, keys, options.Concurrency, func(ctx context.Context, key string) ([]byte, bool, error) {
		data, err := s.Get(ctx, key)
		return data, true, err
	})
	defer bodies.stop()
	for _, key := range keys {
		body := bodies.next()
		if body.err != nil {
			return RankedResult{}, body.err
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
			if len(group) != 1 {
				words = nil
				break
			}
			words = append(words, group[0])
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
		for _, group := range q.All {
			for _, word := range group {
				if strings.Contains(text, normalize(word)) {
					hit.groups++
					break
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
		if a.groups != b.groups {
			return a.groups > b.groups
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

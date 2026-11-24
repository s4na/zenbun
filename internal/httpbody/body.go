// Package httpbody applies the storage adapters' response-body limits.
package httpbody

import (
	"io"
	"math"
	"net/http"

	"github.com/s4na/zenbun"
)

// Read leaves closing the body and interpreting the status to the caller.
func Read(resp *http.Response, r *zenbun.ByteRange) ([]byte, error) {
	if resp.StatusCode >= 400 {
		return io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	}
	if r == nil {
		return io.ReadAll(resp.Body)
	}
	if resp.StatusCode != http.StatusPartialContent {
		// The server ignored Range. Its body may be the entire object.
		return nil, nil
	}
	want := r.End - r.Start
	limit := want
	if limit < math.MaxInt64 {
		limit++ // Detect overlong responses, including those sent without a Content-Length.
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err == nil && int64(len(data)) != want {
		return nil, zenbun.ErrInvalidRange
	}
	return data, err
}

package httpbody

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/s4na/zenbun"
)

func TestRangeBody(t *testing.T) {
	r := &zenbun.ByteRange{Start: 10, End: 12}
	for _, tc := range []struct {
		name        string
		status      int
		input, want string
		consumed    int
		err         error
	}{
		{"ignored", 200, "whole object", "", 0, nil},
		{"exact", 206, "ok", "ok", 2, nil},
		{"short", 206, "a", "", 1, zenbun.ErrInvalidRange},
		{"overlong", 206, "whole object", "", 3, zenbun.ErrInvalidRange},
		{"http error", 404, "error", "error", 5, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.NewReader(tc.input)
			data, err := Read(&http.Response{StatusCode: tc.status, Body: io.NopCloser(body)}, r)
			if !errors.Is(err, tc.err) || string(data) != tc.want || len(tc.input)-body.Len() != tc.consumed {
				t.Fatalf("read %q, consumed %d, error %v", data, len(tc.input)-body.Len(), err)
			}
		})
	}
}

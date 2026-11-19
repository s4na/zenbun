package gcsstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/s4na/zenbun"
	"github.com/s4na/zenbun/internal/partupload"
)

// CreateStream creates an object with ifGenerationMatch=0 and sends large
// objects in resumable chunks. A failed session is cancelled independently
// of the producer's context.
func (s *Store) CreateStream(ctx context.Context, key string, produce func(io.Writer) error) error {
	var session string
	var offset int64
	send := func(p []byte, final bool) error {
		if session == "" {
			target := s.objectURL(true, "", url.Values{"uploadType": {"resumable"}, "name": {key}, "ifGenerationMatch": {"0"}})
			status, h, body, err := s.request(ctx, http.MethodPost, target, http.Header{"X-Upload-Content-Type": {"application/octet-stream"}}, nil, false, nil)
			if err != nil {
				return err
			}
			if status != 200 && status != 201 {
				return responseError("initiate resumable", status, body)
			}
			location, err := url.Parse(h.Get("Location"))
			if err != nil || h.Get("Location") == "" {
				return errors.New("gcs: missing resumable session URI")
			}
			uri := s.endpoint.ResolveReference(location)
			if uri.Scheme != s.endpoint.Scheme || uri.Host != s.endpoint.Host || uri.User != nil {
				return errors.New("gcs: unexpected resumable session origin")
			}
			session = uri.String()
		}
		end := offset + int64(len(p))
		total := "*"
		if final {
			total = fmt.Sprint(end)
		}
		headers := http.Header{"Content-Type": {"application/octet-stream"}, "Content-Range": {fmt.Sprintf("bytes %d-%d/%s", offset, end-1, total)}}
		status, h, body, err := s.request(ctx, http.MethodPut, session, headers, p, false, nil)
		if err != nil {
			return err
		}
		if !final {
			if status != 308 {
				return responseError("resumable chunk", status, body)
			}
			if h.Get("Range") != fmt.Sprintf("bytes=0-%d", end-1) {
				return errors.New("gcs: incomplete chunk acknowledgement")
			}
			offset = end
			return nil
		}
		if status != 200 && status != 201 {
			return responseError("finish resumable", status, body)
		}
		var obj objectResource
		if json.Unmarshal(body, &obj) != nil || obj.Generation == "" {
			return errors.New("gcs: missing completed generation")
		}
		return nil
	}
	return partupload.Run(ctx, produce, func(p []byte) error { _, err := s.Put(ctx, key, p, zenbun.PutOptions{IfNoneMatch: true}); return err }, func(p []byte) error { return send(p, false) }, func(p []byte) error { return send(p, true) }, func(cleanup context.Context) error {
		if session == "" {
			return nil
		}
		status, _, body, err := s.request(cleanup, http.MethodDelete, session, nil, nil, false, nil)
		if err != nil {
			return err
		}
		switch status {
		case 200, 204, 404, 410, 499:
			return nil
		}
		return responseError("cancel resumable", status, body)
	})
}

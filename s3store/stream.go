package s3store

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/s4na/zenbun"
	"github.com/s4na/zenbun/internal/partupload"
)

type completedPart struct {
	Number int    `xml:"PartNumber"`
	ETag   string `xml:"ETag"`
}

// CreateStream conditionally creates an object using multipart upload when
// the encoded data exceeds one part. The endpoint must support conditional
// CompleteMultipartUpload.
func (s *Store) CreateStream(ctx context.Context, key string, produce func(io.Writer) error) error {
	var uploadID string
	completion := struct {
		XMLName xml.Name        `xml:"CompleteMultipartUpload"`
		Parts   []completedPart `xml:"Part"`
	}{}
	upload := func(data []byte) error {
		if uploadID == "" {
			status, _, body, err := s.request(ctx, http.MethodPost, key, url.Values{"uploads": {""}}, nil, nil, false, nil)
			if err != nil {
				return err
			}
			if status != 200 {
				return responseError("initiate multipart", status, body)
			}
			var init struct {
				ID string `xml:"UploadId"`
			}
			if xml.Unmarshal(body, &init) != nil || init.ID == "" {
				return errors.New("s3: missing multipart upload ID")
			}
			uploadID = init.ID
		}
		n := len(completion.Parts) + 1
		if n > 10000 {
			return errors.New("s3: multipart upload exceeds 10000 parts")
		}
		status, h, body, err := s.request(ctx, http.MethodPut, key, url.Values{"uploadId": {uploadID}, "partNumber": {strconv.Itoa(n)}}, nil, data, false, nil)
		if err != nil {
			return err
		}
		if status != 200 {
			return responseError("upload part", status, body)
		}
		etag := h.Get("ETag")
		if etag == "" {
			return errors.New("s3: missing part ETag")
		}
		completion.Parts = append(completion.Parts, completedPart{n, etag})
		return nil
	}
	return partupload.Run(ctx, produce, func(p []byte) error { _, err := s.Put(ctx, key, p, zenbun.PutOptions{IfNoneMatch: true}); return err }, upload, func(p []byte) error {
		if err := upload(p); err != nil {
			return err
		}
		data, err := xml.Marshal(completion)
		if err != nil {
			return err
		}
		status, _, body, err := s.request(ctx, http.MethodPost, key, url.Values{"uploadId": {uploadID}}, http.Header{"If-None-Match": {"*"}, "Content-Type": {"application/xml"}}, data, false, nil)
		if err != nil {
			return err
		}
		if status != 200 {
			return responseError("complete multipart", status, body)
		}
		var result struct {
			XMLName xml.Name
			ETag    string
			Code    string
		}
		if err := xml.Unmarshal(body, &result); err != nil {
			return err
		}
		if result.XMLName.Local != "CompleteMultipartUploadResult" || result.ETag == "" {
			return fmt.Errorf("s3: multipart completion failed (%s)", result.Code)
		}
		return nil
	}, func(cleanup context.Context) error {
		if uploadID == "" {
			return nil
		}
		status, _, body, err := s.request(cleanup, http.MethodDelete, key, url.Values{"uploadId": {uploadID}}, nil, nil, false, nil)
		if err != nil {
			return err
		}
		if status == 204 || status == 200 || status == 404 {
			return nil
		}
		return responseError("abort multipart", status, body)
	})
}

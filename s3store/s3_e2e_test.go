package s3store

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/s4na/zenbun/storetest"
)

func TestS3EndToEnd(t *testing.T) {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_ENDPOINT is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	store, err := New(Config{Endpoint: endpoint, Bucket: "zenbun-test", AccessKey: "testaccess", SecretKey: "testsecret"})
	if err != nil {
		t.Fatal(err)
	}
	status, _, _, err := store.request(ctx, http.MethodPut, "", nil, nil, nil, false, nil)
	if err != nil || (status != http.StatusOK && status != http.StatusConflict) {
		t.Fatalf("create bucket: %d %v", status, err)
	}
	storetest.Run(t, store)
	storetest.RunStream(t, store, store)
	// A GET cannot distinguish an aborted upload from leaked uncommitted
	// parts. Inspect the backend's multipart listing as well.
	status, _, body, err := store.request(ctx, http.MethodGet, "", url.Values{"uploads": {""}, "prefix": {"stream/"}}, nil, nil, true, nil)
	if err != nil || status != http.StatusOK {
		t.Fatalf("list unfinished uploads: %d %v", status, err)
	}
	var uploads struct {
		Items []struct{ Key string } `xml:"Upload"`
	}
	if err := xml.Unmarshal(body, &uploads); err != nil || len(uploads.Items) != 0 {
		t.Fatalf("unfinished uploads: %v, %v", uploads.Items, err)
	}
}

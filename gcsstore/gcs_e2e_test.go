package gcsstore

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/s4na/zenbun/storetest"
)

func TestGCSEndToEnd(t *testing.T) {
	endpoint := os.Getenv("GCS_ENDPOINT")
	if endpoint == "" {
		t.Skip("GCS_ENDPOINT is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 30 * time.Second}
	store, err := New(Config{Endpoint: endpoint, Bucket: "zenbun-test", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	target := strings.TrimRight(endpoint, "/") + "/storage/v1/b?project=test"
	status, _, _, err := store.request(ctx, http.MethodPost, target, http.Header{"Content-Type": {"application/json"}}, []byte(`{"name":"zenbun-test"}`), false, nil)
	if err != nil || (status != http.StatusOK && status != http.StatusConflict) {
		t.Fatalf("create bucket: %d %v", status, err)
	}
	storetest.Run(t, store)
	var cancelled bool
	transport := http.DefaultTransport
	client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := transport.RoundTrip(req)
		if err == nil && req.Method == http.MethodDelete && req.URL.Query().Get("upload_id") != "" && req.URL.Query().Get("name") == "stream/abort" {
			cancelled = resp.StatusCode == 499 || resp.StatusCode == 204 || resp.StatusCode == 200
		}
		return resp, err
	})
	storetest.RunStream(t, store, store)
	if !cancelled {
		t.Fatal("backend did not acknowledge cancellation of aborted upload")
	}
}

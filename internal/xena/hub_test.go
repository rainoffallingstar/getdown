package xena

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"getdown/internal/httpx"
)

type stubTransport struct {
	roundTrip func(*http.Request) (*http.Response, error)
}

func (s stubTransport) RoundTrip(r *http.Request) (*http.Response, error) { return s.roundTrip(r) }

func resp(status int, headers map[string]string, body []byte, req *http.Request) *http.Response {
	h := make(http.Header, len(headers))
	for k, v := range headers {
		h.Set(k, v)
	}
	return &http.Response{
		StatusCode:    status,
		Status:        http.StatusText(status),
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

func withStubTransport(t *testing.T, rt http.RoundTripper, fn func()) {
	t.Helper()
	old := httpx.DefaultRoundTripper
	httpx.DefaultRoundTripper = rt
	t.Cleanup(func() { httpx.DefaultRoundTripper = old })
	fn()
}

func TestListDatasetsByPrefix_AllowsNullFields(t *testing.T) {
	withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
		// hubClient always posts to /data/
		if r.URL.Path != "/data/" || r.Method != http.MethodPost {
			return resp(404, nil, []byte("not found"), r), nil
		}
		return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`[
  {"name":"TCGA-CHOL.star_counts.tsv","type":"genomicMatrix","probemap":"pm","status":"loaded"},
  {"name":"TCGA-CHOL.clinical.tsv","type":null,"probemap":null,"status":null}
]`), r), nil
	}}, func() {
		c := newHubClient("https://xena.test")
		ds, err := c.listDatasetsByPrefix(context.Background(), "TCGA-CHOL.")
		if err != nil {
			t.Fatalf("listDatasetsByPrefix: %v", err)
		}
		if len(ds) != 2 {
			t.Fatalf("datasets len: got %d want %d", len(ds), 2)
		}
		if ds[1].Name != "TCGA-CHOL.clinical.tsv" {
			t.Fatalf("unexpected dataset: %+v", ds[1])
		}
		if ds[1].Type != "" || ds[1].Probemap != "" || ds[1].Status != "" {
			t.Fatalf("expected nulls to decode as empty strings, got: %+v", ds[1])
		}
	})
}


package xena

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
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

func withEnv(t *testing.T, key, value string, fn func()) {
	t.Helper()
	old, had := os.LookupEnv(key)
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("setenv %s: %v", key, err)
	}
	defer func() {
		if !had {
			_ = os.Unsetenv(key)
		} else {
			_ = os.Setenv(key, old)
		}
	}()
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

func TestSearchDatasets_IncludesLongTitle(t *testing.T) {
	withEnv(t, "GETDOWN_XENA_HUB", "https://xena.test", func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			if r.URL.Path != "/data/" || r.Method != http.MethodPost {
				return resp(404, nil, []byte("not found"), r), nil
			}
			return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`[
  {"name":"TCGA-LAML.star_counts.tsv","longtitle":"Acute Myeloid Leukemia STAR counts","type":"genomicMatrix","probemap":"pm1","status":"loaded"}
]`), r), nil
		}}, func() {
			ds, err := SearchDatasets(context.Background(), "leukemia", 10)
			if err != nil {
				t.Fatalf("SearchDatasets: %v", err)
			}
			if len(ds) != 1 {
				t.Fatalf("datasets len: got %d want 1", len(ds))
			}
			if ds[0].LongTitle != "Acute Myeloid Leukemia STAR counts" {
				t.Fatalf("unexpected long title: %+v", ds[0])
			}
		})
	})
}

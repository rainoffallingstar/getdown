package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"getdown/internal/httpx"
)

type stubTransport struct {
	roundTrip func(*http.Request) (*http.Response, error)
}

func (s stubTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return s.roundTrip(r)
}

func withStubTransport(t *testing.T, rt http.RoundTripper, fn func()) {
	t.Helper()
	old := httpx.DefaultRoundTripper
	httpx.DefaultRoundTripper = rt
	t.Cleanup(func() { httpx.DefaultRoundTripper = old })
	fn()
}

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

func TestE2E_GEO_LocalServer(t *testing.T) {
	seriesMatrix := strings.Join([]string{
		"!Series_title\tExample",
		"!Series_supplementary_file\tSUP_URL",
		"!Sample_title\tS1\tS2",
		"!series_matrix_table_begin",
		"ID_REF\tGSM1\tGSM2",
		"geneA\t1\t2",
		"geneB\t3\t4",
		"!series_matrix_table_end",
		"",
	}, "\n")

	var supName = "a.txt"
	var supBody = "hello\n"

	base := "https://geo.test"
	withEnv(t, "GETDOWN_GEO_FTP_BASE", base, func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "_series_matrix.txt.gz"):
				var buf bytes.Buffer
				writeGzip(&buf, []byte(strings.ReplaceAll(seriesMatrix, "SUP_URL", base+"/supp/"+supName)))
				return resp(200, map[string]string{"Content-Type": "application/x-gzip"}, buf.Bytes(), r), nil
			case r.Method == http.MethodGet && r.URL.Path == "/supp/"+supName:
				return resp(200, map[string]string{"Content-Type": "text/plain"}, []byte(supBody), r), nil
			default:
				return resp(404, nil, []byte("not found"), r), nil
			}
		}}, func() {
			outDir := t.TempDir()
			code := runWithArgs([]string{
				"getdown",
				"geo",
				"--gse", "GSE37815",
				"--out", outDir,
				"--sup",
				"--timeout", "30s",
			})
			if code != 0 {
				t.Fatalf("exit code=%d", code)
			}

			expr, err := os.ReadFile(filepath.Join(outDir, "expression.tsv"))
			if err != nil {
				t.Fatalf("read expression.tsv: %v", err)
			}
			if !strings.Contains(string(expr), "ID_REF\tGSM1\tGSM2\n") {
				t.Fatalf("expression.tsv missing header")
			}
			if !strings.Contains(string(expr), "geneA\t1\t2\n") {
				t.Fatalf("expression.tsv missing row")
			}

			if _, err := os.Stat(filepath.Join(outDir, "supplementary", supName)); err != nil {
				t.Fatalf("supplementary missing: %v", err)
			}
		})
	})
}

func TestE2E_GEO_HeaderOnly_FallsBackToSup_And_DownloadsGPLAnnot(t *testing.T) {
	seriesMatrix := strings.Join([]string{
		"!Series_title\tExample",
		"!Series_type\tExpression profiling by array",
		"!Series_platform_id\tGPL123",
		"!Series_supplementary_file\tSUP_URL",
		"!series_matrix_table_begin",
		"ID_REF\tGSM1\tGSM2",
		"!series_matrix_table_end",
		"",
	}, "\n")

	var supName = "a.txt"
	var supBody = "hello\n"
	var gplAnnotBody = "ID\tGene Symbol\np1\tGENE1\n"

	base := "https://geo.test"
	withEnv(t, "GETDOWN_GEO_FTP_BASE", base, func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "_series_matrix.txt.gz"):
				var buf bytes.Buffer
				writeGzip(&buf, []byte(strings.ReplaceAll(seriesMatrix, "SUP_URL", base+"/supp/"+supName)))
				return resp(200, map[string]string{"Content-Type": "application/x-gzip"}, buf.Bytes(), r), nil
			case r.Method == http.MethodGet && r.URL.Path == "/supp/"+supName:
				return resp(200, map[string]string{"Content-Type": "text/plain"}, []byte(supBody), r), nil
			case r.Method == http.MethodGet && r.URL.Path == "/geo/platforms/GPLnnn/GPL123/annot/GPL123.annot.gz":
				var buf bytes.Buffer
				writeGzip(&buf, []byte(gplAnnotBody))
				return resp(200, map[string]string{"Content-Type": "application/x-gzip"}, buf.Bytes(), r), nil
			default:
				return resp(404, nil, []byte("not found"), r), nil
			}
		}}, func() {
			outDir := t.TempDir()
			code := runWithArgs([]string{
				"getdown",
				"geo",
				"--gse", "GSE37815",
				"--out", outDir,
				"--timeout", "30s",
			})
			if code != 0 {
				t.Fatalf("exit code=%d", code)
			}
			if _, err := os.Stat(filepath.Join(outDir, "supplementary", supName)); err != nil {
				t.Fatalf("supplementary missing: %v", err)
			}
			if _, err := os.Stat(filepath.Join(outDir, "platform", "GPL123.annot.gz")); err != nil {
				t.Fatalf("platform annotation missing: %v", err)
			}
		})
	})
}

func TestE2E_TCGA_Xena_DownloadsAllOmics_LocalServer(t *testing.T) {
	project := "TCGA-FAKE"

	base := "https://xena.test"
	withEnv(t, "GETDOWN_XENA_HUB", base, func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			if r.URL.Path != "/data/" || r.Method != http.MethodPost {
				return resp(404, nil, []byte("not found"), r), nil
			}
			b, _ := io.ReadAll(r.Body)
			body := string(b)
			switch {
				case strings.Contains(body, ":from [:dataset]") && strings.Contains(body, `:like :name "TCGA-FAKE.%`):
					return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`[`+
						`{"name":"`+project+`.star_counts.tsv","type":"genomicMatrix","probemap":"pm1","status":"loaded"},`+
						`{"name":"`+project+`.clinical.tsv","type":"clinicalMatrix","probemap":"","status":"loaded"},`+
						`{"name":"`+project+`.mirna.tsv","type":"genomicMatrix","probemap":null,"status":"loaded"},`+
						`{"name":"`+project+`.somaticmutation_wxs.tsv","type":"mutationVector","probemap":"","status":"loaded"}`+
						`]`), r), nil
			case strings.Contains(body, `:field.name "sampleID"`) && strings.Contains(body, project+`.star_counts.tsv`):
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`["S1","S2"]`), r), nil
			case strings.Contains(body, "fn [probemap limit offset]") && strings.Contains(body, "pm1"):
				if strings.HasSuffix(strings.TrimSpace(body), "0)") {
					return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"name":["ENSG1","ENSG2"]}`), r), nil
				}
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"name":[]}`), r), nil
			case strings.Contains(body, "fn [dataset samples probes]") && strings.Contains(body, project+`.star_counts.tsv`):
				// Mixed types: numbers as strings can happen on real hubs.
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`[["5","7"],["10","11"]]`), r), nil
			case strings.Contains(body, ":select [:field.name]") && strings.Contains(body, project+`.clinical.tsv`):
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`["sampleID","age","group"]`), r), nil
			case strings.Contains(body, `:field.name "sampleID"`) && strings.Contains(body, project+`.clinical.tsv`):
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`["S1","S2"]`), r), nil
			case strings.Contains(body, "fn [dataset samples probes]") && strings.Contains(body, project+`.clinical.tsv`):
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`[["10","20"],["A","B"]]`), r), nil
			case strings.Contains(body, ":select [:field.name]") && strings.Contains(body, project+`.somaticmutation_wxs.tsv`):
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`["sample","gene","effect"]`), r), nil
			case strings.Contains(body, "fn [dataset fields limit offset]") && strings.Contains(body, project+`.somaticmutation_wxs.tsv`):
				if strings.HasSuffix(strings.TrimSpace(body), "0)") {
					return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"sample":["S1","S2"],"gene":["TP53","DNMT3A"],"effect":["missense","nonsense"]}`), r), nil
				}
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"sample":[],"gene":[],"effect":[]}`), r), nil
			default:
				return resp(400, nil, []byte("unhandled edn"), r), nil
			}
		}}, func() {
		outDir := t.TempDir()
		code := runWithArgs([]string{
			"getdown",
			"tcga",
			"--project", project,
			"--out", outDir,
			"--provider", "xena",
			"--timeout", "30s",
		})
		if code != 0 {
			t.Fatalf("exit code=%d", code)
		}

		// Stable top-level files.
		expr, err := os.ReadFile(filepath.Join(outDir, "expression.tsv"))
		if err != nil {
			t.Fatalf("read expression.tsv: %v", err)
		}
		if !strings.Contains(string(expr), "ENSG1\t5\t7\n") {
			t.Fatalf("expression.tsv unexpected content")
		}
		if _, err := os.Stat(filepath.Join(outDir, "omics", project+".star_counts.tsv")); err != nil {
			t.Fatalf("omics expression missing: %v", err)
		}
		if _, err := os.Stat(filepath.Join(outDir, "omics", project+".clinical.tsv")); err != nil {
			t.Fatalf("omics clinical missing: %v", err)
		}
			if _, err := os.Stat(filepath.Join(outDir, "omics", project+".somaticmutation_wxs.tsv")); err != nil {
				t.Fatalf("omics mutation missing: %v", err)
			}
			if _, err := os.Stat(filepath.Join(outDir, "omics", "_skipped.tsv")); err != nil {
				t.Fatalf("omics skipped report missing: %v", err)
			}
			})
		})
	}

func TestE2E_TCGA_GDCOnly_LocalServer(t *testing.T) {
	project := "TCGA-OK"

	base := "https://gdc.test/gdc"
	withEnv(t, "GETDOWN_GDC_BASE", base, func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/gdc/files":
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"data":{"hits":[`+
					`{"file_id":"f1","file_name":"f1.txt","cases":[{"case_id":"c1","submitter_id":"`+project+`-C1","samples":[{"submitter_id":"S1"}]}]},`+
					`{"file_id":"f2","file_name":"f2.txt","cases":[{"case_id":"c2","submitter_id":"`+project+`-C2","samples":[{"submitter_id":"S2"}]}]}`+
					`]}}`), r), nil
			case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/gdc/data/"):
				id := strings.TrimPrefix(r.URL.Path, "/gdc/data/")
				if id == "f1" {
					return resp(200, map[string]string{"Content-Type": "text/plain"}, []byte("ENSG1\t5\nENSG2\t10\n__no_feature\t0\n"), r), nil
				}
				return resp(200, map[string]string{"Content-Type": "text/plain"}, []byte("ENSG1\t7\nENSG2\t11\n__no_feature\t0\n"), r), nil
			case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/gdc/cases"):
				return resp(200, map[string]string{"Content-Type": "text/tab-separated-values"}, []byte("submitter_id\tcase_id\n"+project+"-C1\tc1\n"+project+"-C2\tc2\n"), r), nil
			default:
				return resp(404, nil, []byte("not found"), r), nil
			}
		}}, func() {
			outDir := t.TempDir()
			code := runWithArgs([]string{
				"getdown",
				"tcga",
				"--project", project,
				"--out", outDir,
				"--provider", "gdc",
				"--timeout", "30s",
			})
			if code != 0 {
				t.Fatalf("exit code=%d", code)
			}

			expr, err := os.ReadFile(filepath.Join(outDir, "expression.tsv"))
			if err != nil {
				t.Fatalf("read expression.tsv: %v", err)
			}
			if !strings.HasPrefix(string(expr), "gene_id\tS1\tS2\n") {
				t.Fatalf("expression.tsv header unexpected:\n%s", string(expr[:min(len(expr), 80)]))
			}
			if !strings.Contains(string(expr), "ENSG2\t10\t11\n") {
				t.Fatalf("expression.tsv missing expected row")
			}
		})
	})
}

func runWithArgs(args []string) int {
	old := os.Args
	os.Args = args
	defer func() { os.Args = old }()
	return run()
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

func writeGzip(w io.Writer, body []byte) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(body)
	_ = gz.Close()
	_, _ = w.Write(buf.Bytes())
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

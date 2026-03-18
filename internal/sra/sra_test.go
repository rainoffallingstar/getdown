package sra

import (
	"bytes"
	"context"
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

func TestCollectFiles_AutoPrefersFastq(t *testing.T) {
	files := CollectFiles([]RunInfo{{
		RunAccession: "SRR1",
		FastqFTP:     "ftp.sra.ebi.ac.uk/a.fastq.gz;ftp.sra.ebi.ac.uk/b.fastq.gz",
		SubmittedFTP: "ftp.sra.ebi.ac.uk/a.bam",
		SRAFTP:       "ftp.sra.ebi.ac.uk/a.sra",
	}}, "auto")
	if len(files) != 2 {
		t.Fatalf("files len: got %d want 2", len(files))
	}
	if files[0].Kind != "fastq" || files[1].Kind != "fastq" {
		t.Fatalf("unexpected kinds: %+v", files)
	}
	if !strings.HasPrefix(files[0].URL, "https://") {
		t.Fatalf("expected https URL, got %s", files[0].URL)
	}
}

func TestFetchRunInfo_ParsesTSV(t *testing.T) {
	withEnv(t, "GETDOWN_ENA_API_BASE", "https://ena.test/filereport", func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodGet || r.URL.Host != "ena.test" {
				return resp(404, nil, []byte("not found"), r), nil
			}
			body := strings.Join([]string{
				"study_accession\texperiment_accession\tsample_accession\trun_accession\tscientific_name\tinstrument_platform\tinstrument_model\tlibrary_layout\tlibrary_strategy\tlibrary_source\tlibrary_selection\tfastq_ftp\tsubmitted_ftp\tsra_ftp\tfastq_bytes\tsubmitted_bytes\tsra_bytes",
				"SRP1\tSRX1\tSRS1\tSRR1\tHomo sapiens\tILLUMINA\tNovaSeq\tPAIRED\tRNA-Seq\tTRANSCRIPTOMIC\tcDNA\tftp.sra.ebi.ac.uk/a_1.fastq.gz;ftp.sra.ebi.ac.uk/a_2.fastq.gz\t\tftp.sra.ebi.ac.uk/a.sra\t10;20\t\t30",
				"",
			}, "\n")
			return resp(200, map[string]string{"Content-Type": "text/tab-separated-values"}, []byte(body), r), nil
		}}, func() {
			runs, err := FetchRunInfo(context.Background(), "SRR1")
			if err != nil {
				t.Fatalf("FetchRunInfo: %v", err)
			}
			if len(runs) != 1 {
				t.Fatalf("runs len: got %d want 1", len(runs))
			}
			if runs[0].RunAccession != "SRR1" || runs[0].ScientificName != "Homo sapiens" {
				t.Fatalf("unexpected run: %+v", runs[0])
			}
		})
	})
}

func TestDownload_DirectLinks(t *testing.T) {
	withEnv(t, "GETDOWN_ENA_API_BASE", "https://ena.test/filereport", func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && r.URL.Host == "ena.test":
				body := strings.Join([]string{
					"study_accession\texperiment_accession\tsample_accession\trun_accession\tscientific_name\tinstrument_platform\tinstrument_model\tlibrary_layout\tlibrary_strategy\tlibrary_source\tlibrary_selection\tfastq_ftp\tsubmitted_ftp\tsra_ftp\tfastq_bytes\tsubmitted_bytes\tsra_bytes",
					"SRP1\tSRX1\tSRS1\tSRR1\tHomo sapiens\tILLUMINA\tNovaSeq\tPAIRED\tRNA-Seq\tTRANSCRIPTOMIC\tcDNA\tftp.sra.ebi.ac.uk/a_1.fastq.gz;ftp.sra.ebi.ac.uk/a_2.fastq.gz\t\t\t10;20\t\t",
					"",
				}, "\n")
				return resp(200, map[string]string{"Content-Type": "text/tab-separated-values"}, []byte(body), r), nil
			case r.Method == http.MethodGet && r.URL.Host == "ftp.sra.ebi.ac.uk":
				return resp(200, map[string]string{"Content-Type": "application/gzip"}, []byte("FASTQ"), r), nil
			default:
				return resp(404, nil, []byte("not found"), r), nil
			}
		}}, func() {
			outDir := t.TempDir()
			res, err := Download(context.Background(), Request{
				Accession: "SRR1",
				OutDir:    outDir,
				Kind:      "auto",
			})
			if err != nil {
				t.Fatalf("Download: %v", err)
			}
			if len(res.Files) != 2 {
				t.Fatalf("downloaded files len: got %d want 2", len(res.Files))
			}
			if _, err := os.Stat(filepath.Join(outDir, "runinfo.tsv")); err != nil {
				t.Fatalf("missing runinfo.tsv: %v", err)
			}
			if _, err := os.Stat(filepath.Join(outDir, "links.tsv")); err != nil {
				t.Fatalf("missing links.tsv: %v", err)
			}
			if _, err := os.Stat(filepath.Join(outDir, "metadata.json")); err != nil {
				t.Fatalf("missing metadata.json: %v", err)
			}
		})
	})
}

func TestSearchKeyword_ParsesESummary(t *testing.T) {
	withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Host, "eutils.ncbi.nlm.nih.gov") && strings.HasSuffix(r.URL.Path, "/esearch.fcgi"):
			return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"esearchresult":{"idlist":["1"]}}`), r), nil
		case r.Method == http.MethodGet && strings.Contains(r.URL.Host, "eutils.ncbi.nlm.nih.gov") && strings.HasSuffix(r.URL.Path, "/esummary.fcgi"):
			return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"result":{"uids":["1"],"1":{"uid":"1","caption":"SRR123456","title":"AML RNA-seq","extra":"Runs: 1"}}}`), r), nil
		default:
			return resp(404, nil, []byte("not found"), r), nil
		}
	}}, func() {
		records, err := Search(context.Background(), "leukemia", 10)
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(records) != 1 {
			t.Fatalf("records len: got %d want 1", len(records))
		}
		if records[0].Accession != "SRR123456" {
			t.Fatalf("unexpected record: %+v", records[0])
		}
	})
}

package main

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

func captureOutput(t *testing.T, fn func()) (stdout string, stderr string) {
	t.Helper()

	oldOut := os.Stdout
	oldErr := os.Stderr
	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stdout: %v", err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stderr: %v", err)
	}
	os.Stdout = wOut
	os.Stderr = wErr

	var outBuf bytes.Buffer
	var errBuf bytes.Buffer
	doneOut := make(chan struct{})
	doneErr := make(chan struct{})
	go func() {
		_, _ = io.Copy(&outBuf, rOut)
		close(doneOut)
	}()
	go func() {
		_, _ = io.Copy(&errBuf, rErr)
		close(doneErr)
	}()

	fn()

	_ = wOut.Close()
	_ = wErr.Close()
	os.Stdout = oldOut
	os.Stderr = oldErr
	<-doneOut
	<-doneErr
	return outBuf.String(), errBuf.String()
}

func TestSearch_GEO_Accession(t *testing.T) {
	withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			return resp(404, nil, []byte("not found"), r), nil
		}
		switch {
		case strings.Contains(r.URL.Host, "eutils.ncbi.nlm.nih.gov") && strings.HasSuffix(r.URL.Path, "/esearch.fcgi"):
			return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"esearchresult":{"idlist":["200235527"]}}`), r), nil
		case strings.Contains(r.URL.Host, "eutils.ncbi.nlm.nih.gov") && strings.HasSuffix(r.URL.Path, "/esummary.fcgi"):
			return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"result":{"uids":["200235527"],"200235527":{"accession":"GSE235527","title":"T","summary":"S"}}}`), r), nil
		default:
			return resp(404, nil, []byte("not found"), r), nil
		}
	}}, func() {
		stdout, stderr := captureOutput(t, func() {
			code := runWithArgs([]string{"getdown", "search", "--source", "geo", "--no-header", "gse235527"})
			if code != 0 {
				t.Fatalf("exit code=%d", code)
			}
		})
		if stderr != "" {
			t.Fatalf("unexpected stderr: %s", stderr)
		}
		if !strings.Contains(stdout, "geo\tGSE235527\tT\t") {
			t.Fatalf("unexpected stdout:\n%s", stdout)
		}
	})
}

func TestSearch_TCGA_Project_IncludesXenaDatasetCount(t *testing.T) {
	withEnv(t, "GETDOWN_XENA_HUB", "https://xena.test", func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && strings.Contains(r.URL.Host, "api.gdc.cancer.gov") && strings.HasPrefix(r.URL.Path, "/projects/TCGA-LAML"):
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"data":{"project_id":"TCGA-LAML","name":"Acute Myeloid Leukemia","primary_site":["Hematopoietic"],"disease_type":["Myeloid Leukemias"]}}`), r), nil
			case r.Method == http.MethodPost && r.URL.Host == "xena.test" && r.URL.Path == "/data/":
				b, _ := io.ReadAll(r.Body)
				if !strings.Contains(string(b), `:like :name "TCGA-LAML.%`) {
					return resp(400, nil, []byte("unexpected edn"), r), nil
				}
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`[`+
					`{"name":"TCGA-LAML.star_counts.tsv","type":"genomicMatrix","probemap":"pm1","status":"loaded"}`+
					`]`), r), nil
			default:
				return resp(404, nil, []byte("not found"), r), nil
			}
		}}, func() {
			stdout, _ := captureOutput(t, func() {
				code := runWithArgs([]string{"getdown", "search", "--source", "tcga", "--no-header", "TCGA-LAML"})
				if code != 0 {
					t.Fatalf("exit code=%d", code)
				}
			})
			if !strings.Contains(stdout, "tcga\tTCGA-LAML\tAcute Myeloid Leukemia\t") {
				t.Fatalf("unexpected stdout:\n%s", stdout)
			}
			if !strings.Contains(stdout, "xena_datasets=1") {
				t.Fatalf("missing xena_datasets in stdout:\n%s", stdout)
			}
		})
	})
}

func TestSearch_TCGA_Keyword(t *testing.T) {
	withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			return resp(404, nil, []byte("not found"), r), nil
		}
		if strings.Contains(r.URL.Host, "api.gdc.cancer.gov") && r.URL.Path == "/projects" {
			return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"data":{"hits":[`+
				`{"project_id":"TCGA-LAML","name":"Acute Myeloid Leukemia","primary_site":["Hematopoietic"],"disease_type":["Myeloid Leukemias"]},`+
				`{"project_id":"TCGA-CHOL","name":"Cholangiocarcinoma","primary_site":["Liver"],"disease_type":["Adenomas and Adenocarcinomas"]}`+
				`]}}`), r), nil
		}
		return resp(404, nil, []byte("not found"), r), nil
	}}, func() {
		stdout, stderr := captureOutput(t, func() {
			code := runWithArgs([]string{"getdown", "search", "--source", "tcga", "--no-header", "leukemia"})
			if code != 0 {
				t.Fatalf("exit code=%d", code)
			}
		})
		if stderr != "" {
			t.Fatalf("unexpected stderr: %s", stderr)
		}
		if !strings.Contains(stdout, "tcga\tTCGA-LAML\tAcute Myeloid Leukemia\t") {
			t.Fatalf("unexpected stdout:\n%s", stdout)
		}
		if strings.Contains(stdout, "TCGA-CHOL") {
			t.Fatalf("unexpected match TCGA-CHOL:\n%s", stdout)
		}
	})
}

package main

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
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

func captureOutputWithInput(t *testing.T, input string, fn func()) (stdout string, stderr string) {
	t.Helper()

	oldIn := os.Stdin
	rIn, wIn, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stdin: %v", err)
	}
	if _, err := io.WriteString(wIn, input); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	_ = wIn.Close()
	os.Stdin = rIn
	defer func() {
		os.Stdin = oldIn
		_ = rIn.Close()
	}()

	return captureOutput(t, fn)
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

func TestSearch_JSONOutput(t *testing.T) {
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
			code := runWithArgs([]string{"getdown", "search", "--source", "geo", "--json", "gse235527"})
			if code != 0 {
				t.Fatalf("exit code=%d", code)
			}
		})
		if stderr != "" {
			t.Fatalf("unexpected stderr: %s", stderr)
		}
		if !strings.Contains(stdout, `"source": "geo"`) || !strings.Contains(stdout, `"id": "GSE235527"`) {
			t.Fatalf("unexpected json stdout:\n%s", stdout)
		}
	})
}

func TestSearch_SRA_Accession(t *testing.T) {
	withEnv(t, "GETDOWN_ENA_API_BASE", "https://ena.test/filereport", func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && r.URL.Host == "ena.test":
				body := strings.Join([]string{
					"study_accession\texperiment_accession\tsample_accession\trun_accession\tscientific_name\tinstrument_platform\tinstrument_model\tlibrary_layout\tlibrary_strategy\tlibrary_source\tlibrary_selection\tfastq_ftp\tsubmitted_ftp\tsra_ftp\tfastq_bytes\tsubmitted_bytes\tsra_bytes",
					"SRP1\tSRX1\tSRS1\tSRR123456\tHomo sapiens\tILLUMINA\tNovaSeq\tPAIRED\tRNA-Seq\tTRANSCRIPTOMIC\tcDNA\tftp.sra.ebi.ac.uk/a.fastq.gz\t\t\t10\t\t",
					"",
				}, "\n")
				return resp(200, map[string]string{"Content-Type": "text/tab-separated-values"}, []byte(body), r), nil
			default:
				return resp(404, nil, []byte("not found"), r), nil
			}
		}}, func() {
			stdout, stderr := captureOutput(t, func() {
				code := runWithArgs([]string{"getdown", "search", "--source", "sra", "--no-header", "SRR123456"})
				if code != 0 {
					t.Fatalf("exit code=%d", code)
				}
			})
			if stderr != "" {
				t.Fatalf("unexpected stderr: %s", stderr)
			}
			if !strings.Contains(stdout, "sra\tSRR123456\tHomo sapiens\t") {
				t.Fatalf("unexpected stdout:\n%s", stdout)
			}
		})
	})
}

func TestSearch_Interactive_Download_GEO(t *testing.T) {
	seriesMatrix := strings.Join([]string{
		"!Series_title\tExample",
		"!Sample_title\tS1\tS2",
		"!series_matrix_table_begin",
		"ID_REF\tGSM1\tGSM2",
		"geneA\t1\t2",
		"!series_matrix_table_end",
		"",
	}, "\n")

	base := "https://geo.test"
	withEnv(t, "GETDOWN_GEO_FTP_BASE", base, func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && strings.Contains(r.URL.Host, "eutils.ncbi.nlm.nih.gov") && strings.HasSuffix(r.URL.Path, "/esearch.fcgi"):
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"esearchresult":{"idlist":["200235527"]}}`), r), nil
			case r.Method == http.MethodGet && strings.Contains(r.URL.Host, "eutils.ncbi.nlm.nih.gov") && strings.HasSuffix(r.URL.Path, "/esummary.fcgi"):
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"result":{"uids":["200235527"],"200235527":{"accession":"GSE235527","title":"T","summary":"S"}}}`), r), nil
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "_series_matrix.txt.gz"):
				var buf bytes.Buffer
				writeGzip(&buf, []byte(seriesMatrix))
				return resp(200, map[string]string{"Content-Type": "application/x-gzip"}, buf.Bytes(), r), nil
			default:
				return resp(404, nil, []byte("not found"), r), nil
			}
		}}, func() {
			baseOut := t.TempDir()
			stdout, stderr := captureOutputWithInput(t, "1\n", func() {
				code := runWithArgs([]string{
					"getdown", "search",
					"--source", "geo",
					"--interactive",
					"--download-out", baseOut,
					"gse235527",
				})
				if code != 0 {
					t.Fatalf("exit code=%d", code)
				}
			})
			if stderr != "" {
				t.Fatalf("unexpected stderr: %s", stderr)
			}
			if !strings.Contains(stdout, "Downloaded to") {
				t.Fatalf("unexpected stdout:\n%s", stdout)
			}
			if _, err := os.Stat(filepath.Join(baseOut, "geo_GSE235527", "expression.tsv")); err != nil {
				t.Fatalf("missing downloaded expression.tsv: %v", err)
			}
		})
	})
}

func TestSearch_All_Project_ReturnsTCGAAndXena(t *testing.T) {
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
					`{"name":"TCGA-LAML.star_counts.tsv","longtitle":"STAR counts","type":"genomicMatrix","probemap":"pm1","status":"loaded"}`+
					`]`), r), nil
			default:
				return resp(404, nil, []byte("not found"), r), nil
			}
		}}, func() {
			stdout, _ := captureOutput(t, func() {
				code := runWithArgs([]string{"getdown", "search", "--source", "all", "--no-header", "TCGA-LAML"})
				if code != 0 {
					t.Fatalf("exit code=%d", code)
				}
			})
			if !strings.Contains(stdout, "tcga\tTCGA-LAML\tAcute Myeloid Leukemia\t") {
				t.Fatalf("unexpected stdout:\n%s", stdout)
			}
			if !strings.Contains(stdout, "xena\tTCGA-LAML\tXena datasets for TCGA-LAML\t") {
				t.Fatalf("missing xena project row:\n%s", stdout)
			}
			if !strings.Contains(stdout, "datasets=1") {
				t.Fatalf("missing xena dataset count in stdout:\n%s", stdout)
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

func TestSearch_Xena_Keyword(t *testing.T) {
	withEnv(t, "GETDOWN_XENA_HUB", "https://xena.test", func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			if r.Method == http.MethodPost && r.URL.Host == "xena.test" && r.URL.Path == "/data/" {
				b, _ := io.ReadAll(r.Body)
				if !strings.Contains(string(b), `:like :d.name "%leukemia%"`) {
					return resp(400, nil, []byte("unexpected edn"), r), nil
				}
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`[`+
					`{"name":"TCGA-LAML.star_counts.tsv","longtitle":"Acute Myeloid Leukemia STAR counts","type":"genomicMatrix","probemap":"pm1","status":"loaded"}`+
					`]`), r), nil
			}
			return resp(404, nil, []byte("not found"), r), nil
		}}, func() {
			stdout, stderr := captureOutput(t, func() {
				code := runWithArgs([]string{"getdown", "search", "--source", "xena", "--no-header", "leukemia"})
				if code != 0 {
					t.Fatalf("exit code=%d", code)
				}
			})
			if stderr != "" {
				t.Fatalf("unexpected stderr: %s", stderr)
			}
			if !strings.Contains(stdout, "xena\tTCGA-LAML.star_counts.tsv\tAcute Myeloid Leukemia STAR counts\t") {
				t.Fatalf("unexpected stdout:\n%s", stdout)
			}
		})
	})
}

func TestSearch_All_Keyword_QueriesThreeSources(t *testing.T) {
	withEnv(t, "GETDOWN_XENA_HUB", "https://xena.test", func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && strings.Contains(r.URL.Host, "eutils.ncbi.nlm.nih.gov") && strings.HasSuffix(r.URL.Path, "/esearch.fcgi"):
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"esearchresult":{"idlist":["1"]}}`), r), nil
			case r.Method == http.MethodGet && strings.Contains(r.URL.Host, "eutils.ncbi.nlm.nih.gov") && strings.HasSuffix(r.URL.Path, "/esummary.fcgi"):
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"result":{"uids":["1"],"1":{"accession":"GSE1","title":"Leukemia GEO","summary":"S"}}}`), r), nil
			case r.Method == http.MethodGet && strings.Contains(r.URL.Host, "api.gdc.cancer.gov") && r.URL.Path == "/projects":
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"data":{"hits":[{"project_id":"TCGA-LAML","name":"Acute Myeloid Leukemia","primary_site":["Hematopoietic"],"disease_type":["Myeloid Leukemias"]}]}}`), r), nil
			case r.Method == http.MethodPost && r.URL.Host == "xena.test" && r.URL.Path == "/data/":
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`[`+
					`{"name":"TCGA-LAML.star_counts.tsv","longtitle":"Acute Myeloid Leukemia STAR counts","type":"genomicMatrix","probemap":"pm1","status":"loaded"}`+
					`]`), r), nil
			default:
				return resp(404, nil, []byte("not found"), r), nil
			}
		}}, func() {
			stdout, stderr := captureOutput(t, func() {
				code := runWithArgs([]string{"getdown", "search", "--source", "all", "--no-header", "leukemia"})
				if code != 0 {
					t.Fatalf("exit code=%d", code)
				}
			})
			if stderr != "" {
				t.Fatalf("unexpected stderr: %s", stderr)
			}
			if !strings.Contains(stdout, "geo\tGSE1\tLeukemia GEO\t") {
				t.Fatalf("missing geo result:\n%s", stdout)
			}
			if !strings.Contains(stdout, "tcga\tTCGA-LAML\tAcute Myeloid Leukemia\t") {
				t.Fatalf("missing tcga result:\n%s", stdout)
			}
			if !strings.Contains(stdout, "xena\tTCGA-LAML.star_counts.tsv\tAcute Myeloid Leukemia STAR counts\t") {
				t.Fatalf("missing xena result:\n%s", stdout)
			}
		})
	})
}

func TestSearch_All_Keyword_QueriesSRAToo(t *testing.T) {
	withEnv(t, "GETDOWN_XENA_HUB", "https://xena.test", func() {
		withStubTransport(t, stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			switch {
			case r.Method == http.MethodGet && strings.Contains(r.URL.Host, "eutils.ncbi.nlm.nih.gov") && strings.HasSuffix(r.URL.Path, "/esearch.fcgi"):
				if strings.Contains(r.URL.RawQuery, "db=gds") {
					return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"esearchresult":{"idlist":["1"]}}`), r), nil
				}
				if strings.Contains(r.URL.RawQuery, "db=sra") {
					return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"esearchresult":{"idlist":["2"]}}`), r), nil
				}
				return resp(404, nil, []byte("not found"), r), nil
			case r.Method == http.MethodGet && strings.Contains(r.URL.Host, "eutils.ncbi.nlm.nih.gov") && strings.HasSuffix(r.URL.Path, "/esummary.fcgi"):
				if strings.Contains(r.URL.RawQuery, "db=gds") {
					return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"result":{"uids":["1"],"1":{"accession":"GSE1","title":"Leukemia GEO","summary":"S"}}}`), r), nil
				}
				if strings.Contains(r.URL.RawQuery, "db=sra") {
					return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"result":{"uids":["2"],"2":{"caption":"SRR123456","title":"AML RNA-seq","extra":"Runs: 1"}}}`), r), nil
				}
				return resp(404, nil, []byte("not found"), r), nil
			case r.Method == http.MethodGet && strings.Contains(r.URL.Host, "api.gdc.cancer.gov") && r.URL.Path == "/projects":
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`{"data":{"hits":[{"project_id":"TCGA-LAML","name":"Acute Myeloid Leukemia","primary_site":["Hematopoietic"],"disease_type":["Myeloid Leukemias"]}]}}`), r), nil
			case r.Method == http.MethodPost && r.URL.Host == "xena.test" && r.URL.Path == "/data/":
				return resp(200, map[string]string{"Content-Type": "application/json"}, []byte(`[`+
					`{"name":"TCGA-LAML.star_counts.tsv","longtitle":"Acute Myeloid Leukemia STAR counts","type":"genomicMatrix","probemap":"pm1","status":"loaded"}`+
					`]`), r), nil
			default:
				return resp(404, nil, []byte("not found"), r), nil
			}
		}}, func() {
			stdout, stderr := captureOutput(t, func() {
				code := runWithArgs([]string{"getdown", "search", "--source", "all", "--no-header", "leukemia"})
				if code != 0 {
					t.Fatalf("exit code=%d", code)
				}
			})
			if stderr != "" {
				t.Fatalf("unexpected stderr: %s", stderr)
			}
			if !strings.Contains(stdout, "sra\tSRR123456\tAML RNA-seq\t") {
				t.Fatalf("missing sra result:\n%s", stdout)
			}
		})
	})
}

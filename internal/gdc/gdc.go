package gdc

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"getdown/internal/httpx"
)

type Request struct {
	Project  string
	OutDir   string
	RawDir   string // optional
	Workflow string // e.g. "STAR - Counts"
}

type Result struct {
	ExpressionTSV string
	ClinicalTSV   string
}

func apiBaseURL() string {
	if v := strings.TrimSpace(os.Getenv("GETDOWN_GDC_BASE")); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "https://api.gdc.cancer.gov"
}

func Download(ctx context.Context, req Request) (Result, error) {
	if req.Project == "" {
		return Result{}, errors.New("gdc: missing Project")
	}
	if req.OutDir == "" {
		return Result{}, errors.New("gdc: missing OutDir")
	}
	if req.Workflow == "" {
		req.Workflow = "STAR - Counts"
	}
	if err := os.MkdirAll(req.OutDir, 0o755); err != nil {
		return Result{}, err
	}
	if req.RawDir != "" {
		if err := os.MkdirAll(req.RawDir, 0o755); err != nil {
			return Result{}, err
		}
	}

	c := httpx.New()

	files, err := queryExpressionFiles(ctx, c, req.Project, req.Workflow)
	if err != nil {
		return Result{}, err
	}
	if len(files) == 0 {
		return Result{}, fmt.Errorf("gdc: no expression files found for project=%q workflow=%q", req.Project, req.Workflow)
	}

	exprPath := filepath.Join(req.OutDir, "expression.tsv")
	if err := downloadAndMergeCounts(ctx, c, files, req.RawDir, exprPath); err != nil {
		return Result{}, err
	}

	clinicalPath := filepath.Join(req.OutDir, "clinical.tsv")
	if err := downloadClinicalTSV(ctx, c, req.Project, clinicalPath); err != nil {
		return Result{}, err
	}

	return Result{
		ExpressionTSV: exprPath,
		ClinicalTSV:   clinicalPath,
	}, nil
}

type expressionFile struct {
	FileID    string
	FileName  string
	SampleID  string
	CaseID    string
	Submitter string
}

// queryExpressionFiles queries GDC for per-sample gene expression quantification files.
func queryExpressionFiles(ctx context.Context, c *httpx.Client, project, workflow string) ([]expressionFile, error) {
	filters := map[string]any{
		"op": "and",
		"content": []any{
			map[string]any{
				"op": "in",
				"content": map[string]any{
					"field": "cases.project.project_id",
					"value": []string{project},
				},
			},
			map[string]any{
				"op": "in",
				"content": map[string]any{
					"field": "data_category",
					"value": []string{"Transcriptome Profiling"},
				},
			},
			map[string]any{
				"op": "in",
				"content": map[string]any{
					"field": "data_type",
					"value": []string{"Gene Expression Quantification"},
				},
			},
			map[string]any{
				"op": "in",
				"content": map[string]any{
					"field": "analysis.workflow_type",
					"value": []string{workflow},
				},
			},
		},
	}

	payload := map[string]any{
		"filters": filters,
		"format":  "JSON",
		"size":    "20000",
		"fields": strings.Join([]string{
			"file_id",
			"file_name",
			"cases.case_id",
			"cases.submitter_id",
			"cases.samples.submitter_id",
		}, ","),
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	u := apiBaseURL() + "/files"
	respBytes, err := doJSON(ctx, c, http.MethodPost, u, b)
	if err != nil {
		return nil, err
	}

	type hit struct {
		FileID   string `json:"file_id"`
		FileName string `json:"file_name"`
		Cases    []struct {
			CaseID      string `json:"case_id"`
			SubmitterID string `json:"submitter_id"`
			Samples     []struct {
				SubmitterID string `json:"submitter_id"`
			} `json:"samples"`
		} `json:"cases"`
	}
	var parsed struct {
		Data struct {
			Hits []hit `json:"hits"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return nil, fmt.Errorf("gdc: parse /files response: %w", err)
	}

	out := make([]expressionFile, 0, len(parsed.Data.Hits))
	seenSample := map[string]bool{}
	for _, h := range parsed.Data.Hits {
		sampleID := ""
		caseID := ""
		submitter := ""
		if len(h.Cases) > 0 {
			caseID = h.Cases[0].CaseID
			submitter = h.Cases[0].SubmitterID
			if len(h.Cases[0].Samples) > 0 {
				sampleID = h.Cases[0].Samples[0].SubmitterID
			}
		}
		if sampleID == "" {
			// Fallback: use file id as column label.
			sampleID = h.FileID
		}
		if seenSample[sampleID] {
			// Avoid duplicate columns (GDC can have multiple files per sample for different strategies).
			continue
		}
		seenSample[sampleID] = true
		out = append(out, expressionFile{
			FileID:    h.FileID,
			FileName:  h.FileName,
			SampleID:  sampleID,
			CaseID:    caseID,
			Submitter: submitter,
		})
	}
	return out, nil
}

func downloadClinicalTSV(ctx context.Context, c *httpx.Client, project, destPath string) error {
	filters := map[string]any{
		"op": "in",
		"content": map[string]any{
			"field": "project.project_id",
			"value": []string{project},
		},
	}

	fields := []string{
		"submitter_id",
		"case_id",
		"primary_site",
		"disease_type",
		"diagnoses.vital_status",
		"diagnoses.days_to_death",
		"diagnoses.days_to_last_follow_up",
		"diagnoses.age_at_diagnosis",
		"demographic.gender",
		"demographic.race",
		"demographic.ethnicity",
		"demographic.year_of_birth",
	}

	q := url.Values{}
	fb, _ := json.Marshal(filters)
	q.Set("filters", string(fb))
	q.Set("format", "TSV")
	q.Set("size", "20000")
	q.Set("fields", strings.Join(fields, ","))

	u := apiBaseURL() + "/cases?" + q.Encode()
	body, err := doRaw(ctx, c, http.MethodGet, u, "text/tab-separated-values", nil)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(destPath, body, 0o644)
}

func downloadAndMergeCounts(ctx context.Context, c *httpx.Client, files []expressionFile, rawDir, outPath string) error {
	sampleIDs := make([]string, 0, len(files))
	for _, f := range files {
		sampleIDs = append(sampleIDs, f.SampleID)
	}

	type matrix struct {
		GeneIDs []string
		Counts  []int32 // row-major: geneIndex*sampleCount + sampleIndex
		Samples []string
	}

	var m matrix
	m.Samples = sampleIDs
	sampleCount := int32(len(sampleIDs))

	for sampleIndex, f := range files {
		url := apiBaseURL() + "/data/" + f.FileID

		dest := ""
		if rawDir != "" {
			// Keep the filename for readability, but ensure it's safe.
			name := sanitizeFileName(f.FileName)
			if name == "" {
				name = f.FileID + ".txt"
			}
			dest = filepath.Join(rawDir, name)
		} else {
			dest = filepath.Join(os.TempDir(), "getdown-gdc-"+f.FileID)
		}

		if _, err := c.DownloadToFile(ctx, url, dest, false); err != nil {
			return fmt.Errorf("gdc: download %s (%s): %w", f.FileID, f.SampleID, err)
		}

		r, closeFn, err := openMaybeGzip(dest)
		if err != nil {
			return err
		}
		genes, counts, err := parseCounts(r)
		_ = closeFn()
		if err != nil {
			return fmt.Errorf("gdc: parse counts %s (%s): %w", f.FileID, f.SampleID, err)
		}

		if sampleIndex == 0 {
			m.GeneIDs = genes
			m.Counts = make([]int32, int32(len(genes))*sampleCount)
		} else {
			if !sameGenes(m.GeneIDs, genes) {
				return fmt.Errorf("gdc: gene order mismatch for file %s (%s); use Xena provider or normalize inputs", f.FileID, f.SampleID)
			}
		}

		for gi, c := range counts {
			m.Counts[int32(gi)*sampleCount+int32(sampleIndex)] = c
		}

		// Cleanup temp download unless requested to keep.
		if rawDir == "" {
			_ = os.Remove(dest)
		}
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}
	fout, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer fout.Close()

	w := bufio.NewWriterSize(fout, 1<<20)
	defer w.Flush()

	// Header
	if _, err := w.WriteString("gene_id"); err != nil {
		return err
	}
	for _, s := range m.Samples {
		if _, err := w.WriteString("\t" + s); err != nil {
			return err
		}
	}
	if _, err := w.WriteString("\n"); err != nil {
		return err
	}

	// Rows
	rowBuf := make([]byte, 0, 64*1024)
	for gi, gene := range m.GeneIDs {
		rowBuf = rowBuf[:0]
		rowBuf = append(rowBuf, gene...)
		for si := range m.Samples {
			rowBuf = append(rowBuf, '\t')
			rowBuf = strconv.AppendInt(rowBuf, int64(m.Counts[int32(gi)*sampleCount+int32(si)]), 10)
		}
		rowBuf = append(rowBuf, '\n')
		if _, err := w.Write(rowBuf); err != nil {
			return err
		}
	}
	return nil
}

func sameGenes(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func parseCounts(r io.Reader) ([]string, []int32, error) {
	sc := bufio.NewScanner(r)
	// Some GDC files can have long lines; make the buffer larger.
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)

	var genes []string
	var counts []int32
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			continue
		}
		geneID := parts[0]
		if geneID == "" {
			continue
		}
		// Common HTSeq tail rows.
		if strings.HasPrefix(geneID, "__") {
			continue
		}
		n, err := strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			// Some formats have "gene_name\tgene_id\tcount". Try last column.
			last := parts[len(parts)-1]
			n2, err2 := strconv.ParseInt(last, 10, 32)
			if err2 != nil {
				return nil, nil, err
			}
			n = n2
		}
		genes = append(genes, geneID)
		counts = append(counts, int32(n))
	}
	if err := sc.Err(); err != nil {
		return nil, nil, err
	}
	if len(genes) == 0 {
		return nil, nil, errors.New("empty counts file")
	}
	return genes, counts, nil
}

func sanitizeFileName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, string(os.PathSeparator), "_")
	name = strings.ReplaceAll(name, "..", "_")
	return name
}

func openMaybeGzip(path string) (io.Reader, func() error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	closeFn := f.Close

	// Peek header.
	var hdr [2]byte
	n, _ := io.ReadFull(f, hdr[:])
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	isGz := n == 2 && hdr[0] == 0x1f && hdr[1] == 0x8b
	if !isGz {
		return f, closeFn, nil
	}
	gr, err := gzip.NewReader(f)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return gr, func() error {
		_ = gr.Close()
		return f.Close()
	}, nil
}

func doJSON(ctx context.Context, c *httpx.Client, method, u string, body []byte) ([]byte, error) {
	return doRaw(ctx, c, method, u, "application/json", body)
}

func doRaw(ctx context.Context, c *httpx.Client, method, u, contentType string, body []byte) ([]byte, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "getdown/0.1 (+https://github.com)")
	if body != nil {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: %s: %s", method, u, resp.Status, strings.TrimSpace(string(b)))
	}
	return b, nil
}

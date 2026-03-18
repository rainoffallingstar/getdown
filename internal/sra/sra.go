package sra

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"getdown/internal/httpx"
	"getdown/internal/meta"
)

type Request struct {
	Accession string
	OutDir    string
	Kind      string
}

type RunInfo struct {
	StudyAccession      string
	ExperimentAccession string
	SampleAccession     string
	RunAccession        string
	ScientificName      string
	InstrumentPlatform  string
	InstrumentModel     string
	LibraryLayout       string
	LibraryStrategy     string
	LibrarySource       string
	LibrarySelection    string
	FastqFTP            string
	SubmittedFTP        string
	SRAFTP              string
	FastqBytes          string
	SubmittedBytes      string
	SRABytes            string
}

type RemoteFile struct {
	RunAccession string
	Kind         string
	URL          string
	SizeBytes    string
}

type Result struct {
	RunInfoPath string
	LinksPath   string
	Files       []string
}

var reAccession = regexp.MustCompile(`(?i)^(SRP|SRX|SRS|SRR|ERP|ERX|ERS|ERR|DRP|DRX|DRS|DRR)\d+$`)

func NormalizeAccession(s string) string {
	return strings.ToUpper(strings.TrimSpace(s))
}

func IsAccession(s string) bool {
	return reAccession.MatchString(strings.TrimSpace(s))
}

func Download(ctx context.Context, req Request) (Result, error) {
	acc := NormalizeAccession(req.Accession)
	if acc == "" {
		return Result{}, errors.New("sra: missing accession")
	}
	if !IsAccession(acc) {
		return Result{}, fmt.Errorf("sra: invalid accession: %q", req.Accession)
	}
	if strings.TrimSpace(req.OutDir) == "" {
		return Result{}, errors.New("sra: missing OutDir")
	}
	if err := os.MkdirAll(req.OutDir, 0o755); err != nil {
		return Result{}, err
	}

	kind := normalizeKind(req.Kind)
	if kind == "" {
		return Result{}, fmt.Errorf("sra: invalid kind: %q (want auto|fastq|submitted|sra|all)", req.Kind)
	}
	runs, err := FetchRunInfo(ctx, acc)
	if err != nil {
		return Result{}, err
	}
	if len(runs) == 0 {
		return Result{}, fmt.Errorf("sra: no runs found for accession: %s", acc)
	}

	runInfoPath := filepath.Join(req.OutDir, "runinfo.tsv")
	if err := writeRunInfo(runInfoPath, runs); err != nil {
		return Result{}, err
	}

	files := CollectFiles(runs, kind)
	if len(files) == 0 {
		return Result{}, fmt.Errorf("sra: no downloadable links found for accession: %s", acc)
	}
	linksPath := filepath.Join(req.OutDir, "links.tsv")
	if err := writeLinks(linksPath, files); err != nil {
		return Result{}, err
	}

	client := httpx.New()
	outFiles := make([]string, 0, len(files))
	for _, file := range files {
		destPath := filepath.Join(req.OutDir, "files", file.RunAccession, safeLeaf(file.URL))
		if _, err := client.DownloadToFileMaybe(ctx, file.URL, destPath, false); err != nil {
			return Result{}, fmt.Errorf("sra: download %s: %w", file.URL, err)
		}
		outFiles = append(outFiles, destPath)
	}

	params, _ := json.Marshal(req)
	m := meta.File{
		CreatedAt: time.Now(),
		Kind:      "sra",
		Params:    params,
		Source: map[string]any{
			"accession": acc,
			"kind":      kind,
			"provider":  "ena_filereport",
		},
		Files: map[string]any{
			"runinfo": runInfoPath,
			"links":   linksPath,
			"files":   outFiles,
		},
	}
	if err := meta.Write(filepath.Join(req.OutDir, "metadata.json"), m); err != nil {
		return Result{}, err
	}
	return Result{
		RunInfoPath: runInfoPath,
		LinksPath:   linksPath,
		Files:       outFiles,
	}, nil
}

func FetchRunInfo(ctx context.Context, accession string) ([]RunInfo, error) {
	acc := NormalizeAccession(accession)
	if !IsAccession(acc) {
		return nil, fmt.Errorf("sra: invalid accession: %q", accession)
	}
	c := httpx.New()
	resp, err := c.Get(ctx, filereportURL(acc))
	if err != nil {
		return nil, fmt.Errorf("sra filereport: %w", err)
	}
	defer resp.Body.Close()
	return parseRunInfo(resp.Body)
}

func CollectFiles(runs []RunInfo, kind string) []RemoteFile {
	kind = normalizeKind(kind)
	var out []RemoteFile
	for _, run := range runs {
		switch kind {
		case "fastq":
			out = append(out, collectField(run.RunAccession, "fastq", run.FastqFTP, run.FastqBytes)...)
		case "submitted":
			out = append(out, collectField(run.RunAccession, "submitted", run.SubmittedFTP, run.SubmittedBytes)...)
		case "sra":
			out = append(out, collectField(run.RunAccession, "sra", run.SRAFTP, run.SRABytes)...)
		case "all":
			out = append(out, collectField(run.RunAccession, "fastq", run.FastqFTP, run.FastqBytes)...)
			out = append(out, collectField(run.RunAccession, "submitted", run.SubmittedFTP, run.SubmittedBytes)...)
			out = append(out, collectField(run.RunAccession, "sra", run.SRAFTP, run.SRABytes)...)
		default:
			// auto: prefer fastq, then submitted, then sra.
			if files := collectField(run.RunAccession, "fastq", run.FastqFTP, run.FastqBytes); len(files) > 0 {
				out = append(out, files...)
				continue
			}
			if files := collectField(run.RunAccession, "submitted", run.SubmittedFTP, run.SubmittedBytes); len(files) > 0 {
				out = append(out, files...)
				continue
			}
			out = append(out, collectField(run.RunAccession, "sra", run.SRAFTP, run.SRABytes)...)
		}
	}
	return dedupFiles(out)
}

func parseRunInfo(r io.Reader) ([]RunInfo, error) {
	cr := csv.NewReader(r)
	cr.Comma = '\t'
	cr.FieldsPerRecord = -1

	header, err := cr.Read()
	if err != nil {
		return nil, err
	}
	col := make(map[string]int, len(header))
	for i, name := range header {
		col[strings.TrimSpace(name)] = i
	}

	var out []RunInfo
	for {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) == 0 {
			continue
		}
		run := RunInfo{
			StudyAccession:      at(rec, col, "study_accession"),
			ExperimentAccession: at(rec, col, "experiment_accession"),
			SampleAccession:     at(rec, col, "sample_accession"),
			RunAccession:        at(rec, col, "run_accession"),
			ScientificName:      at(rec, col, "scientific_name"),
			InstrumentPlatform:  at(rec, col, "instrument_platform"),
			InstrumentModel:     at(rec, col, "instrument_model"),
			LibraryLayout:       at(rec, col, "library_layout"),
			LibraryStrategy:     at(rec, col, "library_strategy"),
			LibrarySource:       at(rec, col, "library_source"),
			LibrarySelection:    at(rec, col, "library_selection"),
			FastqFTP:            at(rec, col, "fastq_ftp"),
			SubmittedFTP:        at(rec, col, "submitted_ftp"),
			SRAFTP:              at(rec, col, "sra_ftp"),
			FastqBytes:          at(rec, col, "fastq_bytes"),
			SubmittedBytes:      at(rec, col, "submitted_bytes"),
			SRABytes:            at(rec, col, "sra_bytes"),
		}
		if strings.TrimSpace(run.RunAccession) == "" {
			continue
		}
		out = append(out, run)
	}
	return out, nil
}

func at(rec []string, col map[string]int, key string) string {
	i, ok := col[key]
	if !ok || i < 0 || i >= len(rec) {
		return ""
	}
	return strings.TrimSpace(rec[i])
}

func normalizeKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", "auto":
		return "auto"
	case "fastq", "submitted", "sra", "all":
		return strings.ToLower(strings.TrimSpace(kind))
	default:
		return ""
	}
}

func collectField(runAccession, kind, field, sizes string) []RemoteFile {
	urls := splitList(field)
	byteList := splitList(sizes)
	out := make([]RemoteFile, 0, len(urls))
	for i, raw := range urls {
		link := normalizeDownloadURL(raw)
		if link == "" {
			continue
		}
		size := ""
		if i < len(byteList) {
			size = byteList[i]
		}
		out = append(out, RemoteFile{
			RunAccession: runAccession,
			Kind:         kind,
			URL:          link,
			SizeBytes:    size,
		})
	}
	return out
}

func splitList(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(strings.Trim(p, `"`))
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func normalizeDownloadURL(raw string) string {
	raw = strings.TrimSpace(strings.Trim(raw, `"`))
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "//") {
		return "https:" + raw
	}
	if strings.HasPrefix(raw, "ftp://") {
		return "https://" + strings.TrimPrefix(raw, "ftp://")
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	if strings.Contains(raw, "/") {
		return "https://" + strings.TrimPrefix(raw, "/")
	}
	return ""
}

func writeRunInfo(path string, runs []RunInfo) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	w.Comma = '\t'
	rows := [][]string{{
		"study_accession", "experiment_accession", "sample_accession", "run_accession",
		"scientific_name", "instrument_platform", "instrument_model", "library_layout",
		"library_strategy", "library_source", "library_selection", "fastq_ftp",
		"submitted_ftp", "sra_ftp", "fastq_bytes", "submitted_bytes", "sra_bytes",
	}}
	for _, run := range runs {
		rows = append(rows, []string{
			run.StudyAccession, run.ExperimentAccession, run.SampleAccession, run.RunAccession,
			run.ScientificName, run.InstrumentPlatform, run.InstrumentModel, run.LibraryLayout,
			run.LibraryStrategy, run.LibrarySource, run.LibrarySelection, run.FastqFTP,
			run.SubmittedFTP, run.SRAFTP, run.FastqBytes, run.SubmittedBytes, run.SRABytes,
		})
	}
	if err := w.WriteAll(rows); err != nil {
		return err
	}
	return w.Error()
}

func writeLinks(path string, files []RemoteFile) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	w.Comma = '\t'
	rows := [][]string{{"run_accession", "kind", "size_bytes", "url"}}
	for _, file := range files {
		rows = append(rows, []string{file.RunAccession, file.Kind, file.SizeBytes, file.URL})
	}
	if err := w.WriteAll(rows); err != nil {
		return err
	}
	return w.Error()
}

func dedupFiles(files []RemoteFile) []RemoteFile {
	seen := make(map[string]bool, len(files))
	out := make([]RemoteFile, 0, len(files))
	for _, file := range files {
		key := file.RunAccession + "\t" + file.Kind + "\t" + file.URL
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, file)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RunAccession != out[j].RunAccession {
			return out[i].RunAccession < out[j].RunAccession
		}
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].URL < out[j].URL
	})
	return out
}

func safeLeaf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err == nil {
		name := filepath.Base(u.Path)
		if name != "." && name != "/" && name != "" {
			return name
		}
	}
	rawURL = strings.TrimRight(rawURL, "/")
	if i := strings.LastIndex(rawURL, "/"); i >= 0 && i+1 < len(rawURL) {
		return rawURL[i+1:]
	}
	return "download.bin"
}

func filereportURL(accession string) string {
	base := strings.TrimSpace(os.Getenv("GETDOWN_ENA_API_BASE"))
	if base == "" {
		base = "https://www.ebi.ac.uk/ena/portal/api/filereport"
	}
	v := url.Values{}
	v.Set("accession", accession)
	v.Set("result", "read_run")
	v.Set("fields", strings.Join([]string{
		"study_accession",
		"experiment_accession",
		"sample_accession",
		"run_accession",
		"scientific_name",
		"instrument_platform",
		"instrument_model",
		"library_layout",
		"library_strategy",
		"library_source",
		"library_selection",
		"fastq_ftp",
		"submitted_ftp",
		"sra_ftp",
		"fastq_bytes",
		"submitted_bytes",
		"sra_bytes",
	}, ","))
	v.Set("format", "tsv")
	return base + "?" + v.Encode()
}

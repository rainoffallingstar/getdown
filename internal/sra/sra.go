package sra

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"getdown/internal/httpx"
	"getdown/internal/meta"
	"getdown/internal/parallel"
)

type Request struct {
	Accession string
	OutDir    string
	Kind      string
	Decode    string
	Jobs      int
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
	RunInfoPath   string
	LinksPath     string
	Files         []string
	DecodedFiles  []string
	EffectiveKind string
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
	decode := normalizeDecode(req.Decode)
	if decode == "" {
		return Result{}, fmt.Errorf("sra: invalid decode: %q (want none|fastq|fastq.gz)", req.Decode)
	}
	runs, err := FetchRunInfo(ctx, acc)
	if err != nil {
		return Result{}, err
	}
	if len(runs) == 0 {
		return Result{}, fmt.Errorf("sra: no runs found for accession: %s", acc)
	}
	files, effectiveKind, err := selectDownloadFiles(runs, kind, decode)
	if err != nil {
		return Result{}, err
	}

	runInfoPath := filepath.Join(req.OutDir, "runinfo.tsv")
	if err := writeRunInfo(runInfoPath, runs); err != nil {
		return Result{}, err
	}

	if len(files) == 0 {
		return Result{}, fmt.Errorf("sra: no downloadable links found for accession: %s", acc)
	}
	linksPath := filepath.Join(req.OutDir, "links.tsv")
	if err := writeLinks(linksPath, files); err != nil {
		return Result{}, err
	}

	client := httpx.New()
	outFiles := make([]string, 0, len(files))
	paths := make([]string, len(files))
	if err := parallel.ForEach(ctx, req.Jobs, len(files), func(ctx context.Context, i int) error {
		file := files[i]
		destPath := filepath.Join(req.OutDir, "files", file.RunAccession, safeLeaf(file.URL))
		if _, err := client.DownloadToFileMaybe(ctx, file.URL, destPath, false); err != nil {
			return fmt.Errorf("sra: download %s: %w", file.URL, err)
		}
		paths[i] = destPath
		return nil
	}); err != nil {
		return Result{}, err
	}
	for _, path := range paths {
		if path != "" {
			outFiles = append(outFiles, path)
		}
	}
	var decodedFiles []string
	if decode != "none" {
		decodedFiles, err = materializeDecodedOutputs(ctx, req.OutDir, outFiles, decode, req.Jobs)
		if err != nil {
			return Result{}, err
		}
	}

	params, _ := json.Marshal(req)
	m := meta.File{
		CreatedAt: time.Now(),
		Kind:      "sra",
		Params:    params,
		Source: map[string]any{
			"accession":      acc,
			"kind":           kind,
			"effective_kind": effectiveKind,
			"decode":         decode,
			"provider":       "ena_filereport",
		},
		Files: map[string]any{
			"runinfo": runInfoPath,
			"links":   linksPath,
			"files":   outFiles,
		},
	}
	if len(decodedFiles) > 0 {
		m.Files["decoded_files"] = decodedFiles
	}
	if err := meta.Write(filepath.Join(req.OutDir, "metadata.json"), m); err != nil {
		return Result{}, err
	}
	return Result{
		RunInfoPath:   runInfoPath,
		LinksPath:     linksPath,
		Files:         outFiles,
		DecodedFiles:  decodedFiles,
		EffectiveKind: effectiveKind,
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

func normalizeDecode(decode string) string {
	switch strings.ToLower(strings.TrimSpace(decode)) {
	case "", "none":
		return "none"
	case "fastq", "fastq.gz":
		return strings.ToLower(strings.TrimSpace(decode))
	default:
		return ""
	}
}

func selectDownloadFiles(runs []RunInfo, kind, decode string) ([]RemoteFile, string, error) {
	if decode == "none" {
		return CollectFiles(runs, kind), kind, nil
	}
	switch kind {
	case "auto":
		var out []RemoteFile
		usedKinds := make(map[string]bool)
		for _, run := range runs {
			if files := collectField(run.RunAccession, "fastq", run.FastqFTP, run.FastqBytes); len(files) > 0 {
				out = append(out, files...)
				usedKinds["fastq"] = true
				continue
			}
			if files := collectField(run.RunAccession, "sra", run.SRAFTP, run.SRABytes); len(files) > 0 {
				out = append(out, files...)
				usedKinds["sra"] = true
			}
		}
		return dedupFiles(out), joinKinds(usedKinds), nil
	case "fastq":
		return CollectFiles(runs, "fastq"), "fastq", nil
	case "sra":
		return CollectFiles(runs, "sra"), "sra", nil
	case "all":
		files := append(CollectFiles(runs, "fastq"), CollectFiles(runs, "sra")...)
		return dedupFiles(files), "fastq+sra", nil
	default:
		return nil, "", fmt.Errorf("sra: decode requires --kind auto|fastq|sra|all, got %q", kind)
	}
}

func joinKinds(kinds map[string]bool) string {
	if len(kinds) == 0 {
		return ""
	}
	var out []string
	for kind := range kinds {
		out = append(out, kind)
	}
	sort.Strings(out)
	return strings.Join(out, "+")
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

func materializeDecodedOutputs(ctx context.Context, outDir string, files []string, decode string, jobs int) ([]string, error) {
	var out []string
	var sraFiles []string
	for _, path := range files {
		switch {
		case strings.HasSuffix(strings.ToLower(path), ".sra"):
			sraFiles = append(sraFiles, path)
		case isFastqPath(path):
			switch decode {
			case "fastq":
				finalPath, err := ensureFastq(path)
				if err != nil {
					return nil, err
				}
				out = append(out, finalPath)
			case "fastq.gz":
				finalPath, err := ensureFastqGz(path)
				if err != nil {
					return nil, err
				}
				out = append(out, finalPath)
			}
		}
	}
	if len(sraFiles) > 0 {
		decoded, err := decodeDownloadedSRA(ctx, outDir, sraFiles, decode, jobs)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded...)
	}
	if len(out) == 0 {
		return nil, errors.New("sra: decode requested but no FASTQ or .sra inputs were downloaded")
	}
	sort.Strings(out)
	return out, nil
}

func decodeDownloadedSRA(ctx context.Context, outDir string, sraFiles []string, decode string, jobs int) ([]string, error) {
	if len(sraFiles) == 0 {
		return nil, errors.New("sra: decode requested but no .sra files were downloaded")
	}

	bin, err := fasterqDumpPath()
	if err != nil {
		return nil, err
	}

	decodedRoot := filepath.Join(outDir, "decoded")
	if err := os.MkdirAll(decodedRoot, 0o755); err != nil {
		return nil, err
	}

	var decodedFiles []string
	for _, sraPath := range sraFiles {
		runDir := filepath.Join(decodedRoot, strings.TrimSuffix(filepath.Base(sraPath), filepath.Ext(sraPath)))
		if err := os.MkdirAll(runDir, 0o755); err != nil {
			return nil, err
		}
		args := []string{"--split-files", "-O", runDir}
		if jobs > 0 {
			args = append(args, "-e", fmt.Sprintf("%d", jobs))
		}
		args = append(args, sraPath)
		cmd := exec.CommandContext(ctx, bin, args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("sra: decode %s: %w: %s", sraPath, err, strings.TrimSpace(string(out)))
		}

		matches, err := filepath.Glob(filepath.Join(runDir, "*.fastq"))
		if err != nil {
			return nil, err
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("sra: decode %s: no FASTQ files produced", sraPath)
		}
		sort.Strings(matches)
		if decode == "fastq.gz" {
			gzFiles := make([]string, 0, len(matches))
			for _, path := range matches {
				gzPath, err := gzipFile(path)
				if err != nil {
					return nil, err
				}
				gzFiles = append(gzFiles, gzPath)
			}
			decodedFiles = append(decodedFiles, gzFiles...)
			continue
		}
		decodedFiles = append(decodedFiles, matches...)
	}
	return decodedFiles, nil
}

func isFastqPath(path string) bool {
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".fastq") || strings.HasSuffix(lower, ".fq") || strings.HasSuffix(lower, ".fastq.gz") || strings.HasSuffix(lower, ".fq.gz")
}

func ensureFastq(path string) (string, error) {
	lower := strings.ToLower(path)
	if strings.HasSuffix(lower, ".fastq") || strings.HasSuffix(lower, ".fq") {
		return path, nil
	}
	if strings.HasSuffix(lower, ".fastq.gz") || strings.HasSuffix(lower, ".fq.gz") {
		return gunzipFile(path)
	}
	return "", fmt.Errorf("sra: not a FASTQ path: %s", path)
}

func ensureFastqGz(path string) (string, error) {
	lower := strings.ToLower(path)
	if strings.HasSuffix(lower, ".fastq.gz") || strings.HasSuffix(lower, ".fq.gz") {
		return path, nil
	}
	if strings.HasSuffix(lower, ".fastq") || strings.HasSuffix(lower, ".fq") {
		return gzipFile(path)
	}
	return "", fmt.Errorf("sra: not a FASTQ path: %s", path)
}

func fasterqDumpPath() (string, error) {
	if v := strings.TrimSpace(os.Getenv("GETDOWN_FASTERQ_DUMP")); v != "" {
		return v, nil
	}
	path, err := exec.LookPath("fasterq-dump")
	if err != nil {
		return "", errors.New("sra: decode requested but `fasterq-dump` was not found; set GETDOWN_FASTERQ_DUMP or install SRA Toolkit")
	}
	return path, nil
}

func gzipFile(path string) (string, error) {
	in, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer in.Close()

	outPath := path + ".gz"
	out, err := os.Create(outPath)
	if err != nil {
		return "", err
	}

	gw := gzip.NewWriter(out)
	_, copyErr := io.Copy(gw, in)
	closeGzipErr := gw.Close()
	closeFileErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(outPath)
		return "", copyErr
	}
	if closeGzipErr != nil {
		_ = os.Remove(outPath)
		return "", closeGzipErr
	}
	if closeFileErr != nil {
		_ = os.Remove(outPath)
		return "", closeFileErr
	}
	if err := os.Remove(path); err != nil {
		return "", err
	}
	return outPath, nil
}

func gunzipFile(path string) (string, error) {
	in, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer in.Close()

	gr, err := gzip.NewReader(in)
	if err != nil {
		return "", err
	}
	defer gr.Close()

	outPath := strings.TrimSuffix(path, ".gz")
	if outPath == path {
		outPath = path + ".fastq"
	}
	out, err := os.Create(outPath)
	if err != nil {
		return "", err
	}
	_, copyErr := io.Copy(out, gr)
	closeFileErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(outPath)
		return "", copyErr
	}
	if closeFileErr != nil {
		_ = os.Remove(outPath)
		return "", closeFileErr
	}
	if err := os.Remove(path); err != nil {
		return "", err
	}
	return outPath, nil
}

package geo

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"getdown/internal/httpx"
	"getdown/internal/meta"
)

type Request struct {
	GSE     string
	OutDir  string
	Sup     bool
	KeepRaw bool
}

func Download(ctx context.Context, req Request) error {
	gse := strings.ToUpper(strings.TrimSpace(req.GSE))
	if gse == "" {
		return errors.New("geo: missing GSE")
	}
	if !strings.HasPrefix(gse, "GSE") {
		return fmt.Errorf("geo: invalid GSE: %q", req.GSE)
	}
	if req.OutDir == "" {
		return errors.New("geo: missing OutDir")
	}
	if err := os.MkdirAll(req.OutDir, 0o755); err != nil {
		return err
	}

	rawDir := ""
	if req.KeepRaw {
		rawDir = filepath.Join(req.OutDir, "raw", "geo")
		if err := os.MkdirAll(rawDir, 0o755); err != nil {
			return err
		}
	}

	c := httpx.New()
	var platformFiles []string

	seriesMatrixURL := seriesMatrixURL(gse)
	seriesMatrixPath := filepath.Join(req.OutDir, "series_matrix.txt")

	// Try series matrix first (streaming; avoids loading large files into memory).
	matrixResp, err := c.GetMaybe(ctx, seriesMatrixURL)
	if err == nil {
		defer matrixResp.Body.Close()
		rawPath := ""
		var body io.Reader = matrixResp.Body
		if rawDir != "" {
			rawPath = filepath.Join(rawDir, gse+"_series_matrix.txt.gz")
			if err := os.MkdirAll(filepath.Dir(rawPath), 0o755); err != nil {
				return err
			}
			rawFile, err := os.Create(rawPath)
			if err != nil {
				return err
			}
			defer rawFile.Close()
			body = io.TeeReader(matrixResp.Body, rawFile)
		}
		gr, gerr := gzip.NewReader(body)
		if gerr != nil {
			return fmt.Errorf("geo: open gzip series matrix: %w", gerr)
		}
		defer gr.Close()

		if err := os.MkdirAll(filepath.Dir(seriesMatrixPath), 0o755); err != nil {
			return err
		}
		matrixText, err := os.Create(seriesMatrixPath)
		if err != nil {
			return err
		}
		defer matrixText.Close()

		tee := io.TeeReader(gr, matrixText)
		info, perr := parseSeriesMatrix(tee, req.OutDir)
		if perr != nil {
			return perr
		}
		needSup := req.Sup || info.TableRowCount == 0
		supURLs := info.SupURLs
		if needSup && len(supURLs) == 0 {
			// Fall back to family SOFT to locate supplementary URLs when the series matrix has no data rows.
			softResp, serr := c.GetMaybe(ctx, familySoftURL(gse))
			if serr == nil {
				defer softResp.Body.Close()
				gr, gerr := gzip.NewReader(softResp.Body)
				if gerr == nil {
					_, _, supURLs = parseSoft(gr)
					_ = gr.Close()
				}
			}
		}
		if needSup {
			if err := downloadSupplementary(ctx, c, supURLs, filepath.Join(req.OutDir, "supplementary")); err != nil {
				return err
			}
		}

		// If this is array/chip data, also download platform annotation.
		platformIDs := extractGPLs(info.PlatformIDs)
		if isMicroarray(info.SeriesTypes, platformIDs) && len(platformIDs) > 0 {
			for _, gpl := range platformIDs {
				p, err := downloadPlatformAnnotation(ctx, c, gpl, req.OutDir)
				if err != nil {
					return err
				}
				if p != "" {
					platformFiles = append(platformFiles, p)
				}
			}
		}
	} else {
		// Fallback to family SOFT for supplementary URLs + basic metadata.
		softURL := familySoftURL(gse)
		softPath := filepath.Join(req.OutDir, "family.soft")
		softResp, serr := c.GetMaybe(ctx, softURL)
		if serr != nil {
			return errors.Join(fmt.Errorf("geo: series matrix not available: %w", err), fmt.Errorf("geo: family soft download failed: %w", serr))
		}
		defer softResp.Body.Close()

		rawPath := ""
		var body io.Reader = softResp.Body
		if rawDir != "" {
			rawPath = filepath.Join(rawDir, gse+"_family.soft.gz")
			if err := os.MkdirAll(filepath.Dir(rawPath), 0o755); err != nil {
				return err
			}
			rawFile, err := os.Create(rawPath)
			if err != nil {
				return err
			}
			defer rawFile.Close()
			body = io.TeeReader(softResp.Body, rawFile)
		}

		gr, gerr := gzip.NewReader(body)
		if gerr != nil {
			return fmt.Errorf("geo: open gzip family soft: %w", gerr)
		}
		defer gr.Close()

		if err := os.MkdirAll(filepath.Dir(softPath), 0o755); err != nil {
			return err
		}
		softText, err := os.Create(softPath)
		if err != nil {
			return err
		}
		defer softText.Close()

		tee := io.TeeReader(gr, softText)
		seriesMeta, sampleMeta, supURLs := parseSoft(tee)
		if err := writeLongKV(filepath.Join(req.OutDir, "series_kv.tsv"), "series", seriesMeta); err != nil {
			return err
		}
		if err := writeLongKV(filepath.Join(req.OutDir, "sample_kv.tsv"), "sample", sampleMeta); err != nil {
			return err
		}
		if req.Sup {
			if err := downloadSupplementary(ctx, c, supURLs, filepath.Join(req.OutDir, "supplementary")); err != nil {
				return err
			}
		}

		platformIDs := extractGPLs(seriesMeta["Series_platform_id"])
		if isMicroarray(seriesMeta["Series_type"], platformIDs) && len(platformIDs) > 0 {
			for _, gpl := range platformIDs {
				p, err := downloadPlatformAnnotation(ctx, c, gpl, req.OutDir)
				if err != nil {
					return err
				}
				if p != "" {
					platformFiles = append(platformFiles, p)
				}
			}
		}
	}

	params, _ := json.Marshal(req)
	m := meta.File{
		CreatedAt: time.Now(),
		Kind:      "geo",
		Params:    params,
		Source: map[string]any{
			"series_matrix_url": seriesMatrixURL,
		},
	}
	if len(platformFiles) > 0 {
		m.Files = map[string]any{
			"platform_files": platformFiles,
		}
	}
	return meta.Write(filepath.Join(req.OutDir, "metadata.json"), m)
}

func seriesMatrixURL(gse string) string {
	return fmt.Sprintf("%s/geo/series/%s/%s/matrix/%s_series_matrix.txt.gz", geoBaseURL(), geoSeriesGroup(gse), gse, gse)
}

func familySoftURL(gse string) string {
	return fmt.Sprintf("%s/geo/series/%s/%s/soft/%s_family.soft.gz", geoBaseURL(), geoSeriesGroup(gse), gse, gse)
}

func geoBaseURL() string {
	if v := strings.TrimSpace(os.Getenv("GETDOWN_GEO_FTP_BASE")); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "https://ftp.ncbi.nlm.nih.gov"
}

func geoSeriesGroup(gse string) string {
	// GEO uses a group like GSE12nnn for GSE12345.
	digits := strings.TrimPrefix(gse, "GSE")
	if len(digits) <= 3 {
		return "GSEnnn"
	}
	return "GSE" + digits[:len(digits)-3] + "nnn"
}

func geoPlatformGroup(gpl string) string {
	// GEO uses a group like GPL57nnn for GPL570.
	digits := strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(gpl)), "GPL")
	if len(digits) <= 3 {
		return "GPLnnn"
	}
	return "GPL" + digits[:len(digits)-3] + "nnn"
}

type matrixRow struct {
	Field  string
	Values []string
}

type seriesMatrixInfo struct {
	SupURLs       []string
	TableRowCount int
	SeriesTypes   []string
	PlatformIDs   []string
}

func parseSeriesMatrix(r io.Reader, outDir string) (seriesMatrixInfo, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)

	var seriesRows []matrixRow
	var sampleRows []matrixRow

	var sampleIDs []string
	inTable := false
	tableRows := 0

	exprPath := filepath.Join(outDir, "expression.tsv")
	exprFile, err := os.Create(exprPath)
	if err != nil {
		return seriesMatrixInfo{}, err
	}
	defer exprFile.Close()
	exprW := bufio.NewWriterSize(exprFile, 1<<20)
	defer exprW.Flush()

	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "!Series_") {
			k, vals := splitMatrixKV(line)
			if k != "" {
				seriesRows = append(seriesRows, matrixRow{Field: k, Values: vals})
			}
			continue
		}
		if strings.HasPrefix(line, "!Sample_") {
			k, vals := splitMatrixKV(line)
			if k != "" {
				sampleRows = append(sampleRows, matrixRow{Field: k, Values: vals})
			}
			continue
		}
		if strings.HasPrefix(line, "!series_matrix_table_begin") {
			inTable = true
			// Next line should be header.
			if !sc.Scan() {
				return seriesMatrixInfo{}, errors.New("geo: unexpected EOF after table_begin")
			}
			hdr := strings.Split(sc.Text(), "\t")
			if len(hdr) < 2 {
				return seriesMatrixInfo{}, errors.New("geo: invalid series matrix header")
			}
			sampleIDs = hdr[1:]
			// Write header for expression.tsv
			if _, err := exprW.WriteString(strings.Join(hdr, "\t") + "\n"); err != nil {
				return seriesMatrixInfo{}, err
			}
			continue
		}
		if strings.HasPrefix(line, "!series_matrix_table_end") {
			inTable = false
			continue
		}
		if inTable {
			// Expression row; write through unchanged.
			if _, err := exprW.WriteString(line + "\n"); err != nil {
				return seriesMatrixInfo{}, err
			}
			tableRows++
		}
	}
	if err := sc.Err(); err != nil {
		return seriesMatrixInfo{}, err
	}

	// Sample metadata rows: keep 1 row per raw "!Sample_*" line.
	if len(sampleIDs) > 0 && len(sampleRows) > 0 {
		if err := writeMatrixRows(filepath.Join(outDir, "sample_kv.tsv"), sampleIDs, sampleRows); err != nil {
			return seriesMatrixInfo{}, err
		}
	}

	supURLs := make([]string, 0)
	var seriesTypes []string
	var platformIDs []string
	if len(seriesRows) > 0 {
		kv := make(map[string][]string, len(seriesRows))
		for _, r := range seriesRows {
			kv[r.Field] = append(kv[r.Field], r.Values...)
			if r.Field == "Series_supplementary_file" {
				supURLs = append(supURLs, r.Values...)
			}
			if r.Field == "Series_type" {
				seriesTypes = append(seriesTypes, r.Values...)
			}
			if r.Field == "Series_platform_id" {
				platformIDs = append(platformIDs, r.Values...)
			}
		}
		if err := writeLongKV(filepath.Join(outDir, "series_kv.tsv"), "series", kv); err != nil {
			return seriesMatrixInfo{}, err
		}
	}
	for _, r := range sampleRows {
		if r.Field == "Sample_supplementary_file" {
			supURLs = append(supURLs, r.Values...)
		}
	}

	return seriesMatrixInfo{
		SupURLs:       dedupStrings(supURLs),
		TableRowCount: tableRows,
		SeriesTypes:   dedupStrings(seriesTypes),
		PlatformIDs:   dedupStrings(platformIDs),
	}, nil
}

func dedupStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func splitMatrixKV(line string) (key string, values []string) {
	parts := strings.Split(line, "\t")
	if len(parts) < 2 {
		return "", nil
	}
	// Keep the key without leading '!'.
	key = strings.TrimPrefix(parts[0], "!")
	return key, parts[1:]
}

func writeMatrixRows(path string, sampleIDs []string, rows []matrixRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	if _, err := w.WriteString("field"); err != nil {
		return err
	}
	for _, sid := range sampleIDs {
		if _, err := w.WriteString("\t" + sid); err != nil {
			return err
		}
	}
	if _, err := w.WriteString("\n"); err != nil {
		return err
	}

	seen := map[string]int{}
	for _, r := range rows {
		field := r.Field
		seen[field]++
		if seen[field] > 1 {
			field = fmt.Sprintf("%s#%d", field, seen[r.Field])
		}
		if _, err := w.WriteString(field); err != nil {
			return err
		}
		for i := range sampleIDs {
			v := ""
			if i < len(r.Values) {
				v = r.Values[i]
			}
			if _, err := w.WriteString("\t" + v); err != nil {
				return err
			}
		}
		if _, err := w.WriteString("\n"); err != nil {
			return err
		}
	}
	return nil
}

func writeLongKV(path string, prefix string, kv map[string][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	if _, err := w.WriteString(prefix + "\tfield\tvalue\n"); err != nil {
		return err
	}
	for field, vals := range kv {
		for _, v := range vals {
			if _, err := w.WriteString(prefix + "\t" + field + "\t" + strings.ReplaceAll(v, "\n", " ") + "\n"); err != nil {
				return err
			}
		}
	}
	return nil
}

var (
	reSoftSeriesKV = regexp.MustCompile(`^!Series_([A-Za-z0-9_]+)\s*=\s*(.*)$`)
	reSoftSampleKV = regexp.MustCompile(`^!Sample_([A-Za-z0-9_]+)\s*=\s*(.*)$`)
)

func parseSoft(r io.Reader) (seriesKV map[string][]string, sampleKV map[string][]string, supplementaryURLs []string) {
	seriesKV = make(map[string][]string)
	sampleKV = make(map[string][]string)

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if m := reSoftSeriesKV.FindStringSubmatch(line); m != nil {
			k, v := "Series_"+m[1], m[2]
			seriesKV[k] = append(seriesKV[k], v)
			if k == "Series_supplementary_file" {
				supplementaryURLs = append(supplementaryURLs, v)
			}
			continue
		}
		if m := reSoftSampleKV.FindStringSubmatch(line); m != nil {
			k, v := "Sample_"+m[1], m[2]
			sampleKV[k] = append(sampleKV[k], v)
			if k == "Sample_supplementary_file" {
				supplementaryURLs = append(supplementaryURLs, v)
			}
			continue
		}
	}
	return seriesKV, sampleKV, supplementaryURLs
}

func downloadSupplementary(ctx context.Context, c *httpx.Client, urls []string, outDir string) error {
	if len(urls) == 0 {
		return nil
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	for _, u := range urls {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		u = strings.Replace(u, "ftp://", "https://", 1)
		name := safeLeaf(u)
		if name == "" {
			name = "supplementary.bin"
		}
		dest := filepath.Join(outDir, name)
		_, err := c.DownloadToFile(ctx, u, dest, false)
		if err != nil {
			// Best-effort: skip failures.
			continue
		}
	}
	return nil
}

var reGPL = regexp.MustCompile(`GPL[0-9]+`)

func extractGPLs(values []string) []string {
	var out []string
	for _, v := range values {
		for _, g := range reGPL.FindAllString(strings.ToUpper(v), -1) {
			out = append(out, g)
		}
	}
	return dedupStrings(out)
}

func isMicroarray(seriesTypes []string, platformIDs []string) bool {
	_ = platformIDs // reserved for future heuristics
	for _, t := range seriesTypes {
		t = strings.ToLower(strings.TrimSpace(t))
		if strings.Contains(t, "array") || strings.Contains(t, "chip") {
			return true
		}
	}
	return false
}

func platformAnnotURL(gpl string) string {
	gpl = strings.ToUpper(strings.TrimSpace(gpl))
	return fmt.Sprintf("%s/geo/platforms/%s/%s/annot/%s.annot.gz", geoBaseURL(), geoPlatformGroup(gpl), gpl, gpl)
}

func platformSoftURL(gpl string) string {
	gpl = strings.ToUpper(strings.TrimSpace(gpl))
	return fmt.Sprintf("%s/geo/platforms/%s/%s/soft/%s_family.soft.gz", geoBaseURL(), geoPlatformGroup(gpl), gpl, gpl)
}

func downloadPlatformAnnotation(ctx context.Context, c *httpx.Client, gpl string, outDir string) (string, error) {
	gpl = strings.ToUpper(strings.TrimSpace(gpl))
	if gpl == "" {
		return "", nil
	}
	platDir := filepath.Join(outDir, "platform")
	if err := os.MkdirAll(platDir, 0o755); err != nil {
		return "", err
	}

	annotURL := platformAnnotURL(gpl)
	annotPath := filepath.Join(platDir, gpl+".annot.gz")
	if _, err := c.DownloadToFileMaybe(ctx, annotURL, annotPath, false); err == nil {
		return annotPath, nil
	} else if !errors.Is(err, httpx.ErrNotFound) {
		return "", err
	}

	softURL := platformSoftURL(gpl)
	softPath := filepath.Join(platDir, gpl+"_family.soft.gz")
	if _, err := c.DownloadToFileMaybe(ctx, softURL, softPath, false); err == nil {
		return softPath, nil
	} else if !errors.Is(err, httpx.ErrNotFound) {
		return "", err
	}

	// Some platforms don't expose annotation in expected locations.
	return "", nil
}

func safeLeaf(u string) string {
	u = strings.TrimSuffix(u, "/")
	i := strings.LastIndex(u, "/")
	if i >= 0 && i+1 < len(u) {
		return u[i+1:]
	}
	return ""
}

package xena

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"getdown/internal/httpx"
)

type hubClient struct {
	base string
	http *httpx.Client
}

func newHubClient(base string) *hubClient {
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	if base == "" {
		base = "https://gdc.xenahubs.net"
	}
	return &hubClient{
		base: base,
		http: httpx.New(),
	}
}

func (c *hubClient) postEDN(ctx context.Context, edn string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/data/", strings.NewReader(edn))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Redirect-To", "https://xenabrowser.net")
	req.Header.Set("User-Agent", "getdown/0.1 (+https://github.com)")

	resp, err := c.http.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := ioReadAllLimit(resp.Body, 8<<10)
		return fmt.Errorf("xena hub POST %s/data/: %s: %s", c.base, resp.Status, strings.TrimSpace(string(b)))
	}

	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	return dec.Decode(out)
}

func ioReadAllLimit(r io.Reader, limit int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, limit))
}

// Minimal EDN marshalling for our query calls.
func ednCall(fnExpr string, args ...string) string {
	fnExpr = strings.TrimSpace(fnExpr)
	if len(args) == 0 {
		return "(" + fnExpr + ")"
	}
	return "(" + fnExpr + " " + strings.Join(args, " ") + ")"
}

func ednString(s string) string {
	// EDN string quoting, minimal escapes.
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return `"` + s + `"`
}

func ednInt(n int) string { return strconv.Itoa(n) }

func ednVecStrings(v []string) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, s := range v {
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(ednString(s))
	}
	b.WriteByte(']')
	return b.String()
}

const (
	fnDatasetMetadata = `(fn [dataset]
    (let [count-table {:select [[:dataset.name :dname] [:%count.value :count]]
                       :from [:dataset]
                       :join [:field [:= :dataset.id :dataset_id]
                       :code [:= :field.id :field_id]]
                       :group-by [:dataset.name]
                       :where [:= :field.name "sampleID"]}]
        (query {:select [:d.name :d.longtitle :count :d.type :d.datasubtype :d.probemap :d.text :d.status [:pm-dataset.text :pmtext]]
                   :from [[:dataset :d]]
                   :left-join [[:dataset :pm-dataset] [:= :pm-dataset.name :d.probemap]
                                count-table [:= :dname :d.name]]
                   :where [:= :d.name dataset]})))`

	fnDatasetSamples = `(fn [dataset]
    (map :value
      (query
        {:select [:value]
         :from [:dataset]
         :join [:field [:= :dataset.id :dataset_id]
                :code [:= :field.id :field_id]]
         :where [:and
                 [:= :dataset.name dataset]
                 [:= :field.name "sampleID"]]})))`

	fnDatasetField = `(fn [dataset]
  (map :name (query {:select [:field.name]
                     :from [:dataset]
                     :join [:field [:= :dataset.id :dataset_id]]
                     :where [:= :dataset.name dataset]})))`

	fnDatasetFetch = `(fn [dataset samples probes]
  (fetch [{:table dataset
           :columns probes
           :samples samples}]))`

	// Query probemap identifiers with pagination.
	fnProbemapNames = `(fn [probemap limit offset]
  (xena-query {:select ["name"] :from [probemap] :limit limit :offset offset}))`

	// Generic row query for non-matrix datasets (mutation, segment, etc.).
	fnDatasetQueryRows = `(fn [dataset fields limit offset]
  (xena-query {:select fields :from [dataset] :limit limit :offset offset}))`
)

type datasetMeta struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Probemap string `json:"probemap"`
	Status   string `json:"status"`
}

func (c *hubClient) listDatasetsByPrefix(ctx context.Context, prefix string) ([]datasetMeta, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return nil, fmt.Errorf("xena hub: empty prefix")
	}
	// List all datasets whose name begins with "<project>." (covers all omics + resources).
	like := prefix + "%"
	edn := "(query {:select [:name :type :probemap :status] :from [:dataset] :where [:like :name " + ednString(like) + "]})"
	var out []datasetMeta
	if err := c.postEDN(ctx, edn, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *hubClient) getDatasetMeta(ctx context.Context, dataset string) (datasetMeta, error) {
	var out []datasetMeta
	if err := c.postEDN(ctx, ednCall(fnDatasetMetadata, ednString(dataset)), &out); err != nil {
		return datasetMeta{}, err
	}
	if len(out) == 0 {
		return datasetMeta{}, fmt.Errorf("xena hub: dataset not found: %s", dataset)
	}
	return out[0], nil
}

func (c *hubClient) getDatasetSamples(ctx context.Context, dataset string) ([]string, error) {
	var out []string
	if err := c.postEDN(ctx, ednCall(fnDatasetSamples, ednString(dataset)), &out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("xena hub: no samples for dataset: %s", dataset)
	}
	return out, nil
}

func (c *hubClient) getDatasetFields(ctx context.Context, dataset string) ([]string, error) {
	var out []string
	if err := c.postEDN(ctx, ednCall(fnDatasetField, ednString(dataset)), &out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("xena hub: no fields for dataset: %s", dataset)
	}
	return out, nil
}

type probemapNamesResp struct {
	Name []string `json:"name"`
}

func (c *hubClient) listProbemapNames(ctx context.Context, probemap string) ([]string, error) {
	const pageSize = 5000
	var out []string
	for offset := 0; ; offset += pageSize {
		var resp probemapNamesResp
		edn := ednCall(fnProbemapNames, ednString(probemap), ednInt(pageSize), ednInt(offset))
		if err := c.postEDN(ctx, edn, &resp); err != nil {
			return nil, err
		}
		if len(resp.Name) == 0 {
			break
		}
		out = append(out, resp.Name...)
		if len(resp.Name) < pageSize {
			break
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("xena hub: empty probemap: %s", probemap)
	}
	return out, nil
}

func (c *hubClient) fetchFloatMatrix(ctx context.Context, dataset string, samples []string, probes []string) ([][]*float64, error) {
	var out [][]*float64
	edn := ednCall(fnDatasetFetch, ednString(dataset), ednVecStrings(samples), ednVecStrings(probes))
	if err := c.postEDN(ctx, edn, &out); err != nil {
		return nil, err
	}
	if len(out) != len(probes) {
		return nil, fmt.Errorf("xena hub: unexpected row count: got %d want %d", len(out), len(probes))
	}
	return out, nil
}

func (c *hubClient) fetchAnyMatrix(ctx context.Context, dataset string, samples []string, fields []string) ([][]any, error) {
	var out [][]any
	edn := ednCall(fnDatasetFetch, ednString(dataset), ednVecStrings(samples), ednVecStrings(fields))
	if err := c.postEDN(ctx, edn, &out); err != nil {
		return nil, err
	}
	if len(out) != len(fields) {
		return nil, fmt.Errorf("xena hub: unexpected row count: got %d want %d", len(out), len(fields))
	}
	return out, nil
}

type hubDownloadResult struct {
	ExpressionTSV       string
	ClinicalTSV         string
	HubBase             string
	ExpressionDataset   string
	ClinicalDataset     string
	ExpressionProbemap  string
	ExpressionSamples   int
	ExpressionProbes    int
	ClinicalFieldsCount int
}

type hubDownloadAllResult struct {
	HubBase       string
	DatasetFiles  map[string]string // dataset name -> output file path
	ExpressionTSV string            // outDir/expression.tsv (best-effort link/copy)
	ClinicalTSV   string            // outDir/clinical.tsv (best-effort link/copy)

	ExpressionDataset string
	ClinicalDataset   string
}

func downloadTCGACoreFromHub(ctx context.Context, project, outDir string) (hubDownloadAllResult, error) {
	c := newHubClient(os.Getenv("GETDOWN_XENA_HUB"))
	datasets, err := c.listDatasetsByPrefix(ctx, project+".")
	if err != nil {
		return hubDownloadAllResult{}, err
	}
	if len(datasets) == 0 {
		return hubDownloadAllResult{}, fmt.Errorf("xena hub: no datasets for project: %s", project)
	}

	have := make(map[string]bool, len(datasets))
	byName := make(map[string]datasetMeta, len(datasets))
	for _, d := range datasets {
		have[d.Name] = true
		byName[d.Name] = d
	}

	exprDataset := preferredExpressionDataset(project, have)
	clinDataset := preferredClinicalDataset(project, have)
	if exprDataset == "" {
		return hubDownloadAllResult{}, fmt.Errorf("xena hub: no preferred expression dataset for project: %s", project)
	}

	files := make(map[string]string)

	// Expression
	exprMeta := byName[exprDataset]
	if exprMeta.Status != "" && exprMeta.Status != "loaded" {
		return hubDownloadAllResult{}, fmt.Errorf("xena hub: expression dataset not loaded: %s (status=%s)", exprDataset, exprMeta.Status)
	}
	if exprMeta.Type != "genomicMatrix" {
		return hubDownloadAllResult{}, fmt.Errorf("xena hub: expression dataset is not genomicMatrix: %s (type=%s)", exprDataset, exprMeta.Type)
	}
	if exprMeta.Probemap == "" {
		meta, err := c.getDatasetMeta(ctx, exprDataset)
		if err != nil {
			return hubDownloadAllResult{}, err
		}
		exprMeta.Probemap = meta.Probemap
	}
	if exprMeta.Probemap == "" {
		return hubDownloadAllResult{}, fmt.Errorf("xena hub: missing probemap for expression dataset: %s", exprDataset)
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return hubDownloadAllResult{}, err
	}
	exprOut := filepath.Join(outDir, "expression.tsv")
	if _, _, err := downloadMatrixDataset(ctx, c, exprDataset, exprMeta.Probemap, exprOut); err != nil {
		return hubDownloadAllResult{}, err
	}
	files[exprDataset] = exprOut

	// Clinical (best-effort)
	clinicalOut := filepath.Join(outDir, "clinical.tsv")
	exprSamples, _ := c.getDatasetSamples(ctx, exprDataset)
	if clinDataset != "" {
		clinMeta := byName[clinDataset]
		if clinMeta.Status != "" && clinMeta.Status != "loaded" {
			// skip
		} else if clinMeta.Type == "clinicalMatrix" {
			if _, err := downloadClinicalDataset(ctx, c, clinDataset, exprSamples, clinicalOut); err == nil {
				files[clinDataset] = clinicalOut
			}
		}
	}

	return hubDownloadAllResult{
		HubBase:           c.base,
		DatasetFiles:      files,
		ExpressionTSV:     exprOut,
		ClinicalTSV:       clinicalOut,
		ExpressionDataset: exprDataset,
		ClinicalDataset:   clinDataset,
	}, nil
}

func safeDatasetFilename(dataset string) string {
	dataset = strings.TrimSpace(dataset)
	if dataset == "" {
		return "dataset.tsv"
	}
	var b strings.Builder
	b.Grow(len(dataset))
	for _, r := range dataset {
		switch r {
		case '/', '\\', ':', '*', '?', '"', '<', '>', '|':
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func datasetOutName(dataset string) string {
	name := safeDatasetFilename(dataset)
	if strings.HasSuffix(strings.ToLower(name), ".tsv") || strings.HasSuffix(strings.ToLower(name), ".txt") || strings.HasSuffix(strings.ToLower(name), ".csv") || strings.HasSuffix(strings.ToLower(name), ".bed") {
		return name
	}
	return name + ".tsv"
}

func preferredExpressionDataset(project string, have map[string]bool) string {
	cands := []string{
		project + ".star_counts.tsv",
		project + ".htseq_counts.tsv",
		project + ".htseq_fpkm.tsv",
		project + ".htseq_tpm.tsv",
	}
	for _, c := range cands {
		if have[c] {
			return c
		}
	}
	return ""
}

func preferredClinicalDataset(project string, have map[string]bool) string {
	cands := []string{
		project + ".clinical.tsv",
		project + ".GDC_phenotype.tsv",
		project + ".phenotype.tsv",
	}
	for _, c := range cands {
		if have[c] {
			return c
		}
	}
	// Some hubs expose phenotype under other names; accept any "<project>.*clinical*".
	for d := range have {
		ld := strings.ToLower(d)
		if strings.HasPrefix(ld, strings.ToLower(project)+".") && strings.Contains(ld, "clinical") {
			return d
		}
	}
	return ""
}

func (c *hubClient) queryRows(ctx context.Context, dataset string, fields []string, limit, offset int) (map[string][]any, error) {
	out := make(map[string][]any)
	edn := ednCall(fnDatasetQueryRows, ednString(dataset), ednVecStrings(fields), ednInt(limit), ednInt(offset))
	if err := c.postEDN(ctx, edn, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeTSVCell(w *bufio.Writer, v any) error {
	if v == nil {
		return nil
	}
	switch vv := v.(type) {
	case string:
		_, err := w.WriteString(strings.ReplaceAll(vv, "\n", " "))
		return err
	case json.Number:
		_, err := w.WriteString(vv.String())
		return err
	case float64:
		_, err := w.WriteString(strconv.FormatFloat(vv, 'g', -1, 64))
		return err
	case bool:
		if vv {
			_, err := w.WriteString("true")
			return err
		}
		_, err := w.WriteString("false")
		return err
	default:
		_, err := w.WriteString(fmt.Sprint(v))
		return err
	}
}

func linkOrCopyFile(dst, src string) error {
	_ = os.Remove(dst)
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	if err := os.Symlink(src, dst); err == nil {
		return nil
	}
	return copyFile(dst, src)
}

func copyFile(dst, src string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}

func downloadMatrixDataset(ctx context.Context, c *hubClient, dataset, probemap, outPath string) (samplesCount int, probesCount int, err error) {
	samples, err := c.getDatasetSamples(ctx, dataset)
	if err != nil {
		return 0, 0, err
	}
	probes, err := c.listProbemapNames(ctx, probemap)
	if err != nil {
		return 0, 0, err
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return 0, 0, err
	}
	f, err := os.Create(outPath)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	// Header
	if _, err := w.WriteString("gene_id"); err != nil {
		return 0, 0, err
	}
	for _, s := range samples {
		if _, err := w.WriteString("\t" + s); err != nil {
			return 0, 0, err
		}
	}
	if _, err := w.WriteString("\n"); err != nil {
		return 0, 0, err
	}

	// Fetch and write in probe batches.
	const probeBatch = 500
	rowBuf := make([]byte, 0, 64*1024)
	for i := 0; i < len(probes); i += probeBatch {
		j := i + probeBatch
		if j > len(probes) {
			j = len(probes)
		}
		batch := probes[i:j]
		mat, err := c.fetchFloatMatrix(ctx, dataset, samples, batch)
		if err != nil {
			return 0, 0, err
		}
		for r, probe := range batch {
			rowBuf = rowBuf[:0]
			rowBuf = append(rowBuf, probe...)
			vals := mat[r]
			if len(vals) != len(samples) {
				return 0, 0, fmt.Errorf("xena hub: unexpected col count for %s: got %d want %d", probe, len(vals), len(samples))
			}
			for _, v := range vals {
				rowBuf = append(rowBuf, '\t')
				if v == nil {
					continue
				}
				rowBuf = strconv.AppendFloat(rowBuf, *v, 'g', -1, 64)
			}
			rowBuf = append(rowBuf, '\n')
			if _, err := w.Write(rowBuf); err != nil {
				return 0, 0, err
			}
		}
	}
	return len(samples), len(probes), nil
}

func downloadClinicalDataset(ctx context.Context, c *hubClient, dataset string, fallbackSamples []string, outPath string) (fieldsCount int, err error) {
	fields, err := c.getDatasetFields(ctx, dataset)
	if err != nil {
		return 0, err
	}
	samples, err := c.getDatasetSamples(ctx, dataset)
	if err != nil {
		if len(fallbackSamples) == 0 {
			return 0, err
		}
		samples = fallbackSamples
	}
	filtered := make([]string, 0, len(fields))
	for _, f := range fields {
		if f == "sampleID" {
			continue
		}
		filtered = append(filtered, f)
	}
	mat, err := c.fetchAnyMatrix(ctx, dataset, samples, filtered)
	if err != nil {
		return 0, err
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return 0, err
	}
	f, err := os.Create(outPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	// Header: sampleID + fields...
	if _, err := w.WriteString("sampleID"); err != nil {
		return 0, err
	}
	for _, f := range filtered {
		if _, err := w.WriteString("\t" + f); err != nil {
			return 0, err
		}
	}
	if _, err := w.WriteString("\n"); err != nil {
		return 0, err
	}

	// Transpose to sample-major.
	for si, sid := range samples {
		if _, err := w.WriteString(sid); err != nil {
			return 0, err
		}
		for fi := range filtered {
			_ = mat[fi]
			if err := w.WriteByte('\t'); err != nil {
				return 0, err
			}
			if si >= len(mat[fi]) {
				continue
			}
			if err := writeTSVCell(w, mat[fi][si]); err != nil {
				return 0, err
			}
		}
		if _, err := w.WriteString("\n"); err != nil {
			return 0, err
		}
	}
	return len(filtered), nil
}

func downloadQueryDataset(ctx context.Context, c *hubClient, dataset string, outPath string) (int, error) {
	fields, err := c.getDatasetFields(ctx, dataset)
	if err != nil {
		return 0, err
	}
	if len(fields) == 0 {
		return 0, fmt.Errorf("xena hub: no fields for dataset: %s", dataset)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return 0, err
	}
	f, err := os.Create(outPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	// Header
	if _, err := w.WriteString(strings.Join(fields, "\t") + "\n"); err != nil {
		return 0, err
	}

	const pageSize = 50000
	rowsWritten := 0
	for offset := 0; ; offset += pageSize {
		page, err := c.queryRows(ctx, dataset, fields, pageSize, offset)
		if err != nil {
			return rowsWritten, err
		}
		// Determine row count based on the first column.
		n := 0
		if col := page[fields[0]]; len(col) > 0 {
			n = len(col)
		}
		if n == 0 {
			break
		}
		for i := 0; i < n; i++ {
			for fi, f := range fields {
				if fi > 0 {
					if err := w.WriteByte('\t'); err != nil {
						return rowsWritten, err
					}
				}
				col := page[f]
				if i >= len(col) {
					continue
				}
				if err := writeTSVCell(w, col[i]); err != nil {
					return rowsWritten, err
				}
			}
			if _, err := w.WriteString("\n"); err != nil {
				return rowsWritten, err
			}
			rowsWritten++
		}
		if n < pageSize {
			break
		}
	}
	return rowsWritten, nil
}

func downloadTCGAAllFromHub(ctx context.Context, project, outDir string) (hubDownloadAllResult, error) {
	c := newHubClient(os.Getenv("GETDOWN_XENA_HUB"))
	datasets, err := c.listDatasetsByPrefix(ctx, project+".")
	if err != nil {
		return hubDownloadAllResult{}, err
	}
	if len(datasets) == 0 {
		return hubDownloadAllResult{}, fmt.Errorf("xena hub: no datasets for project: %s", project)
	}

	omicsDir := filepath.Join(outDir, "omics")
	if err := os.MkdirAll(omicsDir, 0o755); err != nil {
		return hubDownloadAllResult{}, err
	}

	have := make(map[string]bool, len(datasets))
	for _, d := range datasets {
		have[d.Name] = true
	}
	exprDataset := preferredExpressionDataset(project, have)
	clinDataset := preferredClinicalDataset(project, have)
	var exprSamples []string
	if exprDataset != "" {
		if s, err := c.getDatasetSamples(ctx, exprDataset); err == nil {
			exprSamples = s
		}
	}

	files := make(map[string]string, len(datasets))
	for _, d := range datasets {
		if d.Name == "" {
			continue
		}
		if d.Status != "" && d.Status != "loaded" {
			continue
		}
		outPath := filepath.Join(omicsDir, datasetOutName(d.Name))
		switch d.Type {
		case "genomicMatrix":
			if d.Probemap == "" {
				// Try to look it up via metadata (some hubs omit probemap in query output).
				meta, err := c.getDatasetMeta(ctx, d.Name)
				if err != nil {
					return hubDownloadAllResult{}, err
				}
				d.Probemap = meta.Probemap
			}
			if d.Probemap == "" {
				return hubDownloadAllResult{}, fmt.Errorf("xena hub: missing probemap for genomicMatrix dataset: %s", d.Name)
			}
			if _, _, err := downloadMatrixDataset(ctx, c, d.Name, d.Probemap, outPath); err != nil {
				return hubDownloadAllResult{}, err
			}
			files[d.Name] = outPath
		case "clinicalMatrix":
			if _, err := downloadClinicalDataset(ctx, c, d.Name, exprSamples, outPath); err != nil {
				return hubDownloadAllResult{}, err
			}
			files[d.Name] = outPath
		default:
			if _, err := downloadQueryDataset(ctx, c, d.Name, outPath); err != nil {
				return hubDownloadAllResult{}, err
			}
			files[d.Name] = outPath
		}
	}

	// Best-effort: provide stable top-level names.
	exprTSV := filepath.Join(outDir, "expression.tsv")
	clinTSV := filepath.Join(outDir, "clinical.tsv")
	if exprDataset != "" {
		if src := files[exprDataset]; src != "" {
			_ = linkOrCopyFile(exprTSV, src)
		}
	}
	if clinDataset != "" {
		if src := files[clinDataset]; src != "" {
			_ = linkOrCopyFile(clinTSV, src)
		}
	}

	return hubDownloadAllResult{
		HubBase:            c.base,
		DatasetFiles:       files,
		ExpressionTSV:      exprTSV,
		ClinicalTSV:        clinTSV,
		ExpressionDataset:  exprDataset,
		ClinicalDataset:    clinDataset,
	}, nil
}

func downloadTCGAFromHub(ctx context.Context, project, outDir, rawDir string) (hubDownloadResult, error) {
	c := newHubClient(os.Getenv("GETDOWN_XENA_HUB"))
	exprDataset := project + ".star_counts.tsv"
	clinicalDataset := project + ".clinical.tsv"

	exprMeta, err := c.getDatasetMeta(ctx, exprDataset)
	if err != nil {
		return hubDownloadResult{}, err
	}
	if exprMeta.Status != "" && exprMeta.Status != "loaded" {
		return hubDownloadResult{}, fmt.Errorf("xena hub: expression dataset not loaded: %s (status=%s)", exprDataset, exprMeta.Status)
	}
	if exprMeta.Probemap == "" {
		return hubDownloadResult{}, fmt.Errorf("xena hub: missing probemap for expression dataset: %s", exprDataset)
	}

	samples, err := c.getDatasetSamples(ctx, exprDataset)
	if err != nil {
		return hubDownloadResult{}, err
	}
	probes, err := c.listProbemapNames(ctx, exprMeta.Probemap)
	if err != nil {
		return hubDownloadResult{}, err
	}

	exprOut := filepath.Join(outDir, "expression.tsv")
	if err := os.MkdirAll(filepath.Dir(exprOut), 0o755); err != nil {
		return hubDownloadResult{}, err
	}
	f, err := os.Create(exprOut)
	if err != nil {
		return hubDownloadResult{}, err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	defer w.Flush()

	// Header
	if _, err := w.WriteString("gene_id"); err != nil {
		return hubDownloadResult{}, err
	}
	for _, s := range samples {
		if _, err := w.WriteString("\t" + s); err != nil {
			return hubDownloadResult{}, err
		}
	}
	if _, err := w.WriteString("\n"); err != nil {
		return hubDownloadResult{}, err
	}

	// Fetch and write in probe batches.
	const probeBatch = 500
	rowBuf := make([]byte, 0, 64*1024)
	for i := 0; i < len(probes); i += probeBatch {
		j := i + probeBatch
		if j > len(probes) {
			j = len(probes)
		}
		batch := probes[i:j]
		mat, err := c.fetchFloatMatrix(ctx, exprDataset, samples, batch)
		if err != nil {
			return hubDownloadResult{}, err
		}
		for r, probe := range batch {
			rowBuf = rowBuf[:0]
			rowBuf = append(rowBuf, probe...)
			vals := mat[r]
			if len(vals) != len(samples) {
				return hubDownloadResult{}, fmt.Errorf("xena hub: unexpected col count for %s: got %d want %d", probe, len(vals), len(samples))
			}
			for _, v := range vals {
				rowBuf = append(rowBuf, '\t')
				if v == nil {
					// empty
					continue
				}
				rowBuf = strconv.AppendFloat(rowBuf, *v, 'g', -1, 64)
			}
			rowBuf = append(rowBuf, '\n')
			if _, err := w.Write(rowBuf); err != nil {
				return hubDownloadResult{}, err
			}
		}
	}

	// Clinical
	clinicalOut := filepath.Join(outDir, "clinical.tsv")
	fields, err := c.getDatasetFields(ctx, clinicalDataset)
	if err != nil {
		return hubDownloadResult{}, err
	}
	clinicalSamples, err := c.getDatasetSamples(ctx, clinicalDataset)
	if err != nil {
		// Fall back to expression samples when sampleID field isn't present for clinical.
		clinicalSamples = samples
	}
	filtered := make([]string, 0, len(fields))
	for _, f := range fields {
		if f == "sampleID" {
			continue
		}
		filtered = append(filtered, f)
	}
	mat, err := c.fetchAnyMatrix(ctx, clinicalDataset, clinicalSamples, filtered)
	if err != nil {
		return hubDownloadResult{}, err
	}

	cf, err := os.Create(clinicalOut)
	if err != nil {
		return hubDownloadResult{}, err
	}
	defer cf.Close()
	cw := bufio.NewWriterSize(cf, 1<<20)
	defer cw.Flush()

	// Header: sampleID + fields...
	if _, err := cw.WriteString("sampleID"); err != nil {
		return hubDownloadResult{}, err
	}
	for _, f := range filtered {
		if _, err := cw.WriteString("\t" + f); err != nil {
			return hubDownloadResult{}, err
		}
	}
	if _, err := cw.WriteString("\n"); err != nil {
		return hubDownloadResult{}, err
	}

	// Transpose to sample-major.
	for si, sid := range clinicalSamples {
		if _, err := cw.WriteString(sid); err != nil {
			return hubDownloadResult{}, err
		}
		for fi := range filtered {
			_ = mat[fi]
			cw.WriteByte('\t')
			if si >= len(mat[fi]) {
				continue
			}
			v := mat[fi][si]
			if v == nil {
				continue
			}
			switch vv := v.(type) {
			case string:
				_, _ = cw.WriteString(strings.ReplaceAll(vv, "\n", " "))
			case json.Number:
				_, _ = cw.WriteString(vv.String())
			case float64:
				_, _ = cw.WriteString(strconv.FormatFloat(vv, 'g', -1, 64))
			case bool:
				if vv {
					_, _ = cw.WriteString("true")
				} else {
					_, _ = cw.WriteString("false")
				}
			default:
				_, _ = cw.WriteString(fmt.Sprint(v))
			}
		}
		if _, err := cw.WriteString("\n"); err != nil {
			return hubDownloadResult{}, err
		}
	}

	return hubDownloadResult{
		ExpressionTSV:       exprOut,
		ClinicalTSV:         clinicalOut,
		HubBase:             c.base,
		ExpressionDataset:   exprDataset,
		ClinicalDataset:     clinicalDataset,
		ExpressionProbemap:  exprMeta.Probemap,
		ExpressionSamples:   len(samples),
		ExpressionProbes:    len(probes),
		ClinicalFieldsCount: len(filtered),
	}, nil
}

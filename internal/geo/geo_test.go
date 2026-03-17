package geo

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeoSeriesGroup(t *testing.T) {
	if got, want := geoSeriesGroup("GSE13535"), "GSE13nnn"; got != want {
		t.Fatalf("geoSeriesGroup: got %q want %q", got, want)
	}
	if got, want := geoSeriesGroup("GSE999"), "GSEnnn"; got != want {
		t.Fatalf("geoSeriesGroup: got %q want %q", got, want)
	}
}

func TestParseSeriesMatrixWritesFiles(t *testing.T) {
	dir := t.TempDir()
	input := strings.Join([]string{
		"!Series_title\tMy series",
		"!Series_supplementary_file\thttps://example.com/a.txt",
		"!Sample_title\tSample 1\tSample 2",
		"!Sample_supplementary_file\tftp://ftp.ncbi.nlm.nih.gov/geo/somefile\tftp://ftp.ncbi.nlm.nih.gov/geo/otherfile",
		"!series_matrix_table_begin",
		"ID_REF\tGSM1\tGSM2",
		"geneA\t1\t2",
		"geneB\t3\t4",
	"!series_matrix_table_end",
	"",
}, "\n")

	info, err := parseSeriesMatrix(bytes.NewReader([]byte(input)), dir)
	if err != nil {
		t.Fatalf("parseSeriesMatrix: %v", err)
	}
	if len(info.SupURLs) != 3 {
		t.Fatalf("supplementary urls: got %d want %d", len(info.SupURLs), 3)
	}
	if got, want := info.TableRowCount, 2; got != want {
		t.Fatalf("table rows: got %d want %d", got, want)
	}

	exprPath := filepath.Join(dir, "expression.tsv")
	b, err := os.ReadFile(exprPath)
	if err != nil {
		t.Fatalf("read expression.tsv: %v", err)
	}
	if !strings.Contains(string(b), "ID_REF\tGSM1\tGSM2\n") {
		t.Fatalf("expression.tsv missing header; got:\n%s", string(b))
	}

	seriesPath := filepath.Join(dir, "series_kv.tsv")
	if _, err := os.Stat(seriesPath); err != nil {
		t.Fatalf("series_kv.tsv missing: %v", err)
	}

	samplePath := filepath.Join(dir, "sample_kv.tsv")
	if _, err := os.Stat(samplePath); err != nil {
		t.Fatalf("sample_kv.tsv missing: %v", err)
	}
}

func TestCleanSupplementaryURL_StripsQuotesAndNormalizesFTP(t *testing.T) {
	in := `"ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE235nnn/GSE235527/suppl/GSE235527_RAW.tar"`
	got := cleanSupplementaryURL(in)
	want := "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE235nnn/GSE235527/suppl/GSE235527_RAW.tar"
	if got != want {
		t.Fatalf("cleanSupplementaryURL: got %q want %q", got, want)
	}
}

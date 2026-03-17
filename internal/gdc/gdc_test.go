package gdc

import (
	"strings"
	"testing"
)

func TestParseCounts_STARWithGeneNameHeader(t *testing.T) {
	input := strings.Join([]string{
		"gene_id\tgene_name\tunstranded\tstranded_first\tstranded_second\ttpm\tfpkm\tfpkm_uq",
		"ENSG1\tA\t5\t1\t0\t0.1\t0.2\t0.3",
		"ENSG2\tB\t7\t2\t0\t0.1\t0.2\t0.3",
		"__no_feature\tNA\t0\t0\t0\t0\t0\t0",
		"",
	}, "\n")

	genes, counts, err := parseCounts(strings.NewReader(input))
	if err != nil {
		t.Fatalf("parseCounts: %v", err)
	}
	if got, want := len(genes), 2; got != want {
		t.Fatalf("genes len: got %d want %d", got, want)
	}
	if genes[0] != "ENSG1" || genes[1] != "ENSG2" {
		t.Fatalf("genes: got %v", genes)
	}
	if counts[0] != 5 || counts[1] != 7 {
		t.Fatalf("counts: got %v", counts)
	}
}


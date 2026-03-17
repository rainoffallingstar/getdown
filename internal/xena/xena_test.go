package xena

import (
	"strings"
	"testing"
)

func TestExpressionCandidates(t *testing.T) {
	cands := expressionCandidates("TCGA-LAML")
	if len(cands) == 0 {
		t.Fatal("no candidates")
	}
	want := "https://gdc-hub.s3.us-east-1.amazonaws.com/download/TCGA-LAML.htseq_counts.tsv.gz"
	found := false
	for _, c := range cands {
		if c == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing expected candidate: %s", want)
	}

	seen := map[string]bool{}
	for _, c := range cands {
		if seen[c] {
			t.Fatalf("duplicate candidate: %s", c)
		}
		seen[c] = true
		if !strings.HasPrefix(c, "https://") {
			t.Fatalf("unexpected scheme: %s", c)
		}
	}
}

func TestPhenotypeCandidates(t *testing.T) {
	cands := phenotypeCandidates("TCGA-LAML")
	if len(cands) == 0 {
		t.Fatal("no candidates")
	}
	// Phenotype files are not guaranteed for all mirrors, but the canonical name should be tried.
	want := "TCGA-LAML.GDC_phenotype.tsv.gz"
	has := false
	for _, c := range cands {
		if strings.HasSuffix(c, "/"+want) || strings.HasSuffix(c, want) {
			has = true
			break
		}
	}
	if !has {
		t.Fatalf("missing phenotype candidate suffix: %s", want)
	}
}

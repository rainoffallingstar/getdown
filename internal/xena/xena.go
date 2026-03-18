package xena

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"getdown/internal/httpx"
)

type TCGARequest struct {
	Project string
	OutDir  string
	RawDir  string // optional
	Mode    string // all|core
	Jobs    int
}

type TCGAResult struct {
	ExpressionTSV       string
	PhenotypeTSV        string
	ExpressionSourceURL string
	PhenotypeSourceURL  string
	DatasetFiles        map[string]string
}

// DownloadTCGA downloads a pre-merged TCGA matrix and phenotype table from UCSC Xena
// mirrors (commonly backed by GDC Hub / Xenahubs). This is intended as a fallback when
// GDC per-sample downloads are slow or error-prone.
func DownloadTCGA(ctx context.Context, req TCGARequest) (TCGAResult, error) {
	if req.Project == "" {
		return TCGAResult{}, errors.New("xena: missing Project")
	}
	if req.OutDir == "" {
		return TCGAResult{}, errors.New("xena: missing OutDir")
	}
	if err := os.MkdirAll(req.OutDir, 0o755); err != nil {
		return TCGAResult{}, err
	}
	if req.RawDir != "" {
		if err := os.MkdirAll(req.RawDir, 0o755); err != nil {
			return TCGAResult{}, err
		}
	}

	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	if mode == "" {
		mode = "all"
	}
	if mode != "all" && mode != "core" {
		return TCGAResult{}, fmt.Errorf("xena: invalid Mode: %q (want all|core)", req.Mode)
	}

	// Prefer the hub API path because it supports multi-omics and doesn't rely on static mirrors.
	var hubErr error
	if mode == "all" {
		if hubRes, err := downloadTCGAAllFromHub(ctx, req.Project, req.OutDir, req.Jobs); err == nil {
			return TCGAResult{
				ExpressionTSV:       hubRes.ExpressionTSV,
				PhenotypeTSV:        hubRes.ClinicalTSV,
				ExpressionSourceURL: hubRes.HubBase + "/data/ (" + hubRes.ExpressionDataset + ")",
				PhenotypeSourceURL:  hubRes.HubBase + "/data/ (" + hubRes.ClinicalDataset + ")",
				DatasetFiles:        hubRes.DatasetFiles,
			}, nil
		} else {
			hubErr = err
		}
	} else {
		if hubRes, err := downloadTCGACoreFromHub(ctx, req.Project, req.OutDir, req.Jobs); err == nil {
			return TCGAResult{
				ExpressionTSV:       hubRes.ExpressionTSV,
				PhenotypeTSV:        hubRes.ClinicalTSV,
				ExpressionSourceURL: hubRes.HubBase + "/data/ (" + hubRes.ExpressionDataset + ")",
				PhenotypeSourceURL:  hubRes.HubBase + "/data/ (" + hubRes.ClinicalDataset + ")",
				DatasetFiles:        hubRes.DatasetFiles,
			}, nil
		} else {
			hubErr = err
		}
	}

	c := httpx.New()

	var exprURL, exprRawPath, phenoURL string
	var exprErr, phenoErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		exprCandidates := expressionCandidates(req.Project)
		exprURL, exprRawPath, exprErr = downloadFirstOK(ctx, c, exprCandidates, req.OutDir, req.RawDir, "expression.tsv")
	}()
	go func() {
		defer wg.Done()
		phenoCandidates := phenotypeCandidates(req.Project)
		phenoURL, _, phenoErr = downloadFirstOK(ctx, c, phenoCandidates, req.OutDir, req.RawDir, "clinical.tsv")
	}()
	wg.Wait()

	if exprErr != nil {
		// Legacy fallback: static mirrors only.
		if hubErr != nil {
			return TCGAResult{}, fmt.Errorf("xena hub failed: %v; static mirrors failed: %w", hubErr, exprErr)
		}
		return TCGAResult{}, fmt.Errorf("xena: expression: %w", exprErr)
	}
	_ = exprRawPath // reserved for metadata later if desired

	if phenoErr != nil {
		// Some projects might not have phenotype mirrored. Keep expression download as success signal.
		phenoURL = ""
	}

	return TCGAResult{
		ExpressionTSV:       filepath.Join(req.OutDir, "expression.tsv"),
		PhenotypeTSV:        filepath.Join(req.OutDir, "clinical.tsv"),
		ExpressionSourceURL: exprURL,
		PhenotypeSourceURL:  phenoURL,
	}, nil
}

func downloadFirstOK(ctx context.Context, c *httpx.Client, urls []string, outDir, rawDir, outName string) (sourceURL string, rawPath string, err error) {
	var lastErr error
	for _, u := range urls {
		outPath := filepath.Join(outDir, outName)

		rawPath = ""
		if rawDir != "" {
			rawPath = filepath.Join(rawDir, safeLeaf(u))
			_, derr := c.DownloadToFileMaybe(ctx, u, rawPath, false)
			if derr != nil {
				if errors.Is(derr, httpx.ErrNotFound) {
					lastErr = derr
					continue
				}
				lastErr = derr
				continue
			}
			// Re-download but gunzipped into final output for consistent downstream use.
			_, derr = c.DownloadToFile(ctx, u, outPath, true)
			if derr != nil {
				lastErr = derr
				continue
			}
		} else {
			_, derr := c.DownloadToFileMaybe(ctx, u, outPath, true)
			if derr != nil {
				if errors.Is(derr, httpx.ErrNotFound) {
					lastErr = derr
					continue
				}
				lastErr = derr
				continue
			}
		}
		return u, rawPath, nil
	}

	if lastErr == nil {
		lastErr = errors.New("no candidates")
	}
	return "", "", lastErr
}

func safeLeaf(u string) string {
	// Keep a readable filename.
	u = strings.TrimSpace(u)
	u = strings.TrimSuffix(u, "/")
	i := strings.LastIndex(u, "/")
	if i >= 0 && i+1 < len(u) {
		return u[i+1:]
	}
	return "download.bin"
}

func expressionCandidates(project string) []string {
	bases := xenaBases()
	files := []string{
		project + ".htseq_counts.tsv.gz",
		project + ".htseq_fpkm.tsv.gz",
		project + ".htseq_fpkm-uq.tsv.gz",
		project + ".htseq_tpm.tsv.gz",
	}

	var out []string
	for _, b := range bases {
		for _, f := range files {
			out = append(out, b+f)
		}
		// Some xenahubs have nested paths.
		for _, f := range files {
			out = append(out, b+project+"/Xena_Matrices/"+f)
		}
	}
	return dedup(out)
}

func phenotypeCandidates(project string) []string {
	bases := xenaBases()
	files := []string{
		project + ".GDC_phenotype.tsv.gz",
		project + ".phenotype.tsv.gz",
		project + ".GDC_phenotype.tsv",
		project + ".phenotype.tsv",
	}

	var out []string
	for _, b := range bases {
		for _, f := range files {
			out = append(out, b+f)
		}
		for _, f := range files {
			out = append(out, b+project+"/Xena_Matrices/"+f)
		}
	}
	return dedup(out)
}

func dedup(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func xenaBases() []string {
	if v := strings.TrimSpace(os.Getenv("GETDOWN_XENA_BASES")); v != "" {
		parts := strings.Split(v, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if !strings.HasSuffix(p, "/") {
				p += "/"
			}
			out = append(out, p)
		}
		if len(out) > 0 {
			return out
		}
	}
	return []string{
		"https://gdc-hub.s3.us-east-1.amazonaws.com/download/",
		"https://gdc.xenahubs.net/download/",
		"https://tcga.xenahubs.net/download/",
	}
}

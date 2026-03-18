package tcga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"getdown/internal/gdc"
	"getdown/internal/meta"
	"getdown/internal/xena"
)

type Request struct {
	Project  string
	OutDir   string
	Provider string // auto|gdc|xena
	Workflow string
	KeepRaw  bool
	XenaMode string // all|core
	Jobs     int
}

func Download(ctx context.Context, req Request) error {
	req.Project = strings.ToUpper(strings.TrimSpace(req.Project))
	if req.Project == "" {
		return errors.New("tcga: missing Project")
	}
	if !strings.HasPrefix(req.Project, "TCGA-") {
		return fmt.Errorf("tcga: invalid Project: %q (want TCGA-*)", req.Project)
	}
	if err := os.MkdirAll(req.OutDir, 0o755); err != nil {
		return err
	}

	provider := strings.ToLower(strings.TrimSpace(req.Provider))
	if provider == "" {
		provider = "auto"
	}
	if provider != "auto" && provider != "gdc" && provider != "xena" {
		return fmt.Errorf("invalid --provider: %q (want auto|gdc|xena)", req.Provider)
	}

	params, _ := json.Marshal(req)
	metaPath := filepath.Join(req.OutDir, "metadata.json")

	switch provider {
	case "gdc":
		return downloadGDC(ctx, req, metaPath, params)
	case "xena":
		return downloadXena(ctx, req, metaPath, params)
	default:
		// auto: Xena first, then GDC.
		if err := downloadXena(ctx, req, metaPath, params); err == nil {
			return nil
		} else {
			var xErr = err
			if gerr := downloadGDC(ctx, req, metaPath, params); gerr == nil {
				return nil
			} else {
				return errors.Join(fmt.Errorf("Xena failed: %w", xErr), fmt.Errorf("GDC fallback failed: %w", gerr))
			}
		}
	}
}

func downloadGDC(ctx context.Context, req Request, metaPath string, params []byte) error {
	rawDir := ""
	if req.KeepRaw {
		rawDir = filepath.Join(req.OutDir, "raw", "gdc")
	}
	res, err := gdc.Download(ctx, gdc.Request{
		Project:  req.Project,
		OutDir:   req.OutDir,
		RawDir:   rawDir,
		Workflow: req.Workflow,
		Jobs:     req.Jobs,
	})
	if err != nil {
		return err
	}
	return meta.Write(metaPath, meta.File{
		CreatedAt: time.Now(),
		Kind:      "tcga",
		Params:    params,
		Source: map[string]any{
			"provider": "gdc",
		},
		Files: map[string]any{
			"expression_tsv": res.ExpressionTSV,
			"clinical_tsv":   res.ClinicalTSV,
		},
	})
}

func downloadXena(ctx context.Context, req Request, metaPath string, params []byte) error {
	rawDir := ""
	if req.KeepRaw {
		rawDir = filepath.Join(req.OutDir, "raw", "xena")
	}
	res, err := xena.DownloadTCGA(ctx, xena.TCGARequest{
		Project: req.Project,
		OutDir:  req.OutDir,
		RawDir:  rawDir,
		Mode:    req.XenaMode,
		Jobs:    req.Jobs,
	})
	if err != nil {
		return err
	}
	files := map[string]any{
		"expression_tsv": res.ExpressionTSV,
		"clinical_tsv":   res.PhenotypeTSV,
	}
	if len(res.DatasetFiles) > 0 {
		files["xena_datasets"] = res.DatasetFiles
	}
	return meta.Write(metaPath, meta.File{
		CreatedAt: time.Now(),
		Kind:      "tcga",
		Params:    params,
		Source: map[string]any{
			"provider":       "xena",
			"expression_url": res.ExpressionSourceURL,
			"phenotype_url":  res.PhenotypeSourceURL,
		},
		Files: files,
	})
}

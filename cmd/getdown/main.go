package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"getdown/internal/geo"
	"getdown/internal/tcga"
)

func main() {
	os.Exit(run())
}

func run() int {
	if len(os.Args) < 2 {
		usage()
		return 2
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch os.Args[1] {
	case "tcga":
		return runTCGA(ctx, os.Args[2:])
	case "geo":
		return runGEO(ctx, os.Args[2:])
	case "search":
		return runSearch(ctx, os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %q\n\n", os.Args[1])
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `getdown: download TCGA (GDC/Xena) and GEO datasets

Usage:
  getdown tcga --project TCGA-LAML --out ./out [--provider xena|auto|gdc]
  getdown geo  --gse GSE13535     --out ./out [--sup]
  getdown search [--source all|geo|tcga] [--limit 20] <query...>

`)
}

func runTCGA(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("tcga", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var project string
	var outDir string
	var provider string
	var keepRaw bool
	var timeout time.Duration
	var workflow string
	var xenaMode string

	fs.StringVar(&project, "project", "", "TCGA project id, e.g. TCGA-LAML")
	fs.StringVar(&outDir, "out", "", "output directory")
	fs.StringVar(&provider, "provider", "xena", "xena|auto|gdc")
	fs.BoolVar(&keepRaw, "keep-raw", false, "keep raw downloads under out/raw")
	fs.DurationVar(&timeout, "timeout", 45*time.Minute, "overall timeout")
	fs.StringVar(&workflow, "workflow", "STAR - Counts", "GDC workflow.type for gene expression")
	fs.StringVar(&xenaMode, "xena-mode", "all", "xena download mode: all|core (core=expression+clinical only)")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if project == "" || outDir == "" {
		fs.Usage()
		return 2
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := tcga.Download(ctx, tcga.Request{
		Project:  project,
		OutDir:   outDir,
		Provider: provider,
		Workflow: workflow,
		KeepRaw:  keepRaw,
		XenaMode: xenaMode,
	}); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Fprintf(os.Stderr, "tcga: timed out: %v\n", err)
			return 1
		}
		fmt.Fprintf(os.Stderr, "tcga: %v\n", err)
		return 1
	}
	return 0
}

func runGEO(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("geo", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var gse string
	var outDir string
	var sup bool
	var keepRaw bool
	var timeout time.Duration

	fs.StringVar(&gse, "gse", "", "GEO series accession, e.g. GSE13535")
	fs.StringVar(&outDir, "out", "", "output directory")
	fs.BoolVar(&sup, "sup", false, "download supplementary files when available")
	fs.BoolVar(&keepRaw, "keep-raw", false, "keep raw downloads under out/raw")
	fs.DurationVar(&timeout, "timeout", 30*time.Minute, "overall timeout")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if gse == "" || outDir == "" {
		fs.Usage()
		return 2
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := geo.Download(ctx, geo.Request{
		GSE:     gse,
		OutDir:  outDir,
		Sup:     sup,
		KeepRaw: keepRaw,
	}); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Fprintf(os.Stderr, "geo: timed out: %v\n", err)
			return 1
		}
		fmt.Fprintf(os.Stderr, "geo: %v\n", err)
		return 1
	}
	return 0
}

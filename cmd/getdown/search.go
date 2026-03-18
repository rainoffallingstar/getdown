package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"getdown/internal/geo"
	"getdown/internal/search"
	"getdown/internal/sra"
	"getdown/internal/tcga"
)

func runSearch(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var source string
	var limit int
	var timeout time.Duration
	var jsonOut bool
	var noHeader bool
	var interactive bool
	var downloadOut string
	var includeXena bool
	var listXenaDatasets bool
	var geoSup bool
	var jobs int
	var sraKind string
	var sraDecode string
	var tcgaProvider string
	var tcgaWorkflow string
	var xenaMode string
	var q string

	fs.StringVar(&source, "source", "all", "all|geo|sra|tcga|xena")
	fs.IntVar(&limit, "limit", 20, "max results per source for keyword search")
	fs.DurationVar(&timeout, "timeout", 2*time.Minute, "overall timeout")
	fs.BoolVar(&jsonOut, "json", false, "output JSON (default: TSV)")
	fs.BoolVar(&noHeader, "no-header", false, "disable TSV header row")
	fs.BoolVar(&interactive, "interactive", false, "show numbered results and choose one to download")
	fs.StringVar(&downloadOut, "download-out", "", "base output directory for interactive download")
	fs.BoolVar(&includeXena, "xena", true, "include Xena as an independent search source")
	fs.BoolVar(&listXenaDatasets, "datasets", false, "list individual Xena datasets for TCGA project lookups")
	fs.BoolVar(&geoSup, "geo-sup", false, "when downloading GEO from interactive mode, also fetch supplementary files")
	fs.IntVar(&jobs, "jobs", 0, "max concurrent download/processing jobs for interactive downloads (0=auto)")
	fs.StringVar(&sraKind, "sra-kind", "auto", "when downloading SRA from interactive mode: auto|fastq|submitted|sra|all")
	fs.StringVar(&sraDecode, "sra-decode", "none", "when downloading SRA from interactive mode: none|fastq|fastq.gz")
	fs.StringVar(&tcgaProvider, "tcga-provider", "xena", "when downloading TCGA from interactive mode: xena|auto|gdc")
	fs.StringVar(&tcgaWorkflow, "tcga-workflow", "STAR - Counts", "GDC workflow.type for interactive TCGA downloads")
	fs.StringVar(&xenaMode, "xena-mode", "all", "xena download mode for interactive TCGA downloads: all|core")
	fs.StringVar(&q, "q", "", "query string (allows spaces); if set, positional args are ignored")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if interactive && jsonOut {
		fmt.Fprintln(os.Stderr, "search: --interactive cannot be used with --json")
		return 2
	}
	if interactive && strings.TrimSpace(downloadOut) == "" {
		fmt.Fprintln(os.Stderr, "search: --interactive requires --download-out")
		return 2
	}

	var queries []string
	if strings.TrimSpace(q) != "" {
		queries = []string{q}
	} else {
		queries = fs.Args()
	}
	if len(queries) == 0 {
		fs.Usage()
		return 2
	}

	opt := search.Options{
		Geo:              true,
		SRA:              true,
		TCGA:             true,
		Xena:             includeXena,
		Limit:            limit,
		ListXenaDatasets: listXenaDatasets,
	}
	switch strings.ToLower(strings.TrimSpace(source)) {
	case "all", "":
		// keep defaults
	case "geo":
		opt.SRA = false
		opt.TCGA = false
		opt.Xena = false
	case "sra":
		opt.Geo = false
		opt.SRA = true
		opt.TCGA = false
		opt.Xena = false
	case "tcga":
		opt.Geo = false
		opt.SRA = false
		opt.Xena = false
	case "xena":
		opt.Geo = false
		opt.SRA = false
		opt.TCGA = false
		opt.Xena = true
	default:
		fmt.Fprintf(os.Stderr, "search: invalid --source: %q (want all|geo|sra|tcga|xena)\n", source)
		return 2
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var all []search.Result
	hadAny := false
	var joined error
	for _, qq := range queries {
		rs, err := search.Search(ctx, qq, opt)
		if err != nil {
			joined = errors.Join(joined, err)
		}
		if len(rs) > 0 {
			hadAny = true
		}
		all = append(all, rs...)
	}
	if joined != nil {
		fmt.Fprintf(os.Stderr, "search: warning: %v\n", joined)
		if !hadAny {
			return 1
		}
	}

	if interactive {
		cfg := interactiveDownloadConfig{
			BaseOutDir:   downloadOut,
			GeoSup:       geoSup,
			Jobs:         jobs,
			SRAKind:      sraKind,
			SRADecode:    sraDecode,
			TCGAProvider: tcgaProvider,
			TCGAWorkflow: tcgaWorkflow,
			XenaMode:     xenaMode,
		}
		if err := runInteractiveSearchDownload(ctx, all, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "search: %v\n", err)
			return 1
		}
		return 0
	}

	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(all)
	} else {
		if !noHeader {
			fmt.Fprintf(os.Stdout, "source\tid\ttitle\turl\textra\n")
		}
		for _, r := range all {
			fmt.Fprintf(os.Stdout, "%s\t%s\t%s\t%s\t%s\n",
				cleanTSV(r.Source),
				cleanTSV(r.ID),
				cleanTSV(r.Title),
				cleanTSV(r.URL),
				cleanTSV(r.Extra),
			)
		}
	}
	return 0
}

func cleanTSV(s string) string {
	s = strings.ReplaceAll(s, "\t", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return strings.TrimSpace(s)
}

type interactiveDownloadConfig struct {
	BaseOutDir   string
	GeoSup       bool
	Jobs         int
	SRAKind      string
	SRADecode    string
	TCGAProvider string
	TCGAWorkflow string
	XenaMode     string
}

func runInteractiveSearchDownload(ctx context.Context, results []search.Result, cfg interactiveDownloadConfig) error {
	if len(results) == 0 {
		fmt.Fprintln(os.Stdout, "No results.")
		return nil
	}
	for i, r := range results {
		label := "downloadable"
		if !isDownloadableSearchResult(r) {
			label = "view-only"
		}
		fmt.Fprintf(os.Stdout, "[%d] %s\t%s\t%s\t%s\n", i+1, cleanTSV(r.Source), cleanTSV(r.ID), cleanTSV(r.Title), label)
	}
	fmt.Fprint(os.Stdout, "Select result number to download (0 to cancel): ")

	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, os.ErrClosed) && !errors.Is(err, context.Canceled) && len(strings.TrimSpace(line)) == 0 {
			return err
		}
		line = strings.TrimSpace(line)
		if line == "" || line == "0" {
			fmt.Fprintln(os.Stdout, "Canceled.")
			return nil
		}
		n, convErr := strconv.Atoi(line)
		if convErr != nil || n < 1 || n > len(results) {
			fmt.Fprint(os.Stdout, "Invalid selection, enter a number from the list (or 0 to cancel): ")
			continue
		}
		res := results[n-1]
		outDir, dlErr := downloadSearchResult(ctx, res, cfg)
		if dlErr != nil {
			if !isDownloadableSearchResult(res) {
				fmt.Fprint(os.Stdout, "That result cannot be downloaded directly here; choose another number (or 0 to cancel): ")
				continue
			}
			return dlErr
		}
		fmt.Fprintf(os.Stdout, "Downloaded to %s\n", outDir)
		return nil
	}
}

func isDownloadableSearchResult(r search.Result) bool {
	switch strings.ToLower(strings.TrimSpace(r.Source)) {
	case "geo", "sra", "tcga":
		return true
	case "xena":
		_, ok := tcgaProjectFromXenaResult(r.ID)
		return ok
	default:
		return false
	}
}

func downloadSearchResult(ctx context.Context, r search.Result, cfg interactiveDownloadConfig) (string, error) {
	switch strings.ToLower(strings.TrimSpace(r.Source)) {
	case "geo":
		outDir := filepath.Join(cfg.BaseOutDir, "geo_"+safeSearchPath(r.ID))
		if err := geo.Download(ctx, geo.Request{
			GSE:    r.ID,
			OutDir: outDir,
			Sup:    cfg.GeoSup,
			Jobs:   cfg.Jobs,
		}); err != nil {
			return "", err
		}
		return outDir, nil
	case "sra":
		outDir := filepath.Join(cfg.BaseOutDir, "sra_"+safeSearchPath(r.ID))
		if _, err := sra.Download(ctx, sra.Request{
			Accession: r.ID,
			OutDir:    outDir,
			Kind:      cfg.SRAKind,
			Decode:    cfg.SRADecode,
			Jobs:      cfg.Jobs,
		}); err != nil {
			return "", err
		}
		return outDir, nil
	case "tcga":
		outDir := filepath.Join(cfg.BaseOutDir, "tcga_"+safeSearchPath(r.ID))
		if err := tcga.Download(ctx, tcga.Request{
			Project:  r.ID,
			OutDir:   outDir,
			Provider: cfg.TCGAProvider,
			Workflow: cfg.TCGAWorkflow,
			XenaMode: cfg.XenaMode,
			Jobs:     cfg.Jobs,
		}); err != nil {
			return "", err
		}
		return outDir, nil
	case "xena":
		project, ok := tcgaProjectFromXenaResult(r.ID)
		if !ok {
			return "", fmt.Errorf("selected Xena result is not directly downloadable: %s", r.ID)
		}
		outDir := filepath.Join(cfg.BaseOutDir, "tcga_"+safeSearchPath(project)+"_xena")
		if err := tcga.Download(ctx, tcga.Request{
			Project:  project,
			OutDir:   outDir,
			Provider: "xena",
			Workflow: cfg.TCGAWorkflow,
			XenaMode: cfg.XenaMode,
			Jobs:     cfg.Jobs,
		}); err != nil {
			return "", err
		}
		return outDir, nil
	default:
		return "", fmt.Errorf("unsupported result source: %s", r.Source)
	}
}

var reSearchSafe = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func safeSearchPath(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "result"
	}
	s = reSearchSafe.ReplaceAllString(s, "_")
	s = strings.Trim(s, "._-")
	if s == "" {
		return "result"
	}
	return s
}

func tcgaProjectFromXenaResult(id string) (string, bool) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", false
	}
	if strings.HasPrefix(strings.ToUpper(id), "TCGA-") && !strings.Contains(id, ".") {
		return strings.ToUpper(id), true
	}
	project := id
	if i := strings.Index(project, "."); i >= 0 {
		project = project[:i]
	}
	project = strings.ToUpper(strings.TrimSpace(project))
	if matched, _ := regexp.MatchString(`^TCGA-[A-Z0-9]+$`, project); matched {
		return project, true
	}
	return "", false
}

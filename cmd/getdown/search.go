package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"getdown/internal/search"
)

func runSearch(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var source string
	var limit int
	var timeout time.Duration
	var jsonOut bool
	var noHeader bool
	var includeXena bool
	var listXenaDatasets bool
	var q string

	fs.StringVar(&source, "source", "all", "all|geo|sra|tcga|xena")
	fs.IntVar(&limit, "limit", 20, "max results per source for keyword search")
	fs.DurationVar(&timeout, "timeout", 2*time.Minute, "overall timeout")
	fs.BoolVar(&jsonOut, "json", false, "output JSON (default: TSV)")
	fs.BoolVar(&noHeader, "no-header", false, "disable TSV header row")
	fs.BoolVar(&includeXena, "xena", true, "include Xena as an independent search source")
	fs.BoolVar(&listXenaDatasets, "datasets", false, "list individual Xena datasets for TCGA project lookups")
	fs.StringVar(&q, "q", "", "query string (allows spaces); if set, positional args are ignored")

	if err := fs.Parse(args); err != nil {
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

	if joined != nil {
		fmt.Fprintf(os.Stderr, "search: warning: %v\n", joined)
		if !hadAny {
			return 1
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

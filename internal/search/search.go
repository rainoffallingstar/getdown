package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"getdown/internal/httpx"
	"getdown/internal/xena"
)

type Result struct {
	Source string `json:"source"`
	ID     string `json:"id"`
	Title  string `json:"title,omitempty"`
	URL    string `json:"url,omitempty"`
	Extra  string `json:"extra,omitempty"`
}

type Options struct {
	Geo              bool
	TCGA             bool
	Limit            int
	Xena             bool
	ListXenaDatasets bool
}

var (
	reGSE  = regexp.MustCompile(`(?i)^gse\d+$`)
	reTCGA = regexp.MustCompile(`(?i)^tcga-[a-z0-9]+$`)
)

func Search(ctx context.Context, query string, opt Options) ([]Result, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, errors.New("search: empty query")
	}

	if opt.Limit <= 0 {
		opt.Limit = 20
	}
	if !opt.Geo && !opt.TCGA {
		return nil, errors.New("search: no sources enabled (set Geo and/or TCGA)")
	}

	// Accession lookup mode.
	if reGSE.MatchString(query) {
		if !opt.Geo {
			return nil, fmt.Errorf("search: GEO disabled for query: %s", query)
		}
		s, err := geoLookupAccession(ctx, strings.ToUpper(query))
		if err != nil {
			return nil, err
		}
		return []Result{{
			Source: "geo",
			ID:     s.Accession,
			Title:  s.Title,
			URL:    "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=" + s.Accession,
		}}, nil
	}
	if reTCGA.MatchString(query) {
		if !opt.TCGA {
			return nil, fmt.Errorf("search: TCGA disabled for query: %s", query)
		}
		return tcgaLookupProject(ctx, strings.ToUpper(query), opt)
	}

	// Keyword search mode.
	var out []Result
	var errs []error
	if opt.Geo {
		rs, err := geoSearchKeyword(ctx, query, opt.Limit)
		if err != nil {
			errs = append(errs, err)
		} else {
			out = append(out, rs...)
		}
	}
	if opt.TCGA {
		rs, err := tcgaSearchKeyword(ctx, query, opt.Limit)
		if err != nil {
			errs = append(errs, err)
		} else {
			out = append(out, rs...)
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Source != out[j].Source {
			return out[i].Source < out[j].Source
		}
		return out[i].ID < out[j].ID
	})

	if len(errs) > 0 {
		return out, errors.Join(errs...)
	}
	return out, nil
}

type geoSummary struct {
	Accession string
	Title     string
	Summary   string
}

type geoESearchResp struct {
	ESearchResult struct {
		IDList []string `json:"idlist"`
	} `json:"esearchresult"`
}

func geoESearch(ctx context.Context, term string, retmax int) ([]string, error) {
	v := url.Values{}
	v.Set("db", "gds")
	v.Set("term", term)
	v.Set("retmode", "json")
	v.Set("retmax", strconv.Itoa(retmax))
	u := "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?" + v.Encode()

	c := httpx.New()
	resp, err := c.Get(ctx, u)
	if err != nil {
		return nil, fmt.Errorf("geo esearch: %w", err)
	}
	defer resp.Body.Close()

	var out geoESearchResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("geo esearch: decode: %w", err)
	}
	return out.ESearchResult.IDList, nil
}

type geoESummaryRoot struct {
	Result map[string]json.RawMessage `json:"result"`
}

type geoESummaryItem struct {
	Accession string `json:"accession"`
	Title     string `json:"title"`
	Summary   string `json:"summary"`
}

func geoESummary(ctx context.Context, uids []string) ([]geoSummary, error) {
	const chunk = 200
	var out []geoSummary
	for i := 0; i < len(uids); i += chunk {
		j := i + chunk
		if j > len(uids) {
			j = len(uids)
		}
		ids := strings.Join(uids[i:j], ",")

		v := url.Values{}
		v.Set("db", "gds")
		v.Set("id", ids)
		v.Set("retmode", "json")
		u := "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?" + v.Encode()

		c := httpx.New()
		resp, err := c.Get(ctx, u)
		if err != nil {
			return nil, fmt.Errorf("geo esummary: %w", err)
		}
		var root geoESummaryRoot
		decodeErr := json.NewDecoder(resp.Body).Decode(&root)
		resp.Body.Close()
		if decodeErr != nil {
			return nil, fmt.Errorf("geo esummary: decode: %w", decodeErr)
		}

		var gotUIDs []string
		if b, ok := root.Result["uids"]; ok {
			_ = json.Unmarshal(b, &gotUIDs)
		}
		for _, uid := range gotUIDs {
			b, ok := root.Result[uid]
			if !ok {
				continue
			}
			var it geoESummaryItem
			if err := json.Unmarshal(b, &it); err != nil {
				continue
			}
			out = append(out, geoSummary{Accession: it.Accession, Title: it.Title, Summary: it.Summary})
		}
	}
	return out, nil
}

func geoLookupAccession(ctx context.Context, acc string) (geoSummary, error) {
	uids, err := geoESearch(ctx, acc+"[ACCN]", 50)
	if err != nil {
		return geoSummary{}, err
	}
	if len(uids) == 0 {
		return geoSummary{}, fmt.Errorf("geo: not found: %s", acc)
	}
	sums, err := geoESummary(ctx, uids)
	if err != nil {
		return geoSummary{}, err
	}
	for _, s := range sums {
		if strings.EqualFold(s.Accession, acc) {
			return s, nil
		}
	}
	// Sometimes the first summary is the best/only match.
	if len(sums) > 0 {
		return sums[0], nil
	}
	return geoSummary{}, fmt.Errorf("geo: not found: %s", acc)
}

func geoSearchKeyword(ctx context.Context, term string, limit int) ([]Result, error) {
	// Fetch more than needed to allow filtering to just GSE accessions.
	retmax := limit * 8
	if retmax < 50 {
		retmax = 50
	}
	if retmax > 500 {
		retmax = 500
	}
	uids, err := geoESearch(ctx, term, retmax)
	if err != nil {
		return nil, err
	}
	if len(uids) == 0 {
		return nil, nil
	}
	sums, err := geoESummary(ctx, uids)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	var out []Result
	for _, s := range sums {
		acc := strings.TrimSpace(s.Accession)
		if !reGSE.MatchString(acc) {
			continue
		}
		acc = strings.ToUpper(acc)
		if seen[acc] {
			continue
		}
		seen[acc] = true
		out = append(out, Result{
			Source: "geo",
			ID:     acc,
			Title:  s.Title,
			URL:    "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=" + acc,
		})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

type gdcProject struct {
	ProjectID   string   `json:"project_id"`
	Name        string   `json:"name"`
	PrimarySite []string `json:"primary_site"`
	DiseaseType []string `json:"disease_type"`
}

type gdcProjectGetResp struct {
	Data gdcProject `json:"data"`
}

type gdcProjectListResp struct {
	Data struct {
		Hits []gdcProject `json:"hits"`
	} `json:"data"`
}

func tcgaLookupProject(ctx context.Context, projectID string, opt Options) ([]Result, error) {
	v := url.Values{}
	v.Set("fields", "project_id,name,primary_site,disease_type")
	v.Set("format", "JSON")
	u := "https://api.gdc.cancer.gov/projects/" + url.PathEscape(projectID) + "?" + v.Encode()

	c := httpx.New()
	resp, err := c.Get(ctx, u)
	if err != nil {
		return nil, fmt.Errorf("tcga gdc project: %w", err)
	}
	defer resp.Body.Close()

	var got gdcProjectGetResp
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		return nil, fmt.Errorf("tcga gdc project: decode: %w", err)
	}

	var extraParts []string
	if len(got.Data.DiseaseType) > 0 {
		extraParts = append(extraParts, "disease_type="+strings.Join(got.Data.DiseaseType, "|"))
	}
	if len(got.Data.PrimarySite) > 0 {
		extraParts = append(extraParts, "primary_site="+strings.Join(got.Data.PrimarySite, "|"))
	}

	var datasets []string
	var xenaErr error
	if opt.Xena {
		datasets, xenaErr = xena.ListDatasetNamesByPrefix(ctx, projectID+".")
		if xenaErr == nil {
			extraParts = append(extraParts, fmt.Sprintf("xena_datasets=%d", len(datasets)))
		} else {
			extraParts = append(extraParts, "xena_error="+strings.ReplaceAll(xenaErr.Error(), "\t", " "))
		}
	}

	out := []Result{{
		Source: "tcga",
		ID:     got.Data.ProjectID,
		Title:  got.Data.Name,
		URL:    "https://portal.gdc.cancer.gov/projects/" + got.Data.ProjectID,
		Extra:  strings.Join(extraParts, "\t"),
	}}

	if opt.ListXenaDatasets && len(datasets) > 0 {
		sort.Strings(datasets)
		for _, ds := range datasets {
			out = append(out, Result{
				Source: "xena",
				ID:     ds,
				Title:  "dataset",
				URL:    "",
			})
		}
	}

	if xenaErr != nil {
		return out, xenaErr
	}
	return out, nil
}

func tcgaSearchKeyword(ctx context.Context, term string, limit int) ([]Result, error) {
	v := url.Values{}
	v.Set("size", "200")
	v.Set("fields", "project_id,name,primary_site,disease_type")
	v.Set("format", "JSON")
	u := "https://api.gdc.cancer.gov/projects?" + v.Encode()

	c := httpx.New()
	resp, err := c.Get(ctx, u)
	if err != nil {
		return nil, fmt.Errorf("tcga gdc projects: %w", err)
	}
	defer resp.Body.Close()

	var got gdcProjectListResp
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		return nil, fmt.Errorf("tcga gdc projects: decode: %w", err)
	}

	q := strings.ToLower(strings.TrimSpace(term))
	var out []Result
	for _, p := range got.Data.Hits {
		if !strings.HasPrefix(strings.ToUpper(p.ProjectID), "TCGA-") {
			continue
		}
		if q == "" {
			continue
		}
		hay := strings.ToLower(p.ProjectID + "\n" + p.Name + "\n" + strings.Join(p.PrimarySite, "|") + "\n" + strings.Join(p.DiseaseType, "|"))
		if !strings.Contains(hay, q) {
			continue
		}
		extraParts := make([]string, 0, 2)
		if len(p.DiseaseType) > 0 {
			extraParts = append(extraParts, "disease_type="+strings.Join(p.DiseaseType, "|"))
		}
		if len(p.PrimarySite) > 0 {
			extraParts = append(extraParts, "primary_site="+strings.Join(p.PrimarySite, "|"))
		}
		out = append(out, Result{
			Source: "tcga",
			ID:     p.ProjectID,
			Title:  p.Name,
			URL:    "https://portal.gdc.cancer.gov/projects/" + p.ProjectID,
			Extra:  strings.Join(extraParts, "\t"),
		})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

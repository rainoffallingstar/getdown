package sra

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"getdown/internal/httpx"
)

type SearchRecord struct {
	Accession string
	Title     string
	Extra     string
}

type eSearchResp struct {
	ESearchResult struct {
		IDList []string `json:"idlist"`
	} `json:"esearchresult"`
}

type eSummaryRoot struct {
	Result map[string]json.RawMessage `json:"result"`
}

type eSummaryItem struct {
	UID     string `json:"uid"`
	Caption string `json:"caption"`
	Title   string `json:"title"`
	Extra   string `json:"extra"`
	ExpXML  string `json:"expxml"`
	Runs    string `json:"runs"`
}

var reEmbeddedAccession = regexp.MustCompile(`(?i)\b(SRP|SRX|SRS|SRR|ERP|ERX|ERS|ERR|DRP|DRX|DRS|DRR)\d+\b`)

func Search(ctx context.Context, term string, limit int) ([]SearchRecord, error) {
	term = strings.TrimSpace(term)
	if term == "" {
		return nil, fmt.Errorf("sra search: empty term")
	}
	if limit <= 0 {
		limit = 20
	}
	uids, err := eSearch(ctx, term, limit)
	if err != nil {
		return nil, err
	}
	return eSummary(ctx, uids)
}

func LookupAccession(ctx context.Context, acc string) ([]SearchRecord, error) {
	acc = NormalizeAccession(acc)
	if !IsAccession(acc) {
		return nil, fmt.Errorf("sra search: invalid accession: %q", acc)
	}

	runs, err := FetchRunInfo(ctx, acc)
	if err == nil && len(runs) > 0 {
		title := strings.TrimSpace(runs[0].ScientificName)
		if title == "" {
			title = "SRA runs for " + acc
		}
		return []SearchRecord{{
			Accession: acc,
			Title:     title,
			Extra:     fmt.Sprintf("runs=%d", len(runs)),
		}}, nil
	}

	uids, esErr := eSearch(ctx, acc+"[ACCN]", 20)
	if esErr != nil {
		if err != nil {
			return nil, err
		}
		return nil, esErr
	}
	records, sumErr := eSummary(ctx, uids)
	if sumErr != nil {
		if err != nil {
			return nil, err
		}
		return nil, sumErr
	}
	out := make([]SearchRecord, 0, len(records))
	for _, rec := range records {
		if strings.EqualFold(rec.Accession, acc) {
			out = append(out, rec)
		}
	}
	if len(out) > 0 {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	return records, nil
}

func eSearch(ctx context.Context, term string, limit int) ([]string, error) {
	v := url.Values{}
	v.Set("db", "sra")
	v.Set("term", term)
	v.Set("retmode", "json")
	v.Set("retmax", fmt.Sprintf("%d", limit))
	u := "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?" + v.Encode()

	c := httpx.New()
	resp, err := c.Get(ctx, u)
	if err != nil {
		return nil, fmt.Errorf("sra esearch: %w", err)
	}
	defer resp.Body.Close()

	var out eSearchResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("sra esearch: decode: %w", err)
	}
	return out.ESearchResult.IDList, nil
}

func eSummary(ctx context.Context, uids []string) ([]SearchRecord, error) {
	if len(uids) == 0 {
		return nil, nil
	}
	v := url.Values{}
	v.Set("db", "sra")
	v.Set("id", strings.Join(uids, ","))
	v.Set("retmode", "json")
	u := "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?" + v.Encode()

	c := httpx.New()
	resp, err := c.Get(ctx, u)
	if err != nil {
		return nil, fmt.Errorf("sra esummary: %w", err)
	}
	defer resp.Body.Close()

	var root eSummaryRoot
	if err := json.NewDecoder(resp.Body).Decode(&root); err != nil {
		return nil, fmt.Errorf("sra esummary: decode: %w", err)
	}

	var ids []string
	if raw, ok := root.Result["uids"]; ok {
		_ = json.Unmarshal(raw, &ids)
	}
	out := make([]SearchRecord, 0, len(ids))
	for _, id := range ids {
		raw, ok := root.Result[id]
		if !ok {
			continue
		}
		var item eSummaryItem
		if err := json.Unmarshal(raw, &item); err != nil {
			continue
		}
		acc := strings.TrimSpace(item.Caption)
		if acc == "" {
			acc = firstAccession(item.Runs)
		}
		if acc == "" {
			acc = firstAccession(item.ExpXML)
		}
		if acc == "" {
			acc = id
		}
		title := strings.TrimSpace(item.Title)
		if title == "" {
			title = "SRA record " + acc
		}
		out = append(out, SearchRecord{
			Accession: strings.ToUpper(acc),
			Title:     title,
			Extra:     strings.TrimSpace(item.Extra),
		})
	}
	return out, nil
}

func firstAccession(s string) string {
	if m := reEmbeddedAccession.FindString(strings.ToUpper(s)); m != "" {
		return m
	}
	return ""
}

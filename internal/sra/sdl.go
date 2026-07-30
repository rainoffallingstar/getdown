package sra

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"

	"getdown/internal/httpx"
)

type sdlResponse struct {
	Result []sdlBundle `json:"result"`
}

type sdlBundle struct {
	Files []sdlFile `json:"files"`
}

type sdlFile struct {
	Accession string        `json:"accession"`
	MD5       string        `json:"md5"`
	Name      string        `json:"name"`
	Size      int64         `json:"size"`
	Type      string        `json:"type"`
	Locations []sdlLocation `json:"locations"`
}

type sdlLocation struct {
	Link string `json:"link"`
}

func ResolveNativeSRAArchiveFiles(ctx context.Context, runs []RunInfo) ([]RemoteFile, error) {
	resolvedFiles := make([]RemoteFile, 0, len(runs))
	for _, run := range runs {
		archiveFile, err := resolveNativeSRAArchiveFile(ctx, run.RunAccession)
		if err != nil {
			return nil, err
		}
		resolvedFiles = append(resolvedFiles, archiveFile)
	}
	return dedupFiles(resolvedFiles), nil
}

func resolveNativeSRAArchiveFile(ctx context.Context, runAccession string) (RemoteFile, error) {
	response, err := httpx.New().Get(ctx, sdlURL(runAccession))
	if err != nil {
		return RemoteFile{}, fmt.Errorf("sra SDL resolve %s: %w", runAccession, err)
	}
	defer response.Body.Close()

	var payload sdlResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return RemoteFile{}, fmt.Errorf("sra SDL decode %s: %w", runAccession, err)
	}

	for _, bundle := range payload.Result {
		for _, file := range bundle.Files {
			if !strings.EqualFold(file.Type, "sra") || !strings.EqualFold(file.Accession, runAccession) {
				continue
			}
			if len(file.Locations) == 0 || strings.TrimSpace(file.Locations[0].Link) == "" {
				return RemoteFile{}, fmt.Errorf("sra SDL resolve %s: archive has no downloadable location", runAccession)
			}
			if !isMD5Hex(file.MD5) {
				return RemoteFile{}, fmt.Errorf("sra SDL resolve %s: archive has invalid MD5 %q", runAccession, file.MD5)
			}
			return RemoteFile{
				RunAccession: runAccession,
				Kind:         "sra",
				URL:          file.Locations[0].Link,
				SizeBytes:    fmt.Sprintf("%d", file.Size),
				MD5Hex:       strings.ToLower(file.MD5),
			}, nil
		}
	}
	return RemoteFile{}, fmt.Errorf("sra SDL resolve %s: no archive file found", runAccession)
}

func sdlURL(runAccession string) string {
	baseURL := strings.TrimSpace(os.Getenv("GETDOWN_NCBI_SDL_API_BASE"))
	if baseURL == "" {
		baseURL = "https://locate.ncbi.nlm.nih.gov/sdl/2/retrieve"
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return baseURL
	}
	query := parsedURL.Query()
	query.Set("acc", runAccession)
	parsedURL.RawQuery = query.Encode()
	return parsedURL.String()
}

func providerForEffectiveKind(effectiveKind string) string {
	if effectiveKind == "sra-ncbi-sdl" {
		return "ena_filereport+ncbi_sdl"
	}
	return "ena_filereport"
}

func isMD5Hex(value string) bool {
	if len(value) != 32 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') && !(character >= 'A' && character <= 'F') {
			return false
		}
	}
	return true
}

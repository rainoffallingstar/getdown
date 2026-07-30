package sra

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveNativeSRAArchiveFilesUsesNCBISDL(t *testing.T) {
	withEnv(t, "GETDOWN_NCBI_SDL_API_BASE", "https://sdl.test/retrieve", func() {
		withStubTransport(t, stubTransport{roundTrip: func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodGet || request.URL.Host != "sdl.test" || request.URL.Query().Get("acc") != "SRR123456" {
				return resp(http.StatusNotFound, nil, []byte("not found"), request), nil
			}
			body := `{"result":[{"files":[{"accession":"SRR123456","type":"sra","size":3,"md5":"d2e3f4a5b6c7d8e9f0a1b2c3d4e5f607","locations":[{"link":"https://archive.test/sra/SRR123456/SRR123456"}]}]}]}`
			return resp(http.StatusOK, map[string]string{"Content-Type": "application/json"}, []byte(body), request), nil
		}}, func() {
			files, err := ResolveNativeSRAArchiveFiles(context.Background(), []RunInfo{{RunAccession: "SRR123456"}})
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 1 {
				t.Fatalf("resolved file count: got %d want 1", len(files))
			}
			if files[0].URL != "https://archive.test/sra/SRR123456/SRR123456" || files[0].SizeBytes != "3" || files[0].MD5Hex != "d2e3f4a5b6c7d8e9f0a1b2c3d4e5f607" {
				t.Fatalf("unexpected resolved SRA archive: %#v", files[0])
			}
			if fileName := downloadFileName(files[0]); fileName != "SRR123456.sra" {
				t.Fatalf("expected normalized SRA file name, got %q", fileName)
			}
		})
	})
}

func TestResolveNativeSRAArchiveFilesRejectsMissingMD5(t *testing.T) {
	withEnv(t, "GETDOWN_NCBI_SDL_API_BASE", "https://sdl.test/retrieve", func() {
		withStubTransport(t, stubTransport{roundTrip: func(request *http.Request) (*http.Response, error) {
			body := `{"result":[{"files":[{"accession":"SRR123456","type":"sra","size":3,"locations":[{"link":"https://archive.test/sra/SRR123456/SRR123456"}]}]}]}`
			return resp(http.StatusOK, map[string]string{"Content-Type": "application/json"}, []byte(body), request), nil
		}}, func() {
			_, err := ResolveNativeSRAArchiveFiles(context.Background(), []RunInfo{{RunAccession: "SRR123456"}})
			if err == nil || !strings.Contains(err.Error(), "invalid MD5") {
				t.Fatalf("expected invalid SDL MD5 error, got %v", err)
			}
		})
	})
}

func TestDownloadUsesNativeSDLArchiveWhenENAOmmitsSRAURL(t *testing.T) {
	archiveBytes := []byte("SRA")
	archiveMD5 := md5.Sum(archiveBytes)
	withEnv(t, "GETDOWN_ENA_API_BASE", "https://ena.test/filereport", func() {
		withEnv(t, "GETDOWN_NCBI_SDL_API_BASE", "https://sdl.test/retrieve", func() {
			withStubTransport(t, stubTransport{roundTrip: func(request *http.Request) (*http.Response, error) {
				switch request.URL.Host {
				case "ena.test":
					body := strings.Join([]string{
						"study_accession\texperiment_accession\tsample_accession\trun_accession\tscientific_name\tinstrument_platform\tinstrument_model\tlibrary_layout\tlibrary_strategy\tlibrary_source\tlibrary_selection\tfastq_ftp\tsubmitted_ftp\tsra_ftp\tfastq_bytes\tsubmitted_bytes\tsra_bytes",
						"SRP1\tSRX1\tSRS1\tSRR1\tHomo sapiens\tILLUMINA\tNovaSeq\tPAIRED\tRNA-Seq\tTRANSCRIPTOMIC\tcDNA\t\t\t\t\t\t",
						"",
					}, "\n")
					return resp(http.StatusOK, map[string]string{"Content-Type": "text/tab-separated-values"}, []byte(body), request), nil
				case "sdl.test":
					body := fmt.Sprintf(`{"result":[{"files":[{"accession":"SRR1","type":"sra","size":%d,"md5":"%s","locations":[{"link":"https://archive.test/sra/SRR1/SRR1"}]}]}]}`, len(archiveBytes), hex.EncodeToString(archiveMD5[:]))
					return resp(http.StatusOK, map[string]string{"Content-Type": "application/json"}, []byte(body), request), nil
				case "archive.test":
					return resp(http.StatusOK, map[string]string{"Content-Type": "application/octet-stream"}, archiveBytes, request), nil
				default:
					return resp(http.StatusNotFound, nil, []byte("not found"), request), nil
				}
			}}, func() {
				outDirectory := t.TempDir()
				result, err := Download(context.Background(), Request{Accession: "SRR1", OutDir: outDirectory, Kind: "sra", Decode: "none"})
				if err != nil {
					t.Fatal(err)
				}
				if result.EffectiveKind != "sra-ncbi-sdl" {
					t.Fatalf("unexpected effective kind: %q", result.EffectiveKind)
				}
				archivePath := filepath.Join(outDirectory, "files", "SRR1", "SRR1.sra")
				if _, err := os.Stat(archivePath); err != nil {
					t.Fatalf("missing native SDL archive: %v", err)
				}
				linkData, err := os.ReadFile(filepath.Join(outDirectory, "links.tsv"))
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(linkData), hex.EncodeToString(archiveMD5[:])) {
					t.Fatalf("missing SDL MD5 in link evidence: %s", linkData)
				}
			})
		})
	})
}

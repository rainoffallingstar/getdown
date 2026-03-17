package httpx

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	HTTP *http.Client
}

// DefaultRoundTripper is used by New(). Tests can override it to stub network calls.
var DefaultRoundTripper http.RoundTripper = defaultRoundTripper()

func defaultRoundTripper() http.RoundTripper {
	dialer := &net.Dialer{
		Timeout:       30 * time.Second,
		KeepAlive:     30 * time.Second,
		FallbackDelay: 300 * time.Millisecond, // faster IPv6->IPv4 fallback in broken IPv6 envs
	}
	return &http.Transport{
		Proxy:             http.ProxyFromEnvironment,
		DialContext:       dialer.DialContext,
		ForceAttemptHTTP2: false,
		// Some data hosts (e.g. file mirrors) can behave poorly with HTTP/2 in restricted envs.
		// Disabling it keeps behavior closer to curl defaults and avoids stalls.
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

func New() *Client {
	return NewWithRoundTripper(DefaultRoundTripper)
}

func NewWithRoundTripper(rt http.RoundTripper) *Client {
	if rt == nil {
		rt = defaultRoundTripper()
	}
	return &Client{
		HTTP: &http.Client{
			Transport: rt,
			Timeout:   0, // rely on request context for overall deadline
		},
	}
}

func (c *Client) Get(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "getdown/0.1 (+https://github.com)")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return nil, fmt.Errorf("GET %s: %s: %s", url, resp.Status, strings.TrimSpace(string(b)))
	}
	return resp, nil
}

func (c *Client) HeadOK(ctx context.Context, url string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("User-Agent", "getdown/0.1 (+https://github.com)")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return resp.StatusCode >= 200 && resp.StatusCode < 300, nil
}

type DownloadResult struct {
	Path       string
	SHA256Hex  string
	SizeBytes  int64
	SourceURL  string
	WasGzipped bool
}

func contentRangeTotal(cr string) int64 {
	// Format for 416: "bytes */12345"
	cr = strings.TrimSpace(cr)
	const prefix = "bytes */"
	if !strings.HasPrefix(cr, prefix) {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(cr[len(prefix):]), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func fileSize(path string) (int64, bool) {
	fi, err := os.Stat(path)
	if err != nil || !fi.Mode().IsRegular() {
		return 0, false
	}
	return fi.Size(), true
}

func openPartForWrite(path string, appendMode bool) (*os.File, error) {
	if appendMode {
		return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	}
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
}

func isGzipFile(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	var hdr [2]byte
	n, err := io.ReadFull(f, hdr[:])
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, err
	}
	return n == 2 && hdr[0] == 0x1f && hdr[1] == 0x8b, nil
}

func finalizeInto(destPath, srcPath string) error {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}
	tmpPath := destPath + ".tmp"
	_ = os.Remove(tmpPath)
	_ = os.Remove(destPath)
	if err := os.Rename(srcPath, tmpPath); err != nil {
		return err
	}
	return os.Rename(tmpPath, destPath)
}

func gunzipInto(destPath, srcPath string) (DownloadResult, error) {
	if sz, ok := fileSize(destPath); ok && sz > 0 {
		return DownloadResult{Path: destPath, SizeBytes: sz}, nil
	}
	isGz, err := isGzipFile(srcPath)
	if err != nil {
		return DownloadResult{}, err
	}
	if !isGz {
		if err := finalizeInto(destPath, srcPath); err != nil {
			return DownloadResult{}, err
		}
		sz, _ := fileSize(destPath)
		return DownloadResult{Path: destPath, SizeBytes: sz, WasGzipped: false}, nil
	}

	in, err := os.Open(srcPath)
	if err != nil {
		return DownloadResult{}, err
	}
	defer in.Close()
	gr, err := gzip.NewReader(in)
	if err != nil {
		return DownloadResult{}, err
	}
	defer gr.Close()

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return DownloadResult{}, err
	}
	tmpPath := destPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return DownloadResult{}, err
	}

	h := sha256.New()
	w := io.MultiWriter(f, h)
	n, copyErr := io.Copy(w, gr)
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return DownloadResult{}, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return DownloadResult{}, closeErr
	}
	_ = os.Remove(destPath)
	if err := os.Rename(tmpPath, destPath); err != nil {
		_ = os.Remove(tmpPath)
		return DownloadResult{}, err
	}
	return DownloadResult{
		Path:       destPath,
		SHA256Hex:  hex.EncodeToString(h.Sum(nil)),
		SizeBytes:  n,
		WasGzipped: true,
	}, nil
}

func (c *Client) downloadRawMaybe(ctx context.Context, url, destPath string) (DownloadResult, error) {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return DownloadResult{}, err
	}
	if sz, ok := fileSize(destPath); ok && sz > 0 {
		return DownloadResult{Path: destPath, SizeBytes: sz, SourceURL: url}, nil
	}

	partPath := destPath + ".part"
	start, _ := fileSize(partPath)
	for attempt := 0; attempt < 2; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return DownloadResult{}, err
		}
		req.Header.Set("User-Agent", "getdown/0.1 (+https://github.com)")
		if start > 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
		}

		resp, err := c.HTTP.Do(req)
		if err != nil {
			return DownloadResult{}, err
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return DownloadResult{}, ErrNotFound
		}

		// Decide whether we can safely use this response, or need to restart.
		appendMode := false
		switch resp.StatusCode {
		case http.StatusOK:
			if start > 0 {
				// Server ignored Range; restart from scratch but we can still use this full body.
				_ = os.Remove(partPath)
				start = 0
			}
		case http.StatusPartialContent:
			if start > 0 {
				appendMode = true
			} else {
				// Treat as fresh write (range at 0).
				start = 0
			}
		case http.StatusRequestedRangeNotSatisfiable:
			total := contentRangeTotal(resp.Header.Get("Content-Range"))
			resp.Body.Close()
			if total > 0 && total == start {
				_ = os.Remove(destPath)
				if err := os.Rename(partPath, destPath); err != nil {
					return DownloadResult{}, err
				}
				sz, _ := fileSize(destPath)
				return DownloadResult{Path: destPath, SizeBytes: sz, SourceURL: url}, nil
			}
			// Restart from scratch.
			_ = os.Remove(partPath)
			start = 0
			continue
		default:
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
				resp.Body.Close()
				return DownloadResult{}, fmt.Errorf("GET %s: %s: %s", url, resp.Status, strings.TrimSpace(string(b)))
			}
			// Other 2xx: treat as fresh.
			_ = os.Remove(partPath)
			start = 0
		}

		// Hash existing bytes when resuming (reads part once).
		h := sha256.New()
		if appendMode {
			rf, err := os.Open(partPath)
			if err != nil {
				resp.Body.Close()
				// Can't resume; restart.
				_ = os.Remove(partPath)
				start = 0
				continue
			}
			_, _ = io.Copy(h, rf)
			_ = rf.Close()
		}

		f, err := openPartForWrite(partPath, appendMode)
		if err != nil {
			resp.Body.Close()
			return DownloadResult{}, err
		}

		w := io.MultiWriter(f, h)
		n, copyErr := io.Copy(w, resp.Body)
		closeErr := f.Close()
		resp.Body.Close()
		if copyErr != nil {
			return DownloadResult{}, copyErr
		}
		if closeErr != nil {
			return DownloadResult{}, closeErr
		}

		_ = os.Remove(destPath)
		if err := os.Rename(partPath, destPath); err != nil {
			return DownloadResult{}, err
		}

		return DownloadResult{
			Path:       destPath,
			SHA256Hex:  hex.EncodeToString(h.Sum(nil)),
			SizeBytes:  start + n,
			SourceURL:  url,
			WasGzipped: false,
		}, nil
	}
	return DownloadResult{}, fmt.Errorf("GET %s: resume failed", url)
}

// DownloadToFileMaybe is like DownloadToFile, but returns ErrNotFound when the server responds 404.
func (c *Client) DownloadToFileMaybe(ctx context.Context, url, destPath string, gunzip bool) (DownloadResult, error) {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return DownloadResult{}, err
	}

	// Fast path: file already exists (we only ever place the final file after complete download).
	if sz, ok := fileSize(destPath); ok && sz > 0 {
		return DownloadResult{Path: destPath, SizeBytes: sz, SourceURL: url}, nil
	}

	if gunzip {
		// Resume the compressed payload into a sidecar, then decompress to destPath.
		// If decompression fails, the sidecar is kept for a later retry without re-downloading.
		srcPath := destPath + ".getdown.src"
		if _, ok := fileSize(srcPath); !ok {
			if _, err := c.downloadRawMaybe(ctx, url, srcPath); err != nil {
				return DownloadResult{}, err
			}
		}
		res, err := gunzipInto(destPath, srcPath)
		if err != nil {
			return DownloadResult{}, err
		}
		// Cleanup sidecar after successful finalize.
		_ = os.Remove(srcPath)
		_ = os.Remove(srcPath + ".part")
		res.SourceURL = url
		return res, nil
	}

	return c.downloadRawMaybe(ctx, url, destPath)
}

// DownloadToFile downloads url to destPath, creating parent dirs as needed.
// If gunzip is true, it transparently decompresses gzip content (either via
// Content-Encoding: gzip or by .gz suffix).
func (c *Client) DownloadToFile(ctx context.Context, url, destPath string, gunzip bool) (DownloadResult, error) {
	res, err := c.DownloadToFileMaybe(ctx, url, destPath, gunzip)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return DownloadResult{}, fmt.Errorf("GET %s: 404 not found", url)
		}
		return DownloadResult{}, err
	}
	return res, nil
}

// ReadAll downloads and returns body, optionally transparently gunzipping.
func (c *Client) ReadAll(ctx context.Context, url string, gunzip bool) ([]byte, error) {
	resp, err := c.Get(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var reader io.Reader = resp.Body
	if gunzip && (strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") || strings.HasSuffix(strings.ToLower(url), ".gz")) {
		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer gr.Close()
		reader = gr
	}
	return io.ReadAll(reader)
}

var ErrNotFound = errors.New("not found")

func (c *Client) GetMaybe(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "getdown/0.1 (+https://github.com)")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		return nil, fmt.Errorf("GET %s: %s: %s", url, resp.Status, strings.TrimSpace(string(b)))
	}
	return resp, nil
}

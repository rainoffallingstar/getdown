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

// DownloadToFileMaybe is like DownloadToFile, but returns ErrNotFound when the server responds 404.
func (c *Client) DownloadToFileMaybe(ctx context.Context, url, destPath string, gunzip bool) (DownloadResult, error) {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return DownloadResult{}, err
	}

	resp, err := c.GetMaybe(ctx, url)
	if err != nil {
		return DownloadResult{}, err
	}
	defer resp.Body.Close()

	var reader io.Reader = resp.Body
	wasGzipped := false
	if gunzip && (strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") || strings.HasSuffix(strings.ToLower(url), ".gz") || strings.HasSuffix(strings.ToLower(destPath), ".gz")) {
		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return DownloadResult{}, err
		}
		defer gr.Close()
		reader = gr
		wasGzipped = true
	}

	tmpPath := destPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return DownloadResult{}, err
	}

	h := sha256.New()
	w := io.MultiWriter(f, h)
	n, copyErr := io.Copy(w, reader)
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return DownloadResult{}, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return DownloadResult{}, closeErr
	}
	if err := os.Rename(tmpPath, destPath); err != nil {
		_ = os.Remove(tmpPath)
		return DownloadResult{}, err
	}

	return DownloadResult{
		Path:       destPath,
		SHA256Hex:  hex.EncodeToString(h.Sum(nil)),
		SizeBytes:  n,
		SourceURL:  url,
		WasGzipped: wasGzipped,
	}, nil
}

// DownloadToFile downloads url to destPath, creating parent dirs as needed.
// If gunzip is true, it transparently decompresses gzip content (either via
// Content-Encoding: gzip or by .gz suffix).
func (c *Client) DownloadToFile(ctx context.Context, url, destPath string, gunzip bool) (DownloadResult, error) {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return DownloadResult{}, err
	}

	resp, err := c.Get(ctx, url)
	if err != nil {
		return DownloadResult{}, err
	}
	defer resp.Body.Close()

	var reader io.Reader = resp.Body
	wasGzipped := false
	if gunzip && (strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") || strings.HasSuffix(strings.ToLower(url), ".gz") || strings.HasSuffix(strings.ToLower(destPath), ".gz")) {
		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return DownloadResult{}, err
		}
		defer gr.Close()
		reader = gr
		wasGzipped = true
	}

	tmpPath := destPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return DownloadResult{}, err
	}

	h := sha256.New()
	w := io.MultiWriter(f, h)
	n, copyErr := io.Copy(w, reader)
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return DownloadResult{}, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return DownloadResult{}, closeErr
	}
	if err := os.Rename(tmpPath, destPath); err != nil {
		_ = os.Remove(tmpPath)
		return DownloadResult{}, err
	}

	return DownloadResult{
		Path:       destPath,
		SHA256Hex:  hex.EncodeToString(h.Sum(nil)),
		SizeBytes:  n,
		SourceURL:  url,
		WasGzipped: wasGzipped,
	}, nil
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

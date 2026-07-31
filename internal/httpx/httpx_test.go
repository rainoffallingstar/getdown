package httpx

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type stubTransport struct {
	roundTrip func(*http.Request) (*http.Response, error)
}

func (s stubTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return s.roundTrip(r)
}

func resp(status int, headers map[string]string, body []byte, req *http.Request) *http.Response {
	h := make(http.Header, len(headers))
	for k, v := range headers {
		h.Set(k, v)
	}
	return &http.Response{
		StatusCode:    status,
		Status:        http.StatusText(status),
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

type interruptedReader struct {
	data    []byte
	emitted bool
}

func (reader *interruptedReader) Read(destination []byte) (int, error) {
	if reader.emitted {
		return 0, io.ErrUnexpectedEOF
	}
	reader.emitted = true
	return copy(destination, reader.data), nil
}

func TestDownloadToFileMaybe_ResumeRaw(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dest := filepath.Join(dir, "file.bin")
	part := dest + ".part"

	data := []byte("helloworld")
	if err := os.WriteFile(part, data[:5], 0o644); err != nil {
		t.Fatalf("write part: %v", err)
	}

	var sawRange string
	c := NewWithRoundTripper(stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
		sawRange = r.Header.Get("Range")
		if sawRange != "bytes=5-" {
			return resp(416, map[string]string{"Content-Range": "bytes */10"}, []byte("bad range"), r), nil
		}
		return resp(206, map[string]string{"Content-Type": "application/octet-stream"}, data[5:], r), nil
	}})

	if _, err := c.DownloadToFileMaybe(ctx, "https://example.test/file", dest, false); err != nil {
		t.Fatalf("download: %v", err)
	}
	if sawRange != "bytes=5-" {
		t.Fatalf("Range header: got %q want %q", sawRange, "bytes=5-")
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("content mismatch: got %q want %q", string(got), string(data))
	}
	if _, err := os.Stat(part); err == nil {
		t.Fatalf("expected part to be removed")
	}
}

func TestDownloadToFileMaybe_ResumesAfterInterruptedResponse(t *testing.T) {
	ctx := context.Background()
	destinationPath := filepath.Join(t.TempDir(), "file.bin")
	data := []byte("helloworld")
	requestCount := 0

	client := NewWithRoundTripper(stubTransport{roundTrip: func(request *http.Request) (*http.Response, error) {
		requestCount++
		switch requestCount {
		case 1:
			if request.Header.Get("Range") != "" {
				t.Fatalf("first request unexpectedly used Range: %q", request.Header.Get("Range"))
			}
			response := resp(http.StatusOK, map[string]string{"Content-Type": "application/octet-stream"}, nil, request)
			response.Body = io.NopCloser(&interruptedReader{data: data[:5]})
			return response, nil
		case 2:
			if request.Header.Get("Range") != "bytes=5-" {
				t.Fatalf("resume request Range: got %q want %q", request.Header.Get("Range"), "bytes=5-")
			}
			return resp(http.StatusPartialContent, map[string]string{"Content-Type": "application/octet-stream"}, data[5:], request), nil
		default:
			t.Fatalf("unexpected request count: %d", requestCount)
			return nil, nil
		}
	}})

	if _, err := client.DownloadToFileMaybe(ctx, "https://example.test/file", destinationPath, false); err != nil {
		t.Fatalf("download: %v", err)
	}
	actualData, err := os.ReadFile(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actualData, data) {
		t.Fatalf("downloaded data: got %q want %q", actualData, data)
	}
}

func TestDownloadToFileMaybe_ResumeGunzip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dest := filepath.Join(dir, "out.txt")
	src := dest + ".getdown.src"
	part := src + ".part"

	plain := []byte(strings.Repeat("hello\n", 1000))
	var gzBuf bytes.Buffer
	gzw := gzip.NewWriter(&gzBuf)
	_, _ = gzw.Write(plain)
	_ = gzw.Close()
	gzData := gzBuf.Bytes()

	// Pretend we already downloaded the first chunk.
	have := len(gzData) / 3
	if have < 1 {
		t.Fatalf("gzData too small: %d", len(gzData))
	}
	if err := os.WriteFile(part, gzData[:have], 0o644); err != nil {
		t.Fatalf("write part: %v", err)
	}

	var sawRange string
	c := NewWithRoundTripper(stubTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
		sawRange = r.Header.Get("Range")
		return resp(206, map[string]string{"Content-Type": "application/octet-stream"}, gzData[have:], r), nil
	}})

	if _, err := c.DownloadToFileMaybe(ctx, "https://example.test/file.gz", dest, true); err != nil {
		t.Fatalf("download gunzip: %v", err)
	}
	wantRange := "bytes=" + strconv.Itoa(have) + "-"
	if sawRange != wantRange {
		t.Fatalf("Range header: got %q want %q", sawRange, wantRange)
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read dest: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("gunzip mismatch: got=%d want=%d", len(got), len(plain))
	}
	if _, err := os.Stat(src); err == nil {
		t.Fatalf("expected src sidecar removed")
	}
	if _, err := os.Stat(part); err == nil {
		t.Fatalf("expected src part removed")
	}
}

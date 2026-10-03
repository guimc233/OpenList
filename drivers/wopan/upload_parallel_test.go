package template

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/wopan-sdk-go"
)

// woTestGateway 模拟 upload2C 网关：按 partIndex 收集分片，供测试拼装后比对
type woTestGateway struct {
	delay time.Duration

	mu          sync.Mutex
	parts       map[int][]byte
	fields      []map[string]string
	inFlight    int
	maxInFlight int
}

func (g *woTestGateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	g.inFlight++
	if g.inFlight > g.maxInFlight {
		g.maxInFlight = g.inFlight
	}
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.inFlight--
		g.mu.Unlock()
	}()

	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "multipart/form-data") {
		http.Error(w, "unexpected content type: "+ct, http.StatusBadRequest)
		return
	}
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		http.Error(w, "parse multipart: "+err.Error(), http.StatusBadRequest)
		return
	}
	fields := make(map[string]string, len(r.MultipartForm.Value))
	for k, v := range r.MultipartForm.Value {
		if len(v) > 0 {
			fields[k] = v[0]
		}
	}
	partIndex, err := strconv.Atoi(fields["partIndex"])
	if err != nil {
		http.Error(w, "bad partIndex: "+fields["partIndex"], http.StatusBadRequest)
		return
	}
	fhs := r.MultipartForm.File["file"]
	if len(fhs) != 1 {
		http.Error(w, fmt.Sprintf("expected 1 file part, got %d", len(fhs)), http.StatusBadRequest)
		return
	}
	f, err := fhs[0].Open()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if g.delay > 0 {
		time.Sleep(g.delay)
	}

	g.mu.Lock()
	g.parts[partIndex] = data
	g.fields = append(g.fields, fields)
	g.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"code": "0000",
		"msg":  "ok",
		"data": map[string]string{"fid": "fid-test"},
	})
}

func (g *woTestGateway) snapshot() (map[int][]byte, []map[string]string, int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	parts := make(map[int][]byte, len(g.parts))
	for k, v := range g.parts {
		parts[k] = v
	}
	fields := make([]map[string]string, len(g.fields))
	copy(fields, g.fields)
	return parts, fields, g.maxInFlight
}

// newTestStream 把一个真实的临时文件包装成 FileStream（GetFile() 走零拷贝分支）
func newTestStream(t *testing.T, data []byte, name string) (*stream.FileStream, *os.File) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "wopan-src-*.bin")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return &stream.FileStream{
		Ctx:      context.Background(),
		Obj:      &model.Object{ID: "src-id", Name: name, Size: int64(len(data))},
		Reader:   f,
		Mimetype: "application/octet-stream",
	}, f
}

func newTestDriver(zoneURL string) *Wopan {
	const token = "0123456789abcdef0123456789abcdef"
	client := wopan.DefaultWithAccessToken(token)
	client.ZoneURL = zoneURL
	client.ClassifyRuleData = &wopan.ClassifyRuleData{} // 阻止 GetFileType 触发真实网络请求
	return &Wopan{
		client: client,
		Addition: Addition{
			UploadConcurrency: 4,
		},
	}
}

func TestPutParallelReassemblesParts(t *testing.T) {
	const (
		partSize = 8 * 1024 * 1024
		name     = "random-payload.bin"
	)
	cases := []struct {
		name string
		size int
	}{
		{"one-small-part", 4096},
		{"single-full-part", partSize},
		{"exact-multiple", 2 * partSize},
		{"non-multiple-tail", 2*partSize + 4*1024*1024 + 123},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := make([]byte, tc.size)
			if _, err := rand.New(rand.NewSource(int64(tc.size))).Read(payload); err != nil {
				t.Fatalf("generate payload: %v", err)
			}
			wantHash := sha256.Sum256(payload)

			gateway := &woTestGateway{parts: map[int][]byte{}, delay: 50 * time.Millisecond}
			srv := httptest.NewServer(gateway)
			defer srv.Close()

			d := newTestDriver(srv.URL)
			fs, src := newTestStream(t, payload, name)
			defer src.Close()

			var progress int
			up := func(float64) { progress++ }

			if err := d.putParallel(context.Background(), &model.Object{ID: "target-dir"}, fs, up); err != nil {
				t.Fatalf("putParallel: %v", err)
			}

			wantTotal := tc.size / partSize
			if wantTotal == 0 {
				wantTotal = 1
			}

			parts, fields, maxInFlight := gateway.snapshot()
			if len(parts) != wantTotal {
				t.Fatalf("got %d parts, want %d", len(parts), wantTotal)
			}
			if len(fields) != wantTotal {
				t.Fatalf("got %d field sets, want %d", len(fields), wantTotal)
			}

			// 拼装顺序必须与服务端语义一致：按 partIndex 升序
			var assembled bytes.Buffer
			for i := 1; i <= wantTotal; i++ {
				data, ok := parts[i]
				if !ok {
					t.Fatalf("missing part %d", i)
				}
				offset := (i - 1) * partSize
				wantLen := partSize
				if i == wantTotal {
					wantLen = tc.size - offset
				}
				if len(data) != wantLen {
					t.Fatalf("part %d has %d bytes, want %d", i, len(data), wantLen)
				}
				assembled.Write(data)
			}
			if got := sha256.Sum256(assembled.Bytes()); got != wantHash {
				t.Fatalf("assembled payload sha256 mismatch")
			}
			if progress == 0 {
				t.Fatalf("progress callback was never invoked")
			}
			if wantTotal > 1 && maxInFlight < 2 {
				t.Fatalf("expected concurrent parts, max in-flight = %d", maxInFlight)
			}

			// 线上字段必须逐字保持
			first := fields[0]
			if first["totalPart"] != strconv.Itoa(wantTotal) {
				t.Errorf("totalPart = %q, want %d", first["totalPart"], wantTotal)
			}
			if first["fileSize"] != strconv.Itoa(tc.size) {
				t.Errorf("fileSize = %q, want %d", first["fileSize"], tc.size)
			}
			if first["fileName"] != name {
				t.Errorf("fileName = %q, want %q", first["fileName"], name)
			}
			if first["channel"] != wopan.ChannelWoCloud {
				t.Errorf("channel = %q, want %q", first["channel"], wopan.ChannelWoCloud)
			}
			if first["psToken"] != "undefined" {
				t.Errorf("psToken = %q, want %q", first["psToken"], "undefined")
			}
			if first["directoryId"] != "target-dir" {
				t.Errorf("directoryId = %q, want %q", first["directoryId"], "target-dir")
			}
			if first["accessToken"] != "0123456789abcdef0123456789abcdef" {
				t.Errorf("accessToken = %q", first["accessToken"])
			}
			if first["fileInfo"] == "" {
				t.Error("fileInfo is empty")
			}
			for _, f := range fields {
				if f["uniqueId"] == "" || f["uniqueId"] != first["uniqueId"] {
					t.Errorf("uniqueId not shared across parts: %q vs %q", f["uniqueId"], first["uniqueId"])
				}
				if f["fileInfo"] != first["fileInfo"] {
					t.Error("fileInfo not shared across parts")
				}
				idx, _ := strconv.Atoi(f["partIndex"])
				off := (idx - 1) * partSize
				wantPartSize := partSize
				if idx == wantTotal {
					wantPartSize = tc.size - off
				}
				if f["partSize"] != strconv.Itoa(wantPartSize) {
					t.Errorf("part %d partSize = %q, want %d", idx, f["partSize"], wantPartSize)
				}
			}
		})
	}
}

func TestPutParallelRejectsEmptyFile(t *testing.T) {
	fs, src := newTestStream(t, nil, "empty.bin")
	defer src.Close()

	err := newTestDriver("http://127.0.0.1:1").putParallel(context.Background(), &model.Object{ID: "target-dir"}, fs, func(float64) {})
	if err == nil {
		t.Fatal("expected an error for a 0-byte file")
	}
	if !strings.Contains(err.Error(), "0 字节") {
		t.Fatalf("unexpected error: %v", err)
	}
}

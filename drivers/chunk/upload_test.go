package chunk

import (
	"bytes"
	"crypto/sha256"
	"io"
	"math/rand"
	"sync"
	"testing"
)

func TestPlanParts(t *testing.T) {
	const partSize = int64(1 << 20)
	cases := []struct {
		size      int64
		wantSizes []int64
	}{
		{0, []int64{0}},
		{1, []int64{1}},
		{partSize - 1, []int64{partSize - 1}},
		{partSize, []int64{partSize}},
		{partSize + 1, []int64{partSize, 1}},
		{3 * partSize, []int64{partSize, partSize, partSize}},
		{3*partSize + 7, []int64{partSize, partSize, partSize, 7}},
	}
	for _, tc := range cases {
		parts := planParts(tc.size, partSize)
		if len(parts) != len(tc.wantSizes) {
			t.Fatalf("size %d: got %d parts, want %d", tc.size, len(parts), len(tc.wantSizes))
		}
		var nextOffset int64
		for i, p := range parts {
			if p.index != i {
				t.Errorf("size %d: part %d has index %d", tc.size, i, p.index)
			}
			if p.size != tc.wantSizes[i] {
				t.Errorf("size %d: part %d has size %d, want %d", tc.size, i, p.size, tc.wantSizes[i])
			}
			if p.offset != nextOffset {
				t.Errorf("size %d: part %d has offset %d, want %d", tc.size, i, p.offset, nextOffset)
			}
			nextOffset += p.size
		}
		if nextOffset != tc.size {
			t.Errorf("size %d: parts cover %d bytes", tc.size, nextOffset)
		}
		// 最后一片必须正好覆盖到文件末尾
		last := parts[len(parts)-1]
		if end := last.offset + last.size; end != tc.size {
			t.Errorf("size %d: last part ends at %d", tc.size, end)
		}
	}
}

// TestPartReadersReassembleSource 乱序读取所有分片并比对 sha256，
// 同时覆盖 Read 与 ReadAt（远端驱动零拷贝路径）两条读法。
func TestPartReadersReassembleSource(t *testing.T) {
	const partSize = int64(1 << 20)
	for _, size := range []int64{0, 1, 4096, partSize - 1, partSize, partSize + 1, 2 * partSize, 3*partSize + 12345} {
		t.Run("", func(t *testing.T) {
			payload := make([]byte, size)
			if _, err := rand.New(rand.NewSource(size)).Read(payload); err != nil {
				t.Fatalf("generate payload: %v", err)
			}
			want := sha256.Sum256(payload)

			parts := planParts(size, partSize)
			chunks := make([][]byte, len(parts))

			var (
				mu       sync.Mutex
				reported int64
			)
			report := func(n int) {
				mu.Lock()
				reported += int64(n)
				mu.Unlock()
			}

			// 用共享的 bytes.Reader 模拟并发下的同一个随机访问源
			src := bytes.NewReader(payload)
			var wg sync.WaitGroup
			for i := range parts {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					r := newPartReader(src, parts[i], report)
					var (
						b   []byte
						err error
					)
					if i%2 == 0 {
						b, err = io.ReadAll(r) // Read 路径
					} else {
						b, err = io.ReadAll(io.NewSectionReader(r, 0, parts[i].size)) // ReadAt 路径
					}
					if err != nil {
						t.Errorf("part %d: %v", i, err)
						return
					}
					chunks[i] = b
				}(i)
			}
			wg.Wait()

			var assembled bytes.Buffer
			for i, p := range parts {
				if int64(len(chunks[i])) != p.size {
					t.Fatalf("part %d: read %d bytes, want %d", i, len(chunks[i]), p.size)
				}
				assembled.Write(chunks[i])
			}
			if got := sha256.Sum256(assembled.Bytes()); got != want {
				t.Fatalf("size %d: reassembled sha256 mismatch", size)
			}
			if reported != size {
				t.Fatalf("size %d: progress reported %d bytes", size, reported)
			}
		})
	}
}

func TestNewPartProgress(t *testing.T) {
	var got []float64
	report := newPartProgress(100, func(p float64) { got = append(got, p) })
	report(0) // 忽略
	report(30)
	report(30)
	report(1_000_000) // 超量时夹在 100
	want := []float64{30, 60, 100}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}

	// nil 回调与非法总量都不能 panic
	newPartProgress(0, nil)(10)
	newPartProgress(100, nil)(10)
}

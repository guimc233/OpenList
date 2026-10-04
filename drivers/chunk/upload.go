package chunk

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
)

// chunkPart 描述源文件上的一个分片窗口
type chunkPart struct {
	index  int
	offset int64
	size   int64
}

// planParts 按 partSize 把 size 切分成若干分片。
// 除最后一片外大小均为 partSize，最后一片为剩余部分；
// 空文件也会得到一个大小为 0 的分片，以兼容既有行为。
func planParts(size, partSize int64) []chunkPart {
	fullPartCount := int(size / partSize)
	tailSize := size % partSize
	if tailSize == 0 && fullPartCount > 0 {
		fullPartCount--
		tailSize = partSize
	}
	parts := make([]chunkPart, 0, fullPartCount+1)
	for i := 0; i < fullPartCount; i++ {
		parts = append(parts, chunkPart{index: i, offset: int64(i) * partSize, size: partSize})
	}
	parts = append(parts, chunkPart{index: fullPartCount, offset: int64(fullPartCount) * partSize, size: tailSize})
	return parts
}

// partReader 是某个分片在源上独立的窗口。
// 它同时满足 io.Reader 与 io.ReaderAt（即 model.File），因此远端驱动既可以
// 直接按窗口零拷贝读取，也可以按需缓存；两条读路径都会累计上传进度。
type partReader struct {
	*io.SectionReader
	report func(int)
}

func newPartReader(src io.ReaderAt, part chunkPart, report func(int)) *partReader {
	return &partReader{
		SectionReader: io.NewSectionReader(src, part.offset, part.size),
		report:        report,
	}
}

func (r *partReader) Read(p []byte) (int, error) {
	n, err := r.SectionReader.Read(p)
	if n > 0 {
		r.report(n)
	}
	return n, err
}

func (r *partReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.SectionReader.ReadAt(p, off)
	if n > 0 {
		r.report(n)
	}
	return n, err
}

// newPartProgress 返回并发安全的进度累加器。
// 分片完成顺序不确定，因此按已读取字节数累计，并把结果夹在 100% 以内。
func newPartProgress(total int64, up driver.UpdateProgress) func(int) {
	var (
		consumed atomic.Int64
		mu       sync.Mutex
	)
	return func(n int) {
		if up == nil || n <= 0 || total <= 0 {
			return
		}
		percent := 100 * float64(consumed.Add(int64(n))) / float64(total)
		if percent > 100 {
			percent = 100
		}
		mu.Lock()
		up(percent)
		mu.Unlock()
	}
}

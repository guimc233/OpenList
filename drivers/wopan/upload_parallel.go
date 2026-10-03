package template

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/wopan-sdk-go"
)

const (
	// 必须与 SDK 保持一致：totalPart 公式与末片大小都依赖这个值
	woPartSize      = wopan.DefaultPartSize
	woDefaultConc   = 4
	woMaxConc       = 8
	woMaxRetry      = 2
	woRetryInterval = time.Second
)

// upload2CResp 对应 upload2C 的返回体（与 SDK 内部的 Upload2CResp 一致）
type upload2CResp struct {
	Code string `json:"code"`
	Data struct {
		Fid string `json:"fid"`
	} `json:"data"`
	Msg string `json:"msg"`
}

// woUploader 持有同一文件所有分片共享的上传状态
type woUploader struct {
	client      *wopan.WoClient
	uploadURL   string
	name        string
	mimeType    string
	size        int64
	totalPart   int64
	uniqueID    string
	accessToken string
	fileInfo    string
	directoryID string
}

func (d *Wopan) putParallel(ctx context.Context, dstDir model.Obj, stream model.FileStreamer, up driver.UpdateProgress) error {
	// (C) 0 字节文件：网关必然 400，提前给出明确错误
	if stream.GetSize() == 0 {
		return errors.New("wopan: 该网盘上传接口不支持 0 字节文件（网关会返回 400 Bad Request）")
	}

	// 取可随机访问的数据源，优先零拷贝
	src := stream.GetFile()
	if src == nil {
		// CacheFullAndWriter 内部按指针判空，传 nil 指针会被当成有效回调，这里显式规避
		var upPtr *model.UpdateProgress
		if up != nil {
			upPtr = &up
		}
		cached, err := stream.CacheFullAndWriter(upPtr, nil)
		if err != nil {
			return fmt.Errorf("wopan: 缓存上传流失败: %w", err)
		}
		src = cached
	}

	u, err := d.newWoUploader(stream, dstDir.GetID())
	if err != nil {
		return err
	}

	conc := d.UploadConcurrency
	if conc <= 0 {
		conc = woDefaultConc
	}
	if conc > woMaxConc {
		conc = woMaxConc
	}
	if int64(conc) > u.totalPart {
		conc = int(u.totalPart)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		done     int64
		firstErr error
		errMu    sync.Mutex
		upMu     sync.Mutex // OpenList 的进度回调不是并发安全的，串行化
	)

	report := func(n int64) {
		if up == nil {
			return
		}
		upMu.Lock()
		up(100 * float64(n) / float64(u.size))
		upMu.Unlock()
	}

	work := make(chan int64)
	var wg sync.WaitGroup

	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for partIndex := range work {
				offset := (partIndex - 1) * woPartSize
				partSize := woPartSize
				if partIndex == u.totalPart {
					partSize = u.size - offset // 与 SDK 完全一致：末片可能 > 8 MiB
				}

				var err error
				for attempt := 0; attempt <= woMaxRetry; attempt++ {
					if ctx.Err() != nil {
						err = ctx.Err()
						break
					}
					err = u.sendPart(ctx, src, partIndex, partSize, offset)
					if err == nil {
						break
					}
					select {
					case <-time.After(time.Duration(attempt+1) * woRetryInterval):
					case <-ctx.Done():
						err = ctx.Err()
					}
				}
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("wopan: 分片 %d/%d 上传失败: %w", partIndex, u.totalPart, err)
						cancel() // 让其它分片尽快退出
					}
					errMu.Unlock()
					return
				}
				report(atomic.AddInt64(&done, partSize))
			}
		}()
	}

feed:
	for i := int64(1); i <= u.totalPart; i++ {
		select {
		case work <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(work)
	wg.Wait()

	return firstErr
}

func (d *Wopan) newWoUploader(stream model.FileStreamer, targetDirID string) (*woUploader, error) {
	zoneURL := d.client.ZoneURL
	if zoneURL == "" {
		zoneURL = wopan.DefaultZoneURL
	}
	// 理论上 Init() 已保证；这里兜底，避免留空导致 URL 畸形
	if d.UploadZoneURL != "" {
		zoneURL = d.UploadZoneURL
	}

	accessToken, _ := d.client.GetToken()

	size := stream.GetSize()
	totalPart := size / woPartSize
	if totalPart == 0 {
		totalPart = 1
	}

	fileInfo := wopan.Json{
		"spaceType":   d.getSpaceType(),
		"directoryId": targetDirID,
		"batchNo":     time.Now().Format("20060102150405"),
		"fileName":    stream.GetName(),
		"fileSize":    size,
		"fileType":    d.client.GetFileType(stream.GetName()),
	}
	if d.getSpaceType() == wopan.SpaceTypeFamily {
		fileInfo["familyId"] = d.FamilyID
	}
	encFileInfo, err := d.client.EncryptParam(wopan.ChannelWoHome, fileInfo)
	if err != nil {
		return nil, err
	}

	return &woUploader{
		client:      d.client,
		uploadURL:   zoneURL + "/openapi/client/" + wopan.KeyUpload2C,
		name:        stream.GetName(),
		mimeType:    stream.GetMimetype(),
		size:        size,
		totalPart:   totalPart,
		uniqueID:    strconv.FormatInt(time.Now().UnixMilli(), 10),
		accessToken: accessToken,
		fileInfo:    encFileInfo,
		directoryID: targetDirID,
	}, nil
}

// sendPart 复刻 SDK uploadPart 的线上格式，但数据来源改为 SectionReader（并发安全）
func (u *woUploader) sendPart(ctx context.Context, src io.ReaderAt, partIndex, partSize, offset int64) error {
	fields := map[string]string{
		"uniqueId":    u.uniqueID,
		"accessToken": u.accessToken,
		"fileName":    u.name,
		"psToken":     "undefined",
		"fileSize":    strconv.FormatInt(u.size, 10),
		"totalPart":   strconv.FormatInt(u.totalPart, 10),
		"channel":     wopan.ChannelWoCloud,
		"directoryId": u.directoryID,
		"fileInfo":    u.fileInfo,
		"partSize":    strconv.FormatInt(partSize, 10),
		"partIndex":   strconv.FormatInt(partIndex, 10),
	}

	var resp upload2CResp
	// 每个分片独立包一层服务端上传限速，保持与串行实现一致的限速语义
	partReader := driver.NewLimitedUploadStream(ctx, io.NewSectionReader(src, offset, partSize))
	req := u.client.NewRequest().
		SetResult(&resp).
		ForceContentType("application/json;charset=UTF-8").
		SetHeaders(map[string]string{
			"Origin":     "https://pan.wo.cn",
			"Referer":    "https://pan.wo.cn/",
			"User-Agent": wopan.DefaultUA,
		}).
		SetMultipartFormData(fields).
		SetMultipartField("file", u.name, u.mimeType, partReader).
		SetContext(ctx)

	res, err := req.Post(u.uploadURL)
	if err != nil {
		return err
	}
	if res.IsError() {
		return fmt.Errorf("upload2C HTTP %d: %s", res.StatusCode(), res.String())
	}
	if resp.Code != "0000" {
		return fmt.Errorf("upload2C code=%s msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

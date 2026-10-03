package template

import (
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
)

type Addition struct {
	// Usually one of two
	driver.RootID
	// define other
	RefreshToken string `json:"refresh_token" required:"true"`
	FamilyID     string `json:"family_id" help:"Keep it empty if you want to use your personal drive"`
	SortRule     string `json:"sort_rule" type:"select" options:"name_asc,name_desc,time_asc,time_desc,size_asc,size_desc" default:"name_asc"`

	AccessToken string `json:"access_token"`

	// 字符串字段不写 type tag（internal/op/driver.go 会默认补成 "string"）
	UploadZoneURL string `json:"upload_zone_url" default:"" help:"强制指定上传网关（upload2C），例如 https://tjupload.pan.wo.cn 或 https://bjupload.pan.wo.cn:32443 。留空则使用服务端通过 GetZoneInfo 下发的地址。仅影响上传，不影响列表/下载。"`

	// 并发分片数：<=0 按默认 4 处理；上限 8
	UploadConcurrency int `json:"upload_concurrency" type:"number" default:"4" help:"upload2C 分片并发数，默认 4，建议不超过 6"`
}

var config = driver.Config{
	Name:              "WoPan",
	DefaultRoot:       "0",
	NoOverwriteUpload: true,
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		return &Wopan{}
	})
}

package ims

import (
	"context"
	"fmt"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/mods"
)

const (
	ACTION_UNKNOWN = iota
	ACTION_WATERMARK
	ACTION_RESIZE
)

type ImsAction struct {
	Action int
	Params map[string]string
}

type ImgStoreInfo struct {
	// storage type, currently, only yig is supported.
	Type int `json:"type"`
	// size of the data
	Size int64 `json:"size"`
	// storage information
	Info string `json:"info"`
}

type ImsReq struct {
	// specified source media type
	Type string `json:"type"`
	// source image storage info
	ImgSource ImgStoreInfo `json:"imgStoreInfo`
	// image actions, must keep the sequence from the http request.
	ImsActions []*ImsAction `json:actions"`
}

type ImsResp struct {
	// media type
	Type string
	// the size of the image data buffer.
	Length int64
	// data: the processed image data buffer
	Data []byte
}

type CephStoreInfo struct {
	// type defines whether uses the striper reader or not.
	Type int `json:"type"`
	// fsid defines the ceph cluster fsid
	Fsid string `json:"fsid"`
	// pool defines the pool for the objects
	Pool string `json: "pool"`
	// oid defines the object id
	Oid string `json:"oid"`
	// offset defines where to read
	Offset int64 `json:"offset"`
	// size defines the total length of this read
	Size int64 `json:"size"`
}

/*
* ImgProcessClient: handles the image process logic
*
 */
type ImgProcessPlugin interface {
	/*
	*Supports: check whether the module is supported or not.
	 */
	Supports(module string) bool
	/*
	*Do: process the image according to imsReq.
	*reader: original image data
	*return: processed image data & error
	 */
	Do(ctx context.Context, imsReq *ImsReq) (*ImsResp, error)
}

var imgProcessPlugin ImgProcessPlugin

func CreateImgProcessPlugin(plugins map[string]*mods.YigPlugin) error {
	for name, p := range plugins {
		if p.PluginType == mods.IMG_PROCESS_PLUGIN {
			c, err := p.Create(helper.CONFIG.Plugins[name].Args)
			if err != nil {
				helper.Logger.Error(nil, fmt.Sprintf("failed to create image process plugin, err: %v", err))
				return err
			}
			helper.Logger.Info(nil, "Succeed to create image process plugin")
			imgProcessPlugin = c.(ImgProcessPlugin)
			return nil
		}
	}
	helper.Logger.Warn(nil, "No image process plugin found.")
	return nil
}

func GetImgProcessPlugin() ImgProcessPlugin {
	return imgProcessPlugin
}

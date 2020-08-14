package ims

import (
	"context"
	"fmt"
	"io"
        "time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/mods"
)

const (
	ACTION_UNKNOWN = iota
	ACTION_WATERMARK
	ACTION_RESIZE
)

type ImsAction struct {
	Action int               `json:"action"`
	Params map[string]string `json:"params"`
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
	ImgSource ImgStoreInfo `json:"imgStoreInfo"`
	// image actions, must keep the sequence from the http request.
	ImsActions []*ImsAction `json:"actions"`
}

type ImsResp struct {
	// media type
	Type string
	// the size of the image data buffer.
	Length int64
	// data: the processed image data reader.
	// Note: it the caller's duty to close this reader.
	Reader io.ReadCloser
}

type CephStoreInfo struct {
	// type defines whether uses the striper reader or not.
	Type int `json:"type"`
	// fsid defines the ceph cluster fsid
	Fsid string `json:"fsid"`
	// pool defines the pool for the objects
	Pool string `json:"pool"`
	// oid defines the object id
	Oid string `json:"oid"`
	// offset defines where to read
	Offset int64 `json:"offset"`
	// size defines the total length of this read
	Size int64 `json:"size"`
}

type GetImsReq struct {
        Bucketname string `json:"bucketname"`
}

type CreateStyleBody struct {
        Name string `json:"name"`
        Style string `json:"style"`
}

type CreateStyleReq struct {
        Bucketname string `json:"bucketname"`
        Uid string `json:"uid"`
        Stylename string `json:"stylename"`
        Style string `json:"style"`
        Createtime time.Time `json:"createtime"`
        Deleted int `json:"deleted"`
        Updatetime time.Time `json:"updatetime"`
}

type DeleteStylesReq struct {
        Bucketname string `json:"bucketname"`
        Names []string `json:"names"`
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
        /*
        *GetImageMetrics: get image metrics.
        */
        GetImageMetrics(ctx context.Context, imsReq *GetImsReq) (*ImsResp, error)
        /*
        *CreateImageStyle: create image styles.
         */
        CreateImageStyle(ctx context.Context, imsReq *CreateStyleReq) (*ImsResp, error)
        /*
        *ListImageStyles: list image styles.
         */
        ListImageStyles(ctx context.Context, imsReq *GetImsReq) (*ImsResp, error)
        /*
        *Delete: list image styles.
         */
        DeleteImageStyles(ctx context.Context, imsReq *DeleteStylesReq) (*ImsResp, error)
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

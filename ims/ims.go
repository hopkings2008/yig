package ims

import (
	"context"
	"fmt"
	"io"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/mods"
)

type ImsAction struct {
	Action string
	Params map[string]string
}

type ImsReq struct {
	// specified media type
	Type string
	// image actions, must keep the sequence from the http request.
	ImsActions []*ImsAction
	// reader: the original image data stream
	Reader io.ReadCloser
}

type ImsResp struct {
	// media type
	Type string
	// the size of the read stream.
	Length int64
	// reader: the processed image data stream
	Reader io.ReadCloser
}

/*
* ImgProcessClient: handles the image process logic
*
 */
type ImgProcessClient interface {
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

var imgProcessClient ImgProcessClient

func CreateImgProcessClient(plugins map[string]*mods.YigPlugin) error {
	for name, p := range plugins {
		if p.PluginType == mods.IMG_PROCESS_PLUGIN {
			c, err := p.Create(helper.CONFIG.Plugins[name].Args)
			if err != nil {
				helper.Logger.Error(nil, fmt.Sprintf("failed to create image process plugin, err: %v", err))
				return err
			}
			helper.Logger.Info(nil, "Succeed to create image process plugin")
			imgProcessClient = c.(ImgProcessClient)
			return nil
		}
	}
	helper.Logger.Warn(nil, "No image process plugin found.")
	return nil
}

func GetImgProcessClient() ImgProcessClient {
	return imgProcessClient
}

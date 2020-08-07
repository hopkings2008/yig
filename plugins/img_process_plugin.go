package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/ims"
	"github.com/journeymidnight/yig/mods"
)

const (
	MODULE_IMAGE = "image"
)

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = mods.YigPlugin{
	Name:       "img_plugin",
	PluginType: mods.IMG_PROCESS_PLUGIN,
	Create:     CreatePlugin,
}

type ImgProcessPlugin struct {
	Server string
	client *http.Client
}

func (ipp *ImgProcessPlugin) Supports(module string) bool {
	if module == MODULE_IMAGE {
		return true
	}
	return false
}

func (ipp *ImgProcessPlugin) Do(ctx context.Context, imsReq *ims.ImsReq) (*ims.ImsResp, error) {
	reqStr, err := json.Marshal(imsReq)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to encoding req %v, err: %v", *imsReq, err))
		return nil, err
	}
	helper.Logger.Info(ctx, fmt.Sprintf("req: %s", string(reqStr)))
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/image/proc", ipp.Server), bytes.NewReader(reqStr))
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to new post http request to server %s, err: %v",
			ipp.Server, err))
		return nil, err
	}
	req.Header.Add("Content-Length", strconv.Itoa(len(reqStr)))
	resp, err := ipp.client.Do(req)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to send req %v to server %s, err: %v",
			*imsReq, ipp.Server, err))
		return nil, err
	}
	if resp.StatusCode >= 300 {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to perform image process for %v, return %d",
			*imsReq, resp.StatusCode))
		resp.Body.Close()
		return nil, errors.New(resp.Status)
	}
	lenStr := resp.Header.Get("Content-Length")
	if lenStr == "" {
		helper.Logger.Error(ctx, fmt.Sprintf("got invalid response from img server, missing content-length"))
		resp.Body.Close()
		return nil, errors.New(fmt.Sprintf("got invalid response from img server, missing content-length"))
	}
	size, err := strconv.ParseInt(lenStr, 10, 64)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("got invalid response from img server, invalid content-length: %s", lenStr))
		resp.Body.Close()
		return nil, err
	}
	contentType := resp.Header.Get("Content-Type")
	if "" == contentType {
		helper.Logger.Error(ctx, fmt.Sprintf("got invalid response from img server, missing contnent-Type"))
		resp.Body.Close()
		return nil, errors.New("got invalid response from img server, missing contnent-Type")
	}

	imsResp := &ims.ImsResp{
		Type:   contentType,
		Length: size,
		Reader: resp.Body,
	}
	return imsResp, nil
}

func CreatePlugin(config map[string]interface{}) (interface{}, error) {
	helper.Logger.Info(nil, fmt.Sprintf("Create image process plugin: %v", config))
	tr := &http.Transport{
		MaxIdleConns:       100,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	var imgPlugin *ImgProcessPlugin
	if u, ok := config["img_server_url"]; ok {
		imgPlugin = &ImgProcessPlugin{
			Server: u.(string),
			client: &http.Client{
				Transport: tr,
			},
		}
	} else {
		helper.Logger.Error(nil, fmt.Sprintf("no option img_server_url found"))
		return nil, errors.New(fmt.Sprintf("no option img_server_url found"))
	}

	return interface{}(imgPlugin), nil
}

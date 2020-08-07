package api

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/journeymidnight/yig/api/datatype/policy"
	errs "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/ims"
	"github.com/journeymidnight/yig/meta/types"
)

const (
	IMS_PROCESS_KEY = "x-oss-process"
	IMS_IMG_MODULE  = "image"
)

func (api ObjectAPIHandlers) ImageServiceHandler(w http.ResponseWriter, r *http.Request) {
	// handler process logic:
	// 1. check the auth & get object meta info
	// 2. first check the image process module
	// 3. try to get the plugin according to module, if no plugin found just return error to client.
	// 4. do process logic by calling plugin's interface
	// 5. return the image process result to the client.

	ctx := r.Context()
	var objectName, bucketName string
	vars := mux.Vars(r)
	bucketName = vars["bucket"]
	objectName = vars["object"]
	// 1. check the auth & get object meta info.
	obj, err := api.getObjectInfoFromReq(ctx, bucketName, objectName, r)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to get object data for url: %s, err: %v",
			r.URL.String(), err))
		WriteErrorResponse(w, r, err)
		return
	}
	// 2. first check the image process module
	var processStr string
	switch strings.ToUpper(r.Method) {
	case "GET":
		// url: x-oss-process=image/circle,r_100
		querys := r.URL.Query()
		values := querys[IMS_PROCESS_KEY]
		if len(values) == 0 {
			// this is invalid request.
			helper.Logger.Error(ctx, fmt.Sprintf("got invalid image service url: %s", r.URL.String()))
			WriteErrorResponse(w, r, errs.ErrMissingFields)
			return
		}
		// image/circle,r_100
		processStr = values[0]
	case "POST":
		// body: x-oss-process=image/circle,r_100
		// 0 <= content-length <= 2MB
		if r.ContentLength <= 0 || r.ContentLength > (2<<20) {
			helper.Logger.Error(ctx, fmt.Sprintf("got invalid content-length for image process url:%s", r.ContentLength, r.URL.String()))
			WriteErrorResponse(w, r, errs.ErrInvalidRequestBody)
			return
		}
		queryStr, err := ioutil.ReadAll(r.Body)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("failed to read body for image process url: %s, err: %v",
				r.URL.String(), err))
			WriteErrorResponse(w, r, err)
			return
		}
		if queryStr == nil || len(queryStr) == 0 {
			helper.Logger.Error(ctx, fmt.Sprintf("got invaid image process body for url: %s", r.URL.String()))
			WriteErrorResponse(w, r, errs.ErrInvalidRequestBody)
			return
		}
		elems := strings.Split(string(queryStr), "=")
		if len(elems) < 2 || strings.ToLower(elems[0]) != IMS_PROCESS_KEY {
			helper.Logger.Error(ctx, fmt.Sprintf("got invalid body %s for image process url: %s", string(queryStr), r.URL.String()))
			WriteErrorResponse(w, r, errs.ErrInvalidRequestBody)
			return
		}
		// image/circle,r_100
		processStr = elems[1]
	default:
		helper.Logger.Error(ctx, fmt.Sprintf("got invalid method %s for image process url: %s", r.Method, r.URL.String()))
		WriteErrorResponse(w, r, errs.ErrMethodNotAllowed)
		return
	}
	imgModule, actions, err := api.getModuleActions(ctx, processStr, r)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("got invalid image serivce url: %s, err: %v", r.URL.String(), err))
		WriteErrorResponse(w, r, err)
		return
	}
	// 3. try to get the plugin according to module, if no plugin found just return error to client.
	imgProcessPlugin := ims.GetImgProcessPlugin()
	if imgProcessPlugin == nil {
		helper.Logger.Error(ctx, "image process is unsupported")
		WriteErrorResponse(w, r, errs.ErrNotImplemented)
		return
	}
	if !imgProcessPlugin.Supports(imgModule) {
		helper.Logger.Error(ctx, fmt.Sprintf("image module %s for url %s is unsupported", imgModule, r.URL.String()))
		WriteErrorResponse(w, r, errs.ErrNotImplemented)
		return
	}
	// 4. do process logic by calling plugin's interface
	imgCephStoreInfoStr, err := ims.EncodeCephStoreInfo(obj)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to encode ceph store for obj: %s/%s, err: %v",
			obj.BucketName, obj.Name, err))
		WriteErrorResponse(w, r, err)
		return
	}
	imgStoreInfo := ims.ImgStoreInfo{
		Type: ims.STORAGE_DRIVER_CEPH_STRIP,
		Size: obj.Size,
		Info: imgCephStoreInfoStr,
	}

	imsReq := &ims.ImsReq{
		ImsActions: actions,
		ImgSource:  imgStoreInfo,
	}

	if "" != obj.ContentType {
		contentType := strings.TrimPrefix(obj.ContentType, "image/")
		if contentType != "" {
			imsReq.Type = fmt.Sprintf(".%s", contentType)
		}
	}

	if "" == imsReq.Type {
		// check object name
		elems := strings.Split(obj.Name, ".")
		if len(elems) >= 2 {
			imsReq.Type = fmt.Sprintf(".%s", elems[1])
		}
	}
	if "" == imsReq.Type {
		// set default to png
		imsReq.Type = ".png"
	}

	imsResp, err := imgProcessPlugin.Do(ctx, imsReq)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to perform image process for url: %s, err: %v",
			r.URL.String(), err))
		WriteErrorResponse(w, r, err)
		return
	}
	defer imsResp.Reader.Close()
	// set the response header.
	// content-type should be the original object content-type.
	w.Header().Set("Content-Type", imsResp.Type)
	w.Header().Set("Content-Length", strconv.FormatInt(imsResp.Length, 10))
	n, err := io.Copy(w, imsResp.Reader)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to write image process data to client for url: %s, err: %v",
			r.URL.String(), err))
		WriteErrorResponse(w, r, err)
		return
	}
	if int64(n) != imsResp.Length {
		helper.Logger.Error(ctx, fmt.Sprintf("the whole data size is %d, but only write %d", imsResp.Length, n))
		WriteErrorResponse(w, r, errors.New(fmt.Sprintf("the whole data size is %d, but only write %d", imsResp.Length, n)))
		return
	}
	helper.Logger.Info(ctx, fmt.Sprintf("succeed to perform image process for %s/%s, return %d data.",
		bucketName, objectName, imsResp.Length))
	if n > 0 {
		/*
			If the whole write or only part of write is successfull,
			n should be positive, so record this
		*/
		w.(*ResponseRecorder).size += int64(n)
	}
}

func (api ObjectAPIHandlers) getModuleActions(ctx context.Context, value string, r *http.Request) (string, []*ims.ImsAction, error) {
	if value == "" {
		helper.Logger.Error(ctx, "got invalid input for module actions")
		return "", nil, errs.ErrMissingFields
	}

	// module/action format.
	elems := strings.Split(value, "/")
	num := len(elems)
	if num < 2 {
		helper.Logger.Error(ctx, fmt.Sprintf("no enough keys(%s) for query for module actions", value))
		return "", nil, errs.ErrMissingFields
	}
	// the first element defines module
	imgModule := strings.ToLower(elems[0])
	var actions []*ims.ImsAction
	// action is in action/param_list format.
	// param_list is in param_value,param_value,... foramt.
	// for watermark, param must have: image_xxxx
	for i := 1; i < num; i++ {
		params := strings.Split(elems[i], ",")
		action := &ims.ImsAction{
			Action: getImgAction(params[0]),
			Params: make(map[string]string),
		}
		np := len(params)
		for j := 1; j < np; j++ {
			vals := strings.Split(params[j], "_")
			if len(vals) < 2 {
				helper.Logger.Warn(ctx, fmt.Sprintf("skip the invalid param %s for req %s",
					params[j], r.URL.String()))
				continue
			}
			switch action.Action {
			case ims.ACTION_WATERMARK:
				if vals[0] == "image" {
					// decode the bucket/objectname
					logoBucket, logoName, err := decodeImageInfo(vals[1])
					if err != nil {
						return "", nil, err
					}
					logoObjInfo, err := api.getObjectInfoFromReq(ctx, logoBucket, logoName, r)
					if err != nil {
						return "", nil, err
					}
					logObjeInfoStr, err := ims.EncodeStoreInfo(logoObjInfo)
					if err != nil {
						return "", nil, err
					}
					action.Params[vals[0]] = logObjeInfoStr
				} else {
					action.Params[vals[0]] = vals[1]
				}
			default:
				action.Params[vals[0]] = vals[1]
			}
		}
		actions = append(actions, action)
	}

	return imgModule, actions, nil
}

func (api ObjectAPIHandlers) getObjectInfoFromReq(ctx context.Context, bucketName string, objectName string, r *http.Request) (*types.Object, error) {
	// check the auth
	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(r, policy.GetObjectAction); err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to check auth for (%s/%s), err: %v", bucketName, objectName, err))
		return nil, err
	}

	// check version
	version := r.URL.Query().Get("versionId")
	// Fetch object stat info.
	object, err := api.ObjectAPI.GetObjectInfo(ctx, bucketName, objectName, version, credential)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to get object info for (%s/%s), err: %v",
			bucketName, objectName, err))
		return nil, err
	}

	// check whether the object has already been deleted.
	if object.DeleteMarker {
		// already removed
		err = errs.ErrNoSuchKey
		helper.Logger.Error(ctx, fmt.Sprintf("%s/%s has already been removed", bucketName, objectName))
		return nil, err
	}

	return object, nil
}

func getImgAction(action string) int {
	switch strings.ToLower(action) {
	case "watermark":
		return ims.ACTION_WATERMARK
	case "resize":
		return ims.ACTION_RESIZE
	}
	return ims.ACTION_UNKNOWN
}

func decodeImageInfo(param string) (string, string, error) {
	// - must be changed to +
	param = strings.ReplaceAll(param, "-", "+")
	// _ must be changed to /
	param = strings.ReplaceAll(param, "_", "/")

	info, err := base64.StdEncoding.DecodeString(param)
	if err != nil {
		return "", "", err
	}

	elems := strings.Split(string(info), "/")
	if len(elems) < 2 {
		return "", "", errors.New(fmt.Sprintf("got invalid image store info: %s", param))
	}
	return elems[0], elems[1], nil
}

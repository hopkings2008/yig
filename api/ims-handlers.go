package api

import (
	"context"
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
	// 1. first check the image process module
	// 2. try to get the plugin according to module, if no plugin found just return error to client.
	// 3. check the auth & get object data
	// 4. do process logic by calling plugin's interface
	// 5. return the image process result to the client.

	// 1. first check the image process module
	ctx := r.Context()
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
		defer r.Body.Close()
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
	imgModule, actions, err := api.getModuleActions(ctx, processStr)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("got invalid image serivce url: %s, err: %v", r.URL.String(), err))
		WriteErrorResponse(w, r, err)
		return
	}
	// 2. try to get the plugin according to module, if no plugin found just return error to client.
	imgProcessClient := ims.GetImgProcessClient()
	if imgProcessClient == nil {
		helper.Logger.Error(ctx, "image process is unsupported")
		WriteErrorResponse(w, r, errs.ErrNotImplemented)
		return
	}
	if !imgProcessClient.Supports(imgModule) {
		helper.Logger.Error(ctx, fmt.Sprintf("image module %s for url %s is unsupported", imgModule, r.URL.String()))
		WriteErrorResponse(w, r, errs.ErrNotImplemented)
		return
	}
	// 3. check the auth & get object data
	obj, reader, err := api.getObjectData(ctx, r)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to get object data for url: %s, err: %v",
			r.URL.String(), err))
		WriteErrorResponse(w, r, err)
		return
	}
	// 4. do process logic by calling plugin's interface
	imsReq := &ims.ImsReq{
		Type:       obj.ContentType,
		ImsActions: actions,
		Reader:     reader,
	}

	imsResp, err := imgProcessClient.Do(ctx, imsReq)
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
		return
	}
	if n > 0 {
		/*
			If the whole write or only part of write is successfull,
			n should be positive, so record this
		*/
		w.(*ResponseRecorder).size += int64(n)
	}
}

func (api ObjectAPIHandlers) getModuleActions(ctx context.Context, value string) (string, []*ims.ImsAction, error) {
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
	for i := 1; i < num; i++ {
		params := strings.Split(elems[i], ",")
		action := &ims.ImsAction{
			Action: params[0],
			Params: make(map[string]string),
		}
		np := len(params)
		for j := 1; j < np; j++ {
			vals := strings.Split(params[j], "_")
			action.Params[vals[0]] = vals[1]
		}
		actions = append(actions, action)
	}

	return imgModule, actions, nil
}

func (api ObjectAPIHandlers) getObjectData(ctx context.Context, r *http.Request) (*types.Object, io.ReadCloser, error) {
	var objectName, bucketName string
	vars := mux.Vars(r)
	bucketName = vars["bucket"]
	objectName = vars["object"]

	// check the auth
	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(api, r, policy.GetObjectAction, bucketName, objectName); err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to check auth for (%s/%s), err: %v", bucketName, objectName, err))
		return nil, nil, err
	}

	// check version
	version := r.URL.Query().Get("versionId")
	// Fetch object stat info.
	object, err := api.ObjectAPI.GetObjectInfo(r.Context(), bucketName, objectName, version, credential)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to get object info for (%s/%s), err: %v",
			bucketName, objectName, err))
		if err == errs.ErrNoSuchKey {
			err = api.errAllowableObjectNotFound(r, bucketName, credential)
		}
		return nil, nil, err
	}

	// check whether the object has already been deleted.
	if object.DeleteMarker {
		// already removed
		err = errs.ErrNoSuchKey
		helper.Logger.Error(ctx, fmt.Sprintf("%s/%s has already been removed", bucketName, objectName))
		return nil, nil, err
	}

	// check whether the object is encrypted.
	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to get sse header for %s/%s, err: %v",
			bucketName, objectName, err))
		return nil, nil, err
	}
	if len(sseRequest.CopySourceSseCustomerKey) != 0 {
		err = errs.ErrInvalidSseHeader
		helper.Logger.Error(ctx, fmt.Sprintf("got invalid sse header for (%s/%s)", bucketName, objectName))
		return nil, nil, err
	}

	// Get the object data stream.
	reader, err := api.ObjectAPI.GetObjectStream(ctx, object, 0, object.Size, sseRequest)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("failed to get object stream for (%s/%s), err: %v",
			object.BucketName, object.Name, err))
		return nil, nil, err
	}
	return object, reader, nil
}

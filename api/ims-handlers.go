package api

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/journeymidnight/yig/api/datatype/policy"
	"github.com/journeymidnight/yig/crypto"
	errs "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/ims"
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
		querys := r.URL.Query()
		values := querys[IMS_PROCESS_KEY]
		if len(values) == 0 {
			// this is invalid request.
			helper.Logger.Error(ctx, fmt.Sprintf("got invalid image service url: %s", r.URL.String()))
			WriteErrorResponse(w, r, errs.ErrMissingFields)
			return
		}
		processStr = values[0]
	case "POST":
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
		processStr = elems[1]
	default:
		helper.Logger.Error(ctx, fmt.Sprintf("got invalid method %s for image process url: %s", r.Method, r.URL.String()))
		WriteErrorResponse(w, r, errs.ErrMethodNotAllowed)
		return
	}
	imgModule, actions, err := api.getModuleActions(processStr)
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
}

func (api ObjectAPIHandlers) getModuleActions(value string) (string, []*ims.ImsAction, error) {
	if value == "" {
		helper.Logger.Error("got invalid input for module actions")
		return "", nil, errs.ErrMissingFields
	}

	// module/action format.
	elems := strings.Split(value, "/")
	num := len(elems)
	if num < 2 {
		helper.Logger.Error(fmt.Sprintf("no enough keys(%s) for query for module actions", value))
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

func (api ObjectAPIHandlers) getObjectData(r *http.Request) (io.ReadCloser, error) {
	var objectName, bucketName string
	vars := mux.Vars(r)
	bucketName = vars["bucket"]
	objectName = vars["object"]

	var credential common.Credential
	var err error
	if credential, err = checkRequestAuth(api, r, policy.GetObjectAction, bucketName, objectName); err != nil {
		helper.Logger.Error(fmt.Sprintf("failed to check auth for (%s/%s), err: %v", bucketName, objectName, err))
		return nil, err
	}

	version := r.URL.Query().Get("versionId")
	// Fetch object stat info.
	object, err := api.ObjectAPI.GetObjectInfo(r.Context(), bucketName, objectName, version, credential)
	if err != nil {
		helper.Logger.Error(fmt.Sprintf("failed to get object info for(%s/%s), err: %v",
			bucketName, objectName, err))
		if err == ErrNoSuchKey {
			err = api.errAllowableObjectNotFound(r, bucketName, credential)
		}
		return nil, err
	}

	if object.DeleteMarker {
		// already removed
		err = errs.ErrNoSuchKey
		helper.Logger.Error(fmt.Sprintf("%s/%s has already been removed", bucketName, objectName))
		return nil, err
	}

	sseRequest, err := parseSseHeader(r.Header)
	if err != nil {
		helper.Logger.Error(fmt.Sprintf("failed to get sse header for %s/%s, err: %v",
			bucketName, objectName, err))
		return nil, err
	}
	if len(sseRequest.CopySourceSseCustomerKey) != 0 {
		err = errs.ErrInvalidSseHeader
		helper.Logger.Error(fmt.Sprintf("got invalid sse header for (%s/%s)", bucketName, objectName, err))
		return nil, err
	}

	// Get the object.
	startOffset := int64(0)
	length := object.Size
	// Indicates if any data was written to the http.ResponseWriter
	dataWritten := false

	// io.Writer type which keeps track if any data was written.
	writer := funcToWriter(func(p []byte) (int, error) {
		if !dataWritten {
			// Set headers on the first write.
			// Set standard object headers.
			SetObjectHeaders(w, object, hrange)

			// Set any additional requested response headers.
			setGetRespHeaders(w, r.URL.Query())

			if version != "" {
				w.Header().Set("x-amz-version-id", version)
			}
			dataWritten = true
		}
		n, err := w.Write(p)
		if n > 0 {
			/*
				If the whole write or only part of write is successfull,
				n should be positive, so record this
			*/
			w.(*ResponseRecorder).size += int64(n)
		}
		return n, err
	})

	switch object.SseType {
	case "":
		break
	case crypto.S3KMS.String():
		w.Header().Set("X-Amz-Server-Side-Encryption", "aws:kms")
		// TODO: not implemented yet
	case crypto.S3.String():
		w.Header().Set("X-Amz-Server-Side-Encryption", "AES256")
	case crypto.SSEC.String():
		w.Header().Set("X-Amz-Server-Side-Encryption-Customer-Algorithm", "AES256")
		w.Header().Set("X-Amz-Server-Side-Encryption-Customer-Key-Md5",
			r.Header.Get("X-Amz-Server-Side-Encryption-Customer-Key-Md5"))
	}

	// Reads the object at startOffset and writes to mw.
	if err := api.ObjectAPI.GetObject(r.Context(), object, startOffset, length, writer, sseRequest); err != nil {
		helper.ErrorIf(err, "Unable to write to client.")
		if !dataWritten {
			// Error response only if no data has been written to client yet. i.e if
			// partial data has already been written before an error
			// occurred then no point in setting StatusCode and
			// sending error XML.
			WriteErrorResponse(w, r, err)
		}
		return
	}
	if !dataWritten {
		// If ObjectAPI.GetObject did not return error and no data has
		// been written it would mean that it is a 0-byte object.
		// call wrter.Write(nil) to set appropriate headers.
		writer.Write(nil)
	}
}

package api

import (
	"io"
	"net/http"

	"github.com/gorilla/mux"
	. "github.com/journeymidnight/yig/api/datatype/lifecycle"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/signature"
)

func (api ObjectAPIHandlers) PutBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	r = generateIamCtxRequest(r)

	helper.Logger.Info(r.Context(), "enter PutBucketLCHandler", bucket)

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	lifecycle, err := ParseLifecycleConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		helper.Logger.Error(r.Context(), "Unable to parse lifecycle body:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	helper.Logger.Info(r.Context(), "Setting lifecycle:", *lifecycle)
	err = api.ObjectAPI.SetBucketLifecycle(r.Context(), bucket, *lifecycle, credential)
	if err != nil {
		helper.Logger.Error(r.Context(), "Unable to set lifecycle for bucket:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketLifeCycle"
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) GetBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	r = generateIamCtxRequest(r)

	helper.Logger.Info(r.Context(), "enter GetBucketLifeCycleHandler", bucketName)

	var credential common.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
		return
	}

	lifecycle, err := api.ObjectAPI.GetBucketLifecycle(r.Context(), bucketName, credential)
	if err != nil {
		helper.Logger.Error(r.Context(), bucketName, "error:", err)
		if err == ErrNoSuchBucketLc {
			WriteErrorResponse(w, r, ErrNoSuchBucketLc)
		} else {
			WriteErrorResponse(w, r, ErrInternalError)
		}
		return
	}

	if lifecycle.IsEmpty() {
		helper.Logger.Info(r.Context(), "The bucket does not have LifeCycle configured!")
		WriteErrorResponse(w, r, ErrNoSuchBucketLc)
		return
	}

	lcBuffer, err := xmlFormat(lifecycle)
	if err != nil {
		helper.Logger.Error(r.Context(), bucketName, "error:", err)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w)
	//ResponseRecorder
	w.(*ResponseRecorder).operationName = "GetBucketLifeCycle"
	WriteSuccessResponse(w, lcBuffer)
}

func (api ObjectAPIHandlers) DelBucketLifeCycleHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	r = generateIamCtxRequest(r)

	helper.Logger.Info(r.Context(), "enter DelBucketLifeCycleHandler")

	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	err = api.ObjectAPI.DelBucketLifecycle(r.Context(), bucket, credential)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "DelBucketLifeCycle"
	WriteSuccessNoContent(w)

}

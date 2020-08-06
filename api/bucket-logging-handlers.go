/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api

import (
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"

	. "github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/signature"
)

func islegalLoggingRule(targetBucket, targetPrefix string) bool {
	if targetBucket == "" && targetPrefix == "" {
		return true
	}
	if targetBucket != "" && targetPrefix != "" {
		if len(targetPrefix) > 255 {
			return false
		}
		return true
	}
	return false
}
func (api ObjectAPIHandlers) PutBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	r = generateIamCtxRequest(r)
	ctx := getRequestContext(r)
	var credential common.Credential
	var err error
	if credential, err = signature.IsReqAuthenticated(r); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	var bl BucketLoggingStatus
	blBuffer, err := ioutil.ReadAll(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		helper.Logger.Error(r.Context(), "Unable to read bucket logging body", err)
		WriteErrorResponse(w, r, err)
		return
	}
	err = xml.Unmarshal(blBuffer, &bl)
	if err != nil {
		helper.Logger.Error(r.Context(), "Unable to parse bucket logging XML body", err)
		WriteErrorResponse(w, r, err)
		return
	}

	// check bucket logging  rule
	if !islegalLoggingRule(bl.LoggingEnabled.TargetBucket,
		bl.LoggingEnabled.TargetPrefix) {
		WriteErrorResponse(w, r, ErrInvalidBucketLogging)
		return
	}
	helper.Logger.Info(r.Context(), "Setting bucket logging:", bl)
	err = api.ObjectAPI.SetBucketLogging(r.Context(), ctx.BucketInfo, bl, credential)
	if err != nil {
		helper.Logger.Error(r.Context(), "Unable to set bucket logging for bucket", err)
		WriteErrorResponse(w, r, err)
		return
	}

	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	// ResponseRecorder
	w.(*ResponseRecorder).operationName = "PutBucketLogging"
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) GetBucketLoggingHandler(w http.ResponseWriter, r *http.Request) {
	r = generateIamCtxRequest(r)
	ctx := getRequestContext(r)
	var credential common.Credential
	var err error
	switch signature.GetRequestAuthType(r) {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, err)
		return
	case signature.AuthTypeAnonymous:
		break
	case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
		signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
		if credential, err = signature.IsReqAuthenticated(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}
	}

	bl, err := api.ObjectAPI.GetBucketLogging(r.Context(), ctx.BucketInfo, credential)
	if err != nil {
		helper.Logger.Error(r.Context(), "Failed to get bucket ACL policy for bucket", ctx.BucketName,
			"error")
		WriteErrorResponse(w, r, err)
		return
	}

	if bl.LoggingEnabled.TargetBucket == "" &&
		bl.LoggingEnabled.TargetPrefix == "" {
		WriteErrorResponse(w, r, ErrNoSuchBucketLogging)
		return
	}

	var blBuffer []byte
	blBuffer, err = xmlFormat(bl)
	if err != nil {
		helper.Logger.Error(r.Context(), "Failed to marshal bucket logging XML for bucket", ctx.BucketName,
			"error:", err)
		WriteErrorResponse(w, r, err)
		return
	}

	setXmlHeader(w)
	w.(*ResponseRecorder).operationName = "GetBucketLogging"
	WriteSuccessResponse(w, blBuffer)
}

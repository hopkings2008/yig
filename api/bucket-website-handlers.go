package api

import (
	"io"
	"net/http"
	"strings"

	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/policy"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/signature"
)

func (api ObjectAPIHandlers) PutBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	helper.Logger.Info(r.Context(), "PutBucketWebsiteHandler", "enter")
	ctx := getRequestContext(r)

	var credential common.Credential
	var err error
	switch ctx.AuthType {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
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

	if ctx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return
	}
	if credential.UserId != ctx.BucketInfo.OwnerId {
		WriteErrorResponse(w, r, ErrBucketAccessForbidden)
		return
	}
	// Error out if Content-Length is missing.
	// PutBucketPolicy always needs Content-Length.
	if r.ContentLength <= 0 {
		WriteErrorResponse(w, r, ErrMissingContentLength)
		return
	}

	websiteConfig, err := ParseWebsiteConfig(io.LimitReader(r.Body, r.ContentLength))
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	err = api.ObjectAPI.SetBucketWebsite(ctx.BucketInfo, *websiteConfig)
	if err != nil {
		helper.Logger.Error(r.Context(), err, "Unable to set website for bucket.")
		WriteErrorResponse(w, r, err)
		return
	}
	WriteSuccessResponse(w, nil)
}

func (api ObjectAPIHandlers) GetBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	helper.Logger.Info(r.Context(), "GetBucketWebsiteHandler", "enter")
	ctx := getRequestContext(r)

	var credential common.Credential
	var err error
	switch ctx.AuthType {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
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

	if ctx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return
	}
	if credential.UserId != ctx.BucketInfo.OwnerId {
		WriteErrorResponse(w, r, ErrBucketAccessForbidden)
		return
	}

	// Read bucket access policy.
	bucketWebsite, err := api.ObjectAPI.GetBucketWebsite(ctx.BucketName)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	encodedSuccessResponse, err := xmlFormat(bucketWebsite)
	if err != nil {
		helper.Logger.Info(r.Context(), err, "Failed to marshal Website XML for bucket", ctx.BucketInfo.Name)
		WriteErrorResponse(w, r, ErrInternalError)
		return
	}

	setXmlHeader(w)
	// Write to client.
	WriteSuccessResponse(w, encodedSuccessResponse)
}

func (api ObjectAPIHandlers) DeleteBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	ctx := getRequestContext(r)

	var credential common.Credential
	var err error
	switch ctx.AuthType {
	default:
		// For all unknown auth types return error.
		WriteErrorResponse(w, r, ErrAccessDenied)
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

	if ctx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return
	}
	if credential.UserId != ctx.BucketInfo.OwnerId {
		WriteErrorResponse(w, r, ErrBucketAccessForbidden)
		return
	}
	if err := api.ObjectAPI.DeleteBucketWebsite(ctx.BucketInfo); err != nil {
		WriteErrorResponse(w, r, err)
		return
	}
	// Success.
	WriteSuccessNoContent(w)
}

func (api ObjectAPIHandlers) HandledByWebsite(w http.ResponseWriter, r *http.Request) (handled bool) {
	ctx := getRequestContext(r)
	if ctx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return true
	}
	if ctx.AuthType != signature.AuthTypeAnonymous {
		return false
	}
	helper.Logger.Info(r.Context(), "HandledByWebsite enter:", ctx.BucketName, ctx.ObjectName)

	website := ctx.BucketInfo.Website
	// redirect
	if redirect := website.RedirectAllRequestsTo; redirect != nil && redirect.HostName != "" {
		helper.Logger.Info(r.Context(), "RedirectAllRequestsTo:", redirect.HostName, redirect.Protocol)
		if !ctx.IsBucketDomain {
			WriteErrorResponse(w, r, ErrSecondLevelDomainForbidden)
			return true
		}
		protocol := redirect.Protocol
		if protocol == "" {
			protocol = helper.Ternary(r.URL.Scheme == "", "http", r.URL.Scheme).(string)
		}

		helper.Logger.Info(r.Context(), "Redirect to:", protocol+"://"+redirect.HostName+r.RequestURI)
		http.Redirect(w, r, protocol+"://"+redirect.HostName+r.RequestURI, http.StatusMovedPermanently)

		return true
	}

	if id := website.IndexDocument; id != nil && id.Suffix != "" {
		if !ctx.IsBucketDomain {
			WriteErrorResponse(w, r, ErrSecondLevelDomainForbidden)
			return true
		}

		// match routing rules
		if len(website.RoutingRules) != 0 {
			for _, rule := range website.RoutingRules {
				// If the condition matches, handle redirect
				if rule.Match(ctx.ObjectName, "") {
					rule.DoRedirect(w, r, ctx.ObjectName)
					return true
				}
			}
		}

		// handle IndexDocument
		if strings.HasSuffix(ctx.ObjectName, "/") || ctx.ObjectName == "" {
			helper.Logger.Info(r.Context(), "handle index document:", ctx.ObjectName)
			indexName := ctx.ObjectName + id.Suffix
			credential := common.Credential{}
			isAllow, err := IsBucketPolicyAllowed(credential.UserId, ctx.BucketInfo, r, policy.GetObjectAction, indexName)
			if err != nil {
				WriteErrorResponse(w, r, err)
				return true
			}
			credential.AllowOtherUserAccess = isAllow
			index, err := api.ObjectAPI.GetObjectInfo(r.Context(), ctx.BucketName, indexName, "", credential)
			if err != nil {
				helper.Logger.Error(r.Context(), "HandledByWebsite err for index:", indexName, err)
				if err == ErrNoSuchKey {
					api.errAllowableObjectNotFound(w, r, credential)
					return true
				}
				WriteErrorResponse(w, r, err)
				return true
			}
			writer := newGetObjectResponseWriter(w, r, index, nil, http.StatusOK, "")
			// Reads the object at startOffset and writes to mw.
			if err := api.ObjectAPI.GetObject(r.Context(), index, 0, index.Size, writer, datatype.SseRequest{}); err != nil {
				helper.ErrorIf(err, "Unable to write to client.")
				if !writer.dataWritten {
					// Error response only if no data has been written to client yet. i.e if
					// partial data has already been written before an error
					// occurred then no point in setting StatusCode and
					// sending error XML.
					WriteErrorResponse(w, r, err)
				}
				return true
			}
			if !writer.dataWritten {
				// If ObjectAPI.GetObject did not return error and no data has
				// been written it would mean that it is a 0-byte object.
				// call wrter.Write(nil) to set appropriate headers.
				writer.Write(nil)
			}
			return true
		}

	}
	return false
}

// Return configured website ErrorDocument.
// If successfully sent ErrorDocument, return true which mean it's handled,
// Else return false, and error code will be sent to client by errAllowableObjectNotFound().
func (api ObjectAPIHandlers) ReturnWebsiteErrorDocument(w http.ResponseWriter, r *http.Request, statusCode int) (handled bool) {
	helper.Logger.Info(r.Context(), "ReturnWebsiteErrorDocument statusCode:", statusCode)
	w.(*ResponseRecorder).operationName = "GetObject"
	ctx := getRequestContext(r)
	if ctx.BucketInfo == nil {
		WriteErrorResponse(w, r, ErrNoSuchBucket)
		return true
	}
	website := ctx.BucketInfo.Website
	if ed := website.ErrorDocument; ed != nil && ed.Key != "" {
		indexName := ed.Key
		credential := common.Credential{}
		isAllow, err := IsBucketPolicyAllowed(credential.UserId, ctx.BucketInfo, r, policy.GetObjectAction, indexName)
		if err != nil {
			helper.Logger.Error(r.Context(), "IsBucketPolicyAllowed fail for:", ctx.BucketInfo.Name, indexName, err, isAllow)
			return false
		}

		credential.AllowOtherUserAccess = isAllow
		index, err := api.ObjectAPI.GetObjectInfo(r.Context(), ctx.BucketName, indexName, "", credential)
		if err != nil {
			helper.Logger.Error(r.Context(), "GetObjectInfo failed for:", ctx.BucketName, indexName, err, isAllow)
			return false
		}

		helper.Logger.Info(r.Context(), "ReturnWebsiteErrorDocument: ", ctx.BucketName, indexName)

		writer := newGetObjectResponseWriter(w, r, index, nil, statusCode, "")
		// Reads the object at startOffset and writes to mw.
		if err := api.ObjectAPI.GetObject(r.Context(), index, 0, index.Size, writer, datatype.SseRequest{}); err != nil {
			helper.ErrorIf(err, "Unable to write to client.")
			if !writer.dataWritten {
				// Error response only if no data has been written to client yet. i.e if
				// partial data has already been written before an error
				// occurred then no point in setting StatusCode and
				// sending error XML.
				return false
			}
			return true
		}
		if !writer.dataWritten {
			// If ObjectAPI.GetObject did not return error and no data has
			// been written it would mean that it is a 0-byte object.
			// call wrter.Write(nil) to set appropriate headers.
			writer.Write(nil)
		}
		return true
	} else {
		return false
	}
}

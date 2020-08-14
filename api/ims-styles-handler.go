package api

import (
        "encoding/json"
        "errors"
        "fmt"
        "io"
        "net/http"
        "strconv"

        "github.com/gorilla/mux"
        . "github.com/journeymidnight/yig/error"
        "github.com/journeymidnight/yig/helper"
        "github.com/journeymidnight/yig/iam/common"
        "github.com/journeymidnight/yig/signature"
        "github.com/journeymidnight/yig/ims"
)

func (api ObjectAPIHandlers) CreateImsStylesHandler(w http.ResponseWriter, r *http.Request) {
        // 1. check the auth.
        // 2. check the bucket exist or not and check the user access.
        // 3. check the style and styleName.
        // 4. try to get the plugin according to module, if no plugin found just return error to client.
        // 5. create image styles to ims and return the result to the client.       
        ctx := r.Context()
        vars := mux.Vars(r)
        bucketName := vars["bucket"]

        r = generateIamCtxRequest(r)
        // 1. check the auth.
        var err error
        var credential common.Credential

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
        // 2. check the bucket exist or not and check the user access.
        bucket, err := api.ObjectAPI.GetBucket(r.Context(), bucketName)
        if err != nil {
                WriteErrorResponse(w, r, err)
                return
        }

        if bucket.OwnerId != credential.UserId {
                WriteErrorResponse(w, r, ErrBucketAccessForbidden)
                return
        }

        // 3. check the style and styleName.
        if r.ContentLength <= 0 || r.ContentLength > (2<<20) {
                helper.Logger.Error(ctx, fmt.Sprintf("got invalid content-length: %d for create style url:%s", r.ContentLength, r.URL.String()))
                WriteErrorResponse(w, r, ErrInvalidRequestBody)
                return
        }

        buf := make([]byte, r.ContentLength)
        n, err := r.Body.Read(buf)
        if err != nil {
                if err != io.EOF {
                        WriteErrorResponse(w, r, err)
                        return
                }
        }

        imageStyles := &ims.CreateStyleBody{}
        err = json.Unmarshal(buf[:n], imageStyles)
        if err != nil {
                helper.Logger.Error(ctx, fmt.Sprintf("error to convert imageStyles: %v from json", imageStyles))
                return
        }

        styleName := imageStyles.Name
        if !isValidStyleName(styleName) {
                WriteErrorResponse(w, r, ErrInvalidStyleName)
                return
        }
        style := imageStyles.Style
        if len(style) <= 0 || len(style) > 32768 {
                WriteErrorResponse(w, r, ErrInvalidStyle)
                return
        }

        // 4. try to get the plugin according to module, if no plugin found just return error to client.
        imgProcessPlugin := ims.GetImgProcessPlugin()
        if imgProcessPlugin == nil {
                helper.Logger.Error(ctx, "image process is unsupported")
                WriteErrorResponse(w, r, ErrNotImplemented)
                return
        }

        // 5. create image styles to ims and return the result to the client.
        StylesReq := &ims.CreateStyleReq{
                Bucketname: bucketName,
                Uid: credential.UserId,
                Stylename: styleName,
                Style: style,
                Deleted: 0,
        }
        StylesResp, err := imgProcessPlugin.CreateImageStyle(ctx, StylesReq)
        if err != nil {
                helper.Logger.Error(ctx, fmt.Sprintf("failed to create image styles, stylename is: %s, style is: %s, err: %v", styleName, style, err))
                WriteErrorResponse(w, r, err)
                return
        }
        defer StylesResp.Reader.Close()
        w.Header().Set("Content-Type", StylesResp.Type)
        w.Header().Set("Content-Length", strconv.FormatInt(StylesResp.Length, 10))
        out, err := io.Copy(w, StylesResp.Reader)
        if err != nil {
                helper.Logger.Error(ctx, fmt.Sprintf("failed to write styles data to client for url: %s, err: %v",
                        r.URL.String(), err))
                WriteErrorResponse(w, r, err)
                return
        }
        if int64(out) != StylesResp.Length {
                helper.Logger.Error(ctx, fmt.Sprintf("the whole data size is %d, but only write %d", StylesResp.Length, out))
                WriteErrorResponse(w, r, errors.New(fmt.Sprintf("the whole data size is %d, but only write %d", StylesResp.Length, out)))
                return
        }
        helper.Logger.Info(ctx, fmt.Sprintf("succeed to create styles for %s, return %d data.", bucketName, StylesResp.Length))
        if out > 0 {
                /*
                   If the whole write or only part of write is successful,
                   n should be positive, so record this
                */
                w.(*ResponseRecorder).size += int64(out)
        }
}

func (api ObjectAPIHandlers) ListImsStylesHandler(w http.ResponseWriter, r *http.Request) {
        // 1. check the auth.
        // 2. check the bucket exist or not and check the user permission.
        // 3. try to get the plugin according to module, if no plugin found just return error to client.
        // 4. post list image styles to ims and return the result to the client.
        ctx := r.Context()
        vars := mux.Vars(r)
        bucketName := vars["bucket"]

        r = generateIamCtxRequest(r)
        // 1. check the auth.
        var err error
        var credential common.Credential

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
        // 2. check the bucket exist or not and check the user permission.
        bucket, err := api.ObjectAPI.GetBucket(r.Context(), bucketName)
        if err != nil {
                WriteErrorResponse(w, r, err)
                return
        }
        if bucket.OwnerId != credential.UserId {
                WriteErrorResponse(w, r, ErrBucketAccessForbidden)
                return
        }
        // 3. try to get the plugin according to module, if no plugin found just return error to client.
        imgProcessPlugin := ims.GetImgProcessPlugin()
        if imgProcessPlugin == nil {
                helper.Logger.Error(ctx, "image process is unsupported")
                WriteErrorResponse(w, r, ErrNotImplemented)
                return
        }

        // 4. post list image styles to ims and return the result to the client.
        StylesReq := &ims.GetImsReq{
                Bucketname: bucketName,
        }
        StylesResp, err := imgProcessPlugin.ListImageStyles(ctx, StylesReq)
        if err != nil {
                helper.Logger.Error(ctx, fmt.Sprintf("failed to list bucket %v image styles, err: %v", bucketName, err))
                WriteErrorResponse(w, r, err)
                return
        }
        defer StylesResp.Reader.Close()
        w.Header().Set("Content-Type", StylesResp.Type)
        w.Header().Set("Content-Length", strconv.FormatInt(StylesResp.Length, 10))
        n, err := io.Copy(w, StylesResp.Reader)
        if err != nil {
                helper.Logger.Error(ctx, fmt.Sprintf("failed to write styles data to client for url: %s, err: %v", r.URL.String(), err))
                WriteErrorResponse(w, r, err)
                return
        }
        if int64(n) != StylesResp.Length {
                helper.Logger.Error(ctx, fmt.Sprintf("the whole data size is %d, but only write %d", StylesResp.Length, n))
                WriteErrorResponse(w, r, errors.New(fmt.Sprintf("the whole data size is %d, but only write %d", StylesResp.Length, n)))
                return
        }
        helper.Logger.Info(ctx, fmt.Sprintf("succeed to list styles for %s, return %d data.", bucketName, StylesResp.Length))
        if n > 0 {
                /*
                   If the whole write or only part of write is successful,
                   n should be positive, so record this
                */
                w.(*ResponseRecorder).size += int64(n)
        }
}

func (api ObjectAPIHandlers) DeleteImsStylesHandler(w http.ResponseWriter, r *http.Request) {
        // 1. check the auth.
        // 2. check the bucket exist or not and check the user permission.
        // 3. check the style and styleName.
        // 4. try to get the plugin according to module, if no plugin found just return error to client.
        // 5. post create image styles to ims and return the result to the client.
        ctx := r.Context()
        vars := mux.Vars(r)
        bucketName := vars["bucket"]

        r = generateIamCtxRequest(r)
        // 1. check the auth.
        var err error
        var credential common.Credential

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
        // 2. check the bucket exist or not and check the user permission.
        bucket, err := api.ObjectAPI.GetBucket(r.Context(), bucketName)
        if err != nil {
                WriteErrorResponse(w, r, err)
                return
        }
        if bucket.OwnerId != credential.UserId {
                WriteErrorResponse(w, r, ErrBucketAccessForbidden)
                return
        }
        // 3. check the styleName.
        if r.ContentLength <= 0 || r.ContentLength > (2<<20) {
                helper.Logger.Error(ctx, fmt.Sprintf("got invalid content-length: %d for create style url:%s", r.ContentLength, r.URL.String()))
                WriteErrorResponse(w, r, ErrInvalidRequestBody)
                return
        }

        buf := make([]byte, r.ContentLength)
        n, err := r.Body.Read(buf)
        if err != nil {
                if err != io.EOF {
                        WriteErrorResponse(w, r, err)
                        return
                }
        }

        deleteReq := &ims.DeleteStylesReq{}
        err = json.Unmarshal(buf[:n], deleteReq)
        if err != nil {
                helper.Logger.Error(ctx, fmt.Sprintf("error to convert deleteReq: %v from json", deleteReq))
                return
        }

        styleNames := deleteReq.Names
        for _, name := range styleNames {
                if !isValidStyleName(name) {
                        WriteErrorResponse(w, r, ErrInvalidStyleName)
                        return
                }
        }

        // 4. try to get the plugin according to module, if no plugin found just return error to client.
        imgProcessPlugin := ims.GetImgProcessPlugin()
        if imgProcessPlugin == nil {
                helper.Logger.Error(ctx, "image process is unsupported")
                WriteErrorResponse(w, r, ErrNotImplemented)
                return
        }

        // 5. post delete image styles to ims and return the result to the client.
        deleteReq.Bucketname = bucketName
        StylesResp, err := imgProcessPlugin.DeleteImageStyles(ctx, deleteReq)
        if err != nil {
                helper.Logger.Error(ctx, fmt.Sprintf("failed to delete image styles, stylenames is: %v, err: %v", styleNames, err))
                WriteErrorResponse(w, r, err)
                return
        }
        defer StylesResp.Reader.Close()
        w.Header().Set("Content-Type", StylesResp.Type)
        w.Header().Set("Content-Length", strconv.FormatInt(StylesResp.Length, 10))
        out, err := io.Copy(w, StylesResp.Reader)
        if err != nil {
                helper.Logger.Error(ctx, fmt.Sprintf("failed to write styles data to client for url: %s, err: %v", r.URL.String(), err))
                WriteErrorResponse(w, r, err)
                return
        }
        if int64(out) != StylesResp.Length {
                helper.Logger.Error(ctx, fmt.Sprintf("the whole data size is %d, but only write %d", StylesResp.Length, out))
                WriteErrorResponse(w, r, errors.New(fmt.Sprintf("the whole data size is %d, but only write %d", StylesResp.Length, out)))
                return
        }
        helper.Logger.Info(ctx, fmt.Sprintf("succeed to delete styles for %s, return %d data.", bucketName, StylesResp.Length))
        if out > 0 {
                /*
                   If the whole write or only part of write is successful,
                   out should be positive, so record this
                */
                w.(*ResponseRecorder).size += int64(out)
        }
}


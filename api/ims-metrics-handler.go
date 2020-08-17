package api

import (
        "errors"
        "strconv"
        "io"
        "fmt"
        "net/http"

        "github.com/gorilla/mux"
        . "github.com/journeymidnight/yig/error"
        "github.com/journeymidnight/yig/helper"
        "github.com/journeymidnight/yig/ims"
        "github.com/journeymidnight/yig/signature"
)


func (api ObjectAPIHandlers) GetImsImageMetricsHandler(w http.ResponseWriter, r *http.Request) {
        // handler process logic:
        // 1. check the auth.
        // 2. try to get the plugin according to module, if no plugin found just return error to client.
        // 3. get metrics from ims and return the result to the client.
        ctx := r.Context()
        vars := mux.Vars(r)
        bucketName := vars["bucket"]

        r = generateIamCtxRequest(r)

        // 1. check the auth.
        var err error
        switch signature.GetRequestAuthType(r) {
        case signature.AuthTypeAnonymous:
                break
        case signature.AuthTypePresignedV4, signature.AuthTypeSignedV4,
                signature.AuthTypePresignedV2, signature.AuthTypeSignedV2:
                if _, err = signature.IsReqAuthenticated(r); err != nil {
                        WriteErrorResponse(w, r, err)
                        return
                }
        default:
                // For all unknown auth types return error.
                WriteErrorResponse(w, r, ErrAccessDenied)
                return

        }

        // 2. try to get the plugin according to module, if no plugin found just return error to client.
        imgProcessPlugin := ims.GetImgProcessPlugin()
        if imgProcessPlugin == nil {
                helper.Logger.Error(ctx, "image process is unsupported")
                WriteErrorResponse(w, r, ErrNotImplemented)
                return
        }

        // 3. get metrics from ims and return the result to the client.
        inputMetrics := &ims.GetImsReq{
                Bucketname: bucketName,
        }
        MetricsResp, err := imgProcessPlugin.GetImageMetrics(ctx, inputMetrics)
        if err != nil {
                helper.Logger.Error(ctx, fmt.Sprintf("failed to get metrics from plugin, bucket is: %s, err: %v",
                        bucketName, err))
                WriteErrorResponse(w, r, err)
                return
        }
        defer MetricsResp.Reader.Close()
        w.Header().Set("Content-Type", MetricsResp.Type)
        w.Header().Set("Content-Length", strconv.FormatInt(MetricsResp.Length, 10))
        n, err := io.Copy(w, MetricsResp.Reader)
        if err != nil {
                helper.Logger.Error(ctx, fmt.Sprintf("failed to write metrics data to client for url: %s, err: %v",
                        r.URL.String(), err))
                WriteErrorResponse(w, r, err)
                return
        }
        if int64(n) != MetricsResp.Length {
                helper.Logger.Error(ctx, fmt.Sprintf("the whole data size is %d, but only write %d", MetricsResp.Length, n))
                WriteErrorResponse(w, r, errors.New(fmt.Sprintf("the whole data size is %d, but only write %d", MetricsResp.Length, n)))
                return
        }
        helper.Logger.Info(ctx, fmt.Sprintf("succeed to get iamge metrics for %s, return %d data.", bucketName, MetricsResp.Length))
        if n > 0 {
                /*
                        If the whole write or only part of write is successfull,
                        n should be positive, so record this
                */
                w.(*ResponseRecorder).size += int64(n)
        }
}


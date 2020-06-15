package api

import (
	"context"
	"net/http"
	"runtime"
	"strings"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/signature"
)

type RequestContextKeyType string

const RequestContextKey RequestContextKeyType = "RequestContext"

type RequestIdKeyType string

const RequestIdKey RequestIdKeyType = "RequestID"

type ContextLoggerKeyType string

const ContextLoggerKey ContextLoggerKeyType = "ContextLogger"

type RequestContext struct {
	RequestID      string
	Logger         log.Logger
	BucketName     string
	ObjectName     string
	BucketInfo     *types.Bucket
	ObjectInfo     *types.Object
	AuthType       signature.AuthType
	IsBucketDomain bool
}

type Server struct {
	Server *http.Server
}

func (s *Server) Stop() {
	helper.Logger.Info(nil, "Server stopped")
}

func generateIamCtxRequest(r *http.Request) *http.Request {
	longName := ""
	pc, _, _, ok := runtime.Caller(1)
	if ok {
		longName = runtime.FuncForPC(pc).Name()
	} else {
		helper.Logger.Info(r.Context(), "cannot get action...")
		longName = "unknown action"
	}
	strs := strings.Split(longName, ".")
	actionName := strs[len(strs)-1]
	helper.Logger.Info(nil, "action:", actionName)

	const (
		REQ_NETWORK_TYPE_INTERNAL = 1
		REQ_NETWORK_TYPE_EXTERN   = 2
	)
	networkType := REQ_NETWORK_TYPE_EXTERN
	if strings.Contains(r.Host, helper.CONFIG.InternalDomain) {
		networkType = REQ_NETWORK_TYPE_INTERNAL
	}

	ctx := context.WithValue(r.Context(), common.IamContextKey, common.IamContext{
		NetWorkType:   networkType,
		ReqActionName: actionName,
		Region:        helper.CONFIG.Region,
	})
	return r.WithContext(ctx)
}

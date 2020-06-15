package common

import (
	"context"
	"errors"
	"fmt"
)

// credential container for access and secret keys.
type Credential struct {
	UserId               string
	DisplayName          string
	AccessKeyID          string
	SecretAccessKey      string
	AllowOtherUserAccess bool
}

type CredReq struct {
	NetworkType int
	ActionName  string
	AccessKeyID string
	RegionID    string
}

type IamContext struct {
	NetWorkType   int
	ReqActionName string
	Region        string
}

type IamContextType string

const IamContextKey IamContextType = "IamContext"

func GetIamContext(ctx context.Context) (IamContext, error) {
	if ctx == nil {
		return IamContext{}, fmt.Errorf("ctx is nil when get iam context")
	}

	if result, ok := ctx.Value(IamContextKey).(IamContext); ok {
		return result, nil
	}
	return IamContext{}, fmt.Errorf("failed to find iam context.")
}

func (r CredReq) CacheKey() string {
	var key string
	key = fmt.Sprintf("%d:%s:%s", r.NetworkType, r.ActionName, r.AccessKeyID)
	return key
}

func (a Credential) String() string {
	userId := "UserId: " + a.UserId
	accessStr := "AccessKey: " + a.AccessKeyID
	secretStr := "SecretKey: " + a.SecretAccessKey
	return userId + " " + accessStr + " " + secretStr + "\n"
}

var ErrAccessKeyNotExist = errors.New("Access key does not exist")

package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/md5"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/journeymidnight/yig/circuitbreak"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/mods"
)

const pluginName = "wocloud_iam_oss"

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.IAM_PLUGIN,
	Create:     GetWocloudOssIamClient,
}

// Query is struct for iam request body
type Query struct {
	RegionId    string `json:"regionId"`
	NetworkType int    `json:"networkType"`
	Expression  string `json:"expression"`
	AccessKeyId string `json:"accessKeyId"`
	Sign        string `json:"sign"`
}

// QueryResp is iam response data from iam
type QueryResp struct {
	CurUserId       int    `json:"curUserId"`
	MainUserId      int    `json:"mainUserId"`
	AccessKeyId     string `json:"accessKeyId"`
	AccessKeySecret string `json:""accessKeySecret"`
}

// QueryRespAll is iam respons status from iam
type QueryRespAll struct {
	Status string    `json:"status"`
	Code   string    `json:"code"`
	Msg    string    `json:"msg"`
	Data   QueryResp `json:"data"`
}

type Client struct {
	httpClient  *circuitbreak.CircuitClient
	iamEndpoint string
	region      string
	actions     map[string]string
	privateKey  []byte
}

func getIAMReturnCode(code string) error {
	switch code {
	case "1001":
		return ErrIAMInvalidArgs
	case "50000":	
		return ErrIAMUnauthorized
	case "50001":	
		return ErrIAMAccessKeyIdNotExist
	case "50002":	
		return ErrIAMUserNotExist
	case "50003":	
		return ErrIAMAccountNotExist
	case "50004":	
		return ErrIAMOperationRefused
	case "50005":	
		return ErrIAMNotOpenOSSService
	case "50006":	
		return ErrIAMResourcePackExcess
	case "50007":	
		return ErrIAMResourcePackExpired
	case "50008":	
		return ErrIAMResourcePackExcessAndExpired
	case "50009":	
		return ErrIAMResourcePackNotPurchased
	case "50010":	
		return ErrIAMSignatureFailed
	case "50012":	
		return ErrIAMDownstreamTrafficPackExcess
	case "50013":	
		return ErrIAMDownstreamTrafficPackExpired
	case "50014":	
		return ErrIAMDownstreamTrafficPackExpiredAndExcess
	case "50015":	
		return ErrIAMDownstreamTrafficPackNotPurchased
	case "50016":	
		return ErrIAMRequestsExceeded
	case "50017":	
		return ErrIAMRequestsExpired
	case "50018":	
		return ErrIAMRequestsExpiredAndExceeded
	case "50019":	
		return ErrIAMRequestsPackUnpurchased
	default:	
		return ErrIAMUnknown
	}	
	return nil
}
// GetKeysByUid get credential according to uid
func (a *Client) GetKeysByUid(uid string) (credentials []common.Credential, err error) {
	return nil, fmt.Errorf("unsupported api")
}

func privateEncryptMD5withRSA(data []byte, keyBytes []byte) (string, error) {
	hashMD5 := md5.New()
	hashMD5.Write(data)
	Digest := hashMD5.Sum(nil)
	block, _ := pem.Decode(keyBytes)
	if block == nil {
		return "", errors.New("private key error")
	}
	parsedKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("ParsePKCS8PrivateKey err: %v", err)
	}
	rsaKey, ok := parsedKey.(*rsa.PrivateKey)
	if !ok {
		return "", errors.New("type rsa.PrivateKey assertion failed")
	}

	signData, err := rsa.SignPKCS1v15(nil, rsaKey, crypto.MD5, Digest)
	if err != nil {
		return "", fmt.Errorf("failed to calculate the signature, err: %v", err)
	}

	return url.QueryEscape(base64.StdEncoding.EncodeToString(signData)), nil
}

func (a *Client) getStringToSign(query Query) string {
	m := make(map[string]string)
	keys := make([]string, 0)

	val := reflect.ValueOf(query)
	for i := 0; i < val.Type().NumField(); i++ {
		key := val.Type().Field(i).Tag.Get("json")
		if key == "sign" {
			continue
		}

		m[key] = fmt.Sprintf("%v", val.Field(i).Interface())
		keys = append(keys, key)
	}
	sort.Strings(keys)

	strToSign := ""
	first := true
	for _, key := range keys {
		if !first {
			strToSign += "&"
		} else {
			first = false
		}
		strToSign += key
		strToSign += "="
		strToSign += m[key]
	}

	return strToSign
}

// GetCredential generate iam request and get credential from response
func (a *Client) GetCredential(credReq common.CredReq) (credential common.Credential, err error) {
	var query Query
	query.AccessKeyId = credReq.AccessKeyID
	query.NetworkType = credReq.NetworkType
	query.RegionId = credReq.RegionID
	if expression, ok := a.actions[credReq.ActionName]; ok {
		query.Expression = expression
	} else {
		helper.Logger.Error(nil, "unkown expression for action ", credReq.ActionName)
		return credential, ErrIAMAccessKeyIdNotExist
	}

	strToSign := a.getStringToSign(query)
	helper.Logger.Info(nil, "strToSign:", strToSign)
	sign, err := privateEncryptMD5withRSA([]byte(strToSign), a.privateKey)
	if err != nil {
		helper.Logger.Error(nil, "failed to calc signature. err:", err)
		return credential, ErrIAMAccessKeyIdNotExist
	}
	query.Sign = sign

	b, err := json.Marshal(query)
	if err != nil {
		helper.Logger.Error(nil, "failed to json query. err:", err)
		return credential, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		select {
		case <-time.After(10 * time.Second):
			helper.Logger.Info(nil, "send iam request timeout, over 10s")
		case <-ctx.Done():
			helper.Logger.Info(nil, ctx.Err()) // prints "context deadline exceeded"
		}
	}()

	request, err := http.NewRequest("POST", a.iamEndpoint, bytes.NewReader(b))
	if err != nil {
		helper.Logger.Error(nil, "failed to new POST request. err:", err)

		return credential, ErrIAMRequestFailed
	}

	request.Header.Set("content-type", "application/json")
	request = request.WithContext(ctx)
	response, err := a.httpClient.Do(request)
	if err != nil {
		helper.Logger.Error(nil, "failed to send POST request. err:", err)

		return credential, ErrIAMRequestFailed
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		helper.Logger.Error(nil, "failed to query iam server, http code is not 200. code:", response.StatusCode)

		return credential, ErrIAMQueryFailed
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		helper.Logger.Error(nil, "failed to read response body. err:", err)

		return credential, ErrIAMResponseBodyReadFailed
	}
	helper.Logger.Info(nil, "iam:", a.iamEndpoint)
	helper.Logger.Info(nil, "request:", string(b))
	helper.Logger.Info(nil, "response:", string(body))

	var queryRetAll QueryRespAll
	err = json.Unmarshal(body, &queryRetAll)
	if err != nil {
		helper.Logger.Error(nil, "failed to unmarshal json from response body. err:", err)

		return credential, ErrIAMResponseBodyParseFailed
	}
	if queryRetAll.Status != "200" {
		helper.Logger.Error(nil, "status in iam response is not 200, status:", queryRetAll.Status, " code:", queryRetAll.Code, " msg:", queryRetAll.Msg)
		return credential, getIAMReturnCode(queryRetAll.Code)
	}

	credential.UserId = fmt.Sprintf("%v", queryRetAll.Data.MainUserId)
	credential.DisplayName = credential.UserId
	credential.AccessKeyID = queryRetAll.Data.AccessKeyId
	credential.SecretAccessKey = queryRetAll.Data.AccessKeySecret
	return credential, nil
}

// GetCorrectWocloudIamClient create request type
func GetCorrectWocloudIamClient(url string) *circuitbreak.CircuitClient {
	var c *circuitbreak.CircuitClient = nil
	if strings.HasPrefix(url, "https") {
		c = circuitbreak.NewCircuitClientWithInsecureSSL()
	} else if strings.HasPrefix(url, "http") {
		c = circuitbreak.NewCircuitClient()
	}
	return c
}

// GetWocloudOssIamClient create iam struct for oss
func GetWocloudOssIamClient(config map[string]interface{}) (interface{}, error) {
	helper.Logger.Info(nil, "Get plugin config: \n", config)

	privKeyData, err := ioutil.ReadFile(config["private_key_path"].(string))
	if err != nil {
		helper.Logger.Error(nil, "failed to read private key file. err:", err)
		return nil, err
	}

	actions := make(map[string]string)
	data, err := ioutil.ReadFile(config["action_map_path"].(string))
	if err != nil {
		helper.Logger.Error(nil, "failed to read action map file. err:", err)
		return nil, err
	}
	err = json.Unmarshal(data, &actions)
	if err != nil {
		helper.Logger.Error(nil, "failed to unmarshal action list. err:", err)
		return nil, err
	}

	c := &Client{
		httpClient:  GetCorrectWocloudIamClient(config["iam_endpoint"].(string)),
		iamEndpoint: config["iam_endpoint"].(string),
		actions:     actions,
		privateKey:  privKeyData,
	}

	return interface{}(c), nil
}

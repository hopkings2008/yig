package ims

import (
	"encoding/json"

	"github.com/journeymidnight/yig/meta/types"
)

/*
* storage driver type definition
 */
const (
	STORAGE_DRIVER_UNKNOWN = iota
	STORAGE_DRIVER_CEPH_COMMON
	STORAGE_DRIVER_CEPH_STRIP
)

func EncodeCephStoreInfo(obj *types.Object) (string, error) {
	storeType := STORAGE_DRIVER_CEPH_COMMON
	if obj.Pool != "rabbit" {
		storeType = STORAGE_DRIVER_CEPH_STRIP
	}
	cephInfo := CephStoreInfo{
		Type:   storeType,
		Fsid:   obj.Location,
		Pool:   obj.Pool,
		Oid:    obj.ObjectId,
		Offset: int64(0),
		Size:   obj.Size,
	}

	cephInfoBuf, err := json.Marshal(cephInfo)
	if err != nil {
		return "", err
	}

	return string(cephInfoBuf), nil
}

func EncodeStoreInfo(obj *types.Object) (string, error) {
	storeType := STORAGE_DRIVER_CEPH_COMMON
	if obj.Pool != "rabbit" {
		storeType = STORAGE_DRIVER_CEPH_STRIP
	}
	cephInfo := CephStoreInfo{
		Type:   storeType,
		Fsid:   obj.Location,
		Pool:   obj.Pool,
		Oid:    obj.ObjectId,
		Offset: int64(0),
		Size:   obj.Size,
	}

	cephInfoBuf, err := json.Marshal(cephInfo)
	if err != nil {
		return "", err
	}

	imgStore := ImgStoreInfo{
		Type: storeType,
		Size: obj.Size,
		Info: string(cephInfoBuf),
	}
	buf, err := json.Marshal(imgStore)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

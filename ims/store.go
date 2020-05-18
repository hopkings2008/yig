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
	cephInfo := CephStoreInfo{
		Type:   STORAGE_DRIVER_CEPH_STRIP,
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
	cephInfo := CephStoreInfo{
		Type:   STORAGE_DRIVER_CEPH_STRIP,
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
		Type: STORAGE_DRIVER_CEPH_STRIP,
		Size: obj.Size,
		Info: string(cephInfoBuf),
	}
	buf, err := json.Marshal(imgStore)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

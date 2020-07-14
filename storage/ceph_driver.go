package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/woclouds3/radoshttpd/rados"
)

type CephStorageDriver struct {
	Fsid       string
	Conn       *rados.Conn
	InstanceId uint64
	Logger     log.Logger
	counter    uint64
	// old ceph storage interface.
	cephStripeDriver *CephNativeStripe
}

func (csd *CephStorageDriver) Write(ctx context.Context, pool string, objectId string, meta string, offset int64, data io.Reader) (int64, error) {
	isOldDriver, err := csd.isCephStripeDriver(ctx, meta)
	if err != nil {
		csd.Logger.Error(ctx, fmt.Sprintf("got invalid meta: %s for object(%s), err: %v", meta, objectId, err))
		return 0, err
	}
	if isOldDriver {
		//newly object
		if 0 == offset {
			n, err := csd.cephStripeDriver.Put(pool, objectId, data)
			if err != nil {
				csd.Logger.Error(ctx, fmt.Sprintf("failed to write object(%s/%s), err: %v", pool, objectId, err))
				return n, err
			}
			return n, nil
		}
		// append the object, note that below isExist param does nothing...
		n, err := csd.cephStripeDriver.Append(pool, objectId, data, uint64(offset), true)
		if err != nil {
			csd.Logger.Error(ctx, fmt.Sprintf("failed to write object(%s/%s), err: %v", pool, objectId, err))
			return n, err
		}
		return n, nil
	}
	return 0, nil
}

func (csd *CephStorageDriver) Read(ctx context.Context, pool string, objectId string, meta string, offset int64, length int64) (io.ReadCloser, error) {
	isOldDriver, err := csd.isCephStripeDriver(ctx, meta)
	if err != nil {
		csd.Logger.Error(ctx, fmt.Sprintf("got invalid meta: %s for object(%s), err: %v", meta, objectId, err))
		return nil, err
	}
	if isOldDriver {
		reader, err := csd.cephStripeDriver.getReader(pool, objectId, offset, length)
		if err != nil {
			csd.Logger.Error(ctx, fmt.Sprintf("failed to get reader for (%s/%s), err: %v", pool, objectId, err))
			return nil, err
		}
		return reader, nil
	}
	return nil, nil
}

func (csd *CephStorageDriver) Delete(ctx context.Context, pool string, objectId string, meta string, size int64) error {
	isOldDriver, err := csd.isCephStripeDriver(ctx, meta)
	if err != nil {
		csd.Logger.Error(ctx, fmt.Sprintf("got invalid meta: %s for object(%s), err: %v", meta, objectId, err))
		return err
	}

	if isOldDriver {
		err = csd.cephStripeDriver.Remove(pool, objectId)
		if err != nil {
			csd.Logger.Error(ctx, fmt.Sprintf("failed to remove(%s/%s), err: %v", pool, objectId, err))
			return err
		}
		return nil
	}
	return nil
}

func (csd *CephStorageDriver) isCephStripeDriver(ctx context.Context, meta string) (bool, error) {
	if meta == "" {
		return true, nil
	}
	oi := &types.ObjStoreInfo{}
	err := oi.Decode(meta)
	if err != nil {
		csd.Logger.Error(ctx, fmt.Sprintf("failed to decode %s, err: %v", meta, err))
		return false, err
	}
	if oi.Type == types.STORAGE_DRIVER_CEPH_STRIPE {
		return true, nil
	}
	return false, nil
}

func (csd *CephStorageDriver) GetUniqUploadName() string {
	oid_suffix := atomic.AddUint64(&csd.counter, 1)
	oid := fmt.Sprintf("%d:%d", csd.InstanceId, oid_suffix)
	return oid
}

func (csd *CephStorageDriver) GetUsedSpacePercent() (int, error) {
	stat, err := csd.Conn.GetClusterStats()
	if err != nil {
		return 0, errors.New("Stat error")
	}
	pct := int(stat.Kb_used * uint64(100) / stat.Kb)
	return pct, nil
}

func (csd *CephStorageDriver) GetName() string {
	return csd.Fsid
}

func NewCephStorageDriver(configFile string, logger log.Logger) *CephStorageDriver {
	logger.Info(nil, "Loading Ceph file", configFile)

	Rados, err := rados.NewConn("admin")
	Rados.SetConfigOption("rados_mon_op_timeout", MON_TIMEOUT)
	Rados.SetConfigOption("rados_osd_op_timeout", OSD_TIMEOUT)

	err = Rados.ReadConfigFile(configFile)
	if err != nil {
		logger.Error(nil, "Failed to open ceph.conf: %s", configFile)
		return nil
	}

	err = Rados.Connect()
	if err != nil {
		logger.Error(nil, "Failed to connect to remote cluster: %s", configFile)
		return nil
	}

	name, err := Rados.GetFSID()
	if err != nil {
		logger.Error(nil, "Failed to get FSID: %s", configFile)
		Rados.Shutdown()
		return nil
	}

	id := Rados.GetInstanceID()

	cluster := &CephStorageDriver{
		Fsid:       name,
		Conn:       Rados,
		InstanceId: id,
		Logger:     logger,
		cephStripeDriver: &CephNativeStripe{
			Conn:       Rados,
			Name:       name,
			InstanceId: id,
			Logger:     logger,
			BufPool: &sync.Pool{
				New: func() interface{} {
					return bytes.NewBuffer(make([]byte, BIG_FILE_THRESHOLD))
				},
			},
			BigBufPool: &sync.Pool{
				New: func() interface{} {
					return make([]byte, MAX_CHUNK_SIZE)
				},
			},
		},
	}
	logger.Info(nil, "Ceph Cluster", name, "is ready, InstanceId is", id)
	return cluster
}

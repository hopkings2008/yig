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
	"github.com/journeymidnight/yig/storage/stripe"
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
	// lock for stripeMgrs
	stripeMgrsLock sync.RWMutex
	// stripMgrs
	stripeMgrs map[string]*stripe.StripeMgr
}

/*
* WriteResult: records the result of each write operation to underlying storage.
 */
type WriteResult struct {
	// total bytes written to storage.
	N int64
	// error of write operation.
	Err error
}

func (csd *CephStorageDriver) Write(ctx context.Context, pool string, objectId string, meta string, offset int64, data io.Reader) (int64, error) {
	isOldDriver, oi, err := csd.isCephStripeDriver(ctx, meta)
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
	// use stripe to write the data.
	mgr, err := csd.GetStripeMgr(oi)
	if err != nil {
		csd.Logger.Error(ctx, fmt.Sprintf("failed to get stripe mgr(%v) for %s/%s, err: %v",
			*oi, pool, objectId))
		return 0, err
	}

	cephPool, err := csd.Conn.OpenPool(pool)
	if err != nil {
		csd.Logger.Error(ctx, fmt.Sprintf("failed to open pool(%s) for object(%s), err: %v",
			pool, objectId, err))
		return 0, err
	}

	// resultChan is closed by write goroutine.
	resultChan := make(chan WriteResult)
	// dataChan is closed by read goroutine.
	dataChan := make(chan []byte)
	defer func() {
		cephPool.Destroy()
	}()

	// write goroutine. it must close the resultChan when it exists.
	go func() {
		wr := WriteResult{}
		defer func() {
			resultChan <- wr
			close(resultChan)
		}()
		for data := range dataChan {
			dataSize := int64(len(data))
			dataOffset := int64(0)
			for dataSize > 0 {
				osi := mgr.GetObjectStoreInfo(offset, dataSize)
				oid := osi.GetObjectId(objectId)
				csd.Logger.Info(ctx, fmt.Sprintf("oid: %s, offset: %d", oid, offset))
				// write data to ceph pool.
				// note that osi.Length is the minimum size of dataSize and inner buffer.
				err := cephPool.Write(oid, data[dataOffset:dataOffset+osi.Length], uint64(osi.Offset))
				if err != nil {
					csd.Logger.Error(ctx, fmt.Sprintf("failed to write %s/%s with oid(%s) and offset %d, err: %v",
						pool, objectId, oid, offset, err))
					wr.Err = err
					// release the data buffer.
					mgr.PutBuf(data)
					return
				}
				// update position.
				dataSize -= osi.Length
				offset += osi.Length
				dataOffset += osi.Length
				wr.N += osi.Length
			}
			// relase the data buffer.
			mgr.PutBuf(data)
		}
	}()

	// start to read and write data
	// when break the for loop, must close the dataChan.
	for {
		// get the data buffer from the sync.Pool
		// Note, the data buffer is got in read goroutine and released in write goroutine.
		buf := mgr.GetBuf()
		readLen := 0
		totalRead := len(buf)
		// try to read the data with the size of len(buf)
		for readLen < totalRead {
			// check whether write goroutine returns error
			select {
			case wr := <-resultChan:
				err = wr.Err
				if err != nil {
					csd.Logger.Error(ctx, fmt.Sprintf("failed to write object(%s/%s), err: %v",
						pool, objectId, err))
					close(dataChan)
					return wr.N, err
				}
			default:
			}

			n := 0
			n, err = data.Read(buf[readLen:])
			if err != nil {
				//break, read finish.
				if err == io.EOF {
					readLen += n
					break
				}
				csd.Logger.Error(ctx, fmt.Sprintf("read for %s/%s failed with offset %d, size: %d, err: %v",
					pool, objectId, readLen, totalRead))
				break
			}
			readLen += n
		}
		if err == nil || err == io.EOF {
			// check write goroutine error and put the buf to write.
			csd.Logger.Info(ctx, fmt.Sprintf("read(%s/%s): %d", pool, objectId, readLen))
			select {
			case wr := <-resultChan:
				err = wr.Err
				if err != nil {
					csd.Logger.Error(ctx, fmt.Sprintf("failed to write object(%s/%s), err: %v",
						pool, objectId, err))
					close(dataChan)
					return wr.N, err
				}
			case dataChan <- buf[:readLen]:
			}

			if err == nil {
				continue
			}
			// err is io.EOF
			err = nil
			close(dataChan)
			break
		}

		// read error.
		csd.Logger.Error(ctx, fmt.Sprintf("error(%v) happens during read, return", err))
		close(dataChan)
		break
	}

	// get the write result.
	wr := <-resultChan
	if err == nil {
		err = wr.Err
	}
	csd.Logger.Info(ctx, fmt.Sprintf("finish to write (%s/%s), written: %d, err: %v",
		pool, objectId, wr.N, err))
	return wr.N, err
}

func (csd *CephStorageDriver) Read(ctx context.Context, pool string, objectId string, meta string, offset int64, length int64) (io.ReadCloser, error) {
	isOldDriver, oi, err := csd.isCephStripeDriver(ctx, meta)
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
	mgr, err := csd.GetStripeMgr(oi)
	if err != nil {
		csd.Logger.Error(ctx, fmt.Sprintf("failed to get stripe mgr for reading (%s/%s) with offset %d, err: %v",
			pool, objectId, offset, err))
		return nil, err
	}
	cephPool, err := csd.Conn.OpenPool(pool)
	if err != nil {
		csd.Logger.Error(ctx, fmt.Sprintf("failed to open pool(%s) for object(%s), err: %v",
			pool, objectId, err))
		return nil, err
	}
	sr := &StripeReader{
		Ctx:      ctx,
		Logger:   csd.Logger,
		CephPool: cephPool,
		Mgr:      mgr,
		ObjectId: objectId,
		Offset:   offset,
		Length:   length,
	}
	return sr, nil
}

func (csd *CephStorageDriver) Delete(ctx context.Context, pool string, objectId string, meta string, size int64) error {
	isOldDriver, oi, err := csd.isCephStripeDriver(ctx, meta)
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

	mgr, err := csd.GetStripeMgr(oi)
	if err != nil {
		csd.Logger.Error(ctx, fmt.Sprintf("failed to get stripe mgr for delete (%s/%s), err: %v",
			pool, objectId, err))
		return err
	}
	cephPool, err := csd.Conn.OpenPool(pool)
	if err != nil {
		csd.Logger.Error(ctx, fmt.Sprintf("failed to open pool(%s) for object(%s), err: %v",
			pool, objectId, err))
		return err
	}
	// get all the stripe oids in ceph for this object.
	oids := mgr.GetObjectIds(objectId, size)
	for _, oid := range oids {
		err = cephPool.Delete(oid)
		if err != nil {
			// fix me, must filter the non-existing error.
			ierr := int(err.(rados.RadosError))
			if ierr == -2 {
				err = nil
				continue
			}
			csd.Logger.Error(ctx, fmt.Sprintf("failed to delete(%s) for %s, err: %v", oid, objectId, err))
			return err
		}
	}
	return nil
}

func (csd *CephStorageDriver) isCephStripeDriver(ctx context.Context, meta string) (bool, *types.ObjStoreInfo, error) {
	if meta == "" {
		return true, nil, nil
	}
	oi := &types.ObjStoreInfo{}
	err := oi.Decode(meta)
	if err != nil {
		csd.Logger.Error(ctx, fmt.Sprintf("failed to decode %s, err: %v", meta, err))
		return false, nil, err
	}
	if oi.Type == types.STORAGE_DRIVER_CEPH_STRIPE {
		return true, oi, nil
	}
	return false, oi, nil
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

func (csd *CephStorageDriver) GetStripeMgr(oi *types.ObjStoreInfo) (*stripe.StripeMgr, error) {
	id := fmt.Sprintf("%d:%d:%d", oi.StripeObjectSize, oi.StripeUnit, oi.StripeNum)
	csd.stripeMgrsLock.RLock()
	mgr, ok := csd.stripeMgrs[id]
	csd.stripeMgrsLock.RUnlock()
	if ok {
		return mgr, nil
	}
	mgr, err := stripe.NewStripeMgr(oi.StripeObjectSize, oi.StripeUnit, oi.StripeNum)
	if err != nil {
		return nil, err
	}
	csd.stripeMgrsLock.Lock()
	// check exists again.
	mgr, ok = csd.stripeMgrs[id]
	if ok {
		csd.stripeMgrsLock.Unlock()
		return mgr, nil
	}
	mgr, err = stripe.NewStripeMgr(oi.StripeObjectSize, oi.StripeUnit, oi.StripeNum)
	if err != nil {
		csd.stripeMgrsLock.Unlock()
		return nil, err
	}
	csd.stripeMgrs[id] = mgr
	csd.stripeMgrsLock.Unlock()
	return mgr, nil
}

func NewCephStorageDriver(configFile string, logger log.Logger) (*CephStorageDriver, error) {
	logger.Info(nil, "Loading Ceph file", configFile)

	Rados, err := rados.NewConn("admin")
	Rados.SetConfigOption("rados_mon_op_timeout", MON_TIMEOUT)
	Rados.SetConfigOption("rados_osd_op_timeout", OSD_TIMEOUT)

	err = Rados.ReadConfigFile(configFile)
	if err != nil {
		logger.Error(nil, "Failed to open ceph.conf: %s", configFile)
		return nil, err
	}

	err = Rados.Connect()
	if err != nil {
		logger.Error(nil, "Failed to connect to remote cluster: %s", configFile)
		return nil, err
	}

	name, err := Rados.GetFSID()
	if err != nil {
		logger.Error(nil, "Failed to get FSID: %s", configFile)
		Rados.Shutdown()
		return nil, err
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
		stripeMgrs: make(map[string]*stripe.StripeMgr),
	}
	logger.Info(nil, "Ceph Cluster", name, "is ready, InstanceId is", id)
	return cluster, nil
}

/*
* StripeReader: read data from striped objects of underlying storage.
*
 */

type StripeReader struct {
	// Context
	Ctx context.Context
	// logger
	Logger log.Logger
	// opened ceph pool
	CephPool *rados.Pool
	// StripeMgr to get striped objects
	Mgr *stripe.StripeMgr
	// ObjectId for prefix
	ObjectId string
	// Offset to read
	Offset int64
	// Total length to read
	Length int64
}

func (sr *StripeReader) Read(p []byte) (int, error) {
	if sr.Length <= 0 {
		return 0, io.EOF
	}
	if sr.Offset >= sr.Length {
		return 0, io.EOF
	}

	bufLen := int64(len(p))
	bufOffset := 0
	for bufLen > 0 {
		osi := sr.Mgr.GetObjectStoreInfo(sr.Offset, sr.Length)
		oid := osi.GetObjectId(sr.ObjectId)
		toRead := osi.Length
		if toRead > bufLen {
			toRead = bufLen
		}
		n, err := sr.CephPool.Read(oid, p[bufOffset:int64(bufOffset)+toRead], uint64(osi.Offset))
		if err != nil {
			sr.Logger.Error(sr.Ctx, fmt.Sprintf("failed to read %s with offset %d, err: %v",
				oid, sr.Offset, err))
			return n, err
		}
		if int64(n) > toRead {
			errMsg := fmt.Sprintf("corrupt to read %s, toRead(%d), readed(%d)",
				oid, toRead, n)
			sr.Logger.Error(sr.Ctx, errMsg)
			err = errors.New(errMsg)
			return n, err
		}
		//input buffer offset to receive data.
		bufOffset += n
		// input buffer length to receive data.
		bufLen -= int64(n)
		// offset for the whole input.
		sr.Offset += int64(n)
		if sr.Offset >= sr.Length {
			break
		}
	}
	return bufOffset, nil
}

func (sr *StripeReader) Close() error {
	return nil
}

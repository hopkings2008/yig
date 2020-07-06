package stripe

import (
	"errors"
	"fmt"
	"sync"
)

const (
	//objectName_objectGroupId_objectId
	PATTERN_OBJ_ID = "%s_%d_%d"
)

type ObjectId struct {
	ObjGroupId int
	ObjectId   int
}

func (oi ObjectId) GetObjectId(prefix string) string {
	return fmt.Sprintf(PATTERN_OBJ_ID, prefix, oi.ObjGroupId, oi.ObjectId)
}

type ObjectStoreInfo struct {
	// each group contains stripe num objects.
	ObjGroupId int
	// object identifier
	ObjectId int
	// stripe id, the sequence of stripe in this object.
	StripeId int64
	// offset in this object.
	Offset int64
	// the available bytes can be used in this object.
	Length int64
}

func (osi ObjectStoreInfo) GetObjectId(prefix string) string {
	return fmt.Sprintf(PATTERN_OBJ_ID, prefix, osi.ObjGroupId, osi.ObjectId)
}

/*
* StipeMgr must be stateless and it will be used as global instance.
* Pls refer to the link https://docs.ceph.com/docs/master/dev/file-striping/ for the detail.
 */
type StripeMgr struct {
	// rados object size in bytes, object size must be multiple of strip unit.
	objSize int
	// stripe unit size in bytes
	stripeUnit int
	// number of stripe unit of each stripe
	stripeNum int
	// size of each stripe in bytes
	stripeSize int
	// number of stripes in each rados object.
	objStripeCount int

	// buffer pool which will allcate buffer in stripe unit size.
	bufPool *sync.Pool
}

func (sm *StripeMgr) GetObjectId(prefix string, offset int64, length int64) string {
	info := sm.GetObjectStoreInfo(offset, length)
	return info.GetObjectId(prefix)
}

func (sm *StripeMgr) GetObjectIds(prefix string, length int64) []string {
	var idList []string
	ids := sm.GetObjectIdList(length)
	for _, id := range ids {
		idList = append(idList, id.GetObjectId(prefix))
	}
	return idList
}

/*
* whenever read or write, offset should be the multiple of strip unit.
* Note: caller should compare and use ObjectStoreInfo.Length with length in parameter.
* the size of last object may not be full.
 */
func (sm *StripeMgr) GetObjectStoreInfo(offset int64, length int64) ObjectStoreInfo {
	// Note that stripeId and objectId starts from 0.
	// remain bytes moduled by stripeSize.
	remain := offset % int64(sm.stripeSize)
	// stripe id from offset
	stripeId := offset / int64(sm.stripeSize)

	// remain bytes in the stripe unit.
	// the size of remain bytes is smaller than the size of stripe unit.
	unitRemain := remain % int64(sm.stripeUnit)
	// object id in the stripe
	objectId := int(remain / int64(sm.stripeUnit))

	// object group id. sm.objStripeCount contains the number of stripes in each group.
	objectGroupId := int(stripeId / int64(sm.objStripeCount))
	// the stripe offset in the object.
	stripeRemainId := int(stripeId % int64(sm.objStripeCount))

	// offset in the specified object.
	objOffset := int64(stripeRemainId*sm.stripeUnit) + unitRemain
	// available bytes in this object stripe unit.
	objUnitRemain := int64(sm.stripeUnit) - unitRemain
	osi := ObjectStoreInfo{
		ObjGroupId: objectGroupId,
		ObjectId:   objectId,
		StripeId:   stripeId,
		Offset:     objOffset,
		Length:     objUnitRemain,
	}

	return osi
}

func (sm *StripeMgr) GetObjectIdList(length int64) []ObjectId {
	var objectIdList []ObjectId
	// the total bytes of each object group.
	groupSize := int64(sm.stripeSize * sm.objStripeCount)
	// total number of object groups.
	// note that groupNum starts from 0.
	groupNum := int(length / groupSize)
	// remain bytes in the last group.
	remain := length % groupSize
	// the group which id is smaller than groupNum contains the full objects.
	for i := 0; i < groupNum; i++ {
		for j := 0; j < sm.stripeNum; j++ {
			oi := ObjectId{
				ObjGroupId: i,
				ObjectId:   j,
			}
			objectIdList = append(objectIdList, oi)
		}
	}
	if remain <= 0 {
		return objectIdList
	}
	// all the objects which contain former remain bytes are in the same group whose id is equal to groupNum.
	stripNum := remain / int64(sm.stripeSize)
	// the remain size is larger than one stripe size.
	if stripNum > 0 {
		for i := 0; i < sm.stripeNum; i++ {
			oi := ObjectId{
				ObjGroupId: groupNum,
				ObjectId:   i,
			}
			objectIdList = append(objectIdList, oi)
		}
		return objectIdList
	}
	// the remain size is less than the one stripe size.
	unitNum := int(remain / int64(sm.stripeUnit))
	unitRemain := remain % int64(sm.stripeUnit)
	for i := 0; i < unitNum; i++ {
		oi := ObjectId{
			ObjGroupId: groupNum,
			ObjectId:   i,
		}
		objectIdList = append(objectIdList, oi)
	}
	if unitRemain > 0 {
		oi := ObjectId{
			ObjGroupId: groupNum,
			ObjectId:   unitNum,
		}
		objectIdList = append(objectIdList, oi)
	}

	return objectIdList
}

func (sm *StripeMgr) GetBuf() []byte {
	return sm.bufPool.Get().([]byte)
}

func (sm *StripeMgr) PutBuf(buf []byte) {
	sm.bufPool.Put(buf)
}

func NewStripeMgr(objSize int, stripeUnit int, stripeNum int) (*StripeMgr, error) {
	// object size must not be smaller than stripe unit size.
	if objSize < stripeUnit {
		return nil, errors.New(fmt.Sprintf("invalid parameter, stripeUnit(%d) > objSize(%d)",
			stripeUnit, objSize))
	}
	// object size must be the multiple of stripe unit size.
	if (objSize % stripeUnit) != 0 {
		return nil, errors.New(fmt.Sprintf("invalid parameter, objSize(%d) cannot be divided by stripeUnit(%d)",
			objSize, stripeUnit))
	}
	sm := &StripeMgr{
		objSize:        objSize,
		stripeUnit:     stripeUnit,
		stripeNum:      stripeNum,
		stripeSize:     stripeUnit * stripeNum,
		objStripeCount: objSize / stripeUnit,
		bufPool: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, stripeUnit)
				return buf
			},
		},
	}
	return sm, nil
}

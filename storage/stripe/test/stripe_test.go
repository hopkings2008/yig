package test

import (
	"fmt"

	"github.com/journeymidnight/yig/storage/stripe"
	. "gopkg.in/check.v1"
)

type stripeInfo struct {
	ObjectSize int
	StripeUnit int
	StripeNum  int
}

func (ss *StripeSuite) TestBasicFuncs(c *C) {
	// key: length of object.
	// value: array of stripe unit
	testLens := make(map[int64][]stripeInfo)

	// test 1M. only one object id.
	fileSize := int64(1<<20) + 1
	for i := 1; i <= 2048; i++ {
		unitSize := i << 10
		objSize := 2 << 20
		// testSize is larger than unitSize and can be divided by unitSize.
		if objSize > unitSize && (objSize%unitSize) == 0 {
			testLens[fileSize] = append(testLens[fileSize], stripeInfo{
				ObjectSize: objSize,
				StripeUnit: i << 10,
				StripeNum:  5,
			})
		}
	}
	fileSize = int64(32<<20) + 1
	for i := 1; i <= 8192; i++ {
		unitSize := i << 10
		objSize := 8 << 20
		// testSize is larger than unitSize and can be divided by unitSize.
		if objSize > unitSize && (objSize%unitSize) == 0 {
			testLens[fileSize] = append(testLens[fileSize], stripeInfo{
				ObjectSize: objSize,
				StripeUnit: i << 10,
				StripeNum:  5,
			})
		}
	}
	for k, v := range testLens {
		for _, si := range v {
			ss.verify(k, si, c)
		}
	}
}

func (ss *StripeSuite) verify(objectSize int64, si stripeInfo, c *C) {
	sm, err := stripe.NewStripeMgr(si.ObjectSize, si.StripeUnit, si.StripeNum)
	c.Assert(err, Equals, nil)
	c.Assert(sm, Not(Equals), nil)
	length := objectSize
	offset := int64(0)
	// key: objectGroupId_objectId
	// value: array of ObjectStoreInfo
	infos := make(map[string][]stripe.ObjectStoreInfo)
	for length > 0 {
		oi := sm.GetObjectStoreInfo(offset, length)
		key := fmt.Sprintf("%d_%d", oi.ObjGroupId, oi.ObjectId)
		//c.Logf("key: %s, offset: %d, length: %d\n", key, offset, length)
		infos[key] = append(infos[key], oi)
		if length > oi.Length {
			length -= oi.Length
			offset += oi.Length
		} else {
			length -= length
			offset += length
		}
	}

	// get object id list.
	ids := sm.GetObjectIdList(objectSize)
	c.Assert(len(ids), Not(Equals), 0)
	// check each id in ids can be found in infos.
	idsMap := make(map[string]int)
	for _, id := range ids {
		key := fmt.Sprintf("%d_%d", id.ObjGroupId, id.ObjectId)
		//c.Logf("key: %s", key)
		_, ok := infos[key]
		/*if !ok {
			c.Logf("key: %s, length: %d, si: %v", key, objectSize, si)
			c.Logf("infos: %v", infos)
		}*/
		c.Assert(ok, Equals, true)
		idsMap[key] = 1
	}
	// check all the object groups and object ids in infos are equal to ids.
	for k, v := range infos {
		_, ok := idsMap[k]
		if !ok {
			c.Logf("objectSize: %d, si: %v, key: %s", objectSize, si, k)
		}
		c.Assert(ok, Equals, true)
		for _, info := range v {
			key := fmt.Sprintf("%d_%d", info.ObjGroupId, info.ObjectId)
			_, ok = idsMap[key]
			c.Assert(ok, Equals, true)
		}
	}
}

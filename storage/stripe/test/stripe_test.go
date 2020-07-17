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

	// test 1M.
	fileSize := int64(1<<20) + 1
	for i := 1; i <= 2048; i++ {
		unitSize := i << 10
		objSize := 2 << 20
		// testSize is larger than unitSize and can be divided by unitSize.
		if objSize > unitSize && (objSize%unitSize) == 0 {
			for j := 1; j <= 16; j++ {
				testLens[fileSize] = append(testLens[fileSize], stripeInfo{
					ObjectSize: objSize,
					StripeUnit: i << 10,
					StripeNum:  5,
				})
			}
		}
	}
	// test 2M
	fileSize = int64(2<<20) + 1
	for i := 1; i <= 4096; i++ {
		unitSize := i << 10
		objSize := 4 << 20
		// testSize is larger than unitSize and can be divided by unitSize.
		if objSize > unitSize && (objSize%unitSize) == 0 {
			for j := 1; j <= 16; j++ {
				testLens[fileSize] = append(testLens[fileSize], stripeInfo{
					ObjectSize: objSize,
					StripeUnit: i << 10,
					StripeNum:  j,
				})
			}
		}
	}
	// test 2M with 1M object size
	fileSize = int64(2<<20) + 1
	for i := 1; i <= 1024; i++ {
		unitSize := i << 10
		objSize := 1 << 20
		// testSize is larger than unitSize and can be divided by unitSize.
		if objSize > unitSize && (objSize%unitSize) == 0 {
			for j := 1; j <= 16; j++ {
				testLens[fileSize] = append(testLens[fileSize], stripeInfo{
					ObjectSize: objSize,
					StripeUnit: i << 10,
					StripeNum:  j,
				})
			}
		}
	}
	// test 4M
	fileSize = int64(4<<20) + 1
	for i := 1; i <= 4096; i++ {
		unitSize := i << 10
		objSize := 4 << 20
		// testSize is larger than unitSize and can be divided by unitSize.
		if objSize > unitSize && (objSize%unitSize) == 0 {
			for j := 1; j <= 16; j++ {
				testLens[fileSize] = append(testLens[fileSize], stripeInfo{
					ObjectSize: objSize,
					StripeUnit: i << 10,
					StripeNum:  j,
				})
			}
		}
	}
	// test 32M.
	fileSize = int64(32<<20) + 1
	for i := 1; i <= 8192; i++ {
		unitSize := i << 10
		objSize := 8 << 20
		// testSize is larger than unitSize and can be divided by unitSize.
		if objSize > unitSize && (objSize%unitSize) == 0 {
			for j := 1; j <= 16; j++ {
				testLens[fileSize] = append(testLens[fileSize], stripeInfo{
					ObjectSize: objSize,
					StripeUnit: i << 10,
					StripeNum:  j,
				})
			}
		}
	}
	// test 128M
	fileSize = int64(128<<20) + 1
	for i := 1; i <= 8192; i++ {
		unitSize := i << 10
		objSize := 8 << 20
		// testSize is larger than unitSize and can be divided by unitSize.
		if objSize > unitSize && (objSize%unitSize) == 0 {
			for j := 1; j <= 16; j++ {
				testLens[fileSize] = append(testLens[fileSize], stripeInfo{
					ObjectSize: objSize,
					StripeUnit: i << 10,
					StripeNum:  j,
				})
			}
		}
	}
	// test 256M
	fileSize = int64(256<<20) + 1
	for i := 1; i <= 8192; i++ {
		unitSize := i << 10
		objSize := 8 << 20
		// testSize is larger than unitSize and can be divided by unitSize.
		if objSize > unitSize && (objSize%unitSize) == 0 {
			for j := 1; j <= 16; j++ {
				testLens[fileSize] = append(testLens[fileSize], stripeInfo{
					ObjectSize: objSize,
					StripeUnit: i << 10,
					StripeNum:  j,
				})
			}
		}
	}
	// test 512M
	fileSize = int64(512<<20) + 1
	for i := 1; i <= 8192; i++ {
		unitSize := i << 10
		objSize := 8 << 20
		// testSize is larger than unitSize and can be divided by unitSize.
		if objSize > unitSize && (objSize%unitSize) == 0 {
			for j := 1; j <= 16; j++ {
				testLens[fileSize] = append(testLens[fileSize], stripeInfo{
					ObjectSize: objSize,
					StripeUnit: i << 10,
					StripeNum:  j,
				})
			}
		}
	}
	for k, v := range testLens {
		for _, si := range v {
			ss.verify(k, si, c)
		}
	}
}

func (ss *StripeSuite) BenchmarkGetObjectStoreInfo128M(c *C) {
	sm, err := stripe.NewStripeMgr(8<<20, 4<<20, 5)
	c.Assert(err, Equals, nil)
	for i := 0; i < c.N; i++ {
		length := int64(128 << 20)
		offset := int64(0)
		for length > 0 {
			oi := sm.GetObjectStoreInfo(offset, length)
			length -= oi.Length
			offset += oi.Length
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
	// during below loop, all the ObjectStoreInfo in []stripe.ObjectStoreInfo are ordered ascendly.
	for length > 0 {
		oi := sm.GetObjectStoreInfo(offset, length)
		key := fmt.Sprintf("%d_%d", oi.ObjGroupId, oi.ObjectId)
		//c.Logf("key: %s, offset: %d, length: %d\n", key, offset, length)
		infos[key] = append(infos[key], oi)
		length -= oi.Length
		offset += oi.Length
	}
	//verify that offset should be equal to the objectSize.
	c.Assert(offset, Equals, objectSize)

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
	// verify that in each object in infos, the offset are not overlapped
	totalLength := int64(0)
	for k, v := range infos {
		offset = int64(0)
		for _, oi := range v {
			key := fmt.Sprintf("%d_%d", oi.ObjGroupId, oi.ObjectId)
			c.Assert(key, Equals, k)
			//verify that the offset is the previous offset + length.
			c.Assert(offset, Equals, oi.Offset)
			offset += oi.Length
			totalLength += oi.Length
		}
		//verify that last offset should be equal to or less than the si.ObjectSize.
		c.Assert(offset <= int64(si.ObjectSize), Equals, true)
	}
	c.Assert(totalLength, Equals, objectSize)
}

package test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/storage"
	. "gopkg.in/check.v1"
)

func (ss *StorageSuite) TestCephDriverBasicWrite(c *C) {
	cases := make(map[int]types.ObjStoreInfo)
	// set stripe information.
	osi := types.ObjStoreInfo{
		Type:             types.STORAGE_DRIVER_STRIPE,
		StripeObjectSize: 4 << 20,
		StripeUnit:       2 << 20,
		StripeNum:        5,
	}
	cases[64<<20+3] = osi
	cases[64<<20] = osi
	cases[4<<10] = osi
	cases[1<<10] = osi
	cases[1] = osi
	for dataLen, osi := range cases {
		ss.verify(osi, dataLen, c)
	}
}

func (ss *StorageSuite) verify(osi types.ObjStoreInfo, dataLen int, c *C) {
	ctx := context.Background()
	meta, err := osi.Encode()
	c.Assert(err, Equals, nil)
	body := RandBytes(dataLen)
	bodyReader := bytes.NewReader(body)
	rawMd5 := md5.Sum(body)
	bodyMd5 := hex.EncodeToString(rawMd5[:])
	// create ceph storage driver.
	driver, err := storage.NewCephStorageDriver("/etc/ceph/ceph.conf", helper.Logger)
	c.Assert(err, Equals, nil)
	n, err := driver.Write(ctx, "tiger", "test", meta, 0, bodyReader)
	c.Assert(err, Equals, nil)
	c.Assert(n, Equals, int64(dataLen))

	reader, err := driver.Read(ctx, "tiger", "test", meta, 0, int64(dataLen))
	c.Assert(err, Equals, nil)
	buf := make([]byte, dataLen)
	c.Assert(len(buf), Equals, dataLen)
	nr, err := reader.Read(buf)
	c.Assert(err, Equals, nil)
	c.Assert(nr, Equals, dataLen)
	readMd5 := md5.Sum(buf)
	readMd5Str := hex.EncodeToString(readMd5[:])
	c.Assert(readMd5Str == bodyMd5, Equals, true)
	err = driver.Delete(ctx, "tiger", "test", meta, int64(dataLen))
	c.Assert(err, Equals, nil)
}

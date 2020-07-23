package test

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/journeymidnight/yig/meta/types"
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
		//ss.verify(osi, dataLen, c)
		ss.verifyFromOffset(osi, int64(0), int64(dataLen), "TestCephDriverBasicWrite", c)
	}
}

func (ss *StorageSuite) Test5GBasic(c *C) {
	// change this to cpu_num * 1.5
	numGoroutine := 4
	len5G := int64(1 << 20)
	chs := make(chan TestElem)
	var results []<-chan TestResult
	//stripe object: 512K, 1M, 2M, 4M, 8M, 16M, 32M
	var elems []TestElem
	objSize := 512
	// stripe object size
	for i := 0; i < 6; i++ {
		// stripe unit size
		for j := 1; j <= objSize; j++ {
			unitSize := j << 10
			if ((objSize << 10) % unitSize) == 0 {
				// stripe number
				for k := 1; k <= 16; k++ {
					elem := TestElem{
						Osi: types.ObjStoreInfo{
							Type:             types.STORAGE_DRIVER_STRIPE,
							StripeObjectSize: objSize << 10,
							StripeUnit:       unitSize,
							StripeNum:        k,
						},
						Size: len5G,
					}
					elems = append(elems, elem)
				}
			}
		}
		objSize *= 2
	}

	// start test verify goroutines.
	for i := 0; i < numGoroutine; i++ {
		out := ss.verifyCh(chs, c)
		results = append(results, out)
	}

	// start test elems go routine.
	go func() {
		for _, elem := range elems {
			chs <- elem
		}
		close(chs)
	}()
	// wait for their finish.
	for _, resultCh := range results {
		for result := range resultCh {
			c.Logf("stripe(%v), size: %d test succeeds", result.Osi, result.Size)
		}
	}
}

type TestElem struct {
	Osi  types.ObjStoreInfo
	Size int64
}

type TestResult struct {
	Osi  types.ObjStoreInfo
	Size int64
	Err  error
}

func (ss *StorageSuite) verifyCh(ch <-chan TestElem, c *C) <-chan TestResult {
	out := make(chan TestResult)
	go func() {
		defer func() {
			close(out)
		}()
		for elem := range ch {
			obj := fmt.Sprintf("verifyCh_%d_%d_%d_%d", elem.Osi.StripeObjectSize, elem.Osi.StripeUnit, elem.Osi.StripeNum, elem.Size)
			ss.verifyFromOffset(elem.Osi, 0, elem.Size, obj, c)
			out <- TestResult{
				Osi:  elem.Osi,
				Size: elem.Size,
				Err:  nil,
			}
		}
	}()
	return out
}

func (ss *StorageSuite) verifyFromOffset(osi types.ObjStoreInfo, offset int64, size int64, objectName string, c *C) {
	// note all the write begins at 0.
	// first write to offset. the data which is between 0 and offset-1 is not verified.
	// only verify the data which starts at offset and with length size.
	ctx := context.Background()
	meta, err := osi.Encode()
	c.Assert(err, Equals, nil)
	randomReader := NewRandomReader(offset)
	hasher := md5.New()
	reader := io.TeeReader(randomReader, hasher)
	obj := fmt.Sprintf("%s_%d", objectName, offset)
	n, err := ss.driver.Write(ctx, ss.pool, obj, meta, 0, reader)
	c.Assert(err, Equals, nil)
	c.Assert(n, Equals, offset)
	randomReader = NewRandomReader(size)
	reader = io.TeeReader(randomReader, hasher)
	n, err = ss.driver.Write(ctx, ss.pool, obj, meta, offset, reader)
	c.Assert(err, Equals, nil)
	c.Assert(n, Equals, size)
	// get the body md5.
	md5Sum := hasher.Sum(nil)
	md5Str := hex.EncodeToString(md5Sum[:])
	// read and verify the md5sum.
	driverReader, err := ss.driver.Read(ctx, ss.pool, obj, meta, 0, offset+size)
	c.Assert(err, Equals, nil)
	defer driverReader.Close()
	totalSize := int64(0)
	hasher = md5.New()
	reader = io.TeeReader(driverReader, hasher)
	for {
		buf := make([]byte, 4<<20)
		n, err := reader.Read(buf)
		if err == nil || err == io.EOF {
			totalSize += int64(n)
		}
		if err == io.EOF {
			break
		}
	}
	c.Assert(totalSize, Equals, offset+size)
	md5Sum = hasher.Sum(nil)
	md5ReadStr := hex.EncodeToString(md5Sum[:])
	c.Assert(md5Str, Equals, md5ReadStr)
	err = ss.driver.Delete(ctx, ss.pool, obj, meta, totalSize)
	c.Assert(err, Equals, nil)
}

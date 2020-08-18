package test

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

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

func (ss *StorageSuite) TestBackCompatible(c *C) {
	ctx := context.Background()
	meta := ""
	objectName := "TestBackCompatible"
	size := int64(64<<20 + 1)
	randomReader := NewRandomReader(size)
	hasher := md5.New()
	reader := io.TeeReader(randomReader, hasher)
	obj := fmt.Sprintf("%s_%d", objectName, size)
	n, err := ss.driver.Write(ctx, ss.pool, obj, meta, 0, reader)
	c.Assert(err, Equals, nil)
	c.Assert(n, Equals, size)

	// get the body md5.
	md5Sum := hasher.Sum(nil)
	md5Str := hex.EncodeToString(md5Sum[:])
	// read and verify the md5sum.
	driverReader, err := ss.driver.Read(ctx, ss.pool, obj, meta, 0, size)
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
			if err == io.EOF {
				break
			}
		}
		c.Assert(err, Equals, nil)
	}
	c.Assert(totalSize, Equals, size)
	md5Sum = hasher.Sum(nil)
	md5ReadStr := hex.EncodeToString(md5Sum[:])
	c.Assert(md5ReadStr, Equals, md5Str)

	// test use the stripe to read, it should fail.
	osi := types.ObjStoreInfo{
		Type:             types.STORAGE_DRIVER_STRIPE,
		StripeObjectSize: 4 << 20,
		StripeUnit:       2 << 20,
		StripeNum:        5,
	}
	meta, err = osi.Encode()
	c.Assert(err, Equals, nil)

	driverReader2, err := ss.driver.Read(ctx, ss.pool, obj, meta, 0, size)
	c.Assert(err, Equals, nil)
	defer driverReader2.Close()
	buf := make([]byte, 4<<20)
	nn, err := driverReader2.Read(buf)
	c.Assert(err, Not(Equals), nil)
	c.Assert(nn, Equals, 0)

	err = ss.driver.Delete(ctx, ss.pool, obj, meta, totalSize)
	c.Assert(err, Equals, nil)
	c.Logf("succeed to check backward compatible with size %d", size)
}

func (ss *StorageSuite) Test5GBasic(c *C) {
	// change this to cpu_num * 1.5
	numGoroutine := 4
	len5G := int64(5 << 30)
	chs := make(chan TestElem)
	resultChan := make(chan TestResult)
	//stripe object: 512K, 1M, 2M, 4M, 8M, 16M, 32M
	var elems []TestElem
	objSize := 512
	// stripe object size
	for i := 0; i < 7; i++ {
		// stripe unit size from 4k to stripe object size.
		// each loop will double the former unit size.
		// 4k, 8k, 16k, 32k... upto stripe object size.
		unitSize := 4
		for unitSize <= objSize {
			// stripe number from 1 to 8
			for k := 1; k <= 8; k++ {
				elem := TestElem{
					Osi: types.ObjStoreInfo{
						Type:             types.STORAGE_DRIVER_STRIPE,
						StripeObjectSize: objSize << 10,
						StripeUnit:       unitSize << 10,
						StripeNum:        k,
					},
					Size: len5G,
				}
				elems = append(elems, elem)
			}
			unitSize *= 2
		}
		objSize *= 2
	}

	c.Logf("there are total %d test cases", len(elems))

	// start test verify goroutines.
	wg := &sync.WaitGroup{}
	for i := 0; i < numGoroutine; i++ {
		ss.verifyCh(chs, resultChan, wg, c)
	}

	// start test elems go routine.
	go func() {
		for _, elem := range elems {
			chs <- elem
		}
		close(chs)
		wg.Wait()
		close(resultChan)
	}()
	// wait for their finish.
	totalFin := 0
	for result := range resultChan {
		totalFin += 1
		if result.Err == nil {
			c.Logf("goroutine(%d): stripe(%v), size: %d test succeeds, finished %d cases, total %d cases",
				result.ThrId, result.Osi, result.Size, totalFin, len(elems))
		} else {
			c.Logf("goroutine(%d): stripe(%v), size: %d test failed with err: %v, finished %d cases, total %d cases",
				result.ThrId, result.Osi, result.Size, result.Err, totalFin, len(elems))
		}
	}
}

type TestElem struct {
	Osi  types.ObjStoreInfo
	Size int64
}

type TestResult struct {
	ThrId int64
	Osi   types.ObjStoreInfo
	Size  int64
	Err   error
}

func (ss *StorageSuite) verifyCh(ch <-chan TestElem, out chan<- TestResult, wg *sync.WaitGroup, c *C) {
	go func() {
		wg.Add(1)
		defer func() {
			wg.Done()
		}()
		id := atomic.AddInt64(&ss.thrId, 1)
		for elem := range ch {
			obj := fmt.Sprintf("verifyCh_%d_%d_%d_%d", elem.Osi.StripeObjectSize, elem.Osi.StripeUnit, elem.Osi.StripeNum, elem.Size)
			err := ss.check(elem.Osi, 0, elem.Size, obj, c)
			out <- TestResult{
				ThrId: id,
				Osi:   elem.Osi,
				Size:  elem.Size,
				Err:   err,
			}
		}
	}()
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
	if err != nil {
		c.Logf("ObjStoreInfo(%v) failed to write(%d, %d), err: %v", osi, 0, offset, err)
	}
	c.Assert(err, Equals, nil)
	if n != offset {
		c.Logf("ObjStoreInfo(%v) failed to write(%d, %d), written: %d != %d", osi, 0, offset, n, offset)
	}
	c.Assert(n, Equals, offset)
	randomReader = NewRandomReader(size)
	reader = io.TeeReader(randomReader, hasher)
	n, err = ss.driver.Write(ctx, ss.pool, obj, meta, offset, reader)
	if err != nil {
		c.Logf("ObjStoreInfo(%v) failed to write(%d, %d), err: %v", osi, offset, size, err)
	}
	c.Assert(err, Equals, nil)
	if n != size {
		c.Logf("ObjStoreInfo(%v) failed to write(%d, %d), written: %d != %d", osi, offset, size, n, size)
	}
	c.Assert(n, Equals, size)
	// get the body md5.
	md5Sum := hasher.Sum(nil)
	md5Str := hex.EncodeToString(md5Sum[:])
	// read and verify the md5sum.
	driverReader, err := ss.driver.Read(ctx, ss.pool, obj, meta, 0, offset+size)
	if err != nil {
		c.Logf("ObjStoreInfo(%v) failed to get reader(%d, %d), err: %v", osi, 0, offset+size, err)
	}
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
			if err == io.EOF {
				break
			}
		}
		if err != nil {
			c.Logf("ObjStoreInfo(%v) failed to read data(%d, %d), current totalSize(%d), err: %v",
				osi, 0, offset+size, totalSize, err)
			break
		}
	}
	if totalSize != offset+size {
		c.Logf("ObjStoreInfo(%v) failed to read(%d, %d), readed %d != %d", osi, 0, offset+size, totalSize, offset+size)
	}
	c.Assert(totalSize, Equals, offset+size)
	md5Sum = hasher.Sum(nil)
	md5ReadStr := hex.EncodeToString(md5Sum[:])
	if md5Str != md5ReadStr {
		c.Logf("ObjStoreInfo(%v) failed to read(%d, %d), readed md5 %s != %s",
			osi, 0, offset+size, md5ReadStr, md5Str)
	}
	c.Assert(md5Str, Equals, md5ReadStr)
	err = ss.driver.Delete(ctx, ss.pool, obj, meta, totalSize)
	if err != nil {
		c.Logf("ObjStoreInfo(%v) failed to delete(%d, %d), err: %v", osi, 0, offset+size, err)
	}
	c.Assert(err, Equals, nil)
	c.Logf("succeed to check ObjStoreInfo(%v) with offset %d, size %d", osi, offset, size)
}

func (ss *StorageSuite) check(osi types.ObjStoreInfo, offset int64, size int64, objectName string, c *C) error {
	// note all the write begins at 0.
	// first write to offset. the data which is between 0 and offset-1 is not verified.
	// only verify the data which starts at offset and with length size.
	ctx := context.Background()
	meta, err := osi.Encode()
	if err != nil {
		c.Logf("failed to encode osi %v, err: %v", osi, err)
		return err
	}
	randomReader := NewRandomReader(offset)
	hasher := md5.New()
	reader := io.TeeReader(randomReader, hasher)
	obj := fmt.Sprintf("%s_%d", objectName, offset)
	n, err := ss.driver.Write(ctx, ss.pool, obj, meta, 0, reader)
	if err != nil {
		c.Logf("ObjStoreInfo(%v) failed to write(%d, %d), err: %v", osi, 0, offset, err)
		return err
	}
	if n != offset {
		c.Logf("ObjStoreInfo(%v) failed to write(%d, %d), written: %d != %d", osi, 0, offset, n, offset)
		return errors.New("less written")
	}
	randomReader = NewRandomReader(size)
	reader = io.TeeReader(randomReader, hasher)
	n, err = ss.driver.Write(ctx, ss.pool, obj, meta, offset, reader)
	if err != nil {
		c.Logf("ObjStoreInfo(%v) failed to write(%d, %d), err: %v", osi, offset, size, err)
		return err
	}
	if n != size {
		c.Logf("ObjStoreInfo(%v) failed to write(%d, %d), written: %d != %d", osi, offset, size, n, size)
		return errors.New("less written")
	}
	// get the body md5.
	md5Sum := hasher.Sum(nil)
	md5Str := hex.EncodeToString(md5Sum[:])
	// read and verify the md5sum.
	driverReader, err := ss.driver.Read(ctx, ss.pool, obj, meta, 0, offset+size)
	if err != nil {
		c.Logf("ObjStoreInfo(%v) failed to get reader(%d, %d), err: %v", osi, 0, offset+size, err)
		return err
	}
	defer driverReader.Close()
	totalSize := int64(0)
	hasher = md5.New()
	reader = io.TeeReader(driverReader, hasher)
	for {
		buf := make([]byte, 4<<20)
		n, err := reader.Read(buf)
		if err == nil || err == io.EOF {
			totalSize += int64(n)
			if err == io.EOF {
				break
			}
		}
		if err != nil {
			c.Logf("ObjStoreInfo(%v) failed to read data(%d, %d), current totalSize(%d), err: %v",
				osi, 0, offset+size, totalSize, err)
			return err
		}
	}
	if totalSize != offset+size {
		c.Logf("ObjStoreInfo(%v) failed to read(%d, %d), readed %d != %d", osi, 0, offset+size, totalSize, offset+size)
		return errors.New("less read")
	}
	md5Sum = hasher.Sum(nil)
	md5ReadStr := hex.EncodeToString(md5Sum[:])
	if md5Str != md5ReadStr {
		c.Logf("ObjStoreInfo(%v) failed to read(%d, %d), readed md5 %s != %s",
			osi, 0, offset+size, md5ReadStr, md5Str)
		return errors.New("md5sum check failed.")
	}
	err = ss.driver.Delete(ctx, ss.pool, obj, meta, totalSize)
	if err != nil {
		c.Logf("ObjStoreInfo(%v) failed to delete(%d, %d), err: %v", osi, 0, offset+size, err)
		return err
	}
	c.Logf("succeed to check ObjStoreInfo(%v) with offset %d, size %d", osi, offset, size)
	return nil
}

func (ss *StorageSuite) Benchmark256MCephStorageDriverWrite(c *C) {
	osi := types.ObjStoreInfo{
		Type:             types.STORAGE_DRIVER_STRIPE,
		StripeObjectSize: 64 << 20,
		StripeUnit:       1 << 20,
		StripeNum:        5,
	}
	ctx := context.Background()
	meta, err := osi.Encode()
	c.Assert(err, Equals, nil)
	totalLen := int64(256 << 20)
	var objs []string
	for i := 0; i < c.N; i++ {
		randomReader := NewRandomReader(totalLen)
		obj := fmt.Sprintf("%s_%d", "Benchmark256MCephStorageDriverWrite", i)
		objs = append(objs, obj)
		n, err := ss.driver.Write(ctx, ss.pool, obj, meta, 0, randomReader)
		c.Assert(err, Equals, nil)
		c.Assert(n, Equals, totalLen)
	}
	// remove all the test objects.
	for _, obj := range objs {
		err = ss.driver.Delete(ctx, ss.pool, obj, meta, totalLen)
		c.Assert(err, Equals, nil)
	}
}

func (ss *StorageSuite) Benchmark256MLibradosstripeDriverWrite(c *C) {
	ctx := context.Background()
	meta := ""
	totalLen := int64(256 << 20)
	var objs []string
	for i := 0; i < c.N; i++ {
		randomReader := NewRandomReader(totalLen)
		obj := fmt.Sprintf("%s_%d", "Benchmark256MLibradosstripeDriverWrite", i)
		objs = append(objs, obj)
		n, err := ss.driver.Write(ctx, ss.pool, obj, meta, 0, randomReader)
		c.Assert(err, Equals, nil)
		c.Assert(n, Equals, totalLen)
	}
	// remove all the test objects.
	for _, obj := range objs {
		err := ss.driver.Delete(ctx, ss.pool, obj, meta, totalLen)
		c.Assert(err, Equals, nil)
	}
}

func (ss *StorageSuite) Benchmark128KCephStorageDriverWrite(c *C) {
	osi := types.ObjStoreInfo{
		Type:             types.STORAGE_DRIVER_STRIPE,
		StripeObjectSize: 64 << 20,
		StripeUnit:       1 << 20,
		StripeNum:        5,
	}
	ctx := context.Background()
	meta, err := osi.Encode()
	c.Assert(err, Equals, nil)
	totalLen := int64(128 << 10)
	var objs []string
	for i := 0; i < c.N; i++ {
		randomReader := NewRandomReader(totalLen)
		obj := fmt.Sprintf("%s_%d", "Benchmark128KCephStorageDriverWrite", i)
		objs = append(objs, obj)
		n, err := ss.driver.Write(ctx, ss.pool, obj, meta, 0, randomReader)
		c.Assert(err, Equals, nil)
		c.Assert(n, Equals, totalLen)
	}
	// remove all the test objects.
	for _, obj := range objs {
		err = ss.driver.Delete(ctx, ss.pool, obj, meta, totalLen)
		c.Assert(err, Equals, nil)
	}
}

func (ss *StorageSuite) Benchmark128KLibradosstripeDriverWrite(c *C) {
	ctx := context.Background()
	meta := ""
	totalLen := int64(128 << 10)
	var objs []string
	for i := 0; i < c.N; i++ {
		randomReader := NewRandomReader(totalLen)
		obj := fmt.Sprintf("%s_%d", "Benchmark128KLibradosstripeDriverWrite", i)
		objs = append(objs, obj)
		n, err := ss.driver.Write(ctx, ss.pool, obj, meta, 0, randomReader)
		c.Assert(err, Equals, nil)
		c.Assert(n, Equals, totalLen)
	}
	// remove all the test objects.
	for _, obj := range objs {
		err := ss.driver.Delete(ctx, ss.pool, obj, meta, totalLen)
		c.Assert(err, Equals, nil)
	}
}

func (ss *StorageSuite) Benchmark256MCephStorageDriverRead(c *C) {
	osi := types.ObjStoreInfo{
		Type:             types.STORAGE_DRIVER_STRIPE,
		StripeObjectSize: 64 << 20,
		StripeUnit:       1 << 20,
		StripeNum:        5,
	}
	ctx := context.Background()
	meta, err := osi.Encode()
	c.Assert(err, Equals, nil)
	totalLen := int64(256 << 20)
	randomReader := NewRandomReader(totalLen)
	obj := fmt.Sprintf("%s", "Benchmark256MCephStorageDriverRead")
	n, err := ss.driver.Write(ctx, ss.pool, obj, meta, 0, randomReader)
	c.Assert(err, Equals, nil)
	c.Assert(n, Equals, totalLen)
	for i := 0; i < c.N; i++ {
		reader, err := ss.driver.Read(ctx, ss.pool, obj, meta, 0, totalLen)
		c.Assert(err, Equals, nil)
		totalRead := int64(0)
		for totalRead < totalLen {
			buf := make([]byte, 1<<20)
			n, err := reader.Read(buf)
			if err == nil {
				totalRead += int64(n)
				continue
			}
			if err == io.EOF {
				totalRead += int64(n)
				break
			}
			//error
			c.Assert(err, Not(Equals), nil)
		}
		c.Assert(totalRead, Equals, totalLen)
	}
	// remove all the test objects.
	err = ss.driver.Delete(ctx, ss.pool, obj, meta, totalLen)
	c.Assert(err, Equals, nil)
}

func (ss *StorageSuite) Benchmark256MLibradosstripeDriverRead(c *C) {
	ctx := context.Background()
	meta := ""
	totalLen := int64(256 << 20)
	randomReader := NewRandomReader(totalLen)
	obj := fmt.Sprintf("%s", "Benchmark256MLibradosstripeDriverRead")
	n, err := ss.driver.Write(ctx, ss.pool, obj, meta, 0, randomReader)
	c.Assert(err, Equals, nil)
	c.Assert(n, Equals, totalLen)
	for i := 0; i < c.N; i++ {
		reader, err := ss.driver.Read(ctx, ss.pool, obj, meta, 0, totalLen)
		c.Assert(err, Equals, nil)
		totalRead := int64(0)
		for totalRead < totalLen {
			buf := make([]byte, 1<<20)
			n, err := reader.Read(buf)
			if err == nil {
				totalRead += int64(n)
				continue
			}
			if err == io.EOF {
				totalRead += int64(n)
				break
			}
			//error
			c.Assert(err, Not(Equals), nil)
		}
		c.Assert(totalRead, Equals, totalLen)
	}
	// remove all the test objects.
	err = ss.driver.Delete(ctx, ss.pool, obj, meta, totalLen)
	c.Assert(err, Equals, nil)
}

func (ss *StorageSuite) Benchmark128KCephStorageDriverRead(c *C) {
	osi := types.ObjStoreInfo{
		Type:             types.STORAGE_DRIVER_STRIPE,
		StripeObjectSize: 64 << 20,
		StripeUnit:       1 << 20,
		StripeNum:        5,
	}
	ctx := context.Background()
	meta, err := osi.Encode()
	c.Assert(err, Equals, nil)
	totalLen := int64(128 << 10)
	randomReader := NewRandomReader(totalLen)
	obj := fmt.Sprintf("%s", "Benchmark256MCephStorageDriverRead")
	n, err := ss.driver.Write(ctx, ss.pool, obj, meta, 0, randomReader)
	c.Assert(err, Equals, nil)
	c.Assert(n, Equals, totalLen)
	for i := 0; i < c.N; i++ {
		reader, err := ss.driver.Read(ctx, ss.pool, obj, meta, 0, totalLen)
		c.Assert(err, Equals, nil)
		totalRead := int64(0)
		for totalRead < totalLen {
			buf := make([]byte, 1<<20)
			n, err := reader.Read(buf)
			if err == nil {
				totalRead += int64(n)
				continue
			}
			if err == io.EOF {
				totalRead += int64(n)
				break
			}
			//error
			c.Assert(err, Not(Equals), nil)
		}
		c.Assert(totalRead, Equals, totalLen)
	}
	// remove all the test objects.
	err = ss.driver.Delete(ctx, ss.pool, obj, meta, totalLen)
	c.Assert(err, Equals, nil)
}

func (ss *StorageSuite) Benchmark128KLibradosstripeDriverRead(c *C) {
	ctx := context.Background()
	meta := ""
	totalLen := int64(128 << 10)
	randomReader := NewRandomReader(totalLen)
	obj := fmt.Sprintf("%s", "Benchmark256MLibradosstripeDriverRead")
	n, err := ss.driver.Write(ctx, ss.pool, obj, meta, 0, randomReader)
	c.Assert(err, Equals, nil)
	c.Assert(n, Equals, totalLen)
	for i := 0; i < c.N; i++ {
		reader, err := ss.driver.Read(ctx, ss.pool, obj, meta, 0, totalLen)
		c.Assert(err, Equals, nil)
		totalRead := int64(0)
		for totalRead < totalLen {
			buf := make([]byte, 1<<20)
			n, err := reader.Read(buf)
			if err == nil {
				totalRead += int64(n)
				continue
			}
			if err == io.EOF {
				totalRead += int64(n)
				break
			}
			//error
			c.Assert(err, Not(Equals), nil)
		}
		c.Assert(totalRead, Equals, totalLen)
	}
	// remove all the test objects.
	err = ss.driver.Delete(ctx, ss.pool, obj, meta, totalLen)
	c.Assert(err, Equals, nil)
}

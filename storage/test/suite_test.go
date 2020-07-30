package test

import (
	"testing"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/storage"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&StorageSuite{})

type StorageSuite struct {
	chunkSize int
	pool      string
	driver    *storage.CephStorageDriver
	thrId     int64
}

func (ss *StorageSuite) SetUpSuite(c *C) {
	logLevel := log.ParseLevel(helper.CONFIG.LogLevel)
	helper.Logger = log.NewFileLogger("/var/log/yig/ceph_test.log", logLevel)
	// the test chunk is 256MB
	ss.chunkSize = 256 << 20
	ss.pool = "stripe"
	var err error
	ss.driver, err = storage.NewCephStorageDriver("/etc/ceph/ceph.conf", helper.Logger)
	c.Assert(err, Equals, nil)
}

func (ss *StorageSuite) TearDownSuite(c *C) {
	helper.Logger.Close()
	ss.driver.Close()
}

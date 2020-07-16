package test

import (
	"testing"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&StorageSuite{})

type StorageSuite struct {
}

func (ss *StorageSuite) SetUpSuite(c *C) {
	logLevel := log.ParseLevel(helper.CONFIG.LogLevel)
	helper.Logger = log.NewFileLogger("/var/log/yig/ceph_test.log", logLevel)
}

func (ss *StorageSuite) TearDownSuite(c *C) {
	helper.Logger.Close()
}

package storage

import (
	"github.com/journeymidnight/yig/log"
	"github.com/woclouds3/radoshttpd/rados"
)

type CephStorageDriver struct {
	Fsid       string
	Conn       *rados.Conn
	InstanceId uint64
	Logger     log.Logger
}

func NewCephStorageDriver(configFile string, logger log.Logger) *CephStorageDriver {
	return nil
}

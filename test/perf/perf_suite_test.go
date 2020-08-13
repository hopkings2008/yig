package perf

import (
	"testing"

	"github.com/journeymidnight/yig/log"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type PerfSuite struct {
}

var _ = Suite(&PerfSuite{})

var logger log.Logger

func (ps *PerfSuite) SetUpSuite(c *C) {
}

func (ps *PerfSuite) TearDownSuite(c *C) {
}

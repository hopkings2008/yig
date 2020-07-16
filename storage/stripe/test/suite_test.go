package test

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&StripeSuite{})

type StripeSuite struct {
}

func (ss *StripeSuite) SetUpSuite(c *C) {
}

func (ss *StripeSuite) TearDownSuite(c *C) {
}

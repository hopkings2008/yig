package meta

import (
	"database/sql"
	"errors"

	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func (m *Meta) SetRegion(region string) error {

	c, err := m.Client.GetConfig()
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	if err == sql.ErrNoRows {
		var newConfig Configure
		newConfig.Region = region
		err = m.Client.SetConfig(&newConfig)
		if err != nil {
			return err
		}
		return nil
	}
	c.Region = region
	err = m.Client.UpdataConfig(&c)
	if err != nil {
		return err
	}
	return nil
}

func (m *Meta) GetRegion() (region string, err error) {
	c, err := m.Client.GetConfig()
	if err != nil && err != sql.ErrNoRows {
		return
	}

	if err == sql.ErrNoRows {
		if helper.CONFIG.Region == "" {
			err = errors.New("Invalid region")
			return
		}
		region = helper.CONFIG.Region
		return
	}

	region = c.Region
	return
}

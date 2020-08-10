package tidbclient

import (
	"encoding/json"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) SetConfig(c *Configure) error {
	configure, _ := json.Marshal(c)
	sql := "insert into configure(instanceid,config) values(?,?);"
	args := []interface{}{helper.CONFIG.InstanceId, configure}
	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) UpdataConfig(c *Configure) error {
	configure, _ := json.Marshal(c)
	sql := "update configure set instanceid=?,config=?;"
	args := []interface{}{helper.CONFIG.InstanceId, configure}
	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) GetConfig() (c Configure, err error) {
	sql := fmt.Sprintf("select instanceid,config from configure where instanceid='%s';",
		helper.CONFIG.InstanceId)
	row := t.Client.QueryRow(sql)

	var instanceid, tmp string
	err = row.Scan(&instanceid, &tmp)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(tmp), &c)
	if err != nil {
		return
	}
	return
}

package types

import (
	"time"
)

const (
	LcEnding  = "Ending"
	LcPending = "Pending"
)

type LifeCycle struct {
	BucketName      string
	Status          string // status of this entry, in Pending/Deleting
	LastScannedTime time.Time
}

type ScanLifeCycleResult struct {
	Truncated  bool
	NextMarker string
	// List of LifeCycles info for this request.
	Lcs []LifeCycle
}

func (lc LifeCycle) GetValues() (values map[string]map[string][]byte, err error) {
	values = map[string]map[string][]byte{
		LIFE_CYCLE_COLUMN_FAMILY: map[string][]byte{
			"status": []byte(lc.Status),
		},
	}
	return
}

func (lc LifeCycle) GetRowkey() (string, error) {
	return lc.BucketName, nil
}

func (lc LifeCycle) GetValuesForDelete() map[string]map[string][]byte {
	return map[string]map[string][]byte{
		LIFE_CYCLE_COLUMN_FAMILY: map[string][]byte{},
	}
}

func (lc *LifeCycle) GetCreateLifeCycle() (string, []interface{}) {
	sql := "insert into lifecycle(bucketname,status,lastscannedtime) values (?,?,?);"
	args := []interface{}{lc.BucketName, lc.Status, lc.LastScannedTime.Unix()}
	return sql, args
}

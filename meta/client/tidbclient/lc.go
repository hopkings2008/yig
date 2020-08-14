package tidbclient

import (
	"context"
	"database/sql"
	"time"

	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) PutBucketToLifeCycle(ctx context.Context, bucket *Bucket) error {
	tx, err := t.Client.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	// Update table bucket.
	bucketLifecycleSql, bucketArgs, err := bucket.GetUpdateLifeCycleSql()
	if err != nil {
		helper.Logger.Error(ctx, err)
		return err
	}
	if _, err = tx.Exec(bucketLifecycleSql, bucketArgs...); err != nil {
		helper.Logger.Error(ctx, err)
		return err
	}

	// Insert into table lifecycle if not exist.
	if t.IsBucketExistInLifecycle(ctx, bucket.Name, tx) {
		helper.Logger.Info(ctx, "bucket already in lifecycle.", bucket.Name)
		return nil
	}

	lifeCycle := &LifeCycle{
		BucketName:      bucket.Name,
		Status:          LcPending,
		LastScannedTime: time.Now().UTC(),
	}
	sqltext, args := lifeCycle.GetCreateLifeCycle()
	_, err = tx.Exec(sqltext, args...)
	if err != nil {
		helper.Logger.Error(ctx, "Failed to execute:", sqltext, args, "err:", err)
		return err
	}
	return nil
}

func (t *TidbClient) RemoveBucketFromLifeCycle(ctx context.Context, bucket *Bucket) error {
	tx, err := t.Client.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			tx.Rollback()
		}
	}()

	// Update table bucket.
	bucketLifecycleSql, bucketArgs, err := bucket.GetUpdateLifeCycleSql()
	if err != nil {
		helper.Logger.Error(ctx, err)
		return err
	}
	if _, err = tx.Exec(bucketLifecycleSql, bucketArgs...); err != nil {
		helper.Logger.Error(ctx, err)
		return err
	}

	sqltext := "delete from lifecycle where bucketname=?;"
	if _, err = tx.Exec(sqltext, bucket.Name); err != nil {
		helper.Logger.Error(ctx, "Failed to execute:", sqltext, "err:", err)
		return nil
	}
	return nil
}

func (t *TidbClient) IsBucketExistInLifecycle(ctx context.Context, bucketName string, tx interface{}) bool {
	sqltext := "select 1 from lifecycle where bucketname=?;"
	var row *sql.Row
	if tx != nil {
		sqlTx, _ := tx.(*sql.Tx)
		row = sqlTx.QueryRow(sqltext, bucketName)
	} else {
		row = t.Client.QueryRow(sqltext, bucketName)
	}

	var v int

	err := row.Scan(&v)
	if err == sql.ErrNoRows {
		return false
	} else if err != nil {
		helper.Logger.Error(ctx, err)
		return false
	}

	return true
}

func getLifecycleScanInterval() time.Duration {
	if helper.CONFIG.LcDebug {
		return time.Duration(15) * time.Second
	} else {
		// Scan a bucket every more than 12 hours. TODO: 12 out of air.
		return time.Duration(12) * time.Hour
	}
}

// Scan from table lifecycle beginning, and update lastscannedtime as current time.
func (t *TidbClient) ScanLifeCycle(ctx context.Context, limit int, marker string) (result ScanLifeCycleResult, err error) {
	result.Truncated = false
	sqltext := "select bucketname,status,lastscannedtime from lifecycle where bucketname > ? and lastscannedtime < ? order by bucketname limit ?;"
	args := []interface{}{marker, time.Now().UTC().Add(0 - getLifecycleScanInterval()).Unix(), limit * 3}
	rows, err := t.Client.Query(sqltext, args...)
	if err == sql.ErrNoRows {
		helper.Logger.Info(ctx, "sql.ErrNoRows:", sqltext)
		err = nil
		return
	} else if err != nil {
		return
	}
	defer rows.Close()

	result.Lcs = make([]LifeCycle, 0, limit)
	var lc LifeCycle
	loopCount := 0
	lastBucketName := ""

	for rows.Next() && len(result.Lcs) < limit {
		loopCount++

		var lastScannedTime int64

		err = rows.Scan(
			&lc.BucketName,
			&lc.Status,
			&lastScannedTime)
		if err != nil {
			helper.Logger.Error(ctx, "Failed in ScanLifeCycle:", result.Lcs, result.NextMarker)
			return
		}
		if lc.BucketName == lastBucketName {
			continue
		}

		// update lastscannedtime to now.
		updateScannedTimeSql := "update lifecycle set lastscannedtime=? where bucketname=? and lastscannedtime=? limit 1"
		updateArgs := []interface{}{time.Now().UTC().Unix(), lc.BucketName, lastScannedTime}
		var updateResult sql.Result
		updateResult, err = t.Client.Exec(updateScannedTimeSql, updateArgs...)
		if err != nil {
			helper.Logger.Error(ctx, "Update Scanned Time err:", lc.BucketName, err)
			return
		}

		// There should be only one row affected.
		// If more than one row, there must be something wrong.
		// If 0 row, this bucket must be handled by another go routine (no success guarantee though).
		var affectedRows int64
		affectedRows, err = updateResult.RowsAffected()
		if err != nil {
			helper.Logger.Error(ctx, "ScanLifeCycle: err", err, ", bucket:", lc.BucketName)
			return
		}
		if affectedRows == 0 {
			helper.Logger.Warn(ctx, "bucket", lc.BucketName, "already scanned by other go routine, result:", updateResult)
			continue
		}
		if affectedRows != 1 {
			helper.Logger.Error(ctx, "ScanLifeCycle update failed bucket:",
				lc.BucketName, lastScannedTime, "result:", updateResult, "affectedRows:", affectedRows)
			err = ErrInternalError
			return
		}

		result.Lcs = append(result.Lcs, lc)
		lastBucketName = lc.BucketName
	}

	result.NextMarker = lc.BucketName
	if loopCount >= limit {
		result.Truncated = true
	}

	return result, nil
}

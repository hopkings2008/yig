package tidbclient

import (
	"context"
	"database/sql"

	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) PutBucketToLifeCycle(ctx context.Context, lifeCycle LifeCycle) error {
	sqltext := "select 1 from lifecycle where bucketname=?"
	var result int
	err := t.Client.QueryRow(sqltext, lifeCycle.BucketName).Scan(&result)
	if err == nil {
		helper.Logger.Info(ctx, "bucket already in table lifecycle, ", lifeCycle.BucketName)
		return nil
	}
	if err != sql.ErrNoRows {
		helper.Logger.Error(ctx, err)
		return err
	}
	sqltext = "insert into lifecycle(bucketname,status) values (?,?);"
	_, err = t.Client.Exec(sqltext, lifeCycle.BucketName, lifeCycle.Status)
	if err != nil {
		helper.Logger.Error(ctx, "Failed in PutBucketToLifeCycle: ", sqltext)
		return nil
	}
	return nil
}

func (t *TidbClient) RemoveBucketFromLifeCycle(ctx context.Context, bucket *Bucket) error {
	sqltext := "delete from lifecycle where bucketname=?;"
	_, err := t.Client.Exec(sqltext, bucket.Name)
	if err != nil {
		helper.Logger.Error(ctx, "Failed in RemoveBucketFromLifeCycle:", sqltext)
		return nil
	}
	return nil
}

func (t *TidbClient) ScanLifeCycle(ctx context.Context, limit int, marker string) (result ScanLifeCycleResult, err error) {
	result.Truncated = false
	sqltext := "select * from lifecycle where bucketname > ? order by bucketname limit ? ;"
	rows, err := t.Client.Query(sqltext, marker, limit)
	if err == sql.ErrNoRows {
		helper.Logger.Error(ctx, "Failed in sql.ErrNoRows:", sqltext)
		err = nil
		return
	} else if err != nil {
		return
	}
	defer rows.Close()
	result.Lcs = make([]LifeCycle, 0, limit)
	var lc LifeCycle
	lastBucketName := ""
	for rows.Next() {
		err = rows.Scan(
			&lc.BucketName,
			&lc.Status)
		if err != nil {
			helper.Logger.Error(ctx, "Failed in ScanLifeCycle:", result.Lcs, result.NextMarker)
			return
		}
		if lc.BucketName == lastBucketName {
			continue
		}
		result.Lcs = append(result.Lcs, lc)
		lastBucketName = lc.BucketName
	}
	result.NextMarker = lc.BucketName
	if len(result.Lcs) == limit {
		result.Truncated = true
	}
	return result, nil
}

package tidbclient

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

func (t *TidbClient) GetBucket(bucketName string) (bucket *Bucket, err error) {
	var acl, cors, logging, lc, policy, createTime string
	var updateTime sql.NullString
	var website sql.NullString

	sqltext := "select bucketname,acl,cors,COALESCE(logging,\"\"),lc,uid,policy,createtime,usages,versioning,update_time,website from buckets where bucketname=?;"
	tmp := &Bucket{}
	err = t.Client.QueryRow(sqltext, bucketName).Scan(
		&tmp.Name,
		&acl,
		&cors,
		&logging,
		&lc,
		&tmp.OwnerId,
		&policy,
		&createTime,
		&tmp.Usage,
		&tmp.Versioning,
		&updateTime,
		&website,
	)
	if err != nil && err == sql.ErrNoRows {
		err = ErrNoSuchBucket
		return
	} else if err != nil {
		helper.Logger.Error(nil, err)
		return
	}
	tmp.CreateTime, err = time.Parse(TIME_LAYOUT_TIDB, createTime)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(acl), &tmp.ACL)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(cors), &tmp.CORS)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(logging), &tmp.BucketLogging)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(lc), &tmp.LC)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(policy), &tmp.Policy)
	if err != nil {
		return
	}
	if updateTime.Valid {
		tmp.UpdateTime, err = time.Parse(TIME_LAYOUT_TIDB, updateTime.String)
		if err != nil {
			return
		}
	}
	bucket = tmp

	if website.Valid {
		err = json.Unmarshal([]byte(website.String), &bucket.Website)
		if err != nil {
			helper.Logger.Error(nil, err)
			return
		}
	}

	return
}

func (t *TidbClient) GetBuckets() (buckets []*Bucket, err error) {
	sqltext := "select bucketname,acl,cors,COALESCE(logging,\"\"),lc,uid,policy,createtime,usages,versioning,update_time,website from buckets;"
	rows, err := t.Client.Query(sqltext)
	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tmp Bucket
		var acl, cors, logging, lc, policy, createTime string
		var updateTime sql.NullString
		var website sql.NullString

		err = rows.Scan(
			&tmp.Name,
			&acl,
			&cors,
			&logging,
			&lc,
			&tmp.OwnerId,
			&policy,
			&createTime,
			&tmp.Usage,
			&tmp.Versioning,
			&updateTime,
			&website)
		if err != nil {
			return
		}
		tmp.CreateTime, err = time.Parse(TIME_LAYOUT_TIDB, createTime)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(acl), &tmp.ACL)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(cors), &tmp.CORS)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(lc), &tmp.LC)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(logging), &tmp.BucketLogging)
		if err != nil {
			return
		}
		err = json.Unmarshal([]byte(policy), &tmp.Policy)
		if err != nil {
			return
		}
		if updateTime.Valid {
			tmp.UpdateTime, err = time.Parse(TIME_LAYOUT_TIDB, updateTime.String)
			if err != nil {
				return
			}
		}

		if website.Valid {
			err = json.Unmarshal([]byte(website.String), &tmp.Website)
			if err != nil {
				helper.Logger.Error(nil, err)
				return
			}
		}

		buckets = append(buckets, &tmp)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return
}

//Actually this method is used to update bucket
func (t *TidbClient) PutBucket(bucket *Bucket) error {
	sql, args := bucket.GetUpdateSql()
	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) CheckAndPutBucket(bucket *Bucket) (bool, error) {
	var processed bool
	_, err := t.GetBucket(bucket.Name)
	if err == nil {
		processed = false
		return processed, err
	} else if err != nil && err != ErrNoSuchBucket {
		processed = false
		return processed, err
	} else {
		processed = true
	}
	sql, args := bucket.GetCreateSql()
	_, err = t.Client.Exec(sql, args...)
	return processed, err
}

// ListObjcts is called by both list objects and list object versions, controlled by versioned.
func (t *TidbClient) ListObjects(ctx context.Context, bucketName, marker, verIdMarker, prefix, delimiter string, versioned bool, maxKeys int, withDeleteMarker bool, isBucketVersioning bool) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error) {
	if !isBucketVersioning {
		// If bucket is version disabled, there is no difference in ListObjects and ListObjectVersions.
		// And no delete marker exist.
		return t.ListObjectsNoVersion(ctx, bucketName, marker, prefix, delimiter, maxKeys)
	} else {
		// Versioned bucket.
		if versioned && delimiter != "" {
			return t.ListObjectVersionsWithDelimiter(ctx, bucketName, marker, verIdMarker, prefix, delimiter, maxKeys)
		} else {
			// For both ListObjects, and ListObjectVersions without delimiter.
			return t.ListObjectsVersionedBucket(ctx, bucketName, marker, verIdMarker, prefix, delimiter, versioned, maxKeys, withDeleteMarker)
		}
	}
}

// ListObjects is called by both list objects and list object versions, no difference for versioning disabled bucket.
func (t *TidbClient) ListObjectsNoVersion(ctx context.Context, bucketName, marker, prefix, delimiter string, maxKeys int) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error) {
	const MaxObjectList = 10000
	var count int
	var exit bool
	commonPrefixes := make(map[string]struct{})
	omarker := marker

	helper.Logger.Info(ctx, bucketName, marker, prefix, delimiter, maxKeys)

	for {
		var loopcount int
		var sqltext string
		var rows *sql.Rows
		args := make([]interface{}, 0)

		sqltext = "select bucketname,name from objects where bucketName=?"
		args = append(args, bucketName)
		if prefix != "" {
			sqltext += " and name like ?"
			args = append(args, prefix+"%")
			helper.Logger.Info(ctx, "query prefix:", prefix)
		}
		if marker != "" {
			sqltext += " and name >= ?"
			args = append(args, marker)
			helper.Logger.Info(ctx, "query marker:", marker)
		}
		if delimiter == "" {
			sqltext += " order by bucketname,name limit ?"
			args = append(args, MaxObjectList)
		} else {
			num := len(strings.Split(prefix, delimiter))
			args = append(args, delimiter, num, MaxObjectList)
			sqltext += " group by SUBSTRING_INDEX(name, ?, ?) order by bucketname, name limit ?"
		}

		helper.Logger.Info(ctx, "query sql:", sqltext, "args:", args)

		tstart := time.Now()
		rows, err = t.Client.Query(sqltext, args...)
		if err != nil {
			return
		}
		tqueryend := time.Now()
		tdur := tqueryend.Sub(tstart).Nanoseconds()
		if tdur/1000000 > 5000 {
			helper.Logger.Info(ctx, fmt.Sprintf("slow list objects query: %s,args: %v, takes %d", sqltext, args, tdur))
		}

		for rows.Next() {
			loopcount += 1
			//fetch related date
			var bucketname, name string

			err = rows.Scan(
				&bucketname,
				&name,
			)
			if err != nil {
				helper.Logger.Error(ctx, "rows.Scan() err:", err)
				rows.Close()
				return
			}
			helper.Logger.Info(ctx, bucketname, name)

			//prepare next marker
			//TODU: be sure how tidb/mysql compare strings
			marker = name

			if name == omarker {
				continue
			}

			//filte by delemiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(name, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := prefix + string([]byte(subStr)[0:(n+1)])
					marker = prefixKey[0:(len(prefixKey)-1)] + string(delimiter[len(delimiter)-1]+1)
					if prefixKey == omarker {
						continue
					}
					if _, ok := commonPrefixes[prefixKey]; !ok {
						if count == maxKeys {
							truncated = true
							exit = true
							break
						}
						commonPrefixes[prefixKey] = struct{}{}
						nextMarker = prefixKey
						count += 1
					}
					continue
				}
			}

			var o *Object
			o, err = t.GetObject(bucketname, name, "")
			if err != nil {
				helper.Logger.Error(nil, fmt.Sprintf("ListObjects: failed to GetObject(%s, %s, %s), err: %v", bucketname, name, err))
				rows.Close()
				return
			}

			count += 1
			if count == maxKeys {
				nextMarker = name
			}

			if count > maxKeys {
				truncated = true
				exit = true
				break
			}

			retObjects = append(retObjects, o)
		}
		rows.Close()
		tfor := time.Now()
		tdur = tfor.Sub(tqueryend).Nanoseconds()
		if tdur/1000000 > 5000 {
			helper.Logger.Info(nil, "slow list get objects, takes", tdur)
		}

		if loopcount < MaxObjectList {
			exit = true
		}
		if exit {
			break
		}
	}
	prefixes = helper.SortKeys(commonPrefixes, false)
	return
}

// ListObjects and ListObjectVersions for both versioning Enabled and Suspended buckets.
// !versioned for ListObjects.
// versioned for ListObjectVersions.
// TODO: should keep withDeleteMarker??
func (t *TidbClient) ListObjectsVersionedBucket(ctx context.Context, bucketName, marker, verIdMarker, prefix, delimiter string, versioned bool, maxKeys int, withDeleteMarker bool) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error) {
	var count int
	var exit bool
	commonPrefixes := make(map[string]struct{})
	omarker := marker
	var lastListedVersion uint64
	selectLimit := maxKeys + 10 // +10 Out of air. Just reserve it to skip some markers and return correct truncated.

	rawVersionIdMarker := ""
	if versioned && verIdMarker != "" {
		if verIdMarker == "null" {
			var o *Object
			if o, err = t.GetObject(bucketName, marker, "null"); err != nil {
				return
			}
			verIdMarker = o.VersionId
		}
		if rawVersionIdMarker, err = ConvertS3VersionToRawVersion(verIdMarker); err != nil {
			return
		}
	}

	helper.Logger.Info(ctx, "input:", bucketName, marker, verIdMarker, prefix, delimiter, versioned, maxKeys, withDeleteMarker)

	for i := 0; i < 2; i++ { // To avoid dead loop.
		var loopcount int
		var sqltext string
		var rows *sql.Rows
		args := make([]interface{}, 0)

		if !versioned {
			// list objects, order by bucketname, name, version. So the latest will be returned.
			sqltext = "select bucketname,name from objects where bucketName=?"
		} else {
			// list object versions.
			sqltext = "select bucketname,name,version from objects where bucketName=?"
		}
		args = append(args, bucketName)

		if prefix != "" {
			sqltext += " and name like ?"
			args = append(args, prefix+"%")
			helper.Logger.Info(ctx, "query prefix:", prefix)
		}
		if marker != "" {
			if !versioned {
				// list objects.
				if len(retObjects) > 0 && retObjects[len(retObjects)-1].Name == marker {
					sqltext += " and name > ?"
				} else {
					sqltext += " and name >= ?" // For jumped from delimiter branch.
				}
				args = append(args, marker)
				helper.Logger.Info(ctx, "query marker:", marker)
			} else {
				// list object versions.
				if rawVersionIdMarker == "" {
					// First time to list the object after marker versions, excluding marker because it's listed before.
					sqltext += " and name > ?"
					args = append(args, marker)
				} else {
					// Not first time to list marker. Just start from marker, excluding verIdMarker.
					sqltext += " and name = ? and version > ?"
					args = append(args, marker)
					args = append(args, rawVersionIdMarker)
				}
				helper.Logger.Info(ctx, "query marker for versioned:", marker, rawVersionIdMarker)
			}
		}

		if !versioned {
			sqltext += " and islatest=1 and deletemarker=0"
		}

		if delimiter != "" {
			num := len(strings.Split(prefix, delimiter))
			args = append(args, delimiter, num)
			sqltext += " group by SUBSTRING_INDEX(name, ?, ?)"
		}
		sqltext += " order by bucketname,name,version limit ?"
		args = append(args, selectLimit)

		helper.Logger.Info(ctx, "query sql:", sqltext, "args:", args)

		tstart := time.Now()
		rows, err = t.Client.Query(sqltext, args...)
		if err != nil {
			return
		}
		tqueryend := time.Now()
		tdur := tqueryend.Sub(tstart).Nanoseconds()
		if tdur/1000000 > 5000 {
			helper.Logger.Info(ctx, fmt.Sprintf("slow list objects query: %s,args: %v, takes %d", sqltext, args, tdur))
		}

		for rows.Next() {
			loopcount += 1
			//fetch related date
			var bucketname, name string
			var version uint64 // Internal version, the same as in DB.
			var s3VersionId string
			if !versioned {
				err = rows.Scan(
					&bucketname,
					&name,
				)
				s3VersionId = "" // Get default object later.
			} else {
				err = rows.Scan(
					&bucketname,
					&name,
					&version,
				)
				s3VersionId = ConvertRawVersionToS3Version(version)
			}
			if err != nil {
				helper.Logger.Error(ctx, "rows.Scan() err:", err)
				rows.Close()
				return
			}
			helper.Logger.Info(ctx, bucketname, name, version)

			//prepare next marker
			//TODU: be sure how tidb/mysql compare strings
			marker = name

			if !versioned && name == omarker {
				continue
			}

			//filte by delemiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(name, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := prefix + string([]byte(subStr)[0:(n+1)])
					marker = prefixKey[0:(len(prefixKey)-1)] + string(delimiter[len(delimiter)-1]+1)
					if prefixKey == omarker {
						continue
					}
					if _, ok := commonPrefixes[prefixKey]; !ok {
						if count == maxKeys {
							truncated = true
							exit = true
							break
						}
						commonPrefixes[prefixKey] = struct{}{}
						nextMarker = prefixKey
						lastListedVersion = 0 // ListObjectVersions only show directories, so start from next.
						count += 1
					}
					continue
				}
			}

			var o *Object
			o, err = t.GetObject(bucketname, name, s3VersionId)
			if err != nil {
				helper.Logger.Error(nil, fmt.Sprintf("ListObjects: failed to GetObject(%s, %s, %s), err: %v", bucketname, name, ConvertRawVersionToS3Version(version), err))
				rows.Close()
				return
			}

			count += 1
			if count == maxKeys {
				nextMarker = name
				lastListedVersion = version
			}

			if count > maxKeys {
				truncated = true
				exit = true
				break
			}

			retObjects = append(retObjects, o)
		}
		rows.Close()
		tfor := time.Now()
		tdur = tfor.Sub(tqueryend).Nanoseconds()
		if tdur/1000000 > 5000 {
			helper.Logger.Info(nil, "slow list get objects, takes", tdur)
		}

		if versioned {
			// Looped all the versions in the marker.
			// Start from next object name. Should come here only once.
			helper.Logger.Info(ctx, "Looped all the versions for", bucketName, marker, rawVersionIdMarker)

			if !exit && rawVersionIdMarker != "" {
				rawVersionIdMarker = ""
				continue
			}
		}
	}
	prefixes = helper.SortKeys(commonPrefixes, false)
	if versioned && lastListedVersion != 0 {
		nextVerIdMarker = ConvertRawVersionToS3Version(lastListedVersion)
	}
	return
}

// To handle only ListObjectVersions when delimiter != "".
// First "select bucketname,name and islatest=1" to get object list.
// Then call t.GetAllObject() to list all the versions of the objects.
func (t *TidbClient) ListObjectVersionsWithDelimiter(ctx context.Context, bucketName, marker, verIdMarker, prefix, delimiter string, maxKeys int) (retObjects []*Object, prefixes []string, truncated bool, nextMarker, nextVerIdMarker string, err error) {
	var count int
	var exit bool
	commonPrefixes := make(map[string]struct{})
	omarker := marker
	selectLimit := maxKeys + 10 // +10 Out of air. Just reserve it to skip some markers and return correct truncated.

	rawVersionIdMarker := ""
	if verIdMarker != "" {
		if verIdMarker == "null" {
			var o *Object
			if o, err = t.GetObject(bucketName, marker, "null"); err != nil {
				return
			}
			verIdMarker = o.VersionId
		}
		if rawVersionIdMarker, err = ConvertS3VersionToRawVersion(verIdMarker); err != nil {
			return
		}
	}

	helper.Logger.Info(ctx, "input:", bucketName, marker, verIdMarker, prefix, delimiter, maxKeys, rawVersionIdMarker)

	for i := 0; i < 2; i++ { // To avoid dead loop.
		var loopcount int
		var sqltext string
		var rows *sql.Rows
		args := make([]interface{}, 0)

		// Only select bucketname and name here. Get all the versions in GetAllObject() later.
		sqltext = "select bucketname,name from objects where bucketName=?"
		args = append(args, bucketName)

		if prefix != "" {
			sqltext += " and name like ?"
			args = append(args, prefix+"%")
			helper.Logger.Info(ctx, "query prefix:", prefix)
		}
		if marker != "" {
			// Suppose case like:
			// object1 v1, v2, v3
			// object2 v1, v2, v3
			// if marker is (object1, v2), should return
			// (object1, v3),
			// (object2, v1),
			// (object2, v2),
			// (object2. v3)
			if rawVersionIdMarker == "" {
				// First time to list the object after marker versions, excluding marker because it's listed before.
				sqltext += " and name > ?"
			} else {
				// Not first time to list marker. Just start from marker, excluding verIdMarker.
				sqltext += " and name = ?"
			}
			args = append(args, marker)
			helper.Logger.Info(ctx, "query marker for versioned:", marker, rawVersionIdMarker)
		}

		// This function handles only versioned and delimiter != "".
		// As there may be too many versions, while delimiter requires single item, we have to use islatest=1 here and get all the object versions later.
		sqltext += " and islatest=1"

		num := len(strings.Split(prefix, delimiter))
		sqltext += " group by SUBSTRING_INDEX(name, ?, ?) order by bucketname,name,version limit ?"
		args = append(args, delimiter, num, selectLimit)

		helper.Logger.Info(ctx, "query sql:", sqltext, "args:", args)

		tstart := time.Now()
		rows, err = t.Client.Query(sqltext, args...)
		if err != nil {
			return
		}
		tqueryend := time.Now()
		tdur := tqueryend.Sub(tstart).Nanoseconds()
		if tdur/1000000 > 5000 {
			helper.Logger.Info(ctx, fmt.Sprintf("slow list objects query: %s,args: %v, takes %d", sqltext, args, tdur))
		}

	outer:
		for rows.Next() {
			loopcount += 1
			var bucketname, name string
			err = rows.Scan(
				&bucketname,
				&name,
			)

			helper.Logger.Info(ctx, bucketname, name)

			//prepare next marker
			//TODU: be sure how tidb/mysql compare strings
			marker = name

			//filte by delemiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(name, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := prefix + string([]byte(subStr)[0:(n+1)])
					marker = prefixKey[0:(len(prefixKey)-1)] + string(delimiter[len(delimiter)-1]+1)
					if prefixKey == omarker {
						continue
					}
					if _, ok := commonPrefixes[prefixKey]; !ok {
						if count == maxKeys {
							truncated = true
							exit = true
							break
						}
						commonPrefixes[prefixKey] = struct{}{}
						nextMarker = prefixKey
						count += 1
					}
					continue
				}
			}

			// Get all the object versions including deletemarker.
			var objectList []*Object
			objectList, err = t.GetAllObject(bucketname, name, rawVersionIdMarker, selectLimit-count)
			if err != nil {
				helper.Logger.Error(nil, fmt.Sprintf("ListObjects: failed to GetAllObject(%s, %s), err: %v", bucketname, name, err))
				rows.Close()
				return
			}

			for _, object := range objectList {
				count += 1
				if count == maxKeys {
					nextMarker = name
					nextVerIdMarker = object.VersionId
				}

				if count > maxKeys {
					// This branch just to get correct truncated value.
					truncated = true
					exit = true
					break outer
				}

				retObjects = append(retObjects, object)
			}
		}
		rows.Close()
		tfor := time.Now()
		tdur = tfor.Sub(tqueryend).Nanoseconds()
		if tdur/1000000 > 5000 {
			helper.Logger.Info(nil, "slow list get objects, takes", tdur)
		}

		// Looped all the versions in the marker.
		// Start from next object name.
		helper.Logger.Info(ctx, "Looped all the versions for marker", bucketName, marker, rawVersionIdMarker,
			"last object:", bucketName, nextMarker, nextVerIdMarker, "truncated:", truncated, "exit:", exit)

		// Try to start from after marker.
		// If there is no more data, we'll get here again with rawVersionIdMarker unchanged "" and should exit then.
		if !exit && rawVersionIdMarker != "" {
			rawVersionIdMarker = ""
			continue
		}

		break
	}
	prefixes = helper.SortKeys(commonPrefixes, false)

	return
}

func (t *TidbClient) DeleteBucket(bucket *Bucket) error {
	sqltext := "delete from buckets where bucketname=?;"
	_, err := t.Client.Exec(sqltext, bucket.Name)
	if err != nil {
		return err
	}
	return nil
}

func (t *TidbClient) UpdateUsage(bucketName string, size int64, tx interface{}) (err error) {
	var sqlTx *sql.Tx
	if tx == nil {
		tx, err = t.Client.Begin()

		defer func() {
			if err == nil {
				err = sqlTx.Commit()
			}
			if err != nil {
				sqlTx.Rollback()
			}
		}()
	}
	sqlTx, _ = tx.(*sql.Tx)

	sql := "update buckets set usages=? where bucketname=?;"
	_, err = sqlTx.Exec(sql, size, bucketName)
	return
}

func (t *TidbClient) UpdateBucketInfo(usages map[string]*BucketInfo, tx interface{}) error {
	var sqlTx *sql.Tx
	var err error
	if nil == tx {
		tx, err = t.Client.Begin()
		defer func() {
			if nil == err {
				err = sqlTx.Commit()
			} else {
				sqlTx.Rollback()
			}
		}()
	}
	sqlTx, _ = tx.(*sql.Tx)
	sqlStr := "update buckets set usages = ?, fileNum = ? where bucketname = ?;"
	st, err := sqlTx.Prepare(sqlStr)
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("UpdateBucketInfo: failed to prepare statement: %s, err: %v",
			sqlStr, err))
		return err
	}
	defer st.Close()

	for bucket, info := range usages {
		_, err = st.Exec(info.Usage, info.FileNum, bucket)
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("UpdateBucketInfo: failed to update bucket info for bucket %s, with usage: %d, fileNum: %d, err: %v",
				bucket, info.Usage, info.FileNum, err))
			return err
		}
	}
	return nil
}

func (t *TidbClient) GetAllBucketInfo() (map[string]*BucketInfo, error) {
	query := "select bucketname, count(objectid) as fileNum, sum(size) as usages from objects group by bucketname;"
	rows, err := t.Client.Query(query)
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("failed to query(%s), err: %v", query, err))
		return nil, err
	}

	infos := make(map[string]*BucketInfo)
	defer rows.Close()
	for rows.Next() {
		bi := &BucketInfo{}
		err = rows.Scan(
			&bi.BucketName,
			&bi.FileNum,
			&bi.Usage,
		)
		if err != nil {
			helper.Logger.Error(nil, fmt.Sprintf("failed to scan for query(%s), err: %v", query, err))
			return nil, err
		}
		infos[bi.BucketName] = bi
	}
	err = rows.Err()
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("failed to iterator rows for query(%s), err: %v", query, err))
		return nil, err
	}
	return infos, nil
}

func (t *TidbClient) IsEmptyBucket(ctx context.Context, bucketName string) (isEmpty bool, err error) {
	sqltext := "select 1 from objects where bucketname=? limit 1;"
	row := t.Client.QueryRow(sqltext, bucketName)
	var result int

	err = row.Scan(&result)
	if err == nil {
		// err == nil, which means there are objects in the bucket.
		return false, nil
	} else if err != sql.ErrNoRows {
		helper.Logger.Error(ctx, err, sqltext, bucketName)
		return false, err
	}
	// err == sql.ErrNoRows, which means there is no objects in the bucket.

	sqltext = "select 1 from multiparts where bucketname=? limit 1;"
	row = t.Client.QueryRow(sqltext, bucketName)

	err = row.Scan(&result)
	if err == nil {
		// err == nil, which means there are incomplete multiparts in the bucket.
		return false, nil
	} else if err != sql.ErrNoRows {
		helper.Logger.Error(ctx, err, sqltext, bucketName)
		return false, err
	}
	// err == sql.ErrNoRows, which means there is no multiparts in the bucket.

	return true, nil
}

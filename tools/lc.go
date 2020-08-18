package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/robfig/cron"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/api/datatype/lifecycle"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
)

const (
	DebugSpec = "@every 20s"
	DebugTime = time.Second // DebugTime for test: 1 day == 1 second

	RequestMaxKeys                                 = 1000
	SCAN_LIMIT                                     = 50
	DEFAULT_LC_LOG_PATH                            = "/var/log/yig/lc.log"
	DEFAULT_LC_INTERRUPTIBLE_SLEEP_INTERVAL_SECOND = 5 // TODO, sleep 5s and check stop.
	DEFAULT_LC_TASK_WAIT_FOR_WORKER_THREAD         = 5 // TODO, sleep 5s and check worker thread and exit.
)

var (
	yig             *storage.YigStorage
	signalQueue     chan os.Signal
	taskQ           chan types.LifeCycle
	waitgroup       sync.WaitGroup
	lcHandlerIsOver bool
	stop            bool
	sessionId       int
)

func getLifeCycles(session string) {
	var marker string
	ctx := context.WithValue(context.Background(), "RequestId", "LC Scan "+session)
	helper.Logger.Info(ctx, "all bucket lifecycle handle start")
	waitgroup.Add(1)
	defer func() {
		close(taskQ)
		lcHandlerIsOver = true
		helper.Logger.Warn(ctx, "ScanLifeCycle finished! QUIT. stop:", stop)
		waitgroup.Done()
	}()

	for {
		if stop {
			helper.Logger.Info(ctx, "shutting down...")
			return
		}

		helper.Logger.Info(ctx, "ScanLifeCycle: marker: ", marker)

		result, err := yig.MetaStorage.ScanLifeCycle(ctx, helper.CONFIG.LcThread, marker)
		if err != nil {
			helper.Logger.Error(ctx, "ScanLifeCycle failed, err: ", err, "exit and retry in next job.")
			return
		}

		for _, entry := range result.Lcs {
			if stop {
				helper.Logger.Warn(ctx, "ScanLifeCycle finished for stop!")
				return
			}
			taskQ <- entry
			marker = entry.BucketName
		}

		if result.Truncated == false {
			return
		}
	}
}

func checkIfExpiration(updateTime time.Time, days int) bool {
	if helper.CONFIG.LcDebug == false {
		return int(time.Since(updateTime).Seconds()) >= days*24*3600
	} else {
		return int(time.Since(updateTime).Seconds()) >= days
	}
}

func lifecycleUnit(ctx context.Context, lc types.LifeCycle) error {
	if stop {
		helper.Logger.Warn(ctx, "stop, skip LC check for bucket:", lc.BucketName)
		return nil
	}

	bucket, err := yig.MetaStorage.Client.GetBucket(lc.BucketName)
	if err != nil {
		helper.Logger.Error(ctx, "GetBucket failed!", lc.BucketName, err)
		return err
	}

	var defaultRule *lifecycle.Rule

	for _, rule := range bucket.Lifecycle.Rules {
		if !rule.IsEnabled() {
			continue
		}

		if rule.IsDefaultRule() {
			defaultRule = &rule
			continue
		}

		processLifecycleRule(ctx, bucket, &rule)
	}

	if defaultRule != nil {
		processLifecycleRule(ctx, bucket, defaultRule)
	}

	return nil
}

func processLifecycleRule(ctx context.Context, bucket *types.Bucket, rule *lifecycle.Rule) {
	if bucket.Versioning != "Disabled" && rule.NoncurrentVersionExpiration != nil {
		deleteNoncurrentVersionExpired(ctx,
			bucket,
			rule.GetPrefix(),
			int(rule.NoncurrentVersionExpiration.NoncurrentDays),
			rule.ID,
			rule.IsSetExpiredObjectDeleteMarker())
	}
	if rule.Expiration != nil && !rule.Expiration.IsDaysNull() {
		deleteExpiredObjects(ctx, bucket, rule.GetPrefix(), int(rule.Expiration.Days), rule.ID)
	}
	if rule.AbortIncompleteMultipartUpload != nil {
		abortIncompleteMultipartUpload(ctx, bucket, rule.GetPrefix(), int(rule.AbortIncompleteMultipartUpload.DaysAfterInitiation), rule.ID)
	}
}

// Delete noncurrent version objects if expired.
// Should delete by bucketname+key+version, rather than add delete marker.
func deleteNoncurrentVersionExpired(ctx context.Context, bucket *types.Bucket, prefix string, days int, ID string, expiredObjectDeleteMarker bool) {
	var request datatype.ListObjectsRequest
	request.Versioned = false
	request.MaxKeys = 1000
	request.Prefix = prefix
	for {
		if stop {
			return
		}

		// List all the objects in this buckets, and check versions expiration reversely.
		retObjects, _, truncated, nextMarker, _, err := yig.ListObjectsInternalWithDeleteMarker(ctx, bucket, request)
		if err != nil {
			helper.Logger.Error(nil, err)
			return
		}

		for _, object := range retObjects {
			if stop {
				return
			}
			deleteNoncurrentVersionExpiredPerKey(ctx, bucket, object, days, prefix, ID, expiredObjectDeleteMarker)
		}

		if truncated == true {
			request.KeyMarker = nextMarker
		} else {
			break
		}
	}
}

func deleteNoncurrentVersionExpiredPerKey(ctx context.Context, bucket *types.Bucket, object *types.Object, days int, prefix, ID string, expiredObjectDeleteMarker bool) {
	var versionIdMarker string
	var prevObject *types.Object

outerLoop:
	for versionIdMarker, prevObject = "", (*types.Object)(nil); ; {
		if stop {
			return
		}

		// Get all the versions of this key from oldest to latest.
		objectVersions, err := yig.MetaStorage.GetAllObject(bucket.Name, object.Name, versionIdMarker, 1000, true)
		if err != nil {
			helper.Logger.Error(ctx, "GetAllObject failed!", bucket.Name, object.Name)
			return
		}

		// Scanned all the versions of this key.
		if len(objectVersions) == 0 {
			break outerLoop
		}

		for _, o := range objectVersions {
			if stop {
				return
			}

			versionIdMarker = o.VersionId

			// If an object version LastModifiedTime exceeds the Days, it means the object version before it should be deleted.
			if checkIfExpiration(o.LastModifiedTime, days) {
				// prevObject.IsLatest should be false. Just check it for safe here.
				if prevObject != nil && prevObject.IsLatest != true {
					_, err = yig.DeleteObject(ctx, prevObject.BucketName, prevObject.Name, prevObject.VersionId, common.Credential{})
					if err != nil {
						helper.Logger.Error(ctx, "DeleteObject failed!", prevObject.BucketName, prevObject.Name, prevObject.VersionId)
					} else {
						helper.Logger.Warn(ctx, "[DELETED] Noncurrent", prevObject.BucketName, prevObject.Name, prevObject.VersionId,
							" for NoncurrentExpire Rule:", ID, ", Prefix:", prefix, ", Days:", days, ", next object:", o.VersionId, o.LastModifiedTime.String())
					}
				}

				prevObject = o
			} else {
				// If an object version is newer than Expired Days, all the left versions are newer.
				// Because we iterate it reversely in GetAllObject(), from oldest to latest.
				break outerLoop
			}
		}
	}

	// Check the latest version.
	if prevObject != nil && prevObject.IsLatest && expiredObjectDeleteMarker && prevObject.DeleteMarker {
		// Check if there is only one delete marker left for objects.
		if objectVersions, err := yig.MetaStorage.GetAllObject(bucket.Name, object.Name, "", 3, true); err == nil && len(objectVersions) == 1 {
			_, err = yig.DeleteObject(ctx, prevObject.BucketName, prevObject.Name, prevObject.VersionId, common.Credential{})
			if err != nil {
				helper.Logger.Error(ctx, "DeleteObject failed!", prevObject.BucketName, prevObject.Name, prevObject.VersionId)
			} else {
				helper.Logger.Warn(ctx, "[DELETED] DeleteMarker", prevObject.BucketName, prevObject.Name, prevObject.VersionId,
					" for ExpiredObjectDeleteMarker Rule:", ID, ", Prefix:", prefix)
			}
		}
	}
}

func deleteExpiredObjects(ctx context.Context, bucket *types.Bucket, prefix string, days int, ID string) {
	var request datatype.ListObjectsRequest
	request.Versioned = false
	request.MaxKeys = 1000
	request.Prefix = prefix

	for {
		if stop {
			return
		}

		// TODO should in reverse order.
		retObjects, _, truncated, nextMarker, _, err := yig.ListObjectsInternal(ctx, bucket, request)
		if err != nil {
			helper.Logger.Error(ctx, err)
			return
		}

		for _, object := range retObjects {
			if stop {
				return
			}

			if checkIfExpiration(object.LastModifiedTime, days) {
				_, err = yig.DeleteObject(ctx, object.BucketName, object.Name, types.ObjectDefaultVersion, common.Credential{})
				if err != nil {
					helper.Logger.Error(ctx, "[FAILED]", object.BucketName, object.Name, object.VersionId, err)
					continue
				}
				helper.Logger.Warn(ctx, "[DELETED]", object.BucketName, object.Name, object.VersionId,
					", LastModified:", object.LastModifiedTime.String(), ", Now:", time.Now().UTC().String(),
					", for Rule:", ID, ", Prefix:", prefix, "Days:", days)
			}
		}

		if truncated == true {
			request.KeyMarker = nextMarker
		} else {
			break
		}
	}
}

func abortIncompleteMultipartUpload(ctx context.Context, bucket *types.Bucket, prefix string, days int, ID string) {
	var request datatype.ListUploadsRequest
	request.Prefix = prefix
	request.MaxUploads = 1000
	request.KeyMarker, request.UploadIdMarker, request.Delimiter, request.EncodingType = "", "", "", ""
	for {
		if stop {
			return
		}

		// TODO should in reverse order.
		result, err := yig.ListMultipartUploads(ctx, common.Credential{UserId: bucket.OwnerId}, bucket.Name, request)
		if err != nil {
			helper.Logger.Error(ctx, err)
			return
		}

		for _, upload := range result.Uploads {
			if stop {
				return
			}

			var initiatedTime time.Time
			if initiatedTime, err = time.Parse(types.CREATE_TIME_LAYOUT, upload.Initiated); err != nil {
				helper.Logger.Error(ctx, "time.Parse failed!", bucket.Name, upload.Key, upload.UploadId, upload.Initiated, err)
			}

			if checkIfExpiration(initiatedTime, days) {
				err = yig.AbortMultipartUpload(ctx, common.Credential{UserId: bucket.OwnerId}, bucket.Name, upload.Key, upload.UploadId)
				if err != nil {
					helper.Logger.Error(ctx, "[FAILED]", bucket.Name, upload.Key, upload.UploadId, err)
					continue
				}
				helper.Logger.Warn(ctx, "[DELETED] multipart:", bucket.Name, upload.Key, upload.UploadId,
					", LastModified:", upload.Initiated, ", Now:", time.Now().UTC().String(),
					", for Rule:", ID, ", Prefix:", prefix, "Days:", days)
			}
		}

		if result.IsTruncated == true {
			request.KeyMarker = result.NextKeyMarker
			request.UploadIdMarker = result.NextUploadIdMarker
		} else {
			break
		}
	}
}

func processLifecycle(processId string) {
	waitgroup.Add(1)
	defer waitgroup.Done()
	time.Sleep(time.Second * 1)
	ctx := context.WithValue(context.Background(), "RequestId", "LC thread "+processId)

	for item := range taskQ {
		if stop {
			continue
		}

		helper.Logger.Info(ctx, "receive task bucket:", item.BucketName)
		err := lifecycleUnit(ctx, item)
		if err != nil {
			helper.Logger.Error(ctx, "Bucket", item.BucketName, "Lifecycle process error:", err)
		} else {
			helper.Logger.Info(ctx, "Bucket", item.BucketName, "lifecycle done.")
		}
	}

	helper.Logger.Warn(ctx, "All bucket lifecycle handle complete. QUIT. stop:", stop)
	return
}

func LifecycleStart() {
	if stop {
		return
	}
	if lcHandlerIsOver == false {
		helper.Logger.Warn(nil, "Last Day Scan not finished. QUIT.")
		return
	}

	// Last Day scan finished. Start a new scan for today.
	lcHandlerIsOver = false

	taskQ = make(chan types.LifeCycle, helper.CONFIG.LcThread)

	numOfWorkers := helper.CONFIG.LcThread
	sessionString := "Started " + time.Now().Format(types.TIME_LAYOUT_TIDB) + "-" + strconv.Itoa(sessionId)
	sessionId++
	helper.Logger.Warn(nil, sessionString, "start lc thread:", numOfWorkers)

	for i := 0; i < numOfWorkers; i++ {
		go processLifecycle(sessionString + ":" + strconv.Itoa(i))
	}
	go getLifeCycles(sessionString)

	helper.Logger.Warn(nil, "LifecycleStart session", sessionString, "exit!")
}

func main() {
	stop = false

	helper.SetupConfig()
	logLevel := log.ParseLevel(helper.CONFIG.LcLogLevel)

	helper.Logger = log.NewFileLogger(DEFAULT_LC_LOG_PATH, logLevel)
	defer helper.Logger.Close()
	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
		defer redis.Close()
	}
	helper.Logger.Warn(nil, "Yig lifecycle start!")
	yig = storage.New(helper.Logger, helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, helper.CONFIG.CephConfigPattern)

	lc := LifecycleStart
	lcHandlerIsOver = true

	c := cron.New()
	if helper.CONFIG.LcDebug {
		c.AddFunc(DebugSpec, lc)
	} else {
		c.AddFunc(helper.CONFIG.LifecycleSpec, lc)
	}
	c.Start()
	defer c.Stop()

	signal.Ignore()
	signalQueue = make(chan os.Signal)
	signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP)
	for {
		s := <-signalQueue
		switch s {
		case syscall.SIGHUP:
			// reload config file
			helper.SetupConfig()
		default:
			// stop YIG server, order matters
			helper.Logger.Warn(nil, "Stopping LC")
			c.Stop()
			stop = true
			waitgroup.Wait()
			helper.Logger.Warn(nil, "LC Done!")
			return
		}
	}

}

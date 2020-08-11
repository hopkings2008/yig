package main

import (
	"bytes"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/log"
	msg "github.com/journeymidnight/yig/messagebus/types"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
)

const (
	DEFAULT_BUCKETLOGGING_LOG_PATH = "/var/log/yig/bucketlogging.log"
	KB                             = 1024
)

var (
	logger      log.Logger
	yig         *storage.YigStorage
	signalQueue chan os.Signal
	stop        bool
	consumer    *kafka.Consumer
	cache       *Cache
)

type Item struct {
	Buffer    *bytes.Buffer
	Partition kafka.TopicPartition
}
type Cache struct {
	Mux        sync.Mutex
	BufferSize uint64
	Buffer     map[string]*Item
	q          chan kafka.TopicPartition
	Partition  kafka.TopicPartition
}

func NewItem(v string, p kafka.TopicPartition) *Item {
	i := new(Item)
	i.Buffer = bytes.NewBufferString(v)
	i.Partition = p
	return i
}

func (item *Item) SetV(v string, p kafka.TopicPartition) {
	item.Buffer.WriteString(v)
	item.Partition = p
}

func (item *Item) GetV() *bytes.Buffer {
	return item.Buffer
}

func NewCache() *Cache {
	c := new(Cache)
	c.Buffer = make(map[string]*Item)
	c.q = make(chan kafka.TopicPartition)
	go flushCache()
	return c
}

func (cache *Cache) addItem(k, v string, p kafka.TopicPartition) {
	item, ok := cache.Buffer[k]
	if ok {
		item.SetV(v, p)
		return
	}
	cache.Buffer[k] = NewItem(v, p)
	cache.Partition = p
}

func (cache *Cache) Set(key, value string, partition kafka.TopicPartition) {
	cache.Mux.Lock()
	cache.addItem(key, value, partition)
	cache.BufferSize += uint64(len(value))
	if cache.BufferSize > uint64(helper.CONFIG.LoggingCacheSize)*KB {
		cache.q <- partition
	}
	cache.Mux.Unlock()
}

func (cache *Cache) Reset() (oldmap map[string]*Item) {
	cache.Mux.Lock()
	cache.BufferSize = 0
	oldmap = cache.Buffer
	cache.Buffer = make(map[string]*Item)
	cache.Mux.Unlock()
	return
}

func flushCache() {
	taskTimer := time.NewTimer(time.Duration(helper.CONFIG.LoggingExpireTime) * time.Second)
	var handle_map map[string]*Item
	for true {
		if cache == nil || stop {
			return
		}
		select {
		case <-taskTimer.C:
			handle_map = cache.Reset()
			taskTimer.Reset(time.Duration(helper.CONFIG.LoggingExpireTime) * time.Second)
		case <-cache.q:
			handle_map = cache.Reset()
		}

		offsets := []kafka.TopicPartition{cache.Partition}
		_, err := consumer.CommitOffsets(offsets)
		if err != nil {
			helper.ErrorIf(nil, "flushCache: commit kafka offset failed ", err)
			return
		}
		for k, v := range handle_map {
			commitData(k, v.GetV())
		}
	}

}

func initTaskQ() (err error) {
	params := helper.CONFIG.MsgBus.Server
	if nil == params || len(params) == 0 {
		return
	}

	// get broker list
	v, ok := params[msg.KAFKA_CFG_BROKER_LIST]
	if !ok {
		return
	}
	brokerList := v.(string)

	//max.poll.records
	consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               brokerList,
		"broker.address.family":           "v4",
		"group.id":                        "0000",
		"session.timeout.ms":              6000,
		"enable.auto.commit":              false,
		"go.application.rebalance.enable": true,
		"go.events.channel.enable":        true,
		"enable.partition.eof":            true,
		"auto.offset.reset":               "earliest"})
	if err != nil {
		return
	}

	err = consumer.SubscribeTopics([]string{"LoggingTopic"}, nil)
	return
}

func getTarget(bucketName string) (target_bucket *types.Bucket, prefix string,
	err error) {
	bucket, err := yig.MetaStorage.Client.GetBucket(bucketName)
	if err != nil {
		return
	}

	prefix = bucket.BucketLogging.LoggingEnabled.TargetPrefix
	targetBucket := bucket.BucketLogging.LoggingEnabled.TargetBucket

	if targetBucket == bucketName {
		target_bucket = bucket
	} else {
		target_bucket, err = yig.MetaStorage.GetBucket(nil, targetBucket, true)
		if err != nil {
			helper.ErrorIf(err, "getTarget: get target bucket failed ", targetBucket)
			return
		}
	}
	return
}

func commitData(bucket string, data *bytes.Buffer) error {
	var sseRequest datatype.SseRequest
	targetBucket, targetPrefix, err := getTarget(bucket)
	if err != nil {
		helper.ErrorIf(err, "cmmitData： get logging rule failed bucket ", bucket)
		return err
	}

	timeStr := time.Now().Format("2006-01-02-15:04:05.000")
	log := fmt.Sprintf("%s/bucket-logging-%s", targetPrefix, timeStr)

	_, err = yig.PutObject(nil,
		targetBucket.Name,
		log,
		common.Credential{UserId: targetBucket.OwnerId},
		int64(data.Len()),
		data,
		map[string]string{},
		targetBucket.ACL,
		sseRequest,
		types.ObjectStorageClassStandard)

	if err != nil {
		helper.ErrorIf(err, "commitData: put logging to bucket ", targetBucket.Name, " failed")
		return err
	}
	return nil
}

func processTask() {
	equeue := consumer.Events()
	for true {
		if stop {
			helper.Logger.Info(nil, "bucket logging distribute task stop")
			return
		}

		select {
		case ev := <-equeue:
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				consumer.Unassign()
			case *kafka.Message:
				k := string(e.Key)
				v := string(e.Value)
				if k != "" && v != "" {
					cache.Set(k, v, e.TopicPartition)
				}
			case kafka.PartitionEOF:
			case kafka.Error:
				helper.ErrorIf(nil, "processTask: kafka happend error ")
			}
		}
	}
}

func main() {
	stop = false
	helper.SetupConfig()

	logLevel := log.ParseLevel(helper.CONFIG.LogLevel)
	helper.Logger = log.NewFileLogger(DEFAULT_BUCKETLOGGING_LOG_PATH, logLevel)

	defer helper.Logger.Close()
	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
		defer redis.Close()
	}
	yig = storage.New(helper.Logger,
		helper.CONFIG.MetaCacheType,
		helper.CONFIG.EnableDataCache,
		helper.CONFIG.CephConfigPattern)

	// init cache
	cache = NewCache()
	err := initTaskQ()
	if err != nil {
		helper.ErrorIf(err, "Init kafka consumer failed")
		return
	}

	signal.Ignore()
	signalQueue = make(chan os.Signal)

	signal.Notify(signalQueue,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGHUP)

	go processTask()

	for {
		s := <-signalQueue
		switch s {
		case syscall.SIGHUP:
			helper.SetupConfig()
		default:
			stop = true
			return
		}
	}
}

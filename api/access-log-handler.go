package api

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/journeymidnight/yig/helper"
	bus "github.com/journeymidnight/yig/messagebus"
	"github.com/journeymidnight/yig/messagebus/types"
	msg "github.com/journeymidnight/yig/messagebus/types"
	"github.com/journeymidnight/yig/meta"
)

type ResponseRecorder struct {
	http.ResponseWriter
	status        int
	size          int64
	operationName string
	serverCost    time.Duration
	requestTime   time.Duration
	errorCode     string

	storageClass       string
	targetStorageClass string
	bucketLogging      bool
	cdn_request        bool
}

func NewResponseRecorder(w http.ResponseWriter) *ResponseRecorder {
	return &ResponseRecorder{
		ResponseWriter: w,
		status:         http.StatusOK,
	}
}

func (r *ResponseRecorder) Flush() {
	return
}

type AccessLogHandler struct {
	handler          http.Handler
	responseRecorder *ResponseRecorder
	format           string
	mapPool          *sync.Pool
	replacePool      *sync.Pool
}

func recordLogging(r *http.Request, code int, start time.Time, duration int64) {
	ctx := getRequestContext(r)
	SendMsg := func(topic, key string, value string) {
		sender, err := bus.GetMessageSender()
		if err != nil {
			helper.ErrorIf(err, "SendMsg: send msg to kafka failed")
			return
		}

		msg := &msg.Message{
			Topic:   topic,
			Key:     key,
			ErrChan: nil,
			Value:   []byte(value),
		}
		err = sender.AsyncSend(msg)
		if err != nil {
			helper.ErrorIf(err, "SendMsg: Send msg failed")
			return
		}
	}

	if ctx.BucketInfo == nil && ctx.ObjectName == "" {
		return
	}

	if ctx.BucketInfo.BucketLogging.LoggingEnabled.TargetBucket != "" &&
		ctx.BucketInfo.BucketLogging.LoggingEnabled.TargetPrefix != "" {

		requesttime := start.Format("2006-01-02 15:04:05.000000")

		requesturl := "-"
		if r.RequestURI != "" {
			requesturl = r.RequestURI
		}

		useragent := "-"
		if r.UserAgent() != "" {
			useragent = r.UserAgent()
		}

		str := fmt.Sprintf("%s,%s,%s,%s,%s,%s,%d,%d,%d,%s\n", requesttime,
			ctx.BucketInfo.OwnerId,
			ctx.BucketName,
			r.RemoteAddr,
			r.Method,
			requesturl,
			200,
			r.ContentLength,
			duration,
			useragent)
		SendMsg("LoggingTopic", ctx.BucketName, str)
	}

}
func (a AccessLogHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.responseRecorder = NewResponseRecorder(w)

	startTime := time.Now()
	a.handler.ServeHTTP(a.responseRecorder, r)
	finishTime := time.Now()
	a.responseRecorder.requestTime = finishTime.Sub(startTime)

	valueMap := a.mapPool.Get().(map[string]string)
	newReplacer := NewReplacer(r, a.responseRecorder, "-", valueMap, a.replacePool)
	defer func() {
		for k := range valueMap {
			delete(valueMap, k)
		}
		a.mapPool.Put(valueMap)
	}()
	response := newReplacer.Replace(a.format)

	helper.AccessLogger.Println(response)
	tstart := time.Now()
	// send the entrys in access logger to message bus.
	elems := newReplacer.GetReplacedValues()
	a.notify(elems)
	tend := time.Now()
	dur := tend.Sub(tstart).Nanoseconds() / 1000000
	if dur >= 100 {
		helper.Logger.Warn(r.Context(), fmt.Sprintf("slow log: access_log_handler_notify(%s) spent total %d", response, dur))
	}
	recordLogging(r, a.responseRecorder.status, tstart, dur)
}

func (a AccessLogHandler) notify(elems map[string]string) {
	if !helper.CONFIG.MsgBus.Enabled {
		return
	}
	if len(elems) == 0 {
		return
	}
	val, err := helper.MsgPackMarshal(elems)
	if err != nil {
		helper.Logger.Error(nil,
			fmt.Sprintf("Failed to send message [%v] to message bus, err: %v",
				elems, err))
		return
	}

	sender, err := bus.GetMessageSender()
	if err != nil {
		helper.Logger.Error(nil, "failed to get message bus sender, err:", err)
		return
	}

	// send the message to message bus async.
	// don't set the ErrChan.
	msg := &types.Message{
		Topic:   helper.CONFIG.MsgBus.Topic,
		Key:     "",
		ErrChan: nil,
		Value:   val,
	}

	err = sender.AsyncSend(msg)
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("failed to send message [%v] to message bus, err: %v", elems, err))
		return
	}
	helper.Logger.Info(nil, fmt.Sprintf("Succeed to send message [%v] to message bus.", elems))
}

func NewAccessLogHandler(handler http.Handler, _ *meta.Meta) http.Handler {
	return AccessLogHandler{
		handler: handler,
		format:  CombinedLogFormat,
		mapPool: &sync.Pool{
			New: func() interface{} {
				return make(map[string]string)
			},
		},
		replacePool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 256)
			},
		},
	}
}

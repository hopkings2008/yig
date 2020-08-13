package perf

import (
	"sort"
	"sync"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
	. "gopkg.in/check.v1"
)

type PartInfo struct {
	BucketName string
	ObjectName string
	UploadId   string
	PartNum    int64
	Buf        []byte
}

type PartResult struct {
	PartNum int64
	Etag    string
	Err     error
}

type ByPartNum []PartResult

func (p ByPartNum) Len() int           { return len(p) }
func (p ByPartNum) Less(i, j int) bool { return p[i].PartNum < p[j].PartNum }
func (p ByPartNum) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (ps *PerfSuite) TestMultipartLimitation(c *C) {
	bn := "multiparttests"
	key := "objt1"
	endpoint := "http://obs-langfang.wocloud.cn"
	ak := "HVYTQKLDRr4GTIph1UJL"
	sk := "i8Xvm01YPlXVBcVWxz34PQDhp0fl9uhmi3yGUt7f"
	region := "cn-bj-1"
	sc := NewS3WithCred(endpoint, ak, sk, region)
	//sc := NewS3()
	sc.MakeBucket(bn)
	defer sc.DeleteBucket(bn)

	randUtil := &RandUtil{}
	numParts := 10000
	uploadId, err := sc.CreateMultiPartUpload(bn, key, s3.ObjectStorageClassStandard)
	c.Assert(err, Equals, nil)
	c.Logf("uploadId: %s", uploadId)
	bufchan := make(chan PartInfo)
	resultChan := make(chan PartResult)
	wg := &sync.WaitGroup{}

	for i := 0; i < 4; i++ {
		ps.uploadPart(sc, bufchan, resultChan, wg)
	}

	go func() {
		buf := randUtil.RandBytes(5 << 20)
		for i := 1; i <= numParts; i++ {
			bufchan <- PartInfo{
				BucketName: bn,
				ObjectName: key,
				UploadId:   uploadId,
				PartNum:    int64(i),
				Buf:        buf,
			}
		}
		close(bufchan)
		wg.Wait()
		close(resultChan)
	}()

	var results []PartResult
	for pr := range resultChan {
		if pr.Err == nil {
			c.Logf("succeed to upload part(%d) with etag: %s", pr.PartNum, pr.Etag)
			results = append(results, pr)
			continue
		}
		c.Logf("failed to upload part(%d) with err: %v", pr.PartNum, pr.Err)
	}

	if len(results) != numParts {
		c.Logf("failed to perfporm upload part.")
		sc.AbortMultiPartUpload(bn, key, uploadId)
		return
	}

	sort.Sort(ByPartNum(results))

	completed := &s3.CompletedMultipartUpload{}
	for _, pr := range results {
		cp := &s3.CompletedPart{
			ETag:       aws.String(pr.Etag),
			PartNumber: aws.Int64(pr.PartNum),
		}
		completed.Parts = append(completed.Parts, cp)
	}

	err = sc.CompleteMultiPartUpload(bn, key, uploadId, completed)
	c.Assert(err, Equals, nil)
	err = sc.DeleteObject(bn, key)
	c.Assert(err, Equals, nil)
}

func (ps *PerfSuite) uploadPart(sc *S3Client, bufchan <-chan PartInfo, resultChan chan<- PartResult, wg *sync.WaitGroup) {
	go func(bufchan <-chan PartInfo) {
		defer func() {
			wg.Done()
		}()
		wg.Add(1)
		for pi := range bufchan {
			etag, err := sc.UploadPart(pi.BucketName, pi.ObjectName, pi.Buf, pi.UploadId, pi.PartNum)
			resultChan <- PartResult{
				PartNum: pi.PartNum,
				Etag:    etag,
				Err:     err,
			}
		}
	}(bufchan)
}

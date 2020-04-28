package storage

import (
	"context"
	"fmt"
	"io"

	errs "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/types"
)

/*
* MultipartReader: implements the io.ReadCloser
* it will read the data for the whole object.
 */
type MultipartReader struct {
	// context
	ctx context.Context
	// start offset for the object
	Offset int64
	// total length for the object
	Len int64
	// object meta data. Parts and Sse settings will be used.
	ObjectMeta *types.Object
	// backend storage io
	Yig *YigStorage
	// encrypted key
	EncryptionKey []byte
}

func (mr *MultipartReader) Read(p []byte) (int, error) {
	totalParts := len(mr.ObjectMeta.Parts)
	if totalParts == 0 || mr.Len <= 0 {
		return 0, io.EOF
	}
	lastPart := mr.ObjectMeta.Parts[totalParts-1]
	if lastPart.Offset+lastPart.Size < mr.Offset {
		return 0, io.EOF
	}
	// total length to read this time.
	total := int64(len(p))
	if total > mr.Len {
		total = mr.Len
	}
	// readNum records the postion in buffer where the data should be put.
	// also it contains the number of read bytes.
	readNum := 0
	// get the first part for the current mr.Offset.
	// mr.ObjectMeta.Parts[low].Offset <= mr.Offset
	low := mr.ObjectMeta.PartsIndex.SearchLowerBound(mr.Offset)
	if low == -1 {
		low = 1
	} else {
		low += 1
	}

	// currently, all the parts are in the same cluster...
	cephCluster, ok := mr.Yig.DataStorage[mr.ObjectMeta.Location]
	if !ok {
		helper.Logger.Error(mr.ctx, fmt.Sprintf("cannot get the ceph cluster for object(%s/%s) with location %s",
			mr.ObjectMeta.BucketName, mr.ObjectMeta.Name, mr.ObjectMeta.Location))
		return 0, errs.ErrInvalidObjectName
	}

	for i := low; i < totalParts; i++ {
		part := mr.ObjectMeta.Parts[i]
		// already finish to read the data
		if part.Offset > mr.Offset+mr.Len {
			return readNum, nil
		}
		// define the offset to read at this time.
		offset := mr.Offset - part.Offset
		// define the length to read at this time.
		size := total - int64(readNum)
		if size > part.Size {
			size = part.Size
		}

		var reader io.ReadCloser
		var err error
		// for unencrytped data.
		if mr.ObjectMeta.SseType == "" {
			reader, err = generatePartObjectReader(cephCluster, mr.ObjectMeta,
				part, offset, size)
			if err != nil {
				helper.Logger.Error(mr.ctx, fmt.Sprintf("failed to get reader for object(%s/%s) from %d with length %d, err: %v",
					mr.ObjectMeta.BucketName, mr.ObjectMeta.Name, offset, size, err))
				return readNum, err
			}
		} else { // process the encrypted data
			alignedReader, err := cephCluster.getAlignedReader(mr.ObjectMeta.Pool, part.ObjectId,
				offset, size)
			if err != nil {
				helper.Logger.Error(mr.ctx, fmt.Sprintf("failed to get aligned reader for object(%s/%s), err: %v",
					mr.ObjectMeta.BucketName, mr.ObjectMeta.Name, err))
				return 0, err
			}

			decryptedReader, err := wrapAlignedEncryptionReader(alignedReader, offset,
				mr.EncryptionKey, part.InitializationVector)
			if err != nil {
				helper.Logger.Error(mr.ctx, fmt.Sprintf("failed to get decrypted reader for object(%s/%s), err: %v",
					mr.ObjectMeta.BucketName, mr.ObjectMeta.Name, err))
				return 0, err
			}
			reader = &DecryptionReader{
				AlignedReader: alignedReader,
				DecryptReader: decryptedReader,
			}
		}
		n, err := reader.Read(p[readNum:])
		reader.Close()
		if err != nil {
			if err == io.EOF {
				mr.Offset += int64(n)
				mr.Len -= int64(n)
				readNum += n
				if int64(readNum) < total {
					continue
				}
				return readNum, err
			}
			helper.Logger.Error(mr.ctx, fmt.Sprintf("failed to read(%s/%s) from %d with length %d, err: %v",
				mr.ObjectMeta.BucketName, mr.ObjectMeta.Name, offset, size, err))
			return readNum, err
		}
		mr.Offset += int64(n)
		mr.Len -= int64(n)
		readNum += n
		if int64(readNum) >= total {
			break
		}
	}
	return readNum, nil
}

func (mr *MultipartReader) Close() error {
	// needs nothing to do.
	return nil
}

type DecryptionReader struct {
	AlignedReader io.ReadCloser
	DecryptReader io.Reader
}

func (dr *DecryptionReader) Read(p []byte) (int, error) {
	return dr.DecryptReader.Read(p)
}

func (dr *DecryptionReader) Close() error {
	return dr.AlignedReader.Close()
}

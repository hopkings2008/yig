package test

import "io"

type RandomReader struct {
	chunkSize int
	offset    int64
	size      int64
	buf       []byte
	bufOffset int
}

func (rr *RandomReader) Read(p []byte) (int, error) {
	pOffset := 0
	pLen := len(p)
	totalRead := 0

	for pOffset < pLen {
		if rr.offset >= rr.size {
			return totalRead, io.EOF
		}
		if rr.bufOffset >= rr.chunkSize {
			rr.buf = RandBytes(rr.chunkSize)
			rr.bufOffset = 0
		}

		dataLen := int(rr.size - rr.offset)
		if dataLen > rr.chunkSize-rr.bufOffset {
			dataLen = rr.chunkSize - rr.bufOffset
		}

		n := copy(p[pOffset:], rr.buf[rr.bufOffset:rr.bufOffset+dataLen])
		pOffset += n
		rr.bufOffset += n
		rr.offset += int64(n)
		totalRead += n
	}

	return totalRead, nil
}

func (rr *RandomReader) Close() error {
	return nil
}

func NewRandomReader(size int64) *RandomReader {
	return &RandomReader{
		offset:    0,
		size:      size,
		chunkSize: 64 << 20,
		buf:       RandBytes(64 << 20),
		bufOffset: 0,
	}
}

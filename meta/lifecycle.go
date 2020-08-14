package meta

import (
	"context"

	. "github.com/journeymidnight/yig/meta/types"
)

func (m *Meta) PutBucketToLifeCycle(ctx context.Context, bucket *Bucket) error {
	return m.Client.PutBucketToLifeCycle(ctx, bucket)
}

func (m *Meta) RemoveBucketFromLifeCycle(ctx context.Context, bucket *Bucket) error {
	return m.Client.RemoveBucketFromLifeCycle(ctx, bucket)
}

func (m *Meta) ScanLifeCycle(ctx context.Context, limit int, marker string) (result ScanLifeCycleResult, err error) {
	return m.Client.ScanLifeCycle(ctx, limit, marker)
}

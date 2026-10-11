package gateway

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const defaultMeteringCommitBatch = 32
const defaultMeteringCommitInterval = 100 * time.Millisecond

func meteringCommitOptions() (int, time.Duration, error) {
	read := func(name string, fallback, min, max int) (int, error) {
		if os.Getenv(name) == "" {
			return fallback, nil
		}
		n, err := strconv.Atoi(os.Getenv(name))
		if err != nil || n < min || n > max {
			return 0, fmt.Errorf("%s requires %d..%d", name, min, max)
		}
		return n, nil
	}
	n, err := read("GW_METERING_COMMIT_BATCH_SIZE", defaultMeteringCommitBatch, 1, 256)
	if err != nil {
		return 0, 0, err
	}
	ms, err := read("GW_METERING_COMMIT_INTERVAL_MS", 100, 10, 1000)
	return n, time.Duration(ms) * time.Millisecond, err
}

// 同一分区串行处理。只有所有结算步骤成功，才能推进待提交位置；提交失败停止继续读取。
func consumeSettledPartition(c context.Context, start int64, batchSize int, interval time.Duration,
	fetch func(context.Context) (kafka.Message, error),
	apply func(context.Context, int64, kafka.Message) error,
	commit func(context.Context, int64, int) error,
) error {
	if batchSize < 1 || interval <= 0 || start < 0 {
		return ErrCheckpoint
	}
	cursor, pending := start, 0
	var deadline time.Time
	flush := func(ctx context.Context) error {
		if pending == 0 {
			return nil
		}
		bounded, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := commit(bounded, cursor, pending); err != nil {
			return err
		}
		pending = 0
		deadline = time.Time{}
		return nil
	}
	defer func() {
		if c.Err() != nil {
			// 正常停机尽力保存安全位置；代次已结束时 broker 拒绝，下一 owner 幂等重放。
			_ = flush(context.WithoutCancel(c))
		}
	}()
	for {
		if c.Err() != nil {
			return c.Err()
		}
		if pending > 0 && (pending >= batchSize || !time.Now().Before(deadline)) {
			if err := flush(c); err != nil {
				return err
			}
		}
		read := c
		cancel := func() {}
		if pending > 0 {
			read, cancel = context.WithDeadline(c, deadline)
		}
		m, err := fetch(read)
		readErr := read.Err()
		cancel()
		if err != nil {
			if c.Err() == nil && errors.Is(err, context.DeadlineExceeded) && readErr != nil && pending > 0 {
				if err := flush(c); err != nil {
					return err
				}
				continue
			}
			return err
		}
		if m.Offset < cursor {
			return ErrCheckpoint
		}
		if err := apply(c, cursor, m); err != nil {
			return err
		}
		cursor = m.Offset + 1
		if pending == 0 {
			deadline = time.Now().Add(interval)
		}
		pending++
	}
}

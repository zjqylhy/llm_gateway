package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestSettledPartitionIdleCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	committed := make(chan int64, 1)
	done := make(chan error, 1)
	reads := 0
	go func() {
		done <- consumeSettledPartition(ctx, 10, 32, 20*time.Millisecond, func(c context.Context) (kafka.Message, error) {
			reads++
			if reads == 1 {
				return kafka.Message{Offset: 10}, nil
			}
			<-c.Done()
			return kafka.Message{}, c.Err()
		}, func(context.Context, int64, kafka.Message) error { return nil }, func(c context.Context, next int64, count int) error {
			if c.Err() != nil || count != 1 {
				return ErrCheckpoint
			}
			committed <- next
			return nil
		})
	}()
	select {
	case next := <-committed:
		if next != 11 {
			t.Fatal("idle partition committed wrong position", next)
		}
	case <-time.After(time.Second):
		t.Fatal("idle partition left a settled tail uncommitted")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSettledPartitionStopsAtFailedRecordOrCommit(t *testing.T) {
	for _, commitFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "settlement_failure", true: "commit_failure"}[commitFails], func(t *testing.T) {
			failure := errors.New("injected persistence failure")
			reads, applied, commits := 0, 0, 0
			err := consumeSettledPartition(context.Background(), 20, 2, time.Second, func(context.Context) (kafka.Message, error) {
				m := kafka.Message{Offset: 20 + int64(reads)}
				reads++
				return m, nil
			}, func(_ context.Context, cursor int64, m kafka.Message) error {
				if m.Offset != cursor {
					t.Fatal("cursor skipped an unprocessed record", cursor, m.Offset)
				}
				if m.Offset == 22 {
					return failure
				}
				applied++
				return nil
			}, func(_ context.Context, next int64, count int) error {
				commits++
				if next != 22 || count != 2 || applied != 2 {
					t.Fatal("commit included an unsettled record", next, count, applied)
				}
				if commitFails {
					return failure
				}
				return nil
			})
			wantReads := 3
			if commitFails {
				wantReads = 2
			}
			if !errors.Is(err, failure) || reads != wantReads || commits != 1 {
				t.Fatal("consumer continued beyond failure", err, reads, commits)
			}
		})
	}
}

func TestSettledPartitionShutdownExcludesPartialRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reads, next, count := 0, int64(-1), 0
	err := consumeSettledPartition(ctx, 20, 32, time.Second, func(context.Context) (kafka.Message, error) {
		m := kafka.Message{Offset: 20 + int64(reads)}
		reads++
		return m, nil
	}, func(_ context.Context, _ int64, m kafka.Message) error {
		if m.Offset == 21 {
			cancel()
			return context.Canceled
		}
		return nil
	}, func(c context.Context, position int64, records int) error {
		if c.Err() != nil {
			t.Fatal("shutdown commit inherited cancelled context")
		}
		next, count = position, records
		return nil
	})
	if !errors.Is(err, context.Canceled) || next != 21 || count != 1 {
		t.Fatal("shutdown committed the partial record", err, next, count)
	}
}

func TestMeteringCommitConfigurationBounds(t *testing.T) {
	t.Setenv("GW_METERING_COMMIT_BATCH_SIZE", "")
	t.Setenv("GW_METERING_COMMIT_INTERVAL_MS", "")
	n, interval, err := meteringCommitOptions()
	if err != nil || n != 32 || interval != 100*time.Millisecond {
		t.Fatal(n, interval, err)
	}
	for _, option := range []struct{ name, value string }{
		{"GW_METERING_COMMIT_BATCH_SIZE", "0"}, {"GW_METERING_COMMIT_BATCH_SIZE", "257"},
		{"GW_METERING_COMMIT_INTERVAL_MS", "9"}, {"GW_METERING_COMMIT_INTERVAL_MS", "1001"},
	} {
		t.Run(option.name+option.value, func(t *testing.T) {
			t.Setenv(option.name, option.value)
			if _, _, err := meteringCommitOptions(); err == nil {
				t.Fatal("unsafe commit configuration accepted")
			}
		})
	}
}

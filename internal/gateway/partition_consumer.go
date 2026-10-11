package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func decodeMetering(m kafka.Message) (Event, error) {
	var e Event
	if len(m.Value) > maxEventBytes {
		return e, ErrCheckpoint
	}
	if err := json.Unmarshal(m.Value, &e); err != nil {
		return e, fmt.Errorf("invalid metering record in partition %d at %d", m.Partition, m.Offset)
	}
	return e, CheckEvent(e)
}
func settleRedis(c context.Context, g Guard, e Event) error {
	if redisGuard, ok := g.(RedisGuard); ok {
		redisGuard.Replay = true
		g = redisGuard
	}
	if e.Kind == "started" {
		if err := g.Reserve(c, App{ID: e.AppID}, Reservation{ID: e.AttemptID, AppID: e.AppID, Month: e.Month, Tokens: e.ReserveTokens, Micro: e.ReserveMicro}); err != nil {
			return err
		}
		if rg, ok := g.(RedisGuard); ok {
			if err := stampReservation(c, rg, e); err != nil {
				return err
			}
		}
	} else if e.Purpose != "request" {
		if err := g.Settle(c, e); err != nil {
			return err
		}
	}
	return nil
}
func settleMetering(c context.Context, s *SQLStore, g Guard, e Event) error {
	start := time.Now()
	err := settleRedis(c, g, e)
	s.Metrics.meteringStage("redis_settle", meteringEventKind(e), start, err)
	if err != nil {
		return err
	}
	start = time.Now()
	err = s.DeliveredOutbox(c, e)
	s.Metrics.meteringStage("outbox_deliver", meteringEventKind(e), start, err)
	return err
}
func (r *Runtime) freezeConsumer(c context.Context) {
	if c.Err() != nil {
		return
	}
	ctx, done := context.WithTimeout(context.WithoutCancel(c), time.Second)
	_ = r.Engine.Store.(*SQLStore).FreezeMetering(ctx, "consumer_recovery_required")
	done()
	ctx, done = context.WithTimeout(context.WithoutCancel(c), time.Second)
	_ = r.Redis.Set(ctx, "gw:metering:frozen", "consumer_recovery_required", 0).Err()
	done()
	ctx, done = context.WithTimeout(context.WithoutCancel(c), time.Second)
	r.freezeEtcd(ctx)
	done()
}
func (r *Runtime) commitPositions(c context.Context, generation int, member string, offsets []kafka.OffsetCommit) (result error) {
	start := time.Now()
	defer func() { r.Engine.Metrics.meteringStage("offset_commit", "other", start, result) }()
	res, err := r.KafkaClient.OffsetCommit(c, &kafka.OffsetCommitRequest{GroupID: r.readerConfig.GroupID, GenerationID: generation, MemberID: member, Topics: map[string][]kafka.OffsetCommit{r.readerConfig.Topic: offsets}})
	if err != nil {
		return err
	}
	if len(res.Topics[r.readerConfig.Topic]) != len(offsets) {
		return ErrCheckpoint
	}
	for _, p := range res.Topics[r.readerConfig.Topic] {
		if p.Error != nil {
			return p.Error
		}
	}
	return nil
}

func (r *Runtime) ensureGroupStopped(c context.Context) error {
	groups, err := r.KafkaClient.DescribeGroups(c, &kafka.DescribeGroupsRequest{GroupIDs: []string{r.readerConfig.GroupID}})
	if err != nil {
		return err
	}
	if len(groups.Groups) != 1 {
		return ErrCheckpoint
	}
	group := groups.Groups[0]
	if group.Error != nil {
		if errors.Is(group.Error, kafka.GroupIdNotFound) {
			return nil
		}
		return group.Error
	}
	if len(group.Members) > 0 {
		return errors.New("normal metering consumer group still has active members")
	}
	return nil
}

// 显式管理 Kafka generation，分区 reader 不另行加入消费组。
func (r *Runtime) ConsumeMetering(parent context.Context) (result error) {
	// database/sql 的上下文取消可能表现为 ErrTxDone，正常停机统一返回父上下文错误。
	defer func() {
		if parent.Err() != nil && errors.Is(result, sql.ErrTxDone) {
			result = parent.Err()
		}
	}()
	s := r.Engine.Store.(*SQLStore)
	source, _, err := s.Checkpoints(parent)
	if err != nil {
		if parent.Err() == nil {
			r.freezeConsumer(parent)
		}
		return err
	}
	actual, _, err := r.KafkaRanges(parent)
	if err != nil {
		if parent.Err() == nil {
			r.freezeConsumer(parent)
		}
		return err
	}
	if !sameSource(source, actual) {
		r.freezeConsumer(parent)
		return ErrCheckpoint
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	cfg := r.readerConfig
	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{ID: cfg.GroupID, Brokers: cfg.Brokers, Dialer: cfg.Dialer, Topics: []string{cfg.Topic}, StartOffset: kafka.FirstOffset})
	if err != nil {
		return err
	}
	defer group.Close()
	closed := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = group.Close()
		case <-closed:
		}
	}()
	defer close(closed)
	failures := make(chan error, 1)
	for {
		generation, nextErr := group.Next(ctx)
		if nextErr != nil {
			select {
			case result = <-failures:
				return result
			default:
			}
			return nextErr
		}
		for _, assignment := range generation.Assignments[cfg.Topic] {
			assignment := assignment
			generation.Start(func(genCtx context.Context) {
				partCtx, stop := context.WithCancel(ctx)
				detach := context.AfterFunc(genCtx, stop)
				defer detach()
				defer stop()
				err := r.consumeAssignment(partCtx, generation, assignment)
				// generation 已结束时 AfterFunc 可能尚未传播取消；不把取消事务误判为财务故障。
				if err != nil && !errors.Is(err, ErrFenced) && partCtx.Err() == nil && genCtx.Err() == nil && parent.Err() == nil {
					r.freezeConsumer(partCtx)
					select {
					case failures <- err:
					default:
					}
					cancel()
				}
			})
		}
	}
}
func (r *Runtime) consumeAssignment(c context.Context, generation *kafka.Generation, assignment kafka.PartitionAssignment) error {
	s := r.Engine.Store.(*SQLStore)
	// 重平衡期间提交位置可能变化，取得所有权后重新检查，禁止静默跳过旧备份。
	p, err := s.ClaimPartition(c, assignment.ID, generation.ID, generation.MemberID, false)
	if err != nil {
		return err
	}
	start := assignment.Offset
	if start < 0 {
		if p.Next != 0 {
			return ErrCheckpoint
		}
		start = 0
	}
	if start > p.Next {
		return fmt.Errorf("%w: partition %d group ahead of SQL", ErrCheckpoint, p.Partition)
	}
	gen, err := r.generation(c)
	if err != nil {
		return err
	}
	if err = r.advanceWatermark(c, gen, p.Partition, start, start); err != nil {
		return err
	}
	actual, ranges, err := r.KafkaRanges(c)
	if err != nil {
		return err
	}
	source, _, err := s.Checkpoints(c)
	if err != nil {
		return err
	}
	if !sameSource(source, actual) {
		return ErrCheckpoint
	}
	if start < ranges[p.Partition].First {
		return ErrCheckpoint
	}
	if err = r.verifyAnchor(c, p, ranges[p.Partition]); err != nil {
		return err
	}
	// 空分区也记录明确基线；不能让未提交的 -1 在后续审计中冒充水位。
	if err = r.commitPositions(c, int(generation.ID), generation.MemberID, []kafka.OffsetCommit{{Partition: p.Partition, Offset: start}}); err != nil {
		return err
	}
	reader, err := r.partitionReader(p.Partition, start)
	if err != nil {
		return err
	}
	defer reader.Close()
	batchSize, interval := r.commitBatchSize, r.commitInterval
	if batchSize == 0 {
		batchSize, interval = defaultMeteringCommitBatch, defaultMeteringCommitInterval
	}
	err = consumeSettledPartition(c, start, batchSize, interval, reader.FetchMessage, func(ctx context.Context, cursor int64, m kafka.Message) error {
		e, err := decodeMetering(m)
		if err != nil {
			return err
		}
		if err = verifyRecordSource(source, p, m); err != nil {
			return err
		}
		step := time.Now()
		err = s.ApplyPartition(ctx, p, cursor, m, e, false)
		r.Engine.Metrics.meteringStage("ledger_apply", meteringEventKind(e), step, err)
		if err != nil {
			return err
		}
		if err = settleMetering(ctx, s, r.Engine.Guard, e); err != nil {
			return err
		}
		step = time.Now()
		err = r.advanceWatermark(ctx, gen, p.Partition, cursor, m.Offset+1)
		r.Engine.Metrics.meteringStage("settlement_watermark", meteringEventKind(e), step, err)
		if err != nil {
			return err
		}
		return nil
	}, func(ctx context.Context, next int64, count int) error {
		err := r.commitPositions(ctx, int(generation.ID), generation.MemberID, []kafka.OffsetCommit{{Partition: p.Partition, Offset: next}})
		if err == nil {
			r.Engine.Metrics.MeteringCommitBatch.Observe(float64(count))
		}
		return err
	})
	// 重平衡仅结束本代消费；新 owner 从已提交位置幂等补结算。
	if errors.Is(err, kafka.RebalanceInProgress) || errors.Is(err, kafka.IllegalGeneration) || errors.Is(err, kafka.UnknownMemberId) {
		return nil
	}
	return err
}

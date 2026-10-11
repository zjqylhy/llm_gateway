package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/segmentio/kafka-go"
	"os"
)

// Redis 配额不能在数据丢失后静默归零。首次初始化只允许空账本。
func InitializeQuota(ctx context.Context, e *Engine) error {
	g, ok := e.Guard.(RedisGuard)
	if !ok {
		return nil
	}
	if state, err := g.Client.Get(ctx, "gw:quota:state").Result(); err == nil && state == "ready" {
		return nil
	}
	s := e.Store.(*SQLStore)
	cfg, err := s.Load(ctx)
	if err != nil {
		return err
	}
	// 未消费的 started 事件可能尚未落库；已有应用或 Key 时不能以空 SQL 表证明没有费用。
	if len(cfg.Apps) > 0 || len(cfg.Keys) > 0 {
		return errors.New("existing application configuration requires offline quota-rebuild")
	}
	var n int
	if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM gw_attempts WHERE purpose<>'request'").Scan(&n); err != nil {
		return err
	}
	if n != 0 {
		return errors.New("existing ledger requires offline quota-rebuild; counters will not be reset")
	}
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM gw_metering_outbox WHERE delivered=0`).Scan(&n); err != nil {
		return err
	}
	if n != 0 {
		return errors.New("existing outbox requires offline quota-rebuild")
	}
	if err = initializeMeteringEvidence(ctx, s, g); err != nil {
		return err
	}
	return g.Client.Set(ctx, "gw:quota:state", "ready", 0).Err()
}
func RebuildQuota(ctx context.Context, r *Runtime) error {
	if os.Getenv("GW_QUOTA_REBUILD_ACK") != "writers-stopped" {
		return errors.New("stop all writers and set GW_QUOTA_REBUILD_ACK=writers-stopped")
	}
	// SQL 检查点领先于组位置也从账本水位回放；旧 SQL 备份禁止沿用组 offset。
	s := r.Engine.Store.(*SQLStore)
	rows, err := s.DB.QueryContext(ctx, `SELECT manifest FROM gw_metering_recovery WHERE phase<>'complete' LIMIT 2`)
	if err != nil {
		return err
	}
	var pending []RecoveryManifest
	for rows.Next() {
		var raw []byte
		var m RecoveryManifest
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		if err = json.Unmarshal(raw, &m); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, m)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(pending) > 1 {
		return errors.New("multiple unfinished recoveries require manual review")
	}
	if len(pending) == 1 {
		_, err = applyMeteringRecovery(ctx, r, pending[0])
		return err
	}
	plan, err := PlanMeteringRecovery(ctx, r)
	if err != nil {
		return err
	}
	_, err = applyMeteringRecovery(ctx, r, plan)
	return err
}
func KafkaCaughtUp(ctx context.Context, r *Runtime) (bool, error) {
	config := r.readerConfig
	topic := config.Topic
	metadata, err := r.KafkaClient.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return false, err
	}
	parts := []int{}
	requests := []kafka.OffsetRequest{}
	for _, t := range metadata.Topics {
		if t.Error != nil {
			return false, t.Error
		}
		for _, p := range t.Partitions {
			parts = append(parts, p.ID)
			requests = append(requests, kafka.LastOffsetOf(p.ID))
		}
	}
	if len(parts) == 0 {
		return false, errors.New("event topic has no partitions")
	}
	ends, err := r.KafkaClient.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: requests}})
	if err != nil {
		return false, err
	}
	commits, err := r.KafkaClient.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: config.GroupID, Topics: map[string][]int{topic: parts}})
	if err != nil {
		return false, err
	}
	if commits.Error != nil {
		return false, commits.Error
	}
	return offsetsCaughtUp(ends.Topics[topic], commits.Topics[topic]), nil
}
func offsetsCaughtUp(ends []kafka.PartitionOffsets, commits []kafka.OffsetFetchPartition) bool {
	if len(ends) == 0 {
		return false
	}
	seen := map[int]int64{}
	for _, p := range commits {
		if p.Error != nil {
			return false
		}
		seen[p.Partition] = p.CommittedOffset
	}
	for _, p := range ends {
		if p.Error != nil {
			return false
		}
		offset, ok := seen[p.Partition]
		if p.LastOffset > 0 && (!ok || offset < p.LastOffset) {
			return false
		}
	}
	return true
}

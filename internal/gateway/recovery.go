package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/segmentio/kafka-go"
	"os"
	"time"
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
	return g.Client.Set(ctx, "gw:quota:state", "ready", 0).Err()
}
func RebuildQuota(ctx context.Context, r *Runtime) error {
	if os.Getenv("GW_QUOTA_REBUILD_ACK") != "writers-stopped" {
		return errors.New("stop gateway/admin/worker writers, then set GW_QUOTA_REBUILD_ACK=writers-stopped")
	}
	s, ok := r.Engine.Store.(*SQLStore)
	if !ok {
		return errors.New("SQL ledger required")
	}
	rc := r.Redis
	if err := rc.Set(ctx, "gw:quota:state", "rebuilding", 0).Err(); err != nil {
		return err
	}
	var cursor uint64
	for {
		keys, next, err := rc.Scan(ctx, cursor, "gw:{app:*}", 500).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err = rc.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	// 先从 MySQL 最新 revision 重建，再让 Kafka 从原消费位置继续；幂等键防止重复结算。
	rows, err := s.DB.QueryContext(ctx, "SELECT payload FROM gw_attempts WHERE purpose<>'request'")
	if err != nil {
		return err
	}
	defer rows.Close()
	g := RedisGuard{Client: rc}
	for rows.Next() {
		var raw []byte
		var event Event
		if err = rows.Scan(&raw); err != nil {
			return err
		}
		if err = json.Unmarshal(raw, &event); err != nil {
			return err
		}
		if event.Kind == "started" {
			err = g.Reserve(ctx, App{ID: event.AppID}, Reservation{ID: event.AttemptID, AppID: event.AppID, Month: event.Month, Tokens: event.ReserveTokens, Micro: event.ReserveMicro})
		} else {
			err = g.Settle(ctx, event)
		}
		if err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return err
	}
	_ = rows.Close()
	// 停止写入时排空已持久化、尚未落库的事件，避免仅重建数据库造成漏算。
	for {
		fetchCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		message, fetchErr := r.Reader.FetchMessage(fetchCtx)
		cancel()
		if errors.Is(fetchErr, context.DeadlineExceeded) && ctx.Err() == nil {
			caughtUp, checkErr := KafkaCaughtUp(ctx, r)
			if checkErr != nil {
				return checkErr
			}
			if !caughtUp {
				return errors.New("Kafka partitions not fully drained; quota remains unavailable")
			}
			break
		}
		if fetchErr != nil {
			return fetchErr
		}
		var event Event
		if err = json.Unmarshal(message.Value, &event); err != nil {
			return err
		}
		if err = CheckEvent(event); err != nil {
			return err
		}
		if err = s.Apply(ctx, event); err != nil {
			return err
		}
		if event.Kind == "started" {
			err = g.Reserve(ctx, App{ID: event.AppID}, Reservation{ID: event.AttemptID, AppID: event.AppID, Month: event.Month, Tokens: event.ReserveTokens, Micro: event.ReserveMicro})
		} else if event.Purpose != "request" {
			err = g.Settle(ctx, event)
		}
		if err != nil {
			return err
		}
		if err = r.Reader.CommitMessages(ctx, message); err != nil {
			return err
		}
	}
	return rc.Set(ctx, "gw:quota:state", "ready", 0).Err()
}
func KafkaCaughtUp(ctx context.Context, r *Runtime) (bool, error) {
	topic := r.Reader.Config().Topic
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
	commits, err := r.KafkaClient.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: r.Reader.Config().GroupID, Topics: map[string][]int{topic: parts}})
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

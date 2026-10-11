package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

var ErrCheckpoint = errors.New("metering checkpoint requires offline recovery")
var ErrFenced = errors.New("metering partition ownership lost")

type StreamIdentity struct {
	Cluster    string `json:"cluster"`
	Topic      string `json:"topic"`
	Group      string `json:"group"`
	ID         string `json:"stream_id"`
	Partitions int    `json:"partitions"`
}
type PartitionCheckpoint struct {
	Partition  int    `json:"partition"`
	Base       int64  `json:"baseline"`
	Next       int64  `json:"next"`
	Anchor     int64  `json:"anchor"`
	Hash       string `json:"hash"`
	Epoch      int64  `json:"epoch"`
	Generation int32  `json:"generation"`
	Owner      string `json:"owner"`
}
type PartitionRange struct {
	Partition int   `json:"partition"`
	First     int64 `json:"first"`
	End       int64 `json:"end"`
	Committed int64 `json:"committed"`
}

func (s *SQLStore) Checkpoints(c context.Context) (StreamIdentity, []PartitionCheckpoint, error) {
	var source StreamIdentity
	// 同一快照读取来源与各分区，plan 不混用更新前后的检查点。
	tx, err := s.beginMeteringTx(c, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return source, nil, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(c, `SELECT cluster_id,topic,group_id,stream_id,partitions FROM gw_metering_stream WHERE id=1`).Scan(&source.Cluster, &source.Topic, &source.Group, &source.ID, &source.Partitions)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return source, nil, fmt.Errorf("%w: initialize checkpoints explicitly", ErrCheckpoint)
		}
		return source, nil, err
	}
	if source.Partitions < 1 || source.Partitions > 128 || source.ID == "" {
		return source, nil, ErrCheckpoint
	}
	rows, err := tx.QueryContext(c, `SELECT partition_id,base_offset,next_offset,anchor_offset,anchor_hash,owner_epoch,kafka_generation,owner_id FROM gw_metering_checkpoint ORDER BY partition_id LIMIT 129`)
	if err != nil {
		return source, nil, err
	}
	defer rows.Close()
	var parts []PartitionCheckpoint
	for rows.Next() {
		var p PartitionCheckpoint
		if err = rows.Scan(&p.Partition, &p.Base, &p.Next, &p.Anchor, &p.Hash, &p.Epoch, &p.Generation, &p.Owner); err != nil {
			return source, nil, err
		}
		parts = append(parts, p)
		if p.Partition != len(parts)-1 || p.Base < 0 || p.Next < p.Base {
			return source, nil, ErrCheckpoint
		}
	}
	if err = rows.Err(); err != nil {
		return source, nil, err
	}
	if len(parts) != source.Partitions {
		return source, nil, ErrCheckpoint
	}
	return source, parts, tx.Commit()
}

func (r *Runtime) KafkaRanges(c context.Context) (StreamIdentity, []PartitionRange, error) {
	cfg := r.readerConfig
	source := StreamIdentity{Topic: cfg.Topic, Group: cfg.GroupID}
	metadata, err := r.KafkaClient.Metadata(c, &kafka.MetadataRequest{Topics: []string{cfg.Topic}})
	if err != nil {
		return source, nil, err
	}
	source.Cluster = metadata.ClusterID
	if source.Cluster == "" || len(metadata.Topics) != 1 {
		return source, nil, ErrCheckpoint
	}
	topic := metadata.Topics[0]
	if topic.Error != nil {
		return source, nil, topic.Error
	}
	if topic.Name != cfg.Topic || len(topic.Partitions) < 1 || len(topic.Partitions) > 128 {
		return source, nil, ErrCheckpoint
	}
	source.Partitions = len(topic.Partitions)
	configs, err := r.KafkaClient.DescribeConfigs(c, &kafka.DescribeConfigsRequest{Resources: []kafka.DescribeConfigRequestResource{{ResourceType: kafka.ResourceTypeTopic, ResourceName: cfg.Topic, ConfigNames: []string{"cleanup.policy"}}}})
	if err != nil {
		return source, nil, err
	}
	if len(configs.Resources) != 1 || configs.Resources[0].Error != nil {
		return source, nil, ErrCheckpoint
	}
	deleteOnly := false
	for _, entry := range configs.Resources[0].ConfigEntries {
		if entry.ConfigName == "cleanup.policy" && entry.ConfigValue == "delete" {
			deleteOnly = true
		}
	}
	if !deleteOnly {
		return source, nil, errors.New("metering topic must use cleanup.policy=delete; compaction destroys replay provenance")
	}
	ids := []int{}
	first, last := []kafka.OffsetRequest{}, []kafka.OffsetRequest{}
	for _, p := range topic.Partitions {
		if p.Error != nil {
			return source, nil, p.Error
		}
		ids = append(ids, p.ID)
		first = append(first, kafka.FirstOffsetOf(p.ID))
		last = append(last, kafka.LastOffsetOf(p.ID))
	}
	sort.Ints(ids)
	for i, id := range ids {
		if i != id {
			return source, nil, ErrCheckpoint
		}
	}
	starts, err := r.KafkaClient.ListOffsets(c, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{cfg.Topic: first}})
	if err != nil {
		return source, nil, err
	}
	ends, err := r.KafkaClient.ListOffsets(c, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{cfg.Topic: last}})
	if err != nil {
		return source, nil, err
	}
	commits, err := r.KafkaClient.OffsetFetch(c, &kafka.OffsetFetchRequest{GroupID: cfg.GroupID, Topics: map[string][]int{cfg.Topic: ids}})
	if err != nil {
		return source, nil, err
	}
	if commits.Error != nil {
		return source, nil, commits.Error
	}
	positions := map[int]*PartitionRange{}
	for _, id := range ids {
		positions[id] = &PartitionRange{Partition: id, First: -1, End: -1, Committed: -1}
	}
	for _, p := range starts.Topics[cfg.Topic] {
		if p.Error != nil {
			return source, nil, p.Error
		}
		if positions[p.Partition] == nil {
			return source, nil, ErrCheckpoint
		}
		positions[p.Partition].First = p.FirstOffset
	}
	for _, p := range ends.Topics[cfg.Topic] {
		if p.Error != nil {
			return source, nil, p.Error
		}
		if positions[p.Partition] == nil {
			return source, nil, ErrCheckpoint
		}
		positions[p.Partition].End = p.LastOffset
	}
	for _, p := range commits.Topics[cfg.Topic] {
		if p.Error != nil {
			return source, nil, p.Error
		}
		if positions[p.Partition] == nil {
			return source, nil, ErrCheckpoint
		}
		positions[p.Partition].Committed = p.CommittedOffset
	}
	// 终点与提交位置不是原子快照；消费推进时重读终点，避免把正常进度误判为损坏。
	refresh := []kafka.OffsetRequest{}
	for _, id := range ids {
		if positions[id].Committed > positions[id].End {
			refresh = append(refresh, kafka.LastOffsetOf(id))
		}
	}
	if len(refresh) > 0 {
		latest, e := r.KafkaClient.ListOffsets(c, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{cfg.Topic: refresh}})
		if e != nil {
			return source, nil, e
		}
		for _, p := range latest.Topics[cfg.Topic] {
			if p.Error != nil {
				return source, nil, p.Error
			}
			if positions[p.Partition] == nil {
				return source, nil, ErrCheckpoint
			}
			positions[p.Partition].End = p.LastOffset
		}
	}
	parts := []PartitionRange{}
	for _, id := range ids {
		p := positions[id]
		if p.First < 0 || p.End < p.First || p.Committed > p.End {
			return source, nil, ErrCheckpoint
		}
		parts = append(parts, *p)
	}
	return source, parts, nil
}

func (r *Runtime) partitionReader(partition int, offset int64) (*kafka.Reader, error) {
	cfg := r.readerConfig
	cfg.GroupID = ""
	cfg.Partition = partition
	cfg.StartOffset = 0
	cfg.QueueCapacity = 1
	cfg.MaxWait = time.Second
	cfg.ReadBackoffMin = 10 * time.Millisecond
	cfg.ReadBackoffMax = 100 * time.Millisecond
	cfg.OffsetOutOfRangeError = true
	cfg.ReadLagInterval = -1
	reader := kafka.NewReader(cfg)
	if err := reader.SetOffset(offset); err != nil {
		reader.Close()
		return nil, err
	}
	return reader, nil
}
func recordHash(m kafka.Message) string {
	// 不包含 broker 时间戳；键、载荷和 headers 是消息不可变身份。
	raw, _ := json.Marshal(struct {
		Key, Value []byte
		Headers    []kafka.Header
	}{m.Key, m.Value, m.Headers})
	return Hash(string(raw))
}
func (r *Runtime) verifyAnchor(c context.Context, p PartitionCheckpoint, limits PartitionRange) error {
	if p.Next < limits.First || p.Next > limits.End {
		return fmt.Errorf("%w: partition %d offset outside retention", ErrCheckpoint, p.Partition)
	}
	if p.Anchor < 0 {
		if p.Next != 0 || limits.First != 0 {
			return ErrCheckpoint
		}
		return nil
	}
	if p.Anchor < limits.First || p.Anchor >= p.Next || p.Hash == "" {
		return fmt.Errorf("%w: partition %d anchor unavailable", ErrCheckpoint, p.Partition)
	}
	reader, err := r.partitionReader(p.Partition, p.Anchor)
	if err != nil {
		return err
	}
	defer reader.Close()
	ctx, done := context.WithTimeout(c, 5*time.Second)
	defer done()
	m, err := reader.FetchMessage(ctx)
	if err != nil {
		return err
	}
	if m.Offset != p.Anchor || recordHash(m) != p.Hash {
		return fmt.Errorf("%w: partition %d source fingerprint differs", ErrCheckpoint, p.Partition)
	}
	return nil
}
func sameSource(a, b StreamIdentity) bool {
	return a.Cluster == b.Cluster && a.Topic == b.Topic && a.Group == b.Group && a.Partitions == b.Partitions
}

// 存量基线之前的消息保留原格式；新消息必须携带持久化 stream 身份。
func verifyRecordSource(source StreamIdentity, p PartitionCheckpoint, m kafka.Message) error {
	if m.Offset < p.Base {
		return nil
	}
	count := 0
	for _, h := range m.Headers {
		if h.Key == "gw-stream-id" {
			count++
			if string(h.Value) != source.ID {
				return ErrCheckpoint
			}
		}
	}
	if count != 1 {
		return fmt.Errorf("%w: metering record source header missing or duplicated", ErrCheckpoint)
	}
	return nil
}

// Kafka 代次阻止迟到的旧 assignment 再次夺回 SQL 所有权；epoch 阻止旧事务提交。
func (s *SQLStore) ClaimPartition(c context.Context, part int, generation int32, owner string, recovery bool) (PartitionCheckpoint, error) {
	tx, err := s.beginMeteringTx(c, nil)
	if err != nil {
		return PartitionCheckpoint{}, err
	}
	defer tx.Rollback()
	var state string
	if err = tx.QueryRowContext(c, `SELECT state FROM gw_metering_control WHERE id=1 FOR SHARE`).Scan(&state); err != nil {
		return PartitionCheckpoint{}, err
	}
	if (state == "recovering") != recovery {
		return PartitionCheckpoint{}, ErrFenced
	}
	var p PartitionCheckpoint
	p.Partition = part
	err = tx.QueryRowContext(c, `SELECT base_offset,next_offset,anchor_offset,anchor_hash,owner_epoch,kafka_generation,owner_id FROM gw_metering_checkpoint WHERE partition_id=? FOR UPDATE`, part).Scan(&p.Base, &p.Next, &p.Anchor, &p.Hash, &p.Epoch, &p.Generation, &p.Owner)
	if err != nil {
		return p, err
	}
	if !recovery && (generation < p.Generation || (generation == p.Generation && p.Owner != "" && p.Owner != owner)) {
		return p, ErrFenced
	}
	p.Epoch++
	p.Generation = generation
	p.Owner = owner
	_, err = tx.ExecContext(c, `UPDATE gw_metering_checkpoint SET owner_epoch=?,kafka_generation=?,owner_id=? WHERE partition_id=?`, p.Epoch, p.Generation, p.Owner, part)
	if err != nil {
		return p, err
	}
	return p, tx.Commit()
}

func (s *SQLStore) ApplyPartition(c context.Context, p PartitionCheckpoint, cursor int64, m kafka.Message, event Event, recovery bool) error {
	if m.Partition != p.Partition || m.Offset < cursor || m.Offset < 0 {
		return ErrCheckpoint
	}
	tx, err := s.beginMeteringTx(c, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var state string
	if err = tx.QueryRowContext(c, `SELECT state FROM gw_metering_control WHERE id=1 FOR SHARE`).Scan(&state); err != nil {
		return err
	}
	if (state == "recovering") != recovery {
		return ErrFenced
	}
	var next, epoch int64
	var owner string
	if err = tx.QueryRowContext(c, `SELECT next_offset,owner_epoch,owner_id FROM gw_metering_checkpoint WHERE partition_id=? FOR UPDATE`, p.Partition).Scan(&next, &epoch, &owner); err != nil {
		return err
	}
	if epoch != p.Epoch || owner != p.Owner {
		return ErrFenced
	}
	// cursor 是本 reader 已串行处理的位置，不能以 MAX(offset) 越过未处理消息。
	if cursor > next {
		return ErrCheckpoint
	}
	if err = applyEventTx(c, tx, event); err != nil {
		return err
	}
	if m.Offset >= next {
		_, err = tx.ExecContext(c, `UPDATE gw_metering_checkpoint SET next_offset=?,anchor_offset=?,anchor_hash=? WHERE partition_id=?`, m.Offset+1, m.Offset, recordHash(m), p.Partition)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

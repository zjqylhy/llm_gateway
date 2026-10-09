package gateway

import (
	"github.com/segmentio/kafka-go"
	"testing"
)

func TestRecoveryRequiresAllKafkaPartitionsCaughtUp(t *testing.T) {
	ends := []kafka.PartitionOffsets{{Partition: 0, LastOffset: 5}, {Partition: 1, LastOffset: 3}}
	for _, commits := range [][]kafka.OffsetFetchPartition{{{Partition: 0, CommittedOffset: 5}}, {{Partition: 0, CommittedOffset: 5}, {Partition: 1, CommittedOffset: 2}}} {
		if offsetsCaughtUp(ends, commits) {
			t.Fatal("unconsumed events ignored")
		}
	}
	if !offsetsCaughtUp(ends, []kafka.OffsetFetchPartition{{Partition: 0, CommittedOffset: 5}, {Partition: 1, CommittedOffset: 3}}) {
		t.Fatal("complete recovery rejected")
	}
}

package gateway

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestRecoveryManifestValidation(t *testing.T) {
	m := RecoveryManifest{Version: 1, Source: StreamIdentity{Cluster: "cluster", Topic: "events", Group: "group", ID: "backup-stream", Partitions: 1}, Checkpoints: []PartitionCheckpoint{{Partition: 0, Next: 7, Anchor: 6, Hash: "anchor"}}, Ranges: []PartitionRange{{Partition: 0, First: 0, End: 20, Committed: 18}}}
	m.Hash = manifestHash(m)
	path := filepath.Join(t.TempDir(), "manifest.json")
	raw, _ := json.Marshal(m)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadRecoveryManifest(path); err != nil || got.Hash != m.Hash {
		t.Fatal(got, err)
	}
	changed := m
	changed.Checkpoints = append([]PartitionCheckpoint(nil), m.Checkpoints...)
	changed.Checkpoints[0].Next = 9
	if validateManifest(changed) == nil {
		t.Fatal("tampered checkpoint accepted")
	}
	changed.Hash = manifestHash(changed)
	changed.Ranges = append([]PartitionRange(nil), m.Ranges...)
	changed.Ranges[0].First = 10
	changed.Hash = manifestHash(changed)
	if validateManifest(changed) == nil {
		t.Fatal("retention gap accepted")
	}
	if err := os.WriteFile(path, append(raw, []byte("{}")...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRecoveryManifest(path); err == nil {
		t.Fatal("trailing manifest accepted")
	}
}

func TestRecordFingerprint(t *testing.T) {
	m := kafka.Message{Key: []byte("app"), Value: []byte("usage"), Headers: []kafka.Header{{Key: "origin", Value: []byte("gateway")}}}
	first := recordHash(m)
	m.Offset = 99
	if recordHash(m) != first {
		t.Fatal("broker offset affected content identity")
	}
	m.Headers[0].Value = []byte("other")
	if recordHash(m) == first {
		t.Fatal("changed source header not detected")
	}
}

func TestMeteringRecordSource(t *testing.T) {
	source := StreamIdentity{ID: "verified-stream"}
	p := PartitionCheckpoint{Base: 10}
	m := kafka.Message{Offset: 10}
	if verifyRecordSource(source, p, m) == nil {
		t.Fatal("unidentified new source accepted")
	}
	m.Offset = 9
	if err := verifyRecordSource(source, p, m); err != nil {
		t.Fatal("audited legacy baseline rejected", err)
	}
	m.Offset = 10
	m.Headers = []kafka.Header{{Key: "gw-stream-id", Value: []byte(source.ID)}}
	if err := verifyRecordSource(source, p, m); err != nil {
		t.Fatal(err)
	}
	m.Headers = append(m.Headers, m.Headers[0])
	if verifyRecordSource(source, p, m) == nil {
		t.Fatal("ambiguous duplicate source header accepted")
	}
}

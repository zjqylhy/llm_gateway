package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testPermitContext(t *testing.T, client *redis.Client, ctx context.Context, apps ...string) context.Context {
	t.Helper()
	p := meteringPermit{"generation", time.Now().Add(3 * time.Second).UnixMilli(), "permit"}
	raw, _ := json.Marshal(p)
	if err := client.Set(ctx, "gw:metering:generation", p.Generation, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, "gw:metering:permit", raw, 3*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	for _, app := range apps {
		if err := client.HSet(ctx, appControlKey(app), "state", "healthy", "generation", "app-generation").Err(); err != nil {
			t.Fatal(err)
		}
	}
	return context.WithValue(ctx, admissionProofKey{}, admissionProof{Permit: p, AppGeneration: "app-generation"})
}

func TestMeteringPermitGenerationExpiryAndApplicationIsolation(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	ctx := context.Background()
	_ = client.Set(ctx, "gw:quota:state", "ready", 0).Err()
	ctx = testPermitContext(t, client, ctx, "a", "b")
	g := RedisGuard{Client: client, RequireReady: true}
	reserve := func(c context.Context, app, id string) error {
		return g.Reserve(c, App{ID: app}, Reservation{ID: id, AppID: app, Month: "2026-10", Tokens: 10})
	}
	if err := reserve(context.Background(), "a", "missing"); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if err := reserve(ctx, "a", "valid"); err != nil {
		t.Fatal(err)
	}
	_ = client.HSet(ctx, appControlKey("a"), "state", "quiescing").Err()
	if err := reserve(ctx, "a", "valid"); !errors.Is(err, ErrUnavailable) {
		t.Fatal("duplicate must not bypass barrier", err)
	}
	if err := reserve(ctx, "b", "isolated"); err != nil {
		t.Fatal("one app blocked another", err)
	}
	_ = client.Set(ctx, "gw:metering:generation", "old-backup", 0).Err()
	if err := reserve(ctx, "b", "old"); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	_ = client.Set(ctx, "gw:metering:generation", "generation", 0).Err()
	proof := ctx.Value(admissionProofKey{}).(admissionProof)
	proof.Permit.Expires = time.Now().Add(-time.Second).UnixMilli()
	if err := reserve(context.WithValue(ctx, admissionProofKey{}, proof), "b", "expired"); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	// AOF 恢复出的 TTL 不能让绝对到期的许可重新有效。
	proof.Permit.Expires = time.Now().Add(2 * time.Second).UnixMilli()
	raw, _ := json.Marshal(meteringPermit{"generation", time.Now().Add(-time.Second).UnixMilli(), "old"})
	_ = client.Set(ctx, "gw:metering:permit", raw, 3*time.Second).Err()
	if err := reserve(context.WithValue(ctx, admissionProofKey{}, proof), "b", "restored"); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}

func TestMeteringWatermarkCannotSkipRestoredHistory(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	r := &Runtime{Redis: client}
	ctx := context.Background()
	_ = client.Set(ctx, "gw:metering:generation", "g", 0).Err()
	_ = client.HSet(ctx, "gw:metering:watermarks", "0", 10).Err()
	if err := r.advanceWatermark(ctx, "g", 0, 10, 12); err != nil {
		t.Fatal(err)
	}
	if err := r.advanceWatermark(ctx, "g", 0, 10, 11); err != nil {
		t.Fatal("idempotent replay rejected", err)
	}
	_ = client.HSet(ctx, "gw:metering:watermarks", "0", 3).Err()
	if err := r.advanceWatermark(ctx, "g", 0, 12, 13); !errors.Is(err, ErrCheckpoint) {
		t.Fatal("restored data silently skipped", err)
	}
	if v, _ := client.HGet(ctx, "gw:metering:watermarks", "0").Result(); v != "3" {
		t.Fatal(v)
	}
}

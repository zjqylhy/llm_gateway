package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestHeldBudgetAndCancellation(t *testing.T) {
	for _, backend := range []string{"memory", "redis"} {
		t.Run(backend, func(t *testing.T) {
			var guard Guard = NewMemoryGuard()
			if backend == "redis" {
				server := miniredis.RunT(t)
				client := redis.NewClient(&redis.Options{Addr: server.Addr()})
				t.Cleanup(func() { _ = client.Close() })
				guard = RedisGuard{Client: client}
			}
			ctx := context.Background()
			app := App{ID: "app", BudgetMicro: 100, BudgetAction: "reject"}
			start := Event{ID: "one:1", AttemptID: "one", RequestID: "request", AppID: app.ID, Month: "2026-10", Kind: "started", Revision: 1, Quality: "pending", ReserveTokens: 100, ReserveMicro: 100, At: time.Now().UTC()}
			ctx = context.WithValue(ctx, reservationHashKey{}, eventDigest(start))
			reservation := Reservation{"one", app.ID, start.Month, 100, 100}
			if err := guard.Reserve(ctx, app, reservation); err != nil {
				t.Fatal(err)
			}
			completion := start
			completion.ID, completion.Kind, completion.Revision, completion.Quality = "one:2", "completed", 2, "estimated"
			completion.Usage, completion.CostMicro = Usage{Total: 20}, 20
			for range 3 {
				if err := guard.Settle(ctx, completion); err != nil {
					t.Fatal(err)
				}
			}
			counts, _ := guard.Counters(ctx, app.ID, start.Month)
			if counts != (Counters{ReservedTokens: 100, ReservedMicro: 100}) {
				t.Fatalf("held released budget: %+v", counts)
			}
			if err := guard.Reserve(ctx, app, Reservation{"two", app.ID, start.Month, 1, 1}); !errors.Is(err, ErrLimit) {
				t.Fatalf("async delay oversold held budget: %v", err)
			}
			completion.ID, completion.Revision, completion.Quality = "one:3", 3, "reconciled"
			if err := guard.Settle(ctx, completion); err != nil {
				t.Fatal(err)
			}
			counts, _ = guard.Counters(ctx, app.ID, start.Month)
			if counts != (Counters{Tokens: 20, Micro: 20}) {
				t.Fatal(counts)
			}
			start.ID, start.AttemptID = "cancel:1", "cancel"
			ctx = context.WithValue(ctx, reservationHashKey{}, eventDigest(start))
			if err := guard.Reserve(ctx, app, Reservation{"cancel", app.ID, start.Month, 30, 30}); err != nil {
				t.Fatal(err)
			}
			cancelGuard := guard.(interface {
				Cancel(context.Context, Event) error
			})
			for range 3 {
				if err := cancelGuard.Cancel(ctx, start); err != nil {
					t.Fatal(err)
				}
			}
			if err := guard.Reserve(ctx, app, Reservation{"cancel", app.ID, start.Month, 30, 30}); backend == "memory" && !errors.Is(err, ErrFenced) {
				t.Fatalf("late reserve revived: %v", err)
			}
			counts, _ = guard.Counters(ctx, app.ID, start.Month)
			if counts != (Counters{Tokens: 20, Micro: 20}) {
				t.Fatalf("cancel repeated or revived: %+v", counts)
			}
		})
	}
}

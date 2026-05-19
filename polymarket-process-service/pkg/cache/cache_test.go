package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestMemoryCacheHonorsJSONTTL(t *testing.T) {
	ctx := context.Background()
	cache := NewMemory()

	if err := cache.SetJSON(ctx, "key", map[string]string{"value": "cached"}, 10*time.Millisecond); err != nil {
		t.Fatalf("SetJSON() error = %v", err)
	}
	var got map[string]string
	if err := cache.GetJSON(ctx, "key", &got); err != nil {
		t.Fatalf("GetJSON() before expiry error = %v", err)
	}
	if got["value"] != "cached" {
		t.Fatalf("GetJSON() value = %q", got["value"])
	}

	time.Sleep(20 * time.Millisecond)
	if err := cache.GetJSON(ctx, "key", &got); !errors.Is(err, redis.Nil) {
		t.Fatalf("GetJSON() after expiry error = %v, want redis.Nil", err)
	}
}

func TestMemoryCacheSetNXAllowsReuseAfterTTL(t *testing.T) {
	ctx := context.Background()
	cache := NewMemory()

	ok, err := cache.SetNX(ctx, "dedupe", "first", 10*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("SetNX() first = %v, %v; want true, nil", ok, err)
	}
	ok, err = cache.SetNX(ctx, "dedupe", "second", time.Minute)
	if err != nil || ok {
		t.Fatalf("SetNX() duplicate = %v, %v; want false, nil", ok, err)
	}

	time.Sleep(20 * time.Millisecond)
	ok, err = cache.SetNX(ctx, "dedupe", "second", time.Minute)
	if err != nil || !ok {
		t.Fatalf("SetNX() after expiry = %v, %v; want true, nil", ok, err)
	}
}

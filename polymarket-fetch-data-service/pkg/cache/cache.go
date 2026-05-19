package cache

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache interface {
	SetJSON(context.Context, string, any, time.Duration) error
	GetJSON(context.Context, string, any) error
	SetNX(context.Context, string, string, time.Duration) (bool, error)
	Close() error
}

type RedisCache struct {
	client *redis.Client
}

func NewRedis(addr, password string, db int) *RedisCache {
	return &RedisCache{client: redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db})}
}

func (c *RedisCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *RedisCache) SetJSON(ctx context.Context, key string, value any, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, key, data, ttl).Err()
}

func (c *RedisCache) GetJSON(ctx context.Context, key string, out any) error {
	data, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func (c *RedisCache) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, value, ttl).Result()
}

func (c *RedisCache) Close() error {
	return c.client.Close()
}

type MemoryCache struct {
	mu    sync.RWMutex
	items map[string]memoryItem
}

type memoryItem struct {
	data      []byte
	expiresAt time.Time
}

func NewMemory() *MemoryCache {
	return &MemoryCache{items: map[string]memoryItem{}}
}

func (c *MemoryCache) SetJSON(_ context.Context, key string, value any, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.purgeExpiredLocked(now)
	c.items[key] = memoryItem{data: data, expiresAt: expiresAt(now, ttl)}
	return nil
}

func (c *MemoryCache) GetJSON(_ context.Context, key string, out any) error {
	now := time.Now()
	c.mu.Lock()
	item, ok := c.items[key]
	if !ok {
		c.mu.Unlock()
		return redis.Nil
	}
	if item.expired(now) {
		delete(c.items, key)
		c.mu.Unlock()
		return redis.Nil
	}
	data := append([]byte(nil), item.data...)
	c.mu.Unlock()
	return json.Unmarshal(data, out)
}

func (c *MemoryCache) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.purgeExpiredLocked(now)
	if item, ok := c.items[key]; ok && !item.expired(now) {
		return false, nil
	}
	c.items[key] = memoryItem{data: []byte(value), expiresAt: expiresAt(now, ttl)}
	return true, nil
}

func (c *MemoryCache) Close() error { return nil }

func (c *MemoryCache) purgeExpiredLocked(now time.Time) {
	for key, item := range c.items {
		if item.expired(now) {
			delete(c.items, key)
		}
	}
}

func (item memoryItem) expired(now time.Time) bool {
	return !item.expiresAt.IsZero() && !now.Before(item.expiresAt)
}

func expiresAt(now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}

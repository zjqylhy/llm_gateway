package gateway

import (
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func isolatedRedisOptions(quotaURL, cacheURL string) (*redis.Options, *redis.Options, error) {
	quota, err := redis.ParseURL(quotaURL)
	if err != nil {
		return nil, nil, errors.New("invalid GW_REDIS_URL")
	}
	cache, err := redis.ParseURL(cacheURL)
	if err != nil {
		return nil, nil, errors.New("invalid GW_CACHE_REDIS_URL")
	}
	// Redis 的不同 DB 共用内存及淘汰策略，不能只改变 URL 中的 DB 编号。
	if strings.EqualFold(quota.Addr, cache.Addr) {
		return nil, nil, errors.New("quota and cache require separate Redis instances, not separate DBs")
	}
	for _, opts := range []*redis.Options{quota, cache} {
		opts.ContextTimeoutEnabled = true
		opts.DialTimeout = time.Second
		opts.DialerRetries = 1
		opts.MaxRetries = 1
		opts.PoolTimeout = time.Second
		opts.ReadTimeout = 250 * time.Millisecond
		opts.WriteTimeout = 250 * time.Millisecond
		// 默认池大小随 CPU 核数增长；显式限制连接和池等待的资源占用。
		opts.PoolSize = 32
		opts.MaxActiveConns = 32
	}
	return quota, cache, nil
}

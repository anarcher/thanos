// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"bytes"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Estimate the entry, LRU node and map bookkeeping as well as keys and values.
// In particular, empty values must still consume capacity.
const memcachedClientCacheEntryOverhead = 128

// The high bit marks Thanos expiration metadata; the remaining bits hold the
// positive int32 Unix expiration used by the existing Memcached write path.
const memcachedClientCacheExpirationFlag uint32 = 1 << 31

type memcachedClientCacheEntry struct {
	value      []byte
	expiration int64
}

type memcachedClientCache struct {
	mu      sync.Mutex
	lru     *simplelru.LRU[string, memcachedClientCacheEntry]
	maxSize uint64
	size    uint64
	now     func() time.Time

	requests  prometheus.Counter
	hits      prometheus.Counter
	sizeBytes prometheus.Gauge
}

func newMemcachedClientCache(maxSize uint64, reg prometheus.Registerer) *memcachedClientCache {
	c := &memcachedClientCache{
		maxSize: maxSize,
		now:     time.Now,
		requests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_memcached_client_cache_requests_total",
			Help: "Total number of keys requested from the Memcached client-side cache.",
		}),
		hits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "thanos_memcached_client_cache_hits_total",
			Help: "Total number of keys found in the Memcached client-side cache.",
		}),
		sizeBytes: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "thanos_memcached_client_cache_size_bytes",
			Help: "Current estimated size of Memcached client-side cache entries in bytes.",
		}),
	}
	// The LRU's item-count limit is deliberately unreachable. Capacity is
	// enforced in bytes below, including an estimate for each entry's overhead.
	c.lru, _ = simplelru.NewLRU[string, memcachedClientCacheEntry](int(^uint(0)>>1), c.onEvict)
	return c
}

func memcachedClientCacheEntrySize(key string, value []byte) uint64 {
	return uint64(len(key)) + uint64(len(value)) + memcachedClientCacheEntryOverhead
}

// onEvict runs with mu held by the caller.
func (c *memcachedClientCache) onEvict(key string, entry memcachedClientCacheEntry) {
	size := memcachedClientCacheEntrySize(key, entry.value)
	c.size -= size
	c.sizeBytes.Sub(float64(size))
}

func (c *memcachedClientCache) getMulti(keys []string) (map[string][]byte, []string) {
	c.requests.Add(float64(len(keys)))
	hits := make(map[string][]byte)
	var misses []string
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().Unix()
	for _, key := range keys {
		entry, ok := c.lru.Get(key)
		if ok && entry.expiration <= now {
			c.lru.Remove(key)
			ok = false
		}
		if !ok {
			misses = append(misses, key)
			continue
		}
		// Keep callers' byte slices independent, as with ordinary remote gets.
		hits[key] = bytes.Clone(entry.value)
	}
	c.hits.Add(float64(len(hits)))
	return hits, misses
}

// set never extends an entry beyond its original absolute expiration. A read
// backfill must not replace a value written locally during an in-flight get.
func (c *memcachedClientCache) set(key string, value []byte, expiration int64, replace bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now().Unix()
	if entry, ok := c.lru.Peek(key); ok {
		if !replace && entry.expiration > now {
			return
		}
		c.lru.Remove(key)
	}
	// Memcached's existing write path uses a signed 32-bit absolute expiry.
	// Unknown flags and expirations outside that range cannot be cached safely.
	if expiration <= now || expiration > math.MaxInt32 {
		return
	}
	size := memcachedClientCacheEntrySize(key, value)
	if size > c.maxSize {
		return
	}
	for size > c.maxSize-c.size {
		c.lru.RemoveOldest()
	}
	key = strings.Clone(key)
	value = bytes.Clone(value)
	c.lru.Add(key, memcachedClientCacheEntry{value: value, expiration: expiration})
	c.size += size
	c.sizeBytes.Add(float64(size))
}

func (c *memcachedClientCache) remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Remove(key)
}

func (c *memcachedClientCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Purge()
}

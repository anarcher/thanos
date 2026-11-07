// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package cacheutil

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/efficientgo/core/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/thanos-io/thanos/pkg/model"
)

func newCachingMemcachedClient(t *testing.T, backend memcachedClientBackend, batchSize int) *memcachedClient {
	t.Helper()
	config := defaultMemcachedClientConfig
	config.Addresses = []string{"127.0.0.1:11211"}
	config.CacheSize = 4096
	config.MaxGetMultiBatchSize = batchSize
	client, err := newMemcachedClient(log.NewNopLogger(), backend, &MemcachedJumpHashSelector{}, config, prometheus.NewRegistry(), "test")
	testutil.Ok(t, err)
	t.Cleanup(client.Stop)
	return client
}

func TestMemcachedClientCacheExpiryAndOwnership(t *testing.T) {
	cache := newMemcachedClientCache(4096, nil)
	now := time.Unix(1800000000, 0)
	cache.now = func() time.Time { return now }
	value := []byte("original")
	cache.set("key", value, now.Add(time.Minute).Unix(), true)
	value[0] = 'x'
	hits, misses := cache.getMulti([]string{"key", "missing"})
	testutil.Equals(t, []byte("original"), hits["key"])
	testutil.Equals(t, []string{"missing"}, misses)
	hits["key"][0] = 'x'
	hits, _ = cache.getMulti([]string{"key"})
	testutil.Equals(t, []byte("original"), hits["key"])

	now = now.Add(time.Minute)
	hits, misses = cache.getMulti([]string{"key"})
	testutil.Equals(t, 0, len(hits))
	testutil.Equals(t, []string{"key"}, misses)
	testutil.Equals(t, uint64(0), cache.size)
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.sizeBytes))

	for _, expiration := range []int64{0, now.Unix(), now.Unix() - 1, math.MaxInt32 + 1} {
		cache.set("unknown-expiry", value, expiration, false)
	}
	testutil.Equals(t, 0, cache.lru.Len())
}

func TestMemcachedClientCacheUpdatesAndReadBackfill(t *testing.T) {
	cache := newMemcachedClientCache(4096, nil)
	now := time.Unix(1800000000, 0)
	cache.now = func() time.Time { return now }
	cache.set("key", []byte("old"), now.Add(time.Minute).Unix(), true)
	cache.set("key", []byte("new"), now.Add(10*time.Second).Unix(), true)
	cache.set("key", []byte("old in-flight read"), now.Add(time.Minute).Unix(), false)
	hits, _ := cache.getMulti([]string{"key"})
	testutil.Equals(t, []byte("new"), hits["key"])
	now = now.Add(10 * time.Second)
	hits, misses := cache.getMulti([]string{"key"})
	testutil.Equals(t, 0, len(hits))
	testutil.Equals(t, []string{"key"}, misses)

	cache.set("key", []byte("new"), now.Add(time.Minute).Unix(), true)
	cache.set("key", []byte("expired write"), now.Unix(), true)
	_, misses = cache.getMulti([]string{"key"})
	testutil.Equals(t, []string{"key"}, misses)
}

func TestMemcachedClientCacheCapacity(t *testing.T) {
	entrySize := memcachedClientCacheEntrySize("a", []byte{1})
	cache := newMemcachedClientCache(2*entrySize, nil)
	expiration := time.Now().Add(time.Hour).Unix()
	cache.set("a", []byte{1}, expiration, true)
	cache.set("b", []byte{2}, expiration, true)
	cache.getMulti([]string{"a"})
	cache.set("c", []byte{3}, expiration, true)
	hits, misses := cache.getMulti([]string{"a", "b", "c"})
	testutil.Equals(t, map[string][]byte{"a": {1}, "c": {3}}, hits)
	testutil.Equals(t, []string{"b"}, misses)
	testutil.Equals(t, 2*entrySize, cache.size)

	// Replacing a cached key with an oversized value must remove the old value.
	cache.set("a", make([]byte, 4096), expiration, true)
	_, misses = cache.getMulti([]string{"a"})
	testutil.Equals(t, []string{"a"}, misses)
	cache.clear()
	testutil.Equals(t, uint64(0), cache.size)
	testutil.Equals(t, float64(0), promtest.ToFloat64(cache.sizeBytes))

	// Empty values and keys still consume entry bookkeeping capacity.
	cache = newMemcachedClientCache(2*memcachedClientCacheEntryOverhead, nil)
	for i := range 100 {
		cache.set(fmt.Sprintf("key-%d", i), nil, expiration, true)
	}
	testutil.Equals(t, 1, cache.lru.Len())
	testutil.Assert(t, cache.size <= cache.maxSize)
}

func TestMemcachedClientCacheWriteThroughAndFailure(t *testing.T) {
	backend := newMemcachedClientBackendMock()
	backend.setErrors = 1
	client := newCachingMemcachedClient(t, backend, 0)
	testutil.Ok(t, client.SetAsync("key", []byte("value"), time.Minute))
	testutil.Ok(t, backend.waitSetCount(1))
	hits := client.GetMulti(context.Background(), []string{"key"})
	testutil.Equals(t, map[string][]byte{"key": []byte("value")}, hits)
	testutil.Equals(t, 0, backend.getMultiCount)
	testutil.Equals(t, float64(1), promtest.ToFloat64(client.localCache.hits))

	// A local hit is preserved when a remote miss fails.
	backend.getMultiErrors = 1
	hits = client.GetMulti(context.Background(), []string{"key", "remote-miss"})
	testutil.Equals(t, map[string][]byte{"key": []byte("value")}, hits)
	testutil.Equals(t, 1, backend.getMultiCount)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	testutil.Equals(t, 0, len(client.GetMulti(ctx, []string{"key"})))
	testutil.Equals(t, 1, backend.getMultiCount)
}

func TestMemcachedClientCacheBackfillAndLegacyEntries(t *testing.T) {
	backend := newMemcachedClientBackendMock()
	testutil.Ok(t, backend.Set(&memcache.Item{Key: "legacy", Value: []byte("value")}))
	testutil.Ok(t, backend.Set(&memcache.Item{Key: "other-flags", Value: []byte("value"), Flags: uint32(time.Now().Add(time.Hour).Unix())}))
	reader := newCachingMemcachedClient(t, backend, 0)
	ctx := context.Background()
	for range 2 {
		hits := reader.GetMulti(ctx, []string{"legacy", "other-flags"})
		testutil.Equals(t, []byte("value"), hits["legacy"])
		testutil.Equals(t, []byte("value"), hits["other-flags"])
	}
	testutil.Equals(t, 2, backend.getMultiCount)
	testutil.Equals(t, 0, reader.localCache.lru.Len())

	writer := newCachingMemcachedClient(t, backend, 0)
	testutil.Ok(t, writer.SetAsync("legacy", []byte("updated"), time.Minute))
	testutil.Ok(t, backend.waitSetCount(3))
	backend.lock.Lock()
	item := backend.items["legacy"]
	backend.lock.Unlock()
	testutil.Equals(t, uint32(item.Expiration)|memcachedClientCacheExpirationFlag, item.Flags)
	for range 2 {
		testutil.Equals(t, []byte("updated"), reader.GetMulti(ctx, []string{"legacy"})["legacy"])
	}
	testutil.Equals(t, 3, backend.getMultiCount)
	// Read backfill must retain the original expiry rather than reset the TTL.
	entry, ok := reader.localCache.lru.Peek("legacy")
	testutil.Assert(t, ok)
	testutil.Equals(t, int64(item.Expiration), entry.expiration)
}

func TestMemcachedClientCacheBatchedMisses(t *testing.T) {
	backend := newMemcachedClientBackendMock()
	expiration := uint32(time.Now().Add(time.Hour).Unix()) | memcachedClientCacheExpirationFlag
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		testutil.Ok(t, backend.Set(&memcache.Item{Key: key, Value: []byte(key), Flags: expiration}))
	}
	client := newCachingMemcachedClient(t, backend, 2)
	ctx := context.Background()
	client.GetMulti(ctx, []string{"a", "b"})
	testutil.Equals(t, 1, backend.getMultiCount)
	hits := client.GetMulti(ctx, []string{"a", "b", "c", "d", "e"})
	testutil.Equals(t, 5, len(hits))
	// Only the three local misses require remote batches (two plus one).
	testutil.Equals(t, 3, backend.getMultiCount)
	client.GetMulti(ctx, []string{"a", "b", "c", "d", "e"})
	testutil.Equals(t, 3, backend.getMultiCount)
	testutil.Equals(t, float64(12), promtest.ToFloat64(client.localCache.requests))
	testutil.Equals(t, float64(7), promtest.ToFloat64(client.localCache.hits))
}

type blockingSetMemcachedBackend struct {
	*memcachedClientBackendMock
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingSetMemcachedBackend) Set(item *memcache.Item) error {
	b.once.Do(func() {
		close(b.started)
		<-b.release
	})
	return b.memcachedClientBackendMock.Set(item)
}

func TestMemcachedClientCacheFullAsyncQueue(t *testing.T) {
	backend := &blockingSetMemcachedBackend{
		memcachedClientBackendMock: newMemcachedClientBackendMock(),
		started:                    make(chan struct{}),
		release:                    make(chan struct{}),
	}
	config := defaultMemcachedClientConfig
	config.Addresses = []string{"127.0.0.1:11211"}
	config.CacheSize = 4096
	config.MaxAsyncConcurrency = 1
	config.MaxAsyncBufferSize = 1
	client, err := newMemcachedClient(log.NewNopLogger(), backend, &MemcachedJumpHashSelector{}, config, nil, "test")
	testutil.Ok(t, err)
	t.Cleanup(client.Stop)
	unblock := sync.OnceFunc(func() { close(backend.release) })
	t.Cleanup(unblock)
	testutil.Ok(t, client.SetAsync("blocked", []byte("value"), time.Minute))
	<-backend.started
	testutil.Ok(t, client.SetAsync("queued", []byte("value"), time.Minute))
	testutil.Ok(t, client.SetAsync("dropped", []byte("still cached locally"), time.Minute))
	testutil.Equals(t, float64(1), promtest.ToFloat64(client.skipped.WithLabelValues(opSet, reasonAsyncBufferFull)))
	testutil.Equals(t, []byte("still cached locally"), client.GetMulti(context.Background(), []string{"dropped"})["dropped"])
	testutil.Equals(t, 0, backend.getMultiCount)
}

type inFlightMemcachedBackend struct {
	*memcachedClientBackendMock
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *inFlightMemcachedBackend) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	items, err := b.memcachedClientBackendMock.GetMulti(keys)
	b.once.Do(func() {
		close(b.started)
		<-b.release
	})
	return items, err
}

func TestMemcachedClientCacheInFlightRead(t *testing.T) {
	backend := &inFlightMemcachedBackend{
		memcachedClientBackendMock: newMemcachedClientBackendMock(),
		started:                    make(chan struct{}),
		release:                    make(chan struct{}),
	}
	testutil.Ok(t, backend.Set(&memcache.Item{Key: "key", Value: []byte("old"), Flags: uint32(time.Now().Add(time.Minute).Unix()) | memcachedClientCacheExpirationFlag}))
	client := newCachingMemcachedClient(t, backend, 0)
	unblock := sync.OnceFunc(func() { close(backend.release) })
	t.Cleanup(unblock)
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.GetMulti(context.Background(), []string{"key"})
	}()
	<-backend.started
	testutil.Ok(t, client.SetAsync("key", []byte("new"), time.Minute))
	unblock()
	<-done
	testutil.Equals(t, []byte("new"), client.GetMulti(context.Background(), []string{"key"})["key"])
	testutil.Equals(t, 1, backend.getMultiCount)
}

func TestMemcachedClientCacheConcurrentAccess(t *testing.T) {
	cache := newMemcachedClientCache(1024, nil)
	expiration := time.Now().Add(time.Minute).Unix()
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			key := fmt.Sprintf("key-%d", worker)
			for range 100 {
				cache.set(key, []byte(key), expiration, true)
				hits, _ := cache.getMulti([]string{key})
				for k, value := range hits {
					if string(value) != k {
						t.Errorf("unexpected value %q for key %q", value, k)
					}
				}
			}
		})
	}
	wg.Wait()
	testutil.Assert(t, cache.size <= cache.maxSize)
	cache.clear()
	testutil.Equals(t, uint64(0), cache.size)
}

func TestMemcachedClientCacheConfig(t *testing.T) {
	for _, size := range []model.Bytes{0, 4096} {
		client, err := NewMemcachedClient(log.NewNopLogger(), "test", []byte(fmt.Sprintf("addresses: [127.0.0.1:11211]\ncache_size: %dB", size)), nil)
		testutil.Ok(t, err)
		testutil.Equals(t, size, client.config.CacheSize)
		testutil.Equals(t, size > 0, client.localCache != nil)
		client.Stop()
		if client.localCache != nil {
			testutil.Equals(t, uint64(0), client.localCache.size)
		}
	}
}

func BenchmarkMemcachedClientCache(b *testing.B) {
	for _, keysCount := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("keys=%d", keysCount), func(b *testing.B) {
			cache := newMemcachedClientCache(1024*1024, nil)
			expiration := time.Now().Add(time.Hour).Unix()
			keys := make([]string, keysCount)
			for i := range keys {
				keys[i] = fmt.Sprintf("key-%d", i)
				cache.set(keys[i], []byte(strings.Repeat("x", 128)), expiration, true)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				cache.getMulti(keys)
			}
		})
	}
}

package cache

import (
	"container/list"
	"context"
	"fmt"
	"runtime/trace"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avivklas/jaydb/pkg/storage"
)

// DefaultListTTL is the default freshness window for list cache pages.
const DefaultListTTL = 30 * time.Second

// DefaultListMaxPages is the default cap on cached list pages.
const DefaultListMaxPages = 500

// ListConfig configures the list cache behavior.
type ListConfig struct {
	TTL      time.Duration
	MaxPages int
}

// ListCacheKey identifies a distinct listing query.
type ListCacheKey struct {
	Prefix string
	Cursor string
	Limit  int
}

func (k ListCacheKey) String() string {
	return fmt.Sprintf("%s|%s|%d", k.Prefix, k.Cursor, k.Limit)
}

type listCacheEntry struct {
	key        ListCacheKey
	items      []*storage.KeyMeta
	nextCursor string
	fetchedAt  time.Time
}

type listSingleflightCall struct {
	done chan struct{}
	val  []*storage.KeyMeta
	next string
	err  error
}

// ListCache provides in-memory caching for storage.Driver.List queries with
// LRU eviction, singleflight stampede coalescing, and prefix invalidation on writes.
type ListCache struct {
	driver storage.Driver
	ttl    time.Duration
	max    int

	mu    sync.Mutex
	items map[string]*list.Element // key.String() -> *list.Element(*listCacheEntry)
	lru   *list.List

	sfCalls sync.Map // map[string]*listSingleflightCall

	// Metrics
	hits          uint64
	misses        uint64
	sfHits        uint64
	evictions     uint64
	invalidations uint64
}

// NewListCache initializes a list query cache.
func NewListCache(driver storage.Driver, cfg ListConfig) *ListCache {
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = DefaultListTTL
	}
	max := cfg.MaxPages
	if max <= 0 {
		max = DefaultListMaxPages
	}

	return &ListCache{
		driver: driver,
		ttl:    ttl,
		max:    max,
		items:  make(map[string]*list.Element),
		lru:    list.New(),
	}
}

// ListPage returns a page of listed keys, serving from cache if fresh or fetching from storage.
func (c *ListCache) ListPage(ctx context.Context, prefix string, opts storage.ListOptions) ([]*storage.KeyMeta, string, error) {
	defer trace.StartRegion(ctx, "list_cache.list_page").End()

	k := ListCacheKey{
		Prefix: prefix,
		Cursor: opts.Cursor,
		Limit:  opts.Limit,
	}
	mapKey := k.String()

	// 1. Fast path: check cache
	now := time.Now()
	c.mu.Lock()
	if el, found := c.items[mapKey]; found {
		entry := el.Value.(*listCacheEntry)
		if now.Sub(entry.fetchedAt) < c.ttl {
			c.lru.MoveToFront(el)
			c.mu.Unlock()
			atomic.AddUint64(&c.hits, 1)
			return copyMetas(entry.items), entry.nextCursor, nil
		}
		// Expired: remove immediately
		c.lru.Remove(el)
		delete(c.items, mapKey)
	}
	c.mu.Unlock()

	// 2. Singleflight coalescing for concurrent misses
	call := &listSingleflightCall{done: make(chan struct{})}
	actual, loaded := c.sfCalls.LoadOrStore(mapKey, call)
	if loaded {
		atomic.AddUint64(&c.sfHits, 1)
		sfCall := actual.(*listSingleflightCall)
		select {
		case <-sfCall.done:
			if sfCall.err != nil {
				return nil, "", sfCall.err
			}
			return copyMetas(sfCall.val), sfCall.next, nil
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}

	atomic.AddUint64(&c.misses, 1)

	defer func() {
		c.sfCalls.Delete(mapKey)
		close(call.done)
	}()

	// Re-check cache under lock: another leader may have admitted right before LoadOrStore
	c.mu.Lock()
	if el, found := c.items[mapKey]; found {
		entry := el.Value.(*listCacheEntry)
		if time.Since(entry.fetchedAt) < c.ttl {
			c.lru.MoveToFront(el)
			c.mu.Unlock()
			atomic.AddUint64(&c.hits, 1)
			call.val = entry.items
			call.next = entry.nextCursor
			return copyMetas(entry.items), entry.nextCursor, nil
		}
	}
	c.mu.Unlock()

	metas, next, err := c.driver.List(ctx, prefix, opts)
	call.val = metas
	call.next = next
	call.err = err

	if err != nil {
		return nil, "", err
	}

	// 3. Admit to cache
	c.mu.Lock()
	if el, found := c.items[mapKey]; found {
		entry := el.Value.(*listCacheEntry)
		entry.items = copyMetas(metas)
		entry.nextCursor = next
		entry.fetchedAt = time.Now()
		c.lru.MoveToFront(el)
	} else {
		// Evict oldest if capacity reached
		for c.lru.Len() >= c.max {
			oldest := c.lru.Back()
			if oldest == nil {
				break
			}
			oldEntry := oldest.Value.(*listCacheEntry)
			delete(c.items, oldEntry.key.String())
			c.lru.Remove(oldest)
			atomic.AddUint64(&c.evictions, 1)
		}

		c.items[mapKey] = c.lru.PushFront(&listCacheEntry{
			key:        k,
			items:      copyMetas(metas),
			nextCursor: next,
			fetchedAt:  time.Now(),
		})
	}
	c.mu.Unlock()

	return copyMetas(metas), next, nil
}

// InvalidateKey prunes every cached listing whose Prefix matches key.
// Called whenever a document is written or deleted.
func (c *ListCache) InvalidateKey(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var toRemove []*list.Element
	for _, el := range c.items {
		entry := el.Value.(*listCacheEntry)
		// If key has entry.key.Prefix as prefix, this write affects that list.
		// Example: entry.key.Prefix = "users/", key = "users/1" -> matches
		// Example: entry.key.Prefix = "", key = "users/1" -> matches (root list)
		if entry.key.Prefix == "" || strings.HasPrefix(key, entry.key.Prefix) {
			toRemove = append(toRemove, el)
		}
	}

	for _, el := range toRemove {
		entry := el.Value.(*listCacheEntry)
		c.lru.Remove(el)
		delete(c.items, entry.key.String())
		atomic.AddUint64(&c.invalidations, 1)
	}
}

// InvalidatePrefix prunes every cached listing under or overlapping prefix.
func (c *ListCache) InvalidatePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var toRemove []*list.Element
	for _, el := range c.items {
		entry := el.Value.(*listCacheEntry)
		if prefix == "" || entry.key.Prefix == "" ||
			strings.HasPrefix(entry.key.Prefix, prefix) ||
			strings.HasPrefix(prefix, entry.key.Prefix) {
			toRemove = append(toRemove, el)
		}
	}

	for _, el := range toRemove {
		entry := el.Value.(*listCacheEntry)
		c.lru.Remove(el)
		delete(c.items, entry.key.String())
		atomic.AddUint64(&c.invalidations, 1)
	}
}

// Purge removes all cached listing pages.
func (c *ListCache) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.lru.Init()
}

// Size returns the count of currently cached listing pages.
func (c *ListCache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// Stats returns hit/miss/eviction counters.
func (c *ListCache) Stats() (hits, misses, sfHits, evictions, invalidations uint64) {
	return atomic.LoadUint64(&c.hits),
		atomic.LoadUint64(&c.misses),
		atomic.LoadUint64(&c.sfHits),
		atomic.LoadUint64(&c.evictions),
		atomic.LoadUint64(&c.invalidations)
}

func copyMetas(src []*storage.KeyMeta) []*storage.KeyMeta {
	if src == nil {
		return nil
	}
	dst := make([]*storage.KeyMeta, len(src))
	for i, m := range src {
		if m == nil {
			continue
		}
		cp := *m
		dst[i] = &cp
	}
	return dst
}

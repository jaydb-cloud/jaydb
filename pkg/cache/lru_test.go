package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/storage/memory"
)

func TestLRULastAccessedUpdatedOnHit(t *testing.T) {
	ctx := context.Background()
	mgr, _ := seedManager(t, Config{})

	// 1. Put key
	if _, err := mgr.Put(ctx, "k1", []byte("val1"), ""); err != nil {
		t.Fatalf("put k1: %v", err)
	}

	shard := mgr.shardFor("k1")
	shard.mu.RLock()
	el, found := shard.items["k1"]
	if !found {
		shard.mu.RUnlock()
		t.Fatal("k1 not found in shard")
	}
	initialAccessed := el.Value.(*Item).LastAccessed
	shard.mu.RUnlock()

	time.Sleep(10 * time.Millisecond)

	// 2. Read key (Cache hit)
	if _, err := mgr.Get(ctx, "k1"); err != nil {
		t.Fatalf("get k1: %v", err)
	}

	shard.mu.RLock()
	el, found = shard.items["k1"]
	if !found {
		shard.mu.RUnlock()
		t.Fatal("k1 not found in shard after get")
	}
	afterHitAccessed := el.Value.(*Item).LastAccessed
	shard.mu.RUnlock()

	if !afterHitAccessed.After(initialAccessed) {
		t.Fatalf("expected LastAccessed to be updated on hit: initial=%v, afterHit=%v", initialAccessed, afterHitAccessed)
	}
}

func TestCache_ReadsRefreshEvictionTime(t *testing.T) {
	ctx := context.Background()
	driver := memory.NewDriver()
	mgr := NewManagerWithConfig(driver, Config{TTL: 60 * time.Millisecond})

	if _, err := mgr.Put(ctx, "k1", []byte("val1"), ""); err != nil {
		t.Fatalf("put k1: %v", err)
	}

	// Repeated reads at 25ms intervals.
	// Total elapsed: 4 * 25ms = 100ms > 60ms TTL.
	// Because each read refreshes LastAccessed, the entry must remain cached!
	for i := 0; i < 4; i++ {
		time.Sleep(25 * time.Millisecond)
		obj, err := mgr.Get(ctx, "k1")
		if err != nil {
			t.Fatalf("read %d failed: %v", i, err)
		}
		if string(obj.Value) != "val1" {
			t.Fatalf("read %d got unexpected value: %s", i, string(obj.Value))
		}
	}

	hits, misses, _ := mgr.Stats()
	if hits != 4 || misses != 0 {
		t.Fatalf("expected 4 hits and 0 misses, got hits=%d misses=%d", hits, misses)
	}

	// Wait 70ms without reading: entry should now expire due to idle timeout
	time.Sleep(70 * time.Millisecond)

	// Invalidate driver copy so if Get falls through to driver it gets ErrNotFound
	_ = driver.Delete(ctx, "k1", "")

	_, err := mgr.Get(ctx, "k1")
	if err == nil {
		t.Fatal("expected ErrNotFound from storage after cache expired due to idle timeout")
	}
}

func TestLRUEvictionStrictRecency(t *testing.T) {
	ctx := context.Background()
	budget := NewBudget(300)
	mgr, _ := seedManager(t, Config{Budget: budget})

	keys := []string{"k1", "k2", "k3"}
	for _, k := range keys {
		time.Sleep(5 * time.Millisecond)
		if _, err := mgr.Put(ctx, k, make([]byte, 100), ""); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	// Access k1, then k2. Now k3 is the least recently accessed.
	time.Sleep(5 * time.Millisecond)
	if _, err := mgr.Get(ctx, "k1"); err != nil {
		t.Fatalf("get k1: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := mgr.Get(ctx, "k2"); err != nil {
		t.Fatalf("get k2: %v", err)
	}

	// Insert k4 (needs 100 bytes, forcing eviction).
	if _, err := mgr.Put(ctx, "k4", make([]byte, 100), ""); err != nil {
		t.Fatalf("put k4: %v", err)
	}

	// Verify k3 was evicted and k1, k2, k4 remain
	if mgr.contains("k3") {
		t.Fatalf("expected least recently used item k3 to be evicted, but it is still present")
	}
	if !mgr.contains("k1") || !mgr.contains("k2") || !mgr.contains("k4") {
		t.Fatalf("expected k1, k2, k4 to remain, cache has %v", cachedKeys(mgr))
	}
}

func TestWritesFillCache(t *testing.T) {
	ctx := context.Background()
	mem := memory.NewDriver()
	cd := &countingDriver{Driver: mem}
	mgr := NewManager(cd)

	// 1. Initial write fills cache
	obj, err := mgr.Put(ctx, "doc/1", []byte("initial-content"), "")
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if obj == nil || string(obj.Value) != "initial-content" {
		t.Fatalf("unexpected put response: %v", obj)
	}

	// Cache size must immediately report 1 item
	items, bytes := mgr.GetCacheSize()
	if items != 1 || bytes != int64(len("initial-content")) {
		t.Fatalf("expected 1 item and %d bytes in cache after Put, got items=%d bytes=%d", len("initial-content"), items, bytes)
	}

	// 2. Read immediately hits cache without touching underlying storage driver
	getCallsBefore := atomic.LoadUint64(&cd.getCalls)
	cachedObj, err := mgr.Get(ctx, "doc/1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(cachedObj.Value) != "initial-content" {
		t.Fatalf("expected 'initial-content', got '%s'", cachedObj.Value)
	}
	if getCallsAfter := atomic.LoadUint64(&cd.getCalls); getCallsAfter != getCallsBefore {
		t.Fatalf("expected 0 storage Get calls on cache hit after Put, before=%d after=%d", getCallsBefore, getCallsAfter)
	}

	// 3. Overwrite write updates cached content in-place
	updatedObj, err := mgr.Put(ctx, "doc/1", []byte("updated-content-longer"), obj.ETag)
	if err != nil {
		t.Fatalf("overwrite put failed: %v", err)
	}
	if string(updatedObj.Value) != "updated-content-longer" {
		t.Fatalf("expected 'updated-content-longer', got '%s'", updatedObj.Value)
	}

	// Cache size should reflect new size
	items, bytes = mgr.GetCacheSize()
	if items != 1 || bytes != int64(len("updated-content-longer")) {
		t.Fatalf("expected 1 item and %d bytes after overwrite, got items=%d bytes=%d", len("updated-content-longer"), items, bytes)
	}

	// 4. Subsequent read returns updated value directly from cache
	reReadObj, err := mgr.Get(ctx, "doc/1")
	if err != nil {
		t.Fatalf("re-read failed: %v", err)
	}
	if string(reReadObj.Value) != "updated-content-longer" {
		t.Fatalf("expected 'updated-content-longer', got '%s'", reReadObj.Value)
	}
	if getCallsAfter := atomic.LoadUint64(&cd.getCalls); getCallsAfter != getCallsBefore {
		t.Fatalf("expected 0 storage Get calls after overwrite Put, before=%d after=%d", getCallsBefore, getCallsAfter)
	}
}

func TestDefaultTTLIs24Hours(t *testing.T) {
	mem := memory.NewDriver()
	mgr := NewManager(mem)
	if mgr.ttl != 24*time.Hour {
		t.Fatalf("expected default TTL to be 24h, got %v", mgr.ttl)
	}
}

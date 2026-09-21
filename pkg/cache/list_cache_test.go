package cache_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/cache"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

type countingDriver struct {
	storage.Driver
	listCalls atomic.Uint64
}

func (d *countingDriver) List(ctx context.Context, prefix string, opts storage.ListOptions) ([]*storage.KeyMeta, string, error) {
	d.listCalls.Add(1)
	return d.Driver.List(ctx, prefix, opts)
}

func TestListCache_HitAndMiss(t *testing.T) {
	ctx := context.Background()
	store := memory.NewDriver()
	_, _ = store.Put(ctx, "users/1", []byte("u1"), "")
	_, _ = store.Put(ctx, "users/2", []byte("u2"), "")
	_, _ = store.Put(ctx, "orders/1", []byte("o1"), "")

	mock := &countingDriver{Driver: store}
	lc := cache.NewListCache(mock, cache.ListConfig{
		TTL:      time.Minute,
		MaxPages: 10,
	})

	// Miss 1
	res1, next1, err := lc.ListPage(ctx, "users/", storage.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListPage failed: %v", err)
	}
	if len(res1) != 2 {
		t.Fatalf("got %d items, want 2", len(res1))
	}
	if mock.listCalls.Load() != 1 {
		t.Fatalf("list calls = %d, want 1", mock.listCalls.Load())
	}

	// Hit 1
	res2, next2, err := lc.ListPage(ctx, "users/", storage.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListPage 2 failed: %v", err)
	}
	if len(res2) != 2 {
		t.Fatalf("got %d items, want 2", len(res2))
	}
	if next1 != next2 {
		t.Fatalf("next mismatch: %q != %q", next1, next2)
	}
	if mock.listCalls.Load() != 1 {
		t.Fatalf("list calls after hit = %d, want 1", mock.listCalls.Load())
	}

	hits, misses, _, _, _ := lc.Stats()
	if hits != 1 || misses != 1 {
		t.Errorf("stats mismatch: hits=%d, misses=%d", hits, misses)
	}
}

func TestListCache_InvalidateKey(t *testing.T) {
	ctx := context.Background()
	store := memory.NewDriver()
	_, _ = store.Put(ctx, "users/1", []byte("u1"), "")
	_, _ = store.Put(ctx, "orders/1", []byte("o1"), "")

	mock := &countingDriver{Driver: store}
	lc := cache.NewListCache(mock, cache.ListConfig{
		TTL:      time.Minute,
		MaxPages: 10,
	})

	// Warm caches for "users/", "orders/", and root ""
	_, _, _ = lc.ListPage(ctx, "users/", storage.ListOptions{Limit: 10})
	_, _, _ = lc.ListPage(ctx, "orders/", storage.ListOptions{Limit: 10})
	_, _, _ = lc.ListPage(ctx, "", storage.ListOptions{Limit: 10})

	if mock.listCalls.Load() != 3 {
		t.Fatalf("warmup list calls = %d, want 3", mock.listCalls.Load())
	}

	// Invalidate a users key
	lc.InvalidateKey("users/2")

	// "users/" and root "" should be invalidated; "orders/" should still be cached!
	_, _, _ = lc.ListPage(ctx, "orders/", storage.ListOptions{Limit: 10})
	if mock.listCalls.Load() != 3 {
		t.Errorf("orders/ should still be cached, but list calls = %d", mock.listCalls.Load())
	}

	_, _, _ = lc.ListPage(ctx, "users/", storage.ListOptions{Limit: 10})
	if mock.listCalls.Load() != 4 {
		t.Errorf("users/ should have refreshed, but list calls = %d", mock.listCalls.Load())
	}

	_, _, _ = lc.ListPage(ctx, "", storage.ListOptions{Limit: 10})
	if mock.listCalls.Load() != 5 {
		t.Errorf("root list should have refreshed, but list calls = %d", mock.listCalls.Load())
	}
}

func TestListCache_TTLExpiration(t *testing.T) {
	ctx := context.Background()
	store := memory.NewDriver()
	_, _ = store.Put(ctx, "k1", []byte("v1"), "")

	mock := &countingDriver{Driver: store}
	lc := cache.NewListCache(mock, cache.ListConfig{
		TTL:      50 * time.Millisecond,
		MaxPages: 10,
	})

	_, _, _ = lc.ListPage(ctx, "", storage.ListOptions{Limit: 10})
	if mock.listCalls.Load() != 1 {
		t.Fatalf("first call failed, count=%d", mock.listCalls.Load())
	}

	time.Sleep(60 * time.Millisecond)

	_, _, _ = lc.ListPage(ctx, "", storage.ListOptions{Limit: 10})
	if mock.listCalls.Load() != 2 {
		t.Fatalf("call after TTL should fetch from driver, count=%d", mock.listCalls.Load())
	}
}

type delayedDriver struct {
	storage.Driver
	listCalls atomic.Uint64
	delay     time.Duration
}

func (d *delayedDriver) List(ctx context.Context, prefix string, opts storage.ListOptions) ([]*storage.KeyMeta, string, error) {
	d.listCalls.Add(1)
	if d.delay > 0 {
		time.Sleep(d.delay)
	}
	return d.Driver.List(ctx, prefix, opts)
}

func TestListCache_SingleflightCoalescing(t *testing.T) {
	ctx := context.Background()
	store := memory.NewDriver()
	_, _ = store.Put(ctx, "k1", []byte("v1"), "")

	mock := &delayedDriver{Driver: store, delay: 20 * time.Millisecond}
	lc := cache.NewListCache(mock, cache.ListConfig{
		TTL:      time.Minute,
		MaxPages: 10,
	})

	const concurrent = 20
	var startWg sync.WaitGroup
	startWg.Add(1)
	var doneWg sync.WaitGroup
	doneWg.Add(concurrent)

	for i := 0; i < concurrent; i++ {
		go func() {
			defer doneWg.Done()
			startWg.Wait() // all launch together
			items, _, err := lc.ListPage(ctx, "", storage.ListOptions{Limit: 10})
			if err != nil || len(items) != 1 {
				t.Errorf("unexpected result: len=%d, err=%v", len(items), err)
			}
		}()
	}
	startWg.Done()
	doneWg.Wait()

	if mock.listCalls.Load() != 1 {
		t.Errorf("expected exactly 1 driver call under concurrent load, got %d", mock.listCalls.Load())
	}
	_, _, sfHits, _, _ := lc.Stats()
	if sfHits == 0 {
		t.Logf("sfHits: %d", sfHits)
	}
}

func TestListCache_LRUEviction(t *testing.T) {
	ctx := context.Background()
	store := memory.NewDriver()

	mock := &countingDriver{Driver: store}
	lc := cache.NewListCache(mock, cache.ListConfig{
		TTL:      time.Minute,
		MaxPages: 2,
	})

	_, _, _ = lc.ListPage(ctx, "p1/", storage.ListOptions{Limit: 10})
	_, _, _ = lc.ListPage(ctx, "p2/", storage.ListOptions{Limit: 10})
	if lc.Size() != 2 {
		t.Fatalf("size = %d, want 2", lc.Size())
	}

	// Access p3/ -> should evict p1/
	_, _, _ = lc.ListPage(ctx, "p3/", storage.ListOptions{Limit: 10})
	if lc.Size() != 2 {
		t.Fatalf("size = %d, want 2", lc.Size())
	}

	// p2/ should hit
	_, _, _ = lc.ListPage(ctx, "p2/", storage.ListOptions{Limit: 10})
	if mock.listCalls.Load() != 3 {
		t.Errorf("p2 should hit, calls = %d", mock.listCalls.Load())
	}

	// p1/ should miss and reload
	_, _, _ = lc.ListPage(ctx, "p1/", storage.ListOptions{Limit: 10})
	if mock.listCalls.Load() != 4 {
		t.Errorf("p1 should reload, calls = %d", mock.listCalls.Load())
	}
}

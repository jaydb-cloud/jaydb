package db_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

// cappedDriver wraps a storage driver and clamps every listing to pageCap keys,
// reproducing the behaviour of a real object store: S3 returns at most 1000 keys
// per response regardless of the requested MaxKeys, and hands back a
// continuation token for the rest.
type cappedDriver struct {
	storage.Driver
	pageCap   int
	listCalls int
}

func (d *cappedDriver) List(ctx context.Context, prefix string, opts storage.ListOptions) ([]*storage.KeyMeta, string, error) {
	d.listCalls++
	if opts.Limit <= 0 || opts.Limit > d.pageCap {
		opts.Limit = d.pageCap
	}
	return d.Driver.List(ctx, prefix, opts)
}

func openWithKeys(t *testing.T, pageCap, keys int) (db.DB, *cappedDriver) {
	t.Helper()

	driver := &cappedDriver{Driver: memory.NewDriver(), pageCap: pageCap}
	database, err := db.Open(db.Options{Storage: driver})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	ctx := context.Background()
	for i := 0; i < keys; i++ {
		// Zero-padded so lexicographic order matches numeric order.
		key := fmt.Sprintf("items/%05d", i)
		if _, err := database.Put(ctx, key, map[string]any{"i": i}); err != nil {
			t.Fatalf("put %s failed: %v", key, err)
		}
	}
	return database, driver
}

// TestListSpansStoragePages is the regression test for the truncation bug: List
// discarded the driver's continuation cursor, so a keyspace larger than one
// storage page was silently cut off at the page boundary.
func TestListSpansStoragePages(t *testing.T) {
	const (
		pageCap = 10
		keys    = 35
	)
	database, driver := openWithKeys(t, pageCap, keys)

	items, err := database.List(context.Background(), "items/", 0)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) != keys {
		t.Fatalf("listed %d keys, want %d (listing stopped at a storage page boundary)", len(items), keys)
	}
	if driver.listCalls < keys/pageCap {
		t.Errorf("made %d storage listings for %d keys at page size %d: pagination did not happen",
			driver.listCalls, keys, pageCap)
	}

	// Keys must be complete and unduplicated across page boundaries.
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if _, dup := seen[item.Meta.Key]; dup {
			t.Errorf("key %q returned twice across pages", item.Meta.Key)
		}
		seen[item.Meta.Key] = struct{}{}
	}
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("items/%05d", i)
		if _, ok := seen[key]; !ok {
			t.Errorf("key %q is missing from the listing", key)
		}
	}
}

// TestListHonorsLimitAcrossPages: a limit larger than one storage page must
// return that many keys, not one page's worth.
func TestListHonorsLimitAcrossPages(t *testing.T) {
	database, _ := openWithKeys(t, 10, 50)

	for _, limit := range []int{1, 9, 10, 11, 25, 50} {
		items, err := database.List(context.Background(), "items/", limit)
		if err != nil {
			t.Fatalf("list(limit=%d) failed: %v", limit, err)
		}
		if len(items) != limit {
			t.Errorf("list(limit=%d) returned %d keys", limit, len(items))
		}
	}
}

// TestListLimitBeyondKeyspace must not spin or over-report.
func TestListLimitBeyondKeyspace(t *testing.T) {
	database, _ := openWithKeys(t, 10, 12)

	items, err := database.List(context.Background(), "items/", 1000)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) != 12 {
		t.Errorf("listed %d keys, want 12", len(items))
	}
}

func TestListPageWalksWithCursor(t *testing.T) {
	database, _ := openWithKeys(t, 10, 25)
	ctx := context.Background()

	var (
		all    []string
		cursor string
		pages  int
	)
	for {
		items, next, err := database.ListPage(ctx, "items/", db.ListPageOptions{Limit: 10, Cursor: cursor})
		if err != nil {
			t.Fatalf("page %d failed: %v", pages, err)
		}
		pages++
		for _, item := range items {
			all = append(all, item.Meta.Key)
		}
		if next == "" {
			break
		}
		cursor = next
		if pages > 10 {
			t.Fatal("cursor did not terminate")
		}
	}

	if len(all) != 25 {
		t.Errorf("walked %d keys over %d pages, want 25", len(all), pages)
	}
	if pages != 3 {
		t.Errorf("walked %d pages, want 3 for 25 keys at 10 per page", pages)
	}
	for i := 1; i < len(all); i++ {
		if all[i-1] >= all[i] {
			t.Errorf("keys out of order across pages: %q then %q", all[i-1], all[i])
		}
	}
}

// TestListReportsSize covers the addition that lets a caller measure a keyspace
// without reading it: sizes come from the storage listing, so no payload moves.
func TestListReportsSize(t *testing.T) {
	database, _ := openWithKeys(t, 10, 15)

	items, err := database.List(context.Background(), "items/", 0)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}

	var total int64
	for _, item := range items {
		if item.Meta.Size <= 0 {
			t.Errorf("key %q reported size %d", item.Meta.Key, item.Meta.Size)
		}
		if len(item.Value) != 0 {
			t.Errorf("key %q returned a payload in a listing", item.Meta.Key)
		}
		total += item.Meta.Size
	}
	if total <= 0 {
		t.Error("listing reported no total size")
	}
}

func TestListPrefixIsolation(t *testing.T) {
	driver := &cappedDriver{Driver: memory.NewDriver(), pageCap: 5}
	database, err := db.Open(db.Options{Storage: driver})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer database.Close()

	ctx := context.Background()
	for i := 0; i < 12; i++ {
		if _, err := database.Put(ctx, fmt.Sprintf("users/%03d", i), map[string]any{"i": i}); err != nil {
			t.Fatalf("put failed: %v", err)
		}
		if _, err := database.Put(ctx, fmt.Sprintf("orders/%03d", i), map[string]any{"i": i}); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	items, err := database.List(ctx, "users/", 0)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(items) != 12 {
		t.Fatalf("listed %d keys under users/, want 12", len(items))
	}
	for _, item := range items {
		if len(item.Meta.Key) < 6 || item.Meta.Key[:6] != "users/" {
			t.Errorf("prefix listing leaked %q", item.Meta.Key)
		}
	}
}

// stalledDriver reports a cursor forever while returning no keys, the shape of a
// driver bug that would otherwise make List spin indefinitely.
type stalledDriver struct {
	storage.Driver
	calls int
}

func (d *stalledDriver) List(context.Context, string, storage.ListOptions) ([]*storage.KeyMeta, string, error) {
	d.calls++
	return nil, "always-more", nil
}

func TestListStopsWhenAPageMakesNoProgress(t *testing.T) {
	driver := &stalledDriver{Driver: memory.NewDriver()}
	database, err := db.Open(db.Options{Storage: driver})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer database.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		items, err := database.List(context.Background(), "items/", 0)
		if err != nil {
			t.Errorf("list failed: %v", err)
		}
		if len(items) != 0 {
			t.Errorf("listed %d keys from a driver that returns none", len(items))
		}
	}()

	select {
	case <-done:
	case <-timeoutAfterSeconds(5):
		t.Fatal("List did not terminate against a driver reporting an endless cursor")
	}

	if driver.calls > 2 {
		t.Errorf("made %d listing calls before giving up, want at most 2", driver.calls)
	}
}

func timeoutAfterSeconds(n int) <-chan time.Time {
	return time.After(time.Duration(n) * time.Second)
}

func TestListPageCachingAndInvalidation(t *testing.T) {
	driver := &cappedDriver{Driver: memory.NewDriver(), pageCap: 100}
	database, err := db.Open(db.Options{
		Storage:      driver,
		ListCacheTTL: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer database.Close()

	ctx := context.Background()
	_, _ = database.Put(ctx, "tasks/1", map[string]string{"title": "task 1"})
	_, _ = database.Put(ctx, "tasks/2", map[string]string{"title": "task 2"})

	initialCalls := driver.listCalls

	// 1. First ListPage: cache miss, hits storage
	items, _, err := database.ListPage(ctx, "tasks/", db.ListPageOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListPage 1 failed: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if driver.listCalls != initialCalls+1 {
		t.Fatalf("expected 1 storage list call, got %d", driver.listCalls-initialCalls)
	}

	// 2. Second ListPage: cache hit, storage NOT called!
	items2, _, err := database.ListPage(ctx, "tasks/", db.ListPageOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListPage 2 failed: %v", err)
	}
	if len(items2) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items2))
	}
	if driver.listCalls != initialCalls+1 {
		t.Fatalf("expected storage list calls to remain %d, got %d", initialCalls+1, driver.listCalls)
	}

	// 3. Put new document under "tasks/": must invalidate list cache
	_, err = database.Put(ctx, "tasks/3", map[string]string{"title": "task 3"})
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 4. Third ListPage: cache missed due to invalidation, fetches fresh from storage
	items3, _, err := database.ListPage(ctx, "tasks/", db.ListPageOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListPage 3 failed: %v", err)
	}
	if len(items3) != 3 {
		t.Fatalf("expected 3 items after put, got %d", len(items3))
	}
	if driver.listCalls != initialCalls+2 {
		t.Fatalf("expected storage list calls to be %d, got %d", initialCalls+2, driver.listCalls)
	}

	// 5. Delete document under "tasks/": must invalidate list cache
	err = database.Delete(ctx, "tasks/1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 6. Fourth ListPage: cache missed due to delete invalidation
	items4, _, err := database.ListPage(ctx, "tasks/", db.ListPageOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListPage 4 failed: %v", err)
	}
	if len(items4) != 2 {
		t.Fatalf("expected 2 items after delete, got %d", len(items4))
	}
	if driver.listCalls != initialCalls+3 {
		t.Fatalf("expected storage list calls to be %d, got %d", initialCalls+3, driver.listCalls)
	}
}


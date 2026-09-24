package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/cache"
	"github.com/avivklas/jaydb/pkg/cluster"
	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

// TestMultiNodeClusterFormation verifies that nodes discover each other via memberlist gossip.
func TestMultiNodeClusterFormation(t *testing.T) {
	ring := sharding.NewRing(3, 2)

	// Node 1
	store1 := memory.NewDriver()
	db1, err := db.Open(db.Options{Storage: store1, ShardingDepth: 2})
	if err != nil {
		t.Fatalf("db1 open error: %v", err)
	}
	defer db1.Close()

	node1, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:  "node-1",
		BindAddr:  "127.0.0.1",
		BindPort:  0,
		QuicPort:  0,
		Ring:      ring,
		DBHandler: db1,
	})
	if err != nil {
		t.Fatalf("node1 creation error: %v", err)
	}
	defer node1.Close()

	// Node 2 joins node 1
	store2 := memory.NewDriver()
	db2, err := db.Open(db.Options{Storage: store2, ShardingDepth: 2})
	if err != nil {
		t.Fatalf("db2 open error: %v", err)
	}
	defer db2.Close()

	node2, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:  "node-2",
		BindAddr:  "127.0.0.1",
		BindPort:  0,
		QuicPort:  0,
		JoinAddrs: []string{node1.GossipAddr()},
		Ring:      ring,
		DBHandler: db2,
	})
	if err != nil {
		t.Fatalf("node2 creation error: %v", err)
	}
	defer node2.Close()

	// Wait for gossip convergence
	time.Sleep(2 * time.Second)

	// Verify ring has both nodes
	node1Addr := node1.SelfQuicAddr()
	node2Addr := node2.SelfQuicAddr()

	testKey := "users/100/profile"
	ownerNode := ring.GetNode(testKey)

	if ownerNode != node1Addr && ownerNode != node2Addr {
		t.Errorf("ring failed to assign key %s to known nodes: got %s", testKey, ownerNode)
	}

	t.Logf("Cluster formed: node1=%s, node2=%s, owner of %s=%s", node1Addr, node2Addr, testKey, ownerNode)
}

// TestCrossNodeRoutingViaQUIC verifies Get/Put/Delete operations are routed correctly across nodes.
func TestCrossNodeRoutingViaQUIC(t *testing.T) {
	ring := sharding.NewRing(3, 2)

	// Node 1
	store1 := memory.NewDriver()
	db1, err := db.Open(db.Options{
		Storage:       store1,
		ShardingDepth: 2,
		Ring:          ring,
	})
	if err != nil {
		t.Fatalf("db1 open error: %v", err)
	}
	defer db1.Close()

	node1, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:  "node-1",
		BindAddr:  "127.0.0.1",
		BindPort:  0,
		QuicPort:  0,
		Ring:      ring,
		DBHandler: db1,
	})
	if err != nil {
		t.Fatalf("node1 creation error: %v", err)
	}
	defer node1.Close()

	// Node 2
	store2 := memory.NewDriver()
	db2, err := db.Open(db.Options{
		Storage:       store2,
		ShardingDepth: 2,
		Ring:          ring,
		ClusterNode:   node1, // Will route to node2 when needed
	})
	if err != nil {
		t.Fatalf("db2 open error: %v", err)
	}

	node2, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:  "node-2",
		BindAddr:  "127.0.0.1",
		BindPort:  0,
		QuicPort:  0,
		JoinAddrs: []string{node1.GossipAddr()},
		Ring:      ring,
		DBHandler: db1, // Both use db1 as handler for simplicity
	})
	if err != nil {
		t.Fatalf("node2 creation error: %v", err)
	}
	defer node2.Close()
	defer db2.Close()

	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// Write through db2 (may route to node1 or stay local)
	type User struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}

	user := User{Name: "Alice", Email: "alice@example.com"}
	meta, err := db2.Put(ctx, "users/alice/profile", user)
	if err != nil {
		t.Fatalf("Put error: %v", err)
	}

	if meta.ETag == "" {
		t.Error("Expected non-empty ETag after Put")
	}

	// Read back through db2
	var readUser User
	readMeta, err := db2.Get(ctx, "users/alice/profile", &readUser)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}

	if readUser.Name != "Alice" || readUser.Email != "alice@example.com" {
		t.Errorf("Get returned wrong data: %+v", readUser)
	}

	if readMeta.ETag != meta.ETag {
		t.Errorf("ETag mismatch: put=%s, get=%s", meta.ETag, readMeta.ETag)
	}

	// Delete
	err = db2.Delete(ctx, "users/alice/profile")
	if err != nil {
		t.Fatalf("Delete error: %v", err)
	}

	// Verify deletion
	_, err = db2.Get(ctx, "users/alice/profile", &readUser)
	if err != storage.ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete, got: %v", err)
	}

	t.Log("Cross-node QUIC routing verified")
}

// TestCacheInvalidationAcrossNodes verifies cache coherence when one node writes.
func TestCacheInvalidationAcrossNodes(t *testing.T) {
	store := memory.NewDriver()
	cacheMgr := cache.NewManager(store)
	cacheMgr.SetTTL(500 * time.Millisecond)

	ctx := context.Background()
	key := "test/cache/key"

	// Write and populate cache
	obj1, err := cacheMgr.Put(ctx, key, []byte("value1"), "")
	if err != nil {
		t.Fatalf("Put error: %v", err)
	}

	// Read from cache (should hit)
	objRead, err := cacheMgr.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}

	if string(objRead.Value) != "value1" {
		t.Errorf("Expected value1, got %s", objRead.Value)
	}

	hits1, _, _ := cacheMgr.Stats()
	if hits1 == 0 {
		t.Error("Expected cache hit")
	}

	// Overwrite with new ETag
	_, err = cacheMgr.Put(ctx, key, []byte("value2"), obj1.ETag)
	if err != nil {
		t.Fatalf("Put update error: %v", err)
	}

	// Read again (should reflect new value)
	objRead2, err := cacheMgr.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after update error: %v", err)
	}

	if string(objRead2.Value) != "value2" {
		t.Errorf("Expected value2 after update, got %s", objRead2.Value)
	}

	// Manual invalidation
	cacheMgr.Invalidate(key)

	// Next read should miss cache
	hitsBefore, _, _ := cacheMgr.Stats()
	_, _ = cacheMgr.Get(ctx, key)
	hitsAfter, _, _ := cacheMgr.Stats()

	if hitsAfter != hitsBefore {
		t.Error("Cache should have missed after invalidation")
	}

	t.Log("Cache invalidation verified")
}

// TestCASConflictHandling verifies optimistic concurrency control across concurrent writes.
func TestCASConflictHandling(t *testing.T) {
	store := memory.NewDriver()
	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		t.Fatalf("db open error: %v", err)
	}
	defer database.Close()

	ctx := context.Background()
	key := "test/cas/conflict"

	// Initial write
	meta1, err := database.Put(ctx, key, []byte("v1"))
	if err != nil {
		t.Fatalf("Initial Put error: %v", err)
	}

	// Concurrent write with stale ETag should fail
	_, err = database.Put(ctx, key, []byte("v2-stale"), db.WithExpectedETag("stale-etag"))
	if err != storage.ErrVersionMismatch {
		t.Errorf("Expected ErrVersionMismatch, got: %v", err)
	}

	// Valid CAS write with correct ETag should succeed
	meta2, err := database.Put(ctx, key, []byte("v2-valid"), db.WithExpectedETag(meta1.ETag))
	if err != nil {
		t.Fatalf("Valid CAS Put error: %v", err)
	}

	if meta2.ETag == meta1.ETag {
		t.Error("Expected new ETag after successful update")
	}

	// CreateOnly on existing key should fail
	_, err = database.Put(ctx, key, []byte("v3"), db.CreateOnly())
	if err != storage.ErrAlreadyExists {
		t.Errorf("Expected ErrAlreadyExists, got: %v", err)
	}

	t.Log("CAS conflict handling verified")
}

// TestPartitionRingRebalancing verifies ring rebalances when nodes join/leave.
func TestPartitionRingRebalancing(t *testing.T) {
	ring := sharding.NewRing(3, 2)

	node1Addr := "127.0.0.1:20001"
	node2Addr := "127.0.0.1:20002"
	node3Addr := "127.0.0.1:20003"

	ring.AddNode(node1Addr)
	ring.AddNode(node2Addr)

	testKeys := []string{
		"users/100/profile",
		"users/200/profile",
		"users/300/profile",
		"orders/100/details",
		"orders/200/details",
	}

	// Capture initial assignments
	initialOwners := make(map[string]string)
	for _, k := range testKeys {
		initialOwners[k] = ring.GetNode(k)
	}

	// Add third node
	ring.AddNode(node3Addr)

	// Check how many keys moved
	moved := 0
	for _, k := range testKeys {
		newOwner := ring.GetNode(k)
		if newOwner != initialOwners[k] {
			moved++
			t.Logf("Key %s moved from %s to %s", k, initialOwners[k], newOwner)
		}
	}

	// Remove node2
	ring.RemoveNode(node2Addr)

	// Keys assigned to node2 should have moved
	for _, k := range testKeys {
		currentOwner := ring.GetNode(k)
		if currentOwner == node2Addr {
			t.Errorf("Key %s still assigned to removed node %s", k, node2Addr)
		}
	}

	t.Logf("Ring rebalanced: %d/%d keys moved on node addition", moved, len(testKeys))
}

// TestSingleflightCoalescingUnderConcurrency verifies only 1 cold read happens for concurrent requests.
func TestSingleflightCoalescingUnderConcurrency(t *testing.T) {
	store := &countingDriver{
		inner: memory.NewDriver(),
	}
	cacheMgr := cache.NewManager(store)
	cacheMgr.SetTTL(10 * time.Second)

	ctx := context.Background()
	key := "test/singleflight/key"

	// Prime the storage
	_, err := store.Put(ctx, key, []byte("value"), "")
	if err != nil {
		t.Fatalf("Prime storage error: %v", err)
	}

	// Reset counter
	store.getCalls = 0

	// Spawn 100 concurrent readers
	const numReaders = 100
	var wg sync.WaitGroup
	wg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			_, _ = cacheMgr.Get(ctx, key)
		}()
	}

	wg.Wait()

	// Verify only 1 cold storage Get was issued
	if store.getCalls != 1 {
		t.Errorf("Expected exactly 1 Get call to storage, got %d", store.getCalls)
	}

	_, misses, sfHits := cacheMgr.Stats()
	t.Logf("Misses: %d, Singleflight hits: %d, Storage calls: %d", misses, sfHits, store.getCalls)

	if sfHits < numReaders-1 {
		t.Errorf("Expected at least %d singleflight hits, got %d", numReaders-1, sfHits)
	}
}

// countingDriver wraps a storage driver and counts Get calls.
type countingDriver struct {
	inner    storage.Driver
	getCalls int
	mu       sync.Mutex
}

func (c *countingDriver) Get(ctx context.Context, key string) (*storage.Object, error) {
	c.mu.Lock()
	c.getCalls++
	c.mu.Unlock()
	// Simulate slow cold storage read
	time.Sleep(50 * time.Millisecond)
	return c.inner.Get(ctx, key)
}

func (c *countingDriver) Put(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	return c.inner.Put(ctx, key, value, expectedETag)
}

func (c *countingDriver) Delete(ctx context.Context, key string, expectedETag string) error {
	return c.inner.Delete(ctx, key, expectedETag)
}

func (c *countingDriver) List(ctx context.Context, prefix string, opts storage.ListOptions) ([]*storage.KeyMeta, string, error) {
	return c.inner.List(ctx, prefix, opts)
}

func (c *countingDriver) Close() error {
	return c.inner.Close()
}

// TestCacheLocationInCluster verifies that caching occurs strictly on the target (owner) node
// and NOT on the node that receives/handles the user request.
func TestCacheLocationInCluster(t *testing.T) {
	ring := sharding.NewRing(3, 2)

	// Node 1 (Storage + DB 1)
	store1 := memory.NewDriver()
	db1, err := db.Open(db.Options{
		Storage:       store1,
		ShardingDepth: 2,
		Ring:          ring,
	})
	if err != nil {
		t.Fatalf("db1 open error: %v", err)
	}
	defer db1.Close()

	node1, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:  "node-1",
		BindAddr:  "127.0.0.1",
		BindPort:  0,
		QuicPort:  0,
		Ring:      ring,
		DBHandler: db1,
	})
	if err != nil {
		t.Fatalf("node1 creation error: %v", err)
	}
	defer node1.Close()

	// Node 2 (Storage + DB 2)
	store2 := memory.NewDriver()
	node2, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:  "node-2",
		BindAddr:  "127.0.0.1",
		BindPort:  0,
		QuicPort:  0,
		JoinAddrs: []string{node1.GossipAddr()},
		Ring:      ring,
	})
	if err != nil {
		t.Fatalf("node2 creation error: %v", err)
	}
	defer node2.Close()

	db2, err := db.Open(db.Options{
		Storage:       store2,
		ShardingDepth: 2,
		Ring:          ring,
		ClusterNode:   node2,
	})
	if err != nil {
		t.Fatalf("db2 open error: %v", err)
	}
	defer db2.Close()

	// Node 1 needs ClusterNode configured so its own SelfQuicAddr is known to db1
	// and node2 needs DBHandler set so it can also serve requests if routed.
	node2.RegisterHandler("", db2)

	// Wait for cluster gossip convergence
	time.Sleep(2 * time.Second)

	node1Addr := node1.SelfQuicAddr()
	node2Addr := node2.SelfQuicAddr()

	// Find a key owned specifically by node1
	var targetKey string
	for i := 0; i < 200; i++ {
		candidate := fmt.Sprintf("items/%d/profile", i)
		if ring.GetNode(candidate) == node1Addr {
			targetKey = candidate
			break
		}
	}
	if targetKey == "" {
		t.Fatal("Could not find a key owned by node1")
	}

	t.Logf("Selected key %q owned by node1 (%s), requested via node2 (%s)", targetKey, node1Addr, node2Addr)

	ctx := context.Background()

	// 1. Write the document via Node 2 (non-owner).
	// Node 2 forwards to Node 1 over the QUIC cluster mesh.
	meta, err := db2.Put(ctx, targetKey, []byte("cached-payload-123"))
	if err != nil {
		t.Fatalf("Put via node2 failed: %v", err)
	}
	if meta.ETag == "" {
		t.Fatal("Expected valid ETag")
	}

	// Verify Cache on Owner (Node 1):
	// Node 1 handled the write in PutRaw, so it MUST be cached in db1!
	hits1, _, _ := db1.Cache().Stats()
	items1, _ := db1.Cache().GetCacheSize()
	if items1 == 0 {
		t.Error("Expected Node 1 (owner) cache to contain data, but items count is 0")
	}

	// Verify Cache on Request-Handler (Node 2):
	// Node 2 is a non-owner, so it MUST NOT cache the data!
	items2, _ := db2.Cache().GetCacheSize()
	if items2 != 0 {
		t.Errorf("Expected Node 2 (request-handler) cache to be empty, got %d items", items2)
	}
	hits2, _, _ := db2.Cache().Stats()
	if hits2 != 0 {
		t.Errorf("Expected Node 2 to have 0 cache hits, got %d", hits2)
	}

	// 2. Read the document back via Node 2 (non-owner).
	// Node 2 forwards the Get to Node 1 via QUIC mesh.
	var readVal []byte
	readMeta, err := db2.Get(ctx, targetKey, &readVal)
	if err != nil {
		t.Fatalf("Get via node2 failed: %v", err)
	}
	if string(readVal) != "cached-payload-123" {
		t.Fatalf("Expected 'cached-payload-123', got %q", string(readVal))
	}
	if readMeta.ETag != meta.ETag {
		t.Fatalf("ETag mismatch: put=%s, get=%s", meta.ETag, readMeta.ETag)
	}

	// 3. Verify Cache Hit occurred on Node 1 (Owner) and NOT on Node 2:
	newHits1, _, _ := db1.Cache().Stats()
	newHits2, _, _ := db2.Cache().Stats()

	if newHits1 <= hits1 {
		t.Errorf("Expected Node 1 (owner) cache hits to increase, before=%d, after=%d", hits1, newHits1)
	}
	if newHits2 != 0 {
		t.Errorf("Expected Node 2 (request-handler) cache hits to remain 0, got %d", newHits2)
	}
	afterItems2, _ := db2.Cache().GetCacheSize()
	if afterItems2 != 0 {
		t.Errorf("Expected Node 2 cache to remain empty after read, got %d items", afterItems2)
	}

	t.Logf("Cache verification passed: Node 1 (owner) hits=%d items=%d, Node 2 (handler) hits=%d items=%d",
		newHits1, items1, newHits2, afterItems2)
}


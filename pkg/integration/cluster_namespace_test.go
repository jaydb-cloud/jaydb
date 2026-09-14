package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/avivklas/jaydb/pkg/cluster"
	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

const testClusterSecret = "integration-test-secret"

// twoNodeMesh brings up two real gossip+QUIC nodes sharing one ring, mirroring
// the production topology (two tasks, one ring, several namespaces per process).
func twoNodeMesh(t *testing.T) (*cluster.Node, *cluster.Node, *sharding.Ring) {
	t.Helper()

	ring := sharding.NewRing(100, 2)

	n1, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:      "node-1",
		BindAddr:      "127.0.0.1",
		BindPort:      0,
		QuicPort:      0,
		Ring:          ring,
		ClusterSecret: testClusterSecret,
	})
	if err != nil {
		t.Fatalf("node 1: %v", err)
	}
	t.Cleanup(func() { _ = n1.Close() })

	n2, err := cluster.NewNode(cluster.NodeConfig{
		NodeName:      "node-2",
		BindAddr:      "127.0.0.1",
		BindPort:      0,
		QuicPort:      0,
		Ring:          ring,
		JoinAddrs:     []string{n1.GossipAddr()},
		ClusterSecret: testClusterSecret,
	})
	if err != nil {
		t.Fatalf("node 2: %v", err)
	}
	t.Cleanup(func() { _ = n2.Close() })

	// Wait for both QUIC addresses to appear in the ring.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ring.HasNode(n1.SelfQuicAddr()) && ring.HasNode(n2.SelfQuicAddr()) {
			return n1, n2, ring
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("mesh did not form: ring missing %s and/or %s", n1.SelfQuicAddr(), n2.SelfQuicAddr())
	return nil, nil, nil
}

// This is the production incident, reproduced end to end: two nodes, several
// namespaces per node, and reads that the ring forwards to the peer. Before the
// fix every forwarded read came back empty and surfaced as "document not found"
// for roughly half of all requests.
func TestClusteredReadsAcrossNamespaces(t *testing.T) {
	n1, n2, ring := twoNodeMesh(t)

	namespaces := []string{"org_a:main", "org_a:analytics", "org_b:main"}

	// Both nodes host every namespace, each over its own storage, exactly as
	// jaydb-cloud does. Shared storage is emulated by seeding both sides.
	dbs := map[string]map[string]db.DB{} // node -> namespace -> DB
	for label, node := range map[string]*cluster.Node{"n1": n1, "n2": n2} {
		dbs[label] = map[string]db.DB{}
		for _, ns := range namespaces {
			d, err := db.Open(db.Options{
				Storage:     memory.NewDriver(),
				Ring:        ring,
				ClusterNode: node,
				Namespace:   ns,
			})
			if err != nil {
				t.Fatalf("open %s on %s: %v", ns, label, err)
			}
			dbs[label][ns] = d
		}
	}

	// Seed the same document into both nodes' copies of each namespace, with a
	// value that identifies its namespace so cross-namespace bleed is visible.
	type doc struct {
		Owner string `json:"owner"`
	}
	const key = "users/101/profile"
	for _, ns := range namespaces {
		for _, label := range []string{"n1", "n2"} {
			// Write directly to storage, bypassing ring routing, so setup does
			// not depend on the behaviour under test.
			raw := fmt.Sprintf(`{"owner":%q}`, ns)
			if _, err := dbs[label][ns].PutRaw(context.Background(), key, []byte(raw), ""); err != nil {
				t.Fatalf("seed %s on %s: %v", ns, label, err)
			}
		}
	}

	// Read through the ring from BOTH nodes. For each namespace one of these is
	// local and the other is forwarded over QUIC; we do not care which.
	for _, ns := range namespaces {
		for _, label := range []string{"n1", "n2"} {
			var got doc
			meta, err := dbs[label][ns].Get(context.Background(), key, &got)
			if err != nil {
				t.Errorf("%s reading %s: unexpected error %v (a forwarded read must not fail)", label, ns, err)
				continue
			}
			if meta.Key != key {
				t.Errorf("%s reading %s: got key %q, want %q", label, ns, meta.Key, key)
			}
			if got.Owner != ns {
				t.Errorf("%s reading %s: served a document owned by %q -- cross-namespace routing bug",
					label, ns, got.Owner)
			}
		}
	}
}

// A read for a namespace the peer does not serve must fail loudly rather than
// masquerading as a missing document.
func TestForwardedReadForUnservedNamespaceFailsLoud(t *testing.T) {
	n1, n2, ring := twoNodeMesh(t)

	// Only node 1 serves "lonely". Node 2 holds a DB object for it but never
	// registers it, emulating the original bug (no handler on the owner).
	d1, err := db.Open(db.Options{Storage: memory.NewDriver(), Ring: ring, ClusterNode: n1, Namespace: "lonely"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d1.PutRaw(context.Background(), "k", []byte(`{}`), ""); err != nil {
		t.Fatal(err)
	}
	n2.UnregisterHandler("lonely")

	// Find a key the ring assigns to node 2, so the read from node 1 is forwarded
	// to a node with no handler for this namespace.
	var forwardedKey string
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("probe/%d", i)
		if ring.GetNode(k) == n2.SelfQuicAddr() {
			forwardedKey = k
			break
		}
	}
	if forwardedKey == "" {
		t.Skip("ring never assigned a probe key to node 2")
	}

	_, err = d1.Get(context.Background(), forwardedKey, nil)
	if err == nil {
		t.Fatal("expected an error when the owner has no handler for the namespace")
	}
	// The critical assertion: this must NOT look like a missing document.
	if errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("owner-without-handler was reported as ErrNotFound (%v) -- this is the bug that looked like data loss", err)
	}
	t.Logf("failed loudly, as intended: %v", err)
}

// TestNamespaceCloseDoesNotTearDownSharedClusterNode verifies that closing one
// namespace DB does not shut down the shared ClusterNode, its QUIC listener, or
// its MeshPool. Other namespaces sharing the same ClusterNode must remain
// fully functional and able to route inter-queries.
func TestNamespaceCloseDoesNotTearDownSharedClusterNode(t *testing.T) {
	n1, n2, ring := twoNodeMesh(t)

	d1A, err := db.Open(db.Options{Storage: memory.NewDriver(), Ring: ring, ClusterNode: n1, Namespace: "ns-a"})
	if err != nil {
		t.Fatal(err)
	}
	d1B, err := db.Open(db.Options{Storage: memory.NewDriver(), Ring: ring, ClusterNode: n1, Namespace: "ns-b"})
	if err != nil {
		t.Fatal(err)
	}
	d2B, err := db.Open(db.Options{Storage: memory.NewDriver(), Ring: ring, ClusterNode: n2, Namespace: "ns-b"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = d1B.Close()
		_ = d2B.Close()
	})

	// Close ns-a on node 1. This must NOT close n1 or n1's MeshPool!
	if err := d1A.Close(); err != nil {
		t.Fatalf("d1A.Close(): %v", err)
	}

	// Put a document in ns-b on node 2 for a key owned by node 2
	var localKeyB string
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("testb/%d", i)
		if ring.GetNode(k) == n2.SelfQuicAddr() {
			localKeyB = k
			break
		}
	}
	if localKeyB == "" {
		t.Skip("no key owned by n2 found")
	}

	payload := `{"title":"Task 1"}`
	if _, err := d2B.PutRaw(context.Background(), localKeyB, []byte(payload), ""); err != nil {
		t.Fatal(err)
	}

	// Node 1 reads ns-b over the inter-query mesh. If n1's meshPool was killed, this would fail with
	// "peer pool for <addr> not found" or "peer pool is closed".
	var gotRaw []byte
	_, err = d1B.Get(context.Background(), localKeyB, &gotRaw)
	if err != nil {
		t.Fatalf("inter-query from d1B failed after d1A.Close(): %v", err)
	}
	if string(gotRaw) != payload {
		t.Fatalf("unexpected content: %s, want %s", string(gotRaw), payload)
	}
}


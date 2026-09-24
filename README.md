<div align="center">

<img src="assets/logo_mark.svg" alt="JayDB Logo" width="100" />

# JayDB

**Cache-Accelerated Document Store Backed by S3 and Optimistic Concurrency**

*In-memory caching • S3/R2 durable cold storage • Atomic CAS via ETags • QUIC cluster mesh*

[![CI Status](https://github.com/jaydb-cloud/jaydb/actions/workflows/ci.yml/badge.svg)](https://github.com/jaydb-cloud/jaydb/actions/workflows/ci.yml)
[![Go Reference](https://img.shields.io/badge/Go-Reference-007d9c?logo=go&logoColor=white)](https://pkg.go.dev/github.com/avivklas/jaydb)
[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](#contributing)

</div>

---

**JayDB** is a cache-accelerated document store written in **Go**, designed to pair in-memory read performance with the durability and low cost of cloud object storage.

Running traditional stateful databases (PostgreSQL, MongoDB, Redis) requires provisioning compute instances, managing EBS/SSD volume capacity, configuring replication failover, and paying fixed monthly hosting bills even when workloads are idle.

JayDB takes a different architectural approach by decoupling execution and caching from durable persistence:
- **Authoritative In-Memory Cache**: Hot documents are held in memory with key-owner routing, serving reads in microseconds.
- **Singleflight Coalescing**: Key-level read coalescing guarantees that concurrent cache misses trigger only **1 cold read** to object storage.
- **S3-Backed Persistence**: Durable cold state rests in commodity object storage (**AWS S3**, **Cloudflare R2**, **MinIO**, or local disk)—eliminating database disks, volume resizing, and backup snapshot scripts.
- **Atomic Optimistic Concurrency (CAS)**: Updates use HTTP ETag matching (`If-Match` / `If-None-Match: *`) for lock-free, race-condition-safe writes without connection pool limits.

Because JayDB can run embedded as a pure Go library or with built-in `memory` and `fs` drivers, it runs with **zero Docker containers or cloud credentials during local development, CI test suites, and AI agent execution**.

---

## ⚡ Key Highlights

- **💸 Under $0.32/Month at Scale**: Uses S3-compatible object storage as primary cold storage. Handles 1,000,000 API requests/month for pennies; idle instances cost $0.00.
- **⚡ Microsecond Read Latencies & Singleflight Coalescing**: Authoritative owner-node caching serves hot reads from memory. Concurrent cache misses coalesce at the key level, ensuring only **1 cold read** reaches S3.
- **🔒 Atomic Optimistic Concurrency Control (CAS)**: Race-condition-free updates across nodes using standard HTTP ETags (`If-Match` and `If-None-Match: *`).
- **🌐 Peer-to-Peer Cluster Mesh**: Automatic node discovery via memberlist SWIM gossip (`memberlist`), deterministic partition ring, and multiplexed QUIC streams (`quic-go`) for sub-millisecond inter-query routing.
- **📁 Hierarchical Keys & Schema-Free**: Store structured JSON, MessagePack, or raw binary payloads under intuitive URI-like document paths (`users/123/profile`, `teams/alpha/tasks/987`).
- **🔍 ListCache with Prefix Invalidation**: In-memory caching for directory and prefix listings with automatic invalidation on writes, preserving strict read-after-write consistency.
- **📦 Zero-Dependency Local Dev & Testing**: Built-in `memory` and `fs` storage drivers allow running tests and local development with **zero Docker containers, zero external processes, and zero cloud credentials**.
- **📊 Production-Grade Observability**: Comprehensive Prometheus metrics for cache efficiency, storage latencies, CAS conflicts, and cluster topology.

---

## 📊 Comparison: Why JayDB?

| Metric / Capability | Traditional Managed DB (Postgres / Mongo) | Managed Key-Value (Redis / DynamoDB) | JayDB |
| :--- | :--- | :--- | :--- |
| **Idle Monthly Cost** | $15 – $60+/month base fee | $15 – $30+/month (or per-request floor) | **$0.00 idle** (~$0.32/mo for 1M reqs) |
| **Ops & Maintenance** | Sizing disks/replicas, connection pools, upgrades | Sharding keys, throughput provisioning | **No disks to manage** (durable persistence in S3) |
| **Local Dev & CI** | Requires Docker, daemon, credentials | Requires Docker or mock services | **Zero dependency** (`memory` / `fs` drivers built-in) |
| **Schema & Migrations** | Strict DDL schemas, migration scripts | Partial schema / key limits | **Schema-free document trees** (JSON, MsgPack, Raw) |
| **Cluster Coordination** | Complex read-replicas, proxies, connection pools | Sentinel, Redis Cluster, DynamoDB partitions | **SWIM gossip + QUIC multiplexed mesh** |
| **Concurrency Control** | Row locks, transactions, deadlock management | Mutexes or Lua scripts | **Lock-free HTTP CAS (ETags / If-Match)** |
| **AI Agent Ergonomics** | High configuration friction, migration errors | Low-level key semantics | **Frictionless document store (REST & Go API)** |

---

## 🏗️ Architecture Overview

JayDB can be deployed as an **embedded Go library** or as a **clustered standalone server**:

```
+-------------------------------------------------------------------------------+
|                                  SERVER MODE                                  |
|  - fasthttp RESTful HTTP API (GET, PUT, DELETE, LIST)                         |
|  - Memberlist Gossip Discovery (SWIM Protocol)                                |
|  - Lexicographical Partition Ring (Prefix-based key distribution)             |
|  - Multiplexed QUIC Connection Mesh (Sub-millisecond inter-query execution)   |
+-------------------------------------------------------------------------------+
                                        |
                                        v (Wraps internally)
+-------------------------------------------------------------------------------+
|                                 EMBEDDED MODE                                 |
|                             (Core Engine Library)                             |
|                                                                               |
|  +-------------------------------------------------------------------------+  |
|  | High-Level Go API (Get, Put, Delete, List, ListPage)                    |  |
|  +-------------------------------------------------------------------------+  |
|  | Key-Level Mutex & Singleflight Cache Manager                            |  |
|  |   - Strict Owner-Node In-Memory Caching (LRU eviction + TTL)            |  |
|  |   - Singleflight Read Coalescing (1 S3 GET for concurrent readers)      |  |
|  |   - ListCache with write-time prefix invalidation                       |  |
|  |   - Shared CacheBudget memory accounting                                |  |
|  +-------------------------------------------------------------------------+  |
|  | Pluggable Codecs (JSON default, Raw binary, MessagePack)                |  |
|  +-------------------------------------------------------------------------+  |
|  | Pluggable Cold Storage Drivers:                                         |  |
|  |   - AWS S3 / Cloudflare R2 / MinIO (Production)                         |  |
|  |   - Local Filesystem Driver (Persistent local dev)                      |  |
|  |   - In-Memory Driver (Unit tests, CI, sandbox execution)                |  |
+-------------------------------------------------------------------------------+
```

---

## 🚀 Quickstart

### 1. Embedded Go Usage

Import JayDB directly into your application. Embedded mode provides zero network overhead and programmatic control.

#### Local Development / In-Memory (Zero Dependencies)

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

type Project struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

func main() {
	ctx := context.Background()

	// 1. Open an in-memory database instance (ideal for local dev, agents, & unit tests)
	database, err := db.Open(db.Options{
		Storage: memory.NewDriver(),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	// 2. Put a document with CreateOnly (fails if key exists)
	proj := Project{Name: "Phoenix", Status: "active"}
	meta, err := database.Put(ctx, "projects/101", proj, db.CreateOnly())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Created document. ETag: %s\n", meta.ETag)

	// 3. Read document
	var fetched Project
	readMeta, err := database.Get(ctx, "projects/101", &fetched)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Fetched project: %+v (ETag: %s)\n", fetched, readMeta.ETag)

	// 4. Update with Compare-And-Swap (CAS)
	proj.Status = "completed"
	newMeta, err := database.Put(ctx, "projects/101", proj, db.WithExpectedETag(readMeta.ETag))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Updated document. New ETag: %s\n", newMeta.ETag)
}
```

#### Production S3 Backend with Shared Memory Budget

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/avivklas/jaydb/pkg/cache"
	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/storage/s3"
)

func main() {
	ctx := context.Background()

	// Initialize S3 driver (supports AWS S3, Cloudflare R2, MinIO)
	s3Driver, err := s3.NewDriver(s3.Config{
		Bucket: "my-production-bucket",
		Region: "us-east-1",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Create a shared memory ceiling (e.g., 256MB)
	budget := cache.NewBudget(256 * 1024 * 1024)

	database, err := db.Open(db.Options{
		Storage:            s3Driver,
		CacheBudget:        budget,
		CacheTTL:           30 * time.Minute,
		ListCacheTTL:       60 * time.Second,
		ShardingDepth:      2, // Prefix depth for partitioning (e.g. "org/team")
	})
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	// Query keys by prefix
	items, nextToken, err := database.ListPage(ctx, "projects/", 10, "")
	if err != nil {
		log.Fatal(err)
	}
	for _, item := range items {
		fmt.Printf("Found document: %s (Size: %d bytes)\n", item.Meta.Key, item.Meta.Size)
	}
	_ = nextToken
}
```

---

### 2. Standalone Server Mode (RESTful HTTP API)

Run JayDB as a standalone, containerized microservice powered by `fasthttp`:

```go
package main

import (
	"log"

	"github.com/avivklas/jaydb/pkg/db"
	"github.com/avivklas/jaydb/pkg/server"
	"github.com/avivklas/jaydb/pkg/storage/s3"
)

func main() {
	store, err := s3.NewDriver(s3.Config{Bucket: "production-data"})
	if err != nil {
		log.Fatal(err)
	}

	database, err := db.Open(db.Options{Storage: store})
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	srv, err := server.NewServer(server.Options{DB: database})
	if err != nil {
		log.Fatal(err)
	}

	// Starts FastHTTP listener on :8080 (including /metrics and /v1/health)
	log.Fatal(srv.ListenAndServe(":8080"))
}
```

#### HTTP API Reference

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/v1/kv/{key}` | `GET` | Retrieve document and return its ETag header |
| `/v1/kv/{key}` | `PUT` | Create or update document (supports `If-Match`, `If-None-Match`) |
| `/v1/kv/{key}` | `DELETE` | Delete document (supports `If-Match`) |
| `/v1/kv/{prefix}?list=true&limit=N` | `GET` | List document keys matching prefix with pagination |
| `/metrics` | `GET` | Prometheus metrics scrape endpoint |
| `/v1/health` | `GET` | Liveness and health check |

#### REST API Examples with `curl`

```bash
# 1. Create a document with If-None-Match (fails if key already exists)
curl -i -X PUT http://localhost:8080/v1/kv/users/123/profile \
  -H "If-None-Match: *" \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "role": "engineer"}'

# Response includes: ETag: "3a8f...b2"

# 2. Read document (Cache-accelerated)
curl -i http://localhost:8080/v1/kv/users/123/profile

# 3. Update document with Compare-And-Swap (CAS)
curl -i -X PUT http://localhost:8080/v1/kv/users/123/profile \
  -H "If-Match: \"3a8f...b2\"" \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "role": "staff engineer"}'

# 4. List keys by prefix
curl -i "http://localhost:8080/v1/kv/users/?list=true&limit=50"
```

---

### 3. Distributed Multi-Node Cluster

Deploy multi-node clusters with automatic peer discovery via Memberlist (SWIM gossip) and sub-millisecond query forwarding over a multiplexed QUIC connection mesh:

```go
// Node 1 (Seed node)
node1, _ := cluster.NewNode(cluster.NodeConfig{
    NodeName: "node-1",
    BindAddr: "10.0.0.1",
    BindPort: 19001, // Memberlist gossip port
    QuicPort: 19002, // QUIC mesh routing port
    Ring:     ring,
    DBHandler: dbInstance1,
})

// Node 2 (Joins Node 1)
node2, _ := cluster.NewNode(cluster.NodeConfig{
    NodeName:  "node-2",
    BindAddr:  "10.0.0.2",
    BindPort:  19001,
    QuicPort:  19002,
    JoinAddrs: []string{"10.0.0.1:19001"},
    Ring:      ring,
    DBHandler: dbInstance2,
})
```

#### Dynamic / Ephemeral Ports

Set `BindPort: 0` or `QuicPort: 0` to let the OS assign free ports—ideal for local integration tests, ephemeral test containers, or dynamic microservice environments:

```go
node, _ := cluster.NewNode(cluster.NodeConfig{
    NodeName: "node-worker",
    BindAddr: "127.0.0.1",
    BindPort: 0,
    QuicPort: 0,
    Ring:     ring,
    DBHandler: dbInstance,
})

// Retrieve the bound addresses for peer configuration
gossipAddr := node.GossipAddr()   // e.g. "127.0.0.1:54312"
quicAddr   := node.SelfQuicAddr() // e.g. "127.0.0.1:54313"
```

---

## 💰 AWS S3 Cost Breakdown (<$0.32 / Month)

JayDB is architected to eliminate unnecessary cloud storage fees. By combining **authoritative owner-node caching** with **singleflight read coalescing**, over **95% of reads** are served directly from RAM without touching S3.

### Workload Assumptions (1,000,000 Requests/Month)
- **Active Documents**: 10,000 documents (~2 GB total S3 storage).
- **Application Reads**: 1,000,000 GET requests/month (~33,000 requests/day).
- **Application Writes**: 50,000 PUT/DELETE requests/month.

### Monthly Cost Estimation (AWS S3 Standard, US East):

| Expense Item | Volume | AWS S3 Rate | Effective Monthly Cost |
| :--- | :--- | :--- | :--- |
| **S3 Storage** | 2 GB data | $0.023 / GB / month | **$0.046** |
| **S3 GET Requests** | 50,000 cold reads *(95% absorbed by cache)* | $0.0004 / 1,000 requests | **$0.020** |
| **S3 PUT/POST Requests**| 50,000 write operations | $0.0050 / 1,000 requests | **$0.250** |
| **Data Transfer In** | Unlimited incoming bandwidth | FREE | **$0.000** |
| **Data Transfer Out** | First 100 GB / month | FREE | **$0.000** |
| **TOTAL MONTHLY COST** | | | **~$0.316 / month** |

*Note: When deployed with Cloudflare R2 or MinIO, egress fees and request fees can be even lower.*

---

## 📈 Observability & Monitoring

JayDB provides native Prometheus metrics tracking engine internals out of the box:

- **Cache Effectiveness**: `jaydb_cache_hits_total`, `jaydb_cache_misses_total`, singleflight coalesced requests, byte consumption.
- **Storage Latencies**: `jaydb_storage_operation_duration_seconds` histogram partitioned by driver and operation (`get`, `put`, `delete`, `list`).
- **HTTP Throughput**: `jaydb_http_requests_total` partitioned by route, HTTP method, and status code.
- **Concurrency & Contention**: `jaydb_cas_conflicts_total` tracking optimistic locking collisions.
- **Cluster Mesh Health**: `jaydb_cluster_nodes`, inter-query forwarded requests, and active QUIC streams.

```bash
# Scrape metrics directly from the server
curl http://localhost:8080/metrics

# Or run the metrics example
go run examples/metrics/main.go
```

For full metric definitions, PromQL queries, Grafana dashboards, and production alerting rules, refer to [**OBSERVABILITY.md**](OBSERVABILITY.md).

---

## 🧪 Testing

JayDB is verified under strict concurrency conditions using Go's race detector. Run the complete test suite:

```bash
# Run all unit, integration, and race detection tests
go test -v -race ./...

# Run storage driver tests with in-memory backend
go test -v ./pkg/storage/memory/...

# Run benchmarks
go test -v -bench=. ./pkg/benchmarks/...
```

---

## 🤝 Contributing
<a id="contributing"></a>

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](https://github.com/jaydb-cloud/jaydb/issues).

1. Fork the repository
2. Create your branch (`git checkout -b feature/amazing-feature`)
3. Ensure formatting and race checks pass (`gofmt -s -w .` and `go test -race ./...`)
4. Commit your changes (`git commit -m 'feat: add amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

---

## 📄 License

Distributed under the MIT License. See [LICENSE](LICENSE) for more information.

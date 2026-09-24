# JayDB Observability & Monitoring

JayDB exposes comprehensive **Prometheus metrics** for monitoring cache performance, storage backend operations, HTTP request patterns, cluster activity, and CAS conflicts.

---

## Quick Start

### 1. Access Metrics Endpoint

JayDB automatically exposes metrics at `/metrics` on the HTTP server:

```bash
# Start JayDB server
go run examples/metrics/main.go

# View metrics in Prometheus format
curl http://localhost:8080/metrics
```

### 2. Integrate with Prometheus

Add this scrape configuration to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'jaydb'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:8080']
```

### 3. Visualize in Grafana

Import the provided Grafana dashboard (coming soon) or create custom dashboards using the metrics below.

---

## Available Metrics

### Cache Metrics

| Metric Name | Type | Description |
|------------|------|-------------|
| `jaydb_cache_hits_total` | Counter | Total number of cache hits |
| `jaydb_cache_misses_total` | Counter | Total number of cache misses |
| `jaydb_cache_singleflight_hits_total` | Counter | Total number of coalesced requests (multiple concurrent readers deduplicated to one backend fetch) |
| `jaydb_cache_items` | Gauge | Current number of items in cache |
| `jaydb_cache_size_bytes` | Gauge | Current cache size in bytes |

**Key Insight:** High `cache_hits_total` relative to `cache_misses_total` indicates effective caching. High `singleflight_hits_total` shows successful request coalescing under concurrent load.

---

### Storage Backend Metrics

| Metric Name | Type | Labels | Description |
|------------|------|--------|-------------|
| `jaydb_storage_operations_total` | Counter | `operation`, `status` | Total storage operations (get/put/delete) by status (success/error) |
| `jaydb_storage_operation_duration_seconds` | Histogram | `operation` | Duration of storage operations in seconds |
| `jaydb_storage_bytes_total` | Counter | `operation` | Total bytes transferred to/from storage backend |

**Example Query (PromQL):**
```promql
# Average S3 GET latency over 5 minutes
rate(jaydb_storage_operation_duration_seconds_sum{operation="get"}[5m])
/ rate(jaydb_storage_operation_duration_seconds_count{operation="get"}[5m])
```

---

### HTTP Server Metrics

| Metric Name | Type | Labels | Description |
|------------|------|--------|-------------|
| `jaydb_http_requests_total` | Counter | `method`, `path_prefix`, `status` | Total HTTP requests by endpoint and status code |
| `jaydb_http_request_duration_seconds` | Histogram | `method`, `path_prefix` | Duration of HTTP requests in seconds |
| `jaydb_http_request_size_bytes` | Histogram | - | Size of HTTP request bodies |
| `jaydb_http_response_size_bytes` | Histogram | - | Size of HTTP response bodies |

**Example Query (PromQL):**
```promql
# 99th percentile request latency
histogram_quantile(0.99, 
  rate(jaydb_http_request_duration_seconds_bucket[5m]))
```

---

### Database Operation Metrics

| Metric Name | Type | Labels | Description |
|------------|------|--------|-------------|
| `jaydb_db_operations_total` | Counter | `operation`, `status` | Total database operations by type and status |
| `jaydb_db_operation_duration_seconds` | Histogram | `operation` | Duration of database operations (includes cache + encoding + storage) |

**Status Values:**
- `success` — Operation completed successfully
- `error` — Operation failed (network, storage, encoding error)
- `cas_conflict` — CAS precondition failed (ETag mismatch)
- `not_found` — Document not found (DELETE only)

---

### CAS Conflict Metrics

| Metric Name | Type | Labels | Description |
|------------|------|--------|-------------|
| `jaydb_cas_conflicts_total` | Counter | `operation` | Total CAS (compare-and-swap) conflicts by operation (put/delete) |

**Use Case:** Track optimistic locking contention. High CAS conflict rates may indicate:
- Multiple writers competing for the same keys
- Stale ETags in client retry logic
- Need for application-level locking or coordination

---

### Cluster Metrics

| Metric Name | Type | Labels | Description |
|------------|------|--------|-------------|
| `jaydb_cluster_nodes` | Gauge | - | Current number of nodes in the cluster |
| `jaydb_cluster_forwarded_requests_total` | Counter | `target_node`, `status` | Total requests forwarded to other cluster nodes |
| `jaydb_cluster_mesh_connections` | Gauge | - | Current number of active cluster mesh TCP connections (legacy alias: `jaydb_cluster_quic_connections`) |

**Example Query (PromQL):**
```promql
# Request forwarding rate per node
rate(jaydb_cluster_forwarded_requests_total[5m])
```

---

### Key Distribution Metrics

| Metric Name | Type | Labels | Description |
|------------|------|--------|-------------|
| `jaydb_key_distribution` | Histogram | `shard` | Distribution of key path depths across shards |

**Use Case:** Identify key hotspots and unbalanced sharding.

---

## Integration Examples

### Go Application (Embedded Mode)

```go
package main

import (
    "github.com/avivklas/jaydb/pkg/db"
    "github.com/avivklas/jaydb/pkg/metrics"
    "github.com/avivklas/jaydb/pkg/storage/s3"
)

func main() {
    // Open database
    store, _ := s3.NewDriver(s3.Config{Bucket: "my-bucket"})
    database, _ := db.Open(db.Options{Storage: store})
    defer database.Close()

    // Initialize metrics collector
    collector := metrics.NewCollector(
        database.Cache().Stats,
        database.Cache().GetCacheSize,
    )
    collector.Start()
    defer collector.Stop()

    // Update metrics periodically (e.g., every 10 seconds)
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    go func() {
        for range ticker.C {
            collector.UpdateCacheMetrics()
        }
    }()

    // Your application logic...
}
```

### Server Mode (FastHTTP)

The `/metrics` endpoint is **automatically exposed** when using `server.NewServer()`:

```go
package main

import (
    "github.com/avivklas/jaydb/pkg/db"
    "github.com/avivklas/jaydb/pkg/server"
    "github.com/avivklas/jaydb/pkg/storage/s3"
)

func main() {
    store, _ := s3.NewDriver(s3.Config{Bucket: "my-bucket"})
    database, _ := db.Open(db.Options{Storage: store})

    srv, _ := server.NewServer(server.Options{DB: database})
    
    // Metrics automatically available at http://localhost:8080/metrics
    srv.ListenAndServe(":8080")
}
```

### Separate Metrics Port (Production Pattern)

For production environments, expose metrics on a separate port isolated from the application API:

```go
package main

import (
    "net/http"
    
    "github.com/avivklas/jaydb/pkg/db"
    "github.com/avivklas/jaydb/pkg/server"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
    database, _ := db.Open(db.Options{...})
    srv, _ := server.NewServer(server.Options{DB: database})

    // Application API on port 8080
    go srv.ListenAndServe(":8080")

    // Metrics-only endpoint on port 9090
    mux := http.NewServeMux()
    mux.Handle("/metrics", promhttp.Handler())
    http.ListenAndServe(":9090", mux)
}
```

**Firewall Rules:** Restrict port 9090 to internal monitoring networks only.

---

## Alerting Rules

### Recommended Prometheus Alerts

```yaml
groups:
  - name: jaydb_alerts
    interval: 30s
    rules:
      # High cache miss rate
      - alert: JayDBHighCacheMissRate
        expr: |
          rate(jaydb_cache_misses_total[5m]) 
          / (rate(jaydb_cache_hits_total[5m]) + rate(jaydb_cache_misses_total[5m]))
          > 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "JayDB cache miss rate exceeds 50%"
          description: "Cache hit rate is {{ $value | humanizePercentage }}"

      # High storage operation latency
      - alert: JayDBSlowStorageOps
        expr: |
          histogram_quantile(0.99, 
            rate(jaydb_storage_operation_duration_seconds_bucket{operation="get"}[5m])
          ) > 1.0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "JayDB storage GET p99 latency exceeds 1s"
          description: "Storage backend may be overloaded or experiencing issues"

      # Frequent CAS conflicts
      - alert: JayDBHighCASConflicts
        expr: rate(jaydb_cas_conflicts_total[5m]) > 10
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "JayDB experiencing high CAS conflict rate"
          description: "{{ $value }} CAS conflicts per second detected"

      # Cluster node down
      - alert: JayDBClusterNodeDown
        expr: jaydb_cluster_nodes < 3
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "JayDB cluster has fewer than 3 nodes"
          description: "Only {{ $value }} cluster nodes are active"
```

---

## Performance Tuning Based on Metrics

### Scenario 1: High Cache Miss Rate

**Symptom:**
```promql
rate(jaydb_cache_misses_total[5m]) > rate(jaydb_cache_hits_total[5m])
```

**Root Causes:**
- Cache TTL too short (default 5s)
- Working set larger than available cache memory
- Access patterns are random (poor temporal locality)

**Solutions:**
```go
// Increase cache TTL
database.Cache().SetTTL(30 * time.Second)

// Or use longer-lived cache for read-heavy workloads
database.Cache().SetTTL(5 * time.Minute)
```

---

### Scenario 2: High S3 GET Latency

**Symptom:**
```promql
histogram_quantile(0.99, 
  rate(jaydb_storage_operation_duration_seconds_bucket{operation="get"}[5m])
) > 0.5
```

**Root Causes:**
- S3 bucket in wrong region (cross-region latency)
- No S3 Transfer Acceleration enabled
- Cache TTL too aggressive (forcing frequent S3 reads)

**Solutions:**
- Deploy JayDB in the same AWS region as your S3 bucket
- Enable S3 Transfer Acceleration
- Increase cache TTL for read-heavy workloads

---

### Scenario 3: Singleflight Hits Not Reducing S3 Calls

**Symptom:**
```promql
jaydb_cache_singleflight_hits_total == 0
```

**Root Cause:** No concurrent read pressure on the same keys.

**Expected Behavior:** High singleflight hits indicate multiple goroutines requesting the same cold key simultaneously, and only 1 S3 GET is issued.

---

## Grafana Dashboard Example

### Key Panels

1. **Cache Hit Rate (%)** — `rate(jaydb_cache_hits_total[5m]) / (rate(jaydb_cache_hits_total[5m]) + rate(jaydb_cache_misses_total[5m])) * 100`
2. **Request Rate (req/s)** — `rate(jaydb_http_requests_total[1m])`
3. **p99 Request Latency** — `histogram_quantile(0.99, rate(jaydb_http_request_duration_seconds_bucket[5m]))`
4. **Storage Backend Latency** — `rate(jaydb_storage_operation_duration_seconds_sum[5m]) / rate(jaydb_storage_operation_duration_seconds_count[5m])`
5. **CAS Conflict Rate** — `rate(jaydb_cas_conflicts_total[5m])`
6. **Cluster Node Health** — `jaydb_cluster_nodes`

---

## Cost Monitoring

Track S3 API costs using storage operation metrics:

```promql
# Estimated S3 GET cost per month (USD)
(
  rate(jaydb_storage_operations_total{operation="get", status="success"}[30d]) * 2592000
  * 0.0004 / 1000
)

# Estimated S3 PUT cost per month (USD)
(
  rate(jaydb_storage_operations_total{operation="put", status="success"}[30d]) * 2592000
  * 0.005 / 1000
)
```

**Note:** Based on AWS S3 pricing ($0.0004 per 1000 GETs, $0.005 per 1000 PUTs).

---

## Troubleshooting

### Metrics Not Updating

**Symptom:** Cache metrics remain at 0 despite database activity.

**Solution:** Ensure you call `collector.UpdateCacheMetrics()` periodically:

```go
collector := metrics.NewCollector(database.Cache().Stats, database.Cache().GetCacheSize)
collector.Start()

ticker := time.NewTicker(10 * time.Second)
defer ticker.Stop()

go func() {
    for range ticker.C {
        collector.UpdateCacheMetrics()
    }
}()
```

### Metrics Endpoint 404

**Symptom:** `curl http://localhost:8080/metrics` returns 404.

**Check:**
1. Server is using the latest version with `/metrics` support
2. FastHTTP adapter is properly configured (automatic in `server.HandleRequest`)
3. Prometheus dependencies are imported:
   ```go
   import (
       "github.com/prometheus/client_golang/prometheus/promhttp"
       "github.com/valyala/fasthttp/fasthttpadaptor"
   )
   ```

---

## Best Practices

1. **Scrape Interval:** Use 15-30 second intervals for Prometheus scraping (balance between freshness and storage overhead).

2. **Histogram Buckets:** Default buckets are tuned for typical latencies (1ms - 10s). Adjust if your workload is unusual:
   ```go
   // Custom buckets for ultra-low-latency workloads
   metrics.HTTPRequestDuration = prometheus.NewHistogramVec(
       prometheus.HistogramOpts{
           Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1},
       },
       []string{"method", "path_prefix"},
   )
   ```

3. **Label Cardinality:** Avoid high-cardinality labels (e.g., full key paths). Use path prefixes instead:
   - ✅ `/v1/kv` (low cardinality)
   - ❌ `/v1/kv/users/12345/profile` (unbounded cardinality)

4. **Separate Metrics Port:** In production, expose metrics on a separate port (`:9090`) isolated from the application API (`:8080`).

5. **Alerts:** Set up alerts for cache miss rate, storage latency, and CAS conflicts.

---

## Next Steps

- **View Live Metrics:** Run `go run examples/metrics/main.go` and visit `http://localhost:8080/metrics`
- **Prometheus Setup:** [Download Prometheus](https://prometheus.io/download/) and configure scraping
- **Grafana Dashboard:** Import the provided dashboard JSON (coming soon) or build custom panels
- **Production Deployment:** Use the separate metrics port pattern and configure firewall rules

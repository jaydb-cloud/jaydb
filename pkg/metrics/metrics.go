package metrics

import (
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Cache metrics
	CacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jaydb_cache_hits_total",
		Help: "Total number of cache hits",
	})

	CacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jaydb_cache_misses_total",
		Help: "Total number of cache misses",
	})

	CacheSingleflightHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jaydb_cache_singleflight_hits_total",
		Help: "Total number of coalesced requests (singleflight hits)",
	})

	CacheSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jaydb_cache_size_bytes",
		Help: "Current cache size in bytes",
	})

	CacheItems = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jaydb_cache_items",
		Help: "Current number of items in cache",
	})

	CacheEvictions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jaydb_cache_evictions_total",
		Help: "Total number of cache entries evicted to stay within the byte budget",
	})

	CacheSkippedLarge = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jaydb_cache_skipped_large_total",
		Help: "Total number of objects not cached because they exceeded the per-object size cap",
	})

	CacheBudgetUsed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jaydb_cache_budget_used_bytes",
		Help: "Bytes currently accounted for against the shared cache byte budget",
	})

	CacheBudgetLimit = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jaydb_cache_budget_limit_bytes",
		Help: "Configured shared cache byte budget; 0 means unbounded",
	})

	CachePurgedOwnership = promauto.NewCounter(prometheus.CounterOpts{
		Name: "jaydb_cache_purged_ownership_total",
		Help: "Total number of cache entries dropped because the ring moved their key to another node",
	})

	// ObjectSize spans a single small document up to the server's 10MB body
	// limit, so an oversized-object problem is visible before the per-object
	// cap starts rejecting writes to the cache.
	ObjectSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "jaydb_object_size_bytes",
		Help:    "Size of documents read from or written to storage, in bytes",
		Buckets: prometheus.ExponentialBuckets(256, 4, 8), // 256B .. 4MB
	})

	// Storage backend metrics
	StorageOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaydb_storage_operations_total",
		Help: "Total number of storage operations by type and status",
	}, []string{"operation", "status"})

	StorageOpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "jaydb_storage_operation_duration_seconds",
		Help:    "Duration of storage operations in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	StorageBytesTransferred = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaydb_storage_bytes_total",
		Help: "Total bytes transferred to/from storage backend",
	}, []string{"operation"})

	// HTTP server metrics
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaydb_http_requests_total",
		Help: "Total number of HTTP requests by method, path prefix, and status",
	}, []string{"method", "path_prefix", "status"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "jaydb_http_request_duration_seconds",
		Help:    "Duration of HTTP requests in seconds",
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"method", "path_prefix"})

	HTTPRequestSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "jaydb_http_request_size_bytes",
		Help:    "Size of HTTP request bodies in bytes",
		Buckets: prometheus.ExponentialBuckets(100, 10, 8),
	})

	HTTPResponseSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "jaydb_http_response_size_bytes",
		Help:    "Size of HTTP response bodies in bytes",
		Buckets: prometheus.ExponentialBuckets(100, 10, 8),
	})

	// Cluster metrics
	ClusterNodes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jaydb_cluster_nodes",
		Help: "Current number of nodes in the cluster",
	})

	ClusterForwardedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaydb_cluster_forwarded_requests_total",
		Help: "Total number of forwarded requests by status",
	}, []string{"target_node", "status"})

	ClusterMeshConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jaydb_cluster_mesh_connections",
		Help: "Current number of active cluster mesh TCP connections",
	})

	// ClusterQuicConnections is deprecated: use ClusterMeshConnections instead.
	ClusterQuicConnections = ClusterMeshConnections

	// Database operation metrics
	DBOperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaydb_db_operations_total",
		Help: "Total number of database operations by type and status",
	}, []string{"operation", "status"})

	DBOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "jaydb_db_operation_duration_seconds",
		Help:    "Duration of database operations in seconds",
		Buckets: []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"operation"})

	// CAS conflict metrics
	CASConflicts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jaydb_cas_conflicts_total",
		Help: "Total number of CAS (compare-and-swap) conflicts by operation",
	}, []string{"operation"})

	// Key distribution metrics
	KeyDistribution = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "jaydb_key_distribution",
		Help:    "Distribution of key path depths",
		Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	}, []string{"shard"})
)

// Collector periodically updates gauge metrics from cache stats
type Collector struct {
	getStats func() (hits, misses, sfHits uint64)
	getSize  func() (items int, bytes int64)
	running  uint32

	// getEvictionStats and getBudget are set after construction so the
	// two-argument NewCollector signature keeps working for existing callers.
	getEvictionStats func() (evictions, skippedLarge, purged uint64)
	getBudget        func() (used, limit int64)

	// mu guards the last* delta bookkeeping below. UpdateCacheMetrics is
	// exported and called from a /metrics handler goroutine, so overlapping
	// calls are expected.
	mu sync.Mutex

	// Last values seen for the cumulative cache counters. Prometheus counters
	// only move by deltas, so syncing a running total means adding the
	// difference - adding the total itself would inflate the series on every
	// scrape.
	lastHits         uint64
	lastMisses       uint64
	lastSfHits       uint64
	lastEvictions    uint64
	lastSkippedLarge uint64
	lastPurged       uint64
}

// NewCollector creates a metrics collector
func NewCollector(
	getStats func() (hits, misses, sfHits uint64),
	getSize func() (items int, bytes int64),
) *Collector {
	return &Collector{
		getStats: getStats,
		getSize:  getSize,
	}
}

// WithEvictionStats wires the cache's eviction/skip/purge counters into the
// collector, e.g. db.Cache().EvictionStats.
func (c *Collector) WithEvictionStats(f func() (evictions, skippedLarge, purged uint64)) *Collector {
	c.getEvictionStats = f
	return c
}

// WithBudget wires a shared cache byte budget's usage into the collector.
func (c *Collector) WithBudget(b interface {
	Used() int64
	Limit() int64
}) *Collector {
	if b == nil {
		return c
	}
	c.getBudget = func() (int64, int64) { return b.Used(), b.Limit() }
	return c
}

// UpdateCacheMetrics syncs cache stats to Prometheus gauges/counters.
//
// Safe for concurrent use: the delta bookkeeping below is read-modify-write
// state, and the documented call site is a /metrics handler where overlapping
// scrapes are normal (two Prometheus servers, or a scrape racing a manual
// curl). Two unsynchronised calls would each compute a delta from the same
// `last` value and add the same increment twice, permanently over-reporting the
// counter - the exact failure this delta tracking exists to prevent.
func (c *Collector) UpdateCacheMetrics() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.getStats != nil {
		// cache.Manager.Stats returns running totals, so only the increment
		// since the previous sync may be added. Adding the totals themselves
		// made every scrape re-add the whole history, inflating the series
		// quadratically.
		hits, misses, sfHits := c.getStats()
		CacheHits.Add(float64(delta(&c.lastHits, hits)))
		CacheMisses.Add(float64(delta(&c.lastMisses, misses)))
		CacheSingleflightHits.Add(float64(delta(&c.lastSfHits, sfHits)))
	}

	if c.getSize != nil {
		items, bytes := c.getSize()
		CacheItems.Set(float64(items))
		CacheSize.Set(float64(bytes))
	}

	if c.getEvictionStats != nil {
		evictions, skippedLarge, purged := c.getEvictionStats()
		CacheEvictions.Add(float64(delta(&c.lastEvictions, evictions)))
		CacheSkippedLarge.Add(float64(delta(&c.lastSkippedLarge, skippedLarge)))
		CachePurgedOwnership.Add(float64(delta(&c.lastPurged, purged)))
	}

	if c.getBudget != nil {
		used, limit := c.getBudget()
		CacheBudgetUsed.Set(float64(used))
		CacheBudgetLimit.Set(float64(limit))
	}
}

// delta returns how much a monotonic source counter advanced since the last
// sync and records the new value. A source that went backwards - a Manager
// replaced under the collector, say - yields 0 rather than an unsigned
// underflow, which would otherwise add ~1.8e19 to a Prometheus counter and
// destroy the series permanently.
func delta(last *uint64, current uint64) uint64 {
	if current < *last {
		*last = current
		return 0
	}
	d := current - *last
	*last = current

	return d
}

// Start begins periodic metrics collection
func (c *Collector) Start() {
	if !atomic.CompareAndSwapUint32(&c.running, 0, 1) {
		return
	}
	// Metrics are exposed on-demand via /metrics endpoint
	// Real-time collection happens via cache.Stats() calls
}

// Stop halts metrics collection
func (c *Collector) Stop() {
	atomic.StoreUint32(&c.running, 0)
}

// RecordStorageOp records a storage operation metric
func RecordStorageOp(operation, status string, durationSecs float64, bytes int64) {
	StorageOpsTotal.WithLabelValues(operation, status).Inc()
	StorageOpDuration.WithLabelValues(operation).Observe(durationSecs)
	if bytes > 0 {
		StorageBytesTransferred.WithLabelValues(operation).Add(float64(bytes))
	}
}

// RecordHTTPRequest records an HTTP request metric
func RecordHTTPRequest(method, pathPrefix, status string, durationSecs float64, reqSize, respSize int) {
	HTTPRequestsTotal.WithLabelValues(method, pathPrefix, status).Inc()
	HTTPRequestDuration.WithLabelValues(method, pathPrefix).Observe(durationSecs)
	if reqSize > 0 {
		HTTPRequestSize.Observe(float64(reqSize))
	}
	if respSize > 0 {
		HTTPResponseSize.Observe(float64(respSize))
	}
}

// RecordDBOperation records a database operation metric
func RecordDBOperation(operation, status string, durationSecs float64) {
	DBOperationsTotal.WithLabelValues(operation, status).Inc()
	DBOperationDuration.WithLabelValues(operation).Observe(durationSecs)
}

// ObserveObjectSize records the payload size of a document. It is called from
// the request path rather than from pkg/cache, which must not import this
// package - metrics reads from cache, never the other way round.
func ObserveObjectSize(bytes int) {
	if bytes > 0 {
		ObjectSize.Observe(float64(bytes))
	}
}

// Handler returns a standard HTTP handler for exposing Prometheus metrics.
// Use this in embedded mode to serve metrics on your own HTTP server or port.
//
// Example (embedded mode with separate metrics port):
//
//	http.Handle("/metrics", metrics.Handler())
//	http.ListenAndServe(":9090", nil)
func Handler() http.Handler {
	return promhttp.Handler()
}

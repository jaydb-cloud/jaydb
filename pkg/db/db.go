package db

import (
	"context"
	"errors"
	"fmt"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/avivklas/jaydb/pkg/cache"
	"github.com/avivklas/jaydb/pkg/cluster"
	"github.com/avivklas/jaydb/pkg/encoding"
	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/avivklas/jaydb/pkg/storage/memory"
)

// Meta contains metadata for a database document.
type Meta struct {
	Key     string    `json:"key"`
	ETag    string    `json:"etag"`
	ModTime time.Time `json:"mod_time"`

	// Size is the stored byte size of the document. Populated by listings,
	// where the storage layer reports it without transferring the payload, so
	// callers can measure a keyspace without reading it.
	Size int64 `json:"size,omitempty"`
}

// Item represents a listed key along with optional unmarshaled content or raw bytes.
type Item struct {
	Meta  Meta   `json:"meta"`
	Value []byte `json:"value,omitempty"`
}

// Options configures the embedded database instance.
type Options struct {
	Storage       storage.Driver
	Codec         encoding.Codec
	ShardingDepth int
	Ring          *sharding.Ring
	ClusterNode   *cluster.Node

	// Namespace identifies this database within a clustered process that hosts
	// several of them. It is required for inter-query routing to be correct
	// when more than one DB shares a ClusterNode: the wire request carries the
	// namespace so the owner node can pick the matching local DB. Leave empty
	// for a single embedded database.
	Namespace string

	// CacheTTL is the cache entry freshness window. 0 = cache package default.
	CacheTTL time.Duration

	// CacheMaxObjectSize refuses to cache objects above this size, in bytes.
	// 0 = no cap.
	CacheMaxObjectSize int64

	// CacheBudget is the byte accountant for this database's cache. Share one
	// across every db.Open in the process to give them a single memory
	// ceiling. nil = unbounded, not shared.
	CacheBudget *cache.Budget
}

// PutOptions specifies options for write operations.
type PutOptions struct {
	ExpectedETag string
}

// PutOption is a functional option for Put.
type PutOption func(*PutOptions)

// WithExpectedETag specifies optimistic concurrency control (CAS) tag.
func WithExpectedETag(etag string) PutOption {
	return func(o *PutOptions) {
		o.ExpectedETag = etag
	}
}

// CreateOnly specifies write should fail if key already exists (If-None-Match: *).
func CreateOnly() PutOption {
	return func(o *PutOptions) {
		o.ExpectedETag = storage.MatchAnyETag
	}
}

// DeleteOptions specifies options for delete operations.
type DeleteOptions struct {
	ExpectedETag string
}

// DeleteOption is a functional option for Delete.
type DeleteOption func(*DeleteOptions)

func WithDeleteExpectedETag(etag string) DeleteOption {
	return func(o *DeleteOptions) {
		o.ExpectedETag = etag
	}
}

// DB defines the high-level embedded document database interface.
type DB interface {
	Get(ctx context.Context, key string, dest any) (*Meta, error)
	Put(ctx context.Context, key string, doc any, opts ...PutOption) (*Meta, error)
	Delete(ctx context.Context, key string, opts ...DeleteOption) error
	List(ctx context.Context, prefix string, limit int) ([]*Item, error)
	// ListPage returns one page of keys plus a cursor for the next page. An
	// empty returned cursor means the keyspace is exhausted.
	ListPage(ctx context.Context, prefix string, opts ListPageOptions) (items []*Item, next string, err error)
	Cache() *cache.Manager
	ShardingDepth() int
	GetRaw(ctx context.Context, key string) (*storage.Object, error)
	PutRaw(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error)
	DeleteRaw(ctx context.Context, key string, expectedETag string) error
	Close() error
}

type database struct {
	opts         Options
	cacheMgr     *cache.Manager
	codec        encoding.Codec
	storageDrive storage.Driver

	// reconcileMu serializes the ownership purge, and reconciledGen records the
	// ring generation it last ran for. Both are needed: the atomic makes the
	// common "nothing changed" check free, the mutex keeps N concurrent
	// requests arriving on one membership change from running N full purges.
	reconcileMu   sync.Mutex
	reconciledGen atomic.Uint64

	// hookAfterSelectivePurge is a test seam, nil in production. It fires between
	// a selective purge and the check for a ring that moved underneath it, which
	// is the only way to drive that race deterministically from a test.
	hookAfterSelectivePurge func()
}

// Open initializes and returns an embedded document database.
func Open(opts Options) (DB, error) {
	if opts.Storage == nil {
		opts.Storage = memory.NewDriver()
	}
	if opts.Codec == nil {
		opts.Codec = encoding.NewJSONCodec()
	}
	if opts.ShardingDepth <= 0 {
		opts.ShardingDepth = 2
	}

	cacheMgr := cache.NewManagerWithConfig(opts.Storage, cache.Config{
		TTL:           opts.CacheTTL,
		MaxObjectSize: opts.CacheMaxObjectSize,
		Budget:        opts.CacheBudget,
	})

	d := &database{
		opts:         opts,
		cacheMgr:     cacheMgr,
		codec:        opts.Codec,
		storageDrive: opts.Storage,
	}

	// Register as the owner-side handler for this namespace. Without this the
	// cluster node has nothing to serve forwarded requests with, and peers
	// received an empty response that looked exactly like a missing document.
	if opts.ClusterNode != nil {
		opts.ClusterNode.RegisterHandler(opts.Namespace, d)
	}

	return d, nil
}

// reconcileOwnership drops cached keys this node no longer owns, lazily, on the
// first operation after a membership change.
//
// The ring routes each key to a single owner, so the owner's cache is the only
// cache for that key - coherence comes from ownership, not from the TTL. The
// hole is churn: when ownership moves away, the previous owner keeps its entry,
// and if ownership later flaps back (routine on FARGATE_SPOT, where tasks get
// reclaimed) it serves data that changed in the meantime. Purging on the
// generation change makes that impossible without a background goroutine, and
// without leaning on a short TTL as the only backstop.
func (d *database) reconcileOwnership() {
	if d.opts.Ring == nil || d.opts.ClusterNode == nil {
		return
	}

	gen := d.opts.Ring.Generation()
	if d.reconciledGen.Load() == gen {
		return
	}

	defer trace.StartRegion(context.Background(), "db.reconcile_ownership").End()

	d.reconcileMu.Lock()
	defer d.reconcileMu.Unlock()

	// Re-check: while we queued on the mutex another request may have already
	// purged for this generation, and a purge per concurrent request would turn
	// a single membership change into a stampede.
	if d.reconciledGen.Load() == gen {
		return
	}

	ring := d.opts.Ring
	self := d.opts.ClusterNode.SelfQuicAddr()
	last := d.reconciledGen.Load()

	// A selective purge can only reason about ONE observed transition. Each
	// AddNode/RemoveNode bumps the generation by exactly one, so if more than a
	// single step has elapsed since the last reconcile, a key may have moved
	// away to another node - been written there - and moved back, leaving this
	// node the owner again at the moment we look. Such a key passes the
	// owner==self test and its pre-move value survives, which is exactly the
	// stale read the purge exists to prevent. When generations were skipped
	// there is no way to tell those keys apart, so drop everything. (On the
	// very first reconcile this can fire against an empty cache, which costs
	// nothing - every path that admits an entry reconciles before doing so.)
	skippedGenerations := gen > last+1
	if skippedGenerations {
		d.purgeEverything()
	} else {
		d.cacheMgr.PurgeIf(func(key string) bool {
			owner := ring.GetNode(key)
			// An empty owner means the ring has no members at all; dropping the
			// whole cache on a transient empty ring would be gratuitous.
			return owner != "" && owner != self
		})

		// The selective predicate above reads the ring LIVE, once per key, so a
		// membership change landing mid-purge means those answers did not all
		// come from the same generation. A key can move away, be written on its
		// new owner, and move back before the predicate reaches it - it then
		// reports self as owner and the pre-move value survives, which is the
		// stale read this whole mechanism exists to prevent. Detect the moving
		// ring and fall back to the one decision that is valid no matter which
		// generation each answer came from. A full purge ignores the ring
		// entirely, so it cannot be invalidated the same way and needs no retry.
		if d.hookAfterSelectivePurge != nil {
			d.hookAfterSelectivePurge()
		}
		if ring.Generation() != gen {
			d.purgeEverything()
		}
	}

	// Record the generation observed BEFORE the purge: if membership moved again
	// while we were purging, the next operation must reconcile once more.
	d.reconciledGen.Store(gen)
}

// purgeEverything drops the whole cache. Used when the ring cannot be trusted to
// classify entries - either generations were skipped, or membership moved while a
// selective purge was in flight.
func (d *database) purgeEverything() {
	d.cacheMgr.PurgeIf(func(string) bool { return true })
}

func (d *database) Get(ctx context.Context, key string, dest any) (*Meta, error) {
	defer trace.StartRegion(ctx, "db.get").End()
	d.reconcileOwnership()

	// nonOwner records that another node owns this key and the forwarded read
	// did not answer, so the local read below must not populate the cache.
	// See readLocal.
	nonOwner := false

	// Inter-query routing check
	if d.opts.Ring != nil && d.opts.ClusterNode != nil {
		targetNode := d.opts.Ring.GetNode(key)
		selfAddr := d.opts.ClusterNode.SelfQuicAddr()
		if targetNode != "" && targetNode != selfAddr {
			iqReg := trace.StartRegion(ctx, "db.interquery_get")
			resp, err := d.opts.ClusterNode.ExecuteInterQuery(ctx, targetNode, cluster.InterQueryReq{
				Namespace: d.opts.Namespace,
				Op:        cluster.OpGet,
				Key:       key,
			})
			iqReg.End()
			if err == nil && resp.Err == "" && resp.Object != nil {
				if dest != nil {
					decReg := trace.StartRegion(ctx, "db.unmarshal")
					err := d.codec.Unmarshal(resp.Object.Value, dest)
					decReg.End()
					if err != nil {
						return nil, fmt.Errorf("db decode error: %w", err)
					}
				}
				return &Meta{
					Key:     resp.Object.Key,
					ETag:    resp.Object.ETag,
					ModTime: resp.Object.ModTime,
				}, nil
			}
			// Peer reported a definitive application-level error (e.g.
			// ErrNotFound, ErrVersionMismatch): trust it and do not fall back.
			if err == nil && resp.Err != "" {
				return nil, mapErrorString(resp.Err)
			}
			// Transport failure: fall through to a local read. The document
			// lives in the same cold storage regardless of which node reads it,
			// so routing is a cache optimization, not a correctness requirement
			// for reads. Degrading here costs one cold-storage round-trip but
			// avoids multi-second stalls from unhealthy mesh connections.
			//
			// The result is deliberately NOT cached: this node does not own the
			// key, so nothing would ever invalidate the entry. See readLocal.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			nonOwner = true
			// Fall through to local read below.
		}
	}

	obj, err := d.readLocal(ctx, key, nonOwner)
	if err != nil {
		return nil, err
	}

	if dest != nil {
		decReg := trace.StartRegion(ctx, "db.unmarshal")
		err := d.codec.Unmarshal(obj.Value, dest)
		decReg.End()
		if err != nil {
			return nil, fmt.Errorf("db decode error: %w", err)
		}
	}

	return &Meta{
		Key:     obj.Key,
		ETag:    obj.ETag,
		ModTime: obj.ModTime,
	}, nil
}

// readLocal reads a key from this node's own storage, going through the cache
// ONLY when this node owns the key.
//
// Coherence in a cluster comes from ownership, not from the TTL: the owner's
// cache is the only cache for a key, and it stays current because every write
// for that key routes to that same node. An entry cached by a NON-owner is
// therefore unreachable by invalidation -- the owner's Put updates the owner's
// cache and nothing else -- so it would be served until the namespace TTL
// expired or ring membership changed. With a generous namespace TTL that is
// measured in hours.
//
// Serving a stale ETag is worse than serving a slow read. The client reads the
// ETag, hands it straight back as If-Match, and every conditional write is
// rejected with 412 while the validator it was given looks perfectly valid --
// a failure that is indistinguishable, from the client's side, from a genuine
// concurrent-write conflict.
//
// So a non-owner reads cold storage directly and remembers nothing.
func (d *database) readLocal(ctx context.Context, key string, nonOwner bool) (*storage.Object, error) {
	if nonOwner {
		defer trace.StartRegion(ctx, "db.uncached_read").End()
		return d.storageDrive.Get(ctx, key)
	}

	return d.cacheMgr.Get(ctx, key)
}

func (d *database) Put(ctx context.Context, key string, doc any, opts ...PutOption) (*Meta, error) {
	defer trace.StartRegion(ctx, "db.put").End()
	d.reconcileOwnership()

	var po PutOptions
	for _, opt := range opts {
		opt(&po)
	}

	encReg := trace.StartRegion(ctx, "db.marshal")
	data, err := d.codec.Marshal(doc)
	encReg.End()
	if err != nil {
		return nil, fmt.Errorf("db encode error: %w", err)
	}

	// Inter-query routing check
	if d.opts.Ring != nil && d.opts.ClusterNode != nil {
		targetNode := d.opts.Ring.GetNode(key)
		selfAddr := d.opts.ClusterNode.SelfQuicAddr()
		if targetNode != "" && targetNode != selfAddr {
			iqReg := trace.StartRegion(ctx, "db.interquery_put")
			resp, err := d.opts.ClusterNode.ExecuteInterQuery(ctx, targetNode, cluster.InterQueryReq{
				Namespace:    d.opts.Namespace,
				Op:           cluster.OpPut,
				Key:          key,
				Value:        data,
				ExpectedETag: po.ExpectedETag,
			})
			iqReg.End()
			if err != nil {
				return nil, fmt.Errorf("inter-query error: %w", err)
			}
			if resp.Err != "" {
				return nil, mapErrorString(resp.Err)
			}
			// Guard the dereference: a peer that answered with neither an object
			// nor an error would otherwise panic the process here.
			if resp.Object == nil {
				return nil, fmt.Errorf("inter-query to %s: %w", targetNode, cluster.ErrIncompleteResponse)
			}
			return &Meta{
				Key:     resp.Object.Key,
				ETag:    resp.Object.ETag,
				ModTime: resp.Object.ModTime,
			}, nil
		}
	}

	obj, err := d.cacheMgr.Put(ctx, key, data, po.ExpectedETag)
	if err != nil {
		return nil, err
	}

	return &Meta{
		Key:     obj.Key,
		ETag:    obj.ETag,
		ModTime: obj.ModTime,
	}, nil
}

func (d *database) Delete(ctx context.Context, key string, opts ...DeleteOption) error {
	defer trace.StartRegion(ctx, "db.delete").End()
	d.reconcileOwnership()

	var do DeleteOptions
	for _, opt := range opts {
		opt(&do)
	}

	// Inter-query routing check
	if d.opts.Ring != nil && d.opts.ClusterNode != nil {
		targetNode := d.opts.Ring.GetNode(key)
		selfAddr := d.opts.ClusterNode.SelfQuicAddr()
		if targetNode != "" && targetNode != selfAddr {
			iqReg := trace.StartRegion(ctx, "db.interquery_delete")
			resp, err := d.opts.ClusterNode.ExecuteInterQuery(ctx, targetNode, cluster.InterQueryReq{
				Namespace:    d.opts.Namespace,
				Op:           cluster.OpDelete,
				Key:          key,
				ExpectedETag: do.ExpectedETag,
			})
			iqReg.End()
			if err != nil {
				return fmt.Errorf("inter-query error: %w", err)
			}
			if resp.Err != "" {
				return mapErrorString(resp.Err)
			}
			return nil
		}
	}

	return d.cacheMgr.Delete(ctx, key, do.ExpectedETag)
}

// GetRaw, PutRaw and DeleteRaw are the owner-side entry points the cluster mesh
// calls for a FORWARDED request, so they must reconcile ownership just like the
// public methods do. Reconciling on the forwarding node is no help: the stale
// entry lives in the cache of the node that receives the forward, and the
// forwarded path is the only path that exists in a cluster - precisely where the
// ownership purge has to work.
func (d *database) GetRaw(ctx context.Context, key string) (*storage.Object, error) {
	defer trace.StartRegion(ctx, "db.get_raw").End()
	d.reconcileOwnership()

	return d.cacheMgr.Get(ctx, key)
}

func (d *database) PutRaw(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error) {
	defer trace.StartRegion(ctx, "db.put_raw").End()
	d.reconcileOwnership()

	return d.cacheMgr.Put(ctx, key, value, expectedETag)
}

func (d *database) DeleteRaw(ctx context.Context, key string, expectedETag string) error {
	defer trace.StartRegion(ctx, "db.delete_raw").End()
	d.reconcileOwnership()

	return d.cacheMgr.Delete(ctx, key, expectedETag)
}

// listPageSize is the number of keys requested from storage per underlying
// listing call. It matches the S3 maximum, which is the tightest limit among the
// drivers, so one page here is always exactly one storage request.
const listPageSize = 1000

// ListPageOptions configures a single page of a listing.
type ListPageOptions struct {
	// Limit caps the number of keys in this page. Zero or negative means the
	// driver's natural page size (listPageSize).
	Limit int

	// Cursor resumes after a previous page. Empty starts from the beginning.
	Cursor string
}

// List returns up to limit keys under prefix.
//
// The limit is honored across page boundaries. This used to issue exactly one
// storage listing and pass the limit straight through, discarding the cursor the
// driver returned - so on S3, which caps a response at 1000 keys no matter what
// MaxKeys asks for, List(prefix, 5000) silently returned 1000 keys and no
// indication that more existed. Every caller inherited that truncation: a
// listing endpoint under-reported, and anything counting a keyspace stopped
// counting at 1000.
//
// A limit of zero or less means "every matching key", which is unbounded by
// definition - prefer ListPage when the keyspace may be large.
func (d *database) List(ctx context.Context, prefix string, limit int) ([]*Item, error) {
	defer trace.StartRegion(ctx, "db.list").End()
	var (
		items  []*Item
		cursor string
	)

	for {
		pageLimit := listPageSize
		if limit > 0 {
			if remaining := limit - len(items); remaining < pageLimit {
				pageLimit = remaining
			}
		}

		page, next, err := d.ListPage(ctx, prefix, ListPageOptions{
			Limit:  pageLimit,
			Cursor: cursor,
		})
		if err != nil {
			return nil, err
		}
		items = append(items, page...)

		if limit > 0 && len(items) >= limit {
			return items[:limit], nil
		}
		// A cursor with no keys behind it means no progress: stop rather than
		// spin, so a driver that reports a stale cursor cannot hang the caller.
		if next == "" || len(page) == 0 {
			return items, nil
		}
		cursor = next
	}
}

// ListPage returns one page of keys under prefix, plus the cursor to resume from.
func (d *database) ListPage(ctx context.Context, prefix string, opts ListPageOptions) ([]*Item, string, error) {
	defer trace.StartRegion(ctx, "db.list_page").End()
	limit := opts.Limit
	if limit <= 0 {
		limit = listPageSize
	}

	metas, next, err := d.storageDrive.List(ctx, prefix, storage.ListOptions{
		Limit:  limit,
		Cursor: opts.Cursor,
	})
	if err != nil {
		return nil, "", err
	}

	items := make([]*Item, 0, len(metas))
	for _, m := range metas {
		if m == nil {
			continue
		}
		items = append(items, &Item{
			Meta: Meta{
				Key:     m.Key,
				ETag:    m.ETag,
				ModTime: m.ModTime,
				Size:    m.Size,
			},
		})
	}
	return items, next, nil
}

func (d *database) Cache() *cache.Manager {
	return d.cacheMgr
}

func (d *database) ShardingDepth() int {
	return d.opts.ShardingDepth
}

func (d *database) PartitionKey(key string) string {
	return sharding.PartitionKey(key, d.opts.ShardingDepth)
}

func (d *database) Close() error {
	if d.opts.ClusterNode != nil {
		// Stop serving forwarded requests for this namespace before tearing the
		// storage driver down, so peers get an explicit error instead of hitting
		// a closed driver.
		d.opts.ClusterNode.UnregisterHandler(d.opts.Namespace)
		// Only close ClusterNode if Namespace was not set (single-database embedded
		// legacy mode). For multi-tenant hosts sharing a ClusterNode across
		// namespaces, the host manages ClusterNode's lifecycle.
		if d.opts.Namespace == "" {
			_ = d.opts.ClusterNode.Close()
		}
	}

	// Hand this database's cached bytes back to the shared budget and stop being
	// an eviction target. A process that opens one DB per namespace and deletes
	// namespaces at runtime would otherwise leak every closed namespace's cache
	// for the life of the process, and charge its bytes to the ceiling that the
	// surviving namespaces have to share. Unregister drains the Manager, so
	// nothing else has to purge first.
	if budget := d.cacheMgr.Budget(); budget != nil {
		budget.Unregister(d.cacheMgr)
	}

	return d.storageDrive.Close()
}

// ClusterNode returns the configured ClusterNode, or nil if running in standalone mode.
func (d *database) ClusterNode() *cluster.Node {
	return d.opts.ClusterNode
}

func mapErrorString(errStr string) error {
	switch errStr {
	case storage.ErrNotFound.Error():
		return storage.ErrNotFound
	case storage.ErrAlreadyExists.Error():
		return storage.ErrAlreadyExists
	case storage.ErrVersionMismatch.Error():
		return storage.ErrVersionMismatch
	default:
		return errors.New(errStr)
	}
}

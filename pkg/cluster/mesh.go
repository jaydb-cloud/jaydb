package cluster

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/avivklas/jaydb/pkg/metrics"
	"github.com/avivklas/jaydb/pkg/sharding"
	"github.com/avivklas/jaydb/pkg/storage"
	"github.com/hashicorp/memberlist"
)

// OpType indicates inter-query operation type.
type OpType byte

const (
	OpGet OpType = iota + 1
	OpPut
	OpDelete
)

const (
	defaultMeshPort = 7947
	defaultQuicPort = defaultMeshPort
)

var (
	ErrUnauthenticated    = errors.New("jaydb cluster: unauthenticated inter-query request")
	ErrNoHandler          = errors.New("jaydb cluster: no DB handler registered for namespace")
	ErrIncompleteResponse = errors.New("jaydb cluster: response contained neither an object nor an error")
	// authSkew bounds clock difference between cluster nodes when verifying HMAC
	// timestamps, and prevents replaying captured requests indefinitely.
	authSkew = 30 * time.Second
)

// Handler is the local interface a database backend exposes to serve
// forwarded inter-query requests on this node.
type Handler interface {
	GetRaw(ctx context.Context, key string) (*storage.Object, error)
	PutRaw(ctx context.Context, key string, value []byte, expectedETag string) (*storage.Object, error)
	DeleteRaw(ctx context.Context, key string, expectedETag string) error
}

// InterQueryReq is sent over the mesh connection to request an operation on the key owner node.
type InterQueryReq struct {
	Namespace    string `json:"namespace,omitempty"`
	Op           OpType `json:"op"`
	Key          string `json:"key"`
	Value        []byte `json:"value,omitempty"`
	ExpectedETag string `json:"expected_etag,omitempty"`
	TS           int64  `json:"ts,omitempty"`
	Auth         string `json:"auth,omitempty"`
}

// InterQueryResp is returned over the mesh connection from the key owner node.
type InterQueryResp struct {
	Object *storage.Object `json:"object,omitempty"`
	Err    string          `json:"err,omitempty"`
}

// MemberInfo describes an active node in the cluster ring.
type MemberInfo struct {
	Name       string `json:"name"`
	Addr       string `json:"addr"`
	GossipPort int    `json:"gossip_port"`
	QuicPort   int    `json:"quic_port"` // Deprecated alias to MeshPort for backward compatibility
	QuicAddr   string `json:"quic_addr"` // Deprecated alias to MeshAddr for backward compatibility
	MeshPort   int    `json:"mesh_port"`
	MeshAddr   string `json:"mesh_addr"`
}

// NodeConfig specifies memberlist and TCP mesh parameters.
//
// BindPort and MeshPort (or QuicPort) may each be 0 to request an OS-assigned ephemeral port.
// The kernel then guarantees a free port, which removes the "bind: address
// already in use" class of failure entirely.
type NodeConfig struct {
	NodeName  string
	BindAddr  string
	BindPort  int
	MeshPort  int
	QuicPort  int // Deprecated: alias to MeshPort for backward compatibility
	JoinAddrs []string
	Ring      *sharding.Ring

	// PoolSize specifies the number of pre-opened TCP connections to maintain per peer.
	// Defaults to 8 if <= 0.
	PoolSize int

	// DBHandler is the legacy single-database handler, used only for requests
	// that carry no namespace. Multi-namespace deployments call
	// Node.RegisterHandler instead.
	DBHandler Handler

	// ClusterSecret authenticates inter-query requests.
	ClusterSecret string

	// DialTimeout bounds establishing a TCP connection to a peer. Default defaultDialTimeout.
	DialTimeout time.Duration

	// StreamOpenTimeout bounds connection establishment. Deprecated.
	StreamOpenTimeout time.Duration

	// RequestTimeout bounds how long the owner node spends serving one
	// inter-query before abandoning it. Default defaultRequestTimeout.
	RequestTimeout time.Duration
}

const (
	defaultDialTimeout       = 3 * time.Second
	defaultStreamOpenTimeout = 100 * time.Millisecond
	defaultRequestTimeout    = 10 * time.Second
)

func (c NodeConfig) dialTimeout() time.Duration {
	if c.DialTimeout > 0 {
		return c.DialTimeout
	}
	return defaultDialTimeout
}

func (c NodeConfig) streamOpenTimeout() time.Duration {
	if c.StreamOpenTimeout > 0 {
		return c.StreamOpenTimeout
	}
	return defaultStreamOpenTimeout
}

func (c NodeConfig) requestTimeout() time.Duration {
	if c.RequestTimeout > 0 {
		return c.RequestTimeout
	}
	return defaultRequestTimeout
}

// Node manages memberlist cluster membership and high-performance TCP inter-query mesh.
type Node struct {
	cfg       NodeConfig
	mlist     *memberlist.Memberlist
	meshLn    net.Listener
	ring      *sharding.Ring
	meshPool  *MeshPool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once

	bindPort int
	meshPort int
	quicPort int // alias for backwards compatibility

	handlersMu sync.RWMutex
	handlers   map[string]Handler
}

// RegisterHandler registers the local DB that serves namespace. Safe to call
// concurrently and after the node is running; db.Open calls it automatically
// when both a Ring and a ClusterNode are configured.
func (n *Node) RegisterHandler(namespace string, h Handler) {
	n.handlersMu.Lock()
	defer n.handlersMu.Unlock()
	if n.handlers == nil {
		n.handlers = make(map[string]Handler)
	}
	n.handlers[namespace] = h
}

// UnregisterHandler removes a namespace's handler, so a closed DB stops
// receiving forwarded requests.
func (n *Node) UnregisterHandler(namespace string) {
	n.handlersMu.Lock()
	defer n.handlersMu.Unlock()
	delete(n.handlers, namespace)
}

// handlerFor resolves the handler serving namespace. An empty namespace maps to
// the legacy single DBHandler so embedded single-database callers keep working.
func (n *Node) handlerFor(namespace string) (Handler, bool) {
	n.handlersMu.RLock()
	h, ok := n.handlers[namespace]
	n.handlersMu.RUnlock()
	if ok {
		return h, true
	}

	if n.cfg.DBHandler != nil {
		return n.cfg.DBHandler, true
	}
	return nil, false
}

type eventDelegate struct {
	node *Node
}

func (ed *eventDelegate) NotifyJoin(n *memberlist.Node) {
	if ed.node != nil {
		meshAddr := fmt.Sprintf("%s:%d", n.Addr.String(), getMeshPort(n))
		if ed.node.ring != nil {
			ed.node.ring.AddNode(meshAddr)
		}
		if ed.node.meshPool != nil && meshAddr != ed.node.SelfMeshAddr() {
			ed.node.meshPool.AddPeer(meshAddr)
		}
	}
}

func (ed *eventDelegate) NotifyLeave(n *memberlist.Node) {
	if ed.node != nil {
		meshAddr := fmt.Sprintf("%s:%d", n.Addr.String(), getMeshPort(n))
		if ed.node.ring != nil {
			ed.node.ring.RemoveNode(meshAddr)
		}
		if ed.node.meshPool != nil {
			ed.node.meshPool.RemovePeer(meshAddr)
		}
	}
}

func (ed *eventDelegate) NotifyUpdate(n *memberlist.Node) {
	if ed.node != nil {
		meshAddr := fmt.Sprintf("%s:%d", n.Addr.String(), getMeshPort(n))
		if ed.node.ring != nil {
			ed.node.ring.AddNode(meshAddr)
		}
		if ed.node.meshPool != nil && meshAddr != ed.node.SelfMeshAddr() {
			ed.node.meshPool.AddPeer(meshAddr)
		}
	}
}

// NewNode initializes a cluster Node.
func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.BindAddr == "" {
		cfg.BindAddr = "127.0.0.1"
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = DefaultPoolSize
	}
	if cfg.Ring == nil {
		cfg.Ring = sharding.NewRing(100, 2)
	}

	meshPort := cfg.MeshPort
	if meshPort == 0 && cfg.QuicPort != 0 {
		meshPort = cfg.QuicPort
	}

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:      cfg,
		ring:     cfg.Ring,
		ctx:      ctx,
		cancel:   cancel,
		handlers: make(map[string]Handler),
	}

	// 1. Setup TCP Mesh Listener
	meshAddr := fmt.Sprintf("%s:%d", cfg.BindAddr, meshPort)
	meshLn, err := net.Listen("tcp", meshAddr)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("mesh tcp listen error on %s: %w", meshAddr, err)
	}
	n.meshLn = meshLn

	n.meshPort, err = portFromAddr(meshLn.Addr())
	if err != nil {
		_ = meshLn.Close()
		cancel()
		return nil, fmt.Errorf("mesh tcp listen: resolve bound port: %w", err)
	}
	n.quicPort = n.meshPort

	n.meshPool = NewMeshPool(ctx, cfg.PoolSize, nil, cfg.dialTimeout(), cfg.streamOpenTimeout(), n)

	n.wg.Add(1)
	go n.acceptLoop()

	// Register self in ring
	n.ring.AddNode(n.SelfMeshAddr())

	// 2. Setup Memberlist
	mlConfig := memberlist.DefaultLANConfig()
	mlConfig.Name = cfg.NodeName
	mlConfig.BindAddr = cfg.BindAddr
	mlConfig.BindPort = cfg.BindPort
	mlConfig.Events = &eventDelegate{node: n}

	// Advertise the resolved mesh port
	meta := make([]byte, 2)
	binary.BigEndian.PutUint16(meta, uint16(n.meshPort))
	mlConfig.Delegate = &nodeDelegate{meta: meta}

	mlist, err := memberlist.Create(mlConfig)
	if err != nil {
		_ = meshLn.Close()
		n.meshPool.Close()
		cancel()
		return nil, fmt.Errorf("memberlist create error: %w", err)
	}
	n.mlist = mlist
	n.bindPort = int(mlist.LocalNode().Port)

	if len(cfg.JoinAddrs) > 0 {
		_, _ = mlist.Join(cfg.JoinAddrs)
	}

	// Sync initial memberlist members into ring and mesh connection pool
	for _, member := range mlist.Members() {
		mAddr := fmt.Sprintf("%s:%d", member.Addr.String(), getMeshPort(member))
		n.ring.AddNode(mAddr)
		if mAddr != n.SelfMeshAddr() {
			n.meshPool.AddPeer(mAddr)
		}
	}

	return n, nil
}

func portFromAddr(addr net.Addr) (int, error) {
	if addr == nil {
		return 0, fmt.Errorf("nil listener address")
	}
	if tcp, ok := addr.(*net.TCPAddr); ok {
		return tcp.Port, nil
	}
	if udp, ok := addr.(*net.UDPAddr); ok {
		return udp.Port, nil
	}

	_, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", addr.String(), err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, fmt.Errorf("parse port %q: %w", portStr, err)
	}
	return port, nil
}

type nodeDelegate struct {
	meta []byte
}

func (d *nodeDelegate) NodeMeta(limit int) []byte {
	return d.meta
}

func (d *nodeDelegate) NotifyMsg(b []byte)                         {}
func (d *nodeDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *nodeDelegate) LocalState(join bool) []byte                { return nil }
func (d *nodeDelegate) MergeRemoteState(buf []byte, join bool)     {}

func (n *Node) acceptLoop() {
	defer n.wg.Done()
	for {
		conn, err := n.meshLn.Accept()
		if err != nil {
			select {
			case <-n.ctx.Done():
				return
			default:
				continue
			}
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		n.wg.Add(1)
		go n.handleConn(conn)
	}
}

func (n *Node) handleConn(conn net.Conn) {
	defer n.wg.Done()
	defer conn.Close()

	for {
		req, err := readBinaryReq(conn)
		if err != nil {
			return
		}

		resp := n.serve(req)

		if err := writeBinaryResp(conn, resp); err != nil {
			return
		}
	}
}

func (n *Node) serve(req InterQueryReq) InterQueryResp {
	if err := n.authenticate(req); err != nil {
		return InterQueryResp{Err: err.Error()}
	}

	handler, ok := n.handlerFor(req.Namespace)
	if !ok {
		return InterQueryResp{Err: fmt.Sprintf("%s %q", ErrNoHandler.Error(), req.Namespace)}
	}

	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.requestTimeout())
	defer cancel()

	switch req.Op {
	case OpGet:
		obj, err := handler.GetRaw(ctx, req.Key)
		if err != nil {
			return InterQueryResp{Err: err.Error()}
		}
		if obj == nil {
			return InterQueryResp{Err: storage.ErrNotFound.Error()}
		}
		return InterQueryResp{Object: obj}

	case OpPut:
		obj, err := handler.PutRaw(ctx, req.Key, req.Value, req.ExpectedETag)
		if err != nil {
			return InterQueryResp{Err: err.Error()}
		}
		if obj == nil {
			return InterQueryResp{Err: "jaydb cluster: put returned no object"}
		}
		return InterQueryResp{Object: obj}

	case OpDelete:
		if err := handler.DeleteRaw(ctx, req.Key, req.ExpectedETag); err != nil {
			return InterQueryResp{Err: err.Error()}
		}
		return InterQueryResp{}

	default:
		return InterQueryResp{Err: fmt.Sprintf("jaydb cluster: unknown op %d", req.Op)}
	}
}

func (n *Node) authenticate(req InterQueryReq) error {
	if n.cfg.ClusterSecret == "" {
		return nil
	}

	if req.Auth == "" {
		return ErrUnauthenticated
	}
	skew := time.Since(time.Unix(req.TS, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > authSkew {
		return ErrUnauthenticated
	}

	want := computeAuth(n.cfg.ClusterSecret, req)
	if subtle.ConstantTimeCompare([]byte(want), []byte(req.Auth)) != 1 {
		return ErrUnauthenticated
	}
	return nil
}

// ExecuteInterQuery routes an inter-query operation to targetNode over the connection pool.
func (n *Node) ExecuteInterQuery(ctx context.Context, targetNode string, req InterQueryReq) (*InterQueryResp, error) {
	if n.cfg.ClusterSecret != "" {
		req.TS = time.Now().Unix()
		req.Auth = computeAuth(n.cfg.ClusterSecret, req)
	}

	resp, err := n.roundTrip(ctx, targetNode, req)
	if err == nil {
		metrics.ClusterForwardedRequests.WithLabelValues(targetNode, "success").Inc()
		return resp, nil
	}

	// One retry: if connection dropped or stale, retry on another pooled connection if ctx not expired
	if ctx.Err() != nil {
		metrics.ClusterForwardedRequests.WithLabelValues(targetNode, "error").Inc()
		return nil, err
	}
	resp, err = n.roundTrip(ctx, targetNode, req)
	if err != nil {
		metrics.ClusterForwardedRequests.WithLabelValues(targetNode, "error").Inc()
		return nil, err
	}
	metrics.ClusterForwardedRequests.WithLabelValues(targetNode, "success").Inc()
	return resp, nil
}

func (n *Node) roundTrip(ctx context.Context, targetNode string, req InterQueryReq) (*InterQueryResp, error) {
	if n.meshPool == nil {
		return nil, errors.New("jaydb cluster: mesh pool not initialized")
	}

	conn, err := n.meshPool.GetConn(ctx, targetNode)
	if err != nil {
		return nil, err
	}

	if d, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(d)
	} else {
		_ = conn.SetDeadline(time.Now().Add(n.cfg.requestTimeout()))
	}

	if err := writeBinaryReq(conn, req); err != nil {
		n.meshPool.DropConn(targetNode, conn)
		return nil, err
	}

	resp, err := readBinaryResp(conn)
	if err != nil {
		n.meshPool.DropConn(targetNode, conn)
		return nil, err
	}

	_ = conn.SetDeadline(time.Time{})
	n.meshPool.PutConn(targetNode, conn)
	return &resp, nil
}

// SelfMeshAddr returns the node's TCP mesh address string.
func (n *Node) SelfMeshAddr() string {
	return fmt.Sprintf("%s:%d", n.cfg.BindAddr, n.meshPort)
}

// SelfQuicAddr returns the node's mesh address string (alias kept for backward compatibility).
func (n *Node) SelfQuicAddr() string {
	return n.SelfMeshAddr()
}

// MeshPort returns the bound TCP mesh port.
func (n *Node) MeshPort() int {
	return n.meshPort
}

// QuicPort returns the bound mesh port (alias kept for backward compatibility).
func (n *Node) QuicPort() int {
	return n.meshPort
}

// BindPort returns the memberlist gossip port actually bound.
func (n *Node) BindPort() int {
	return n.bindPort
}

// GossipAddr returns the host:port other nodes should use to join this node.
func (n *Node) GossipAddr() string {
	return fmt.Sprintf("%s:%d", n.cfg.BindAddr, n.bindPort)
}

// Members returns the list of active cluster members discovered via memberlist.
func (n *Node) Members() []MemberInfo {
	if n.mlist == nil {
		return nil
	}
	mlMembers := n.mlist.Members()
	members := make([]MemberInfo, len(mlMembers))
	for i, m := range mlMembers {
		mPort := getMeshPort(m)
		mAddr := fmt.Sprintf("%s:%d", m.Addr.String(), mPort)
		members[i] = MemberInfo{
			Name:       m.Name,
			Addr:       m.Addr.String(),
			GossipPort: int(m.Port),
			QuicPort:   mPort,
			QuicAddr:   mAddr,
			MeshPort:   mPort,
			MeshAddr:   mAddr,
		}
	}
	return members
}

// MemberCount returns the total number of members currently in the cluster.
func (n *Node) MemberCount() int {
	if n.mlist == nil {
		if n.ring != nil {
			return n.ring.NodeCount()
		}
		return 0
	}
	return n.mlist.NumMembers()
}

// Ring returns the deterministic sharding ring associated with this node.
func (n *Node) Ring() *sharding.Ring {
	return n.ring
}

// IsOwner returns whether this node is the designated shard owner for the given key.
func (n *Node) IsOwner(key string) bool {
	if n.ring == nil {
		return true
	}
	return n.ring.IsOwner(key, n.SelfMeshAddr())
}

// MeshPool returns the underlying MeshPool.
func (n *Node) MeshPool() *MeshPool {
	return n.meshPool
}

// Close gracefully terminates node connection pool and memberlist.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.cancel()

		if n.mlist != nil {
			_ = n.mlist.Leave(2 * time.Second)
			_ = n.mlist.Shutdown()
		}

		if n.meshLn != nil {
			_ = n.meshLn.Close()
		}

		if n.meshPool != nil {
			n.meshPool.Close()
		}

		n.wg.Wait()
	})

	return nil
}

func computeAuth(secret string, req InterQueryReq) string {
	mac := hmac.New(sha256.New, []byte(secret))
	valHash := sha256.Sum256(req.Value)
	canonical := fmt.Sprintf("%s:%d:%s:%s:%s:%d",
		req.Namespace,
		req.Op,
		req.Key,
		req.ExpectedETag,
		hex.EncodeToString(valHash[:]),
		req.TS,
	)
	mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

func getMeshPort(m *memberlist.Node) int {
	if len(m.Meta) >= 2 {
		return int(binary.BigEndian.Uint16(m.Meta[:2]))
	}
	return defaultMeshPort
}

func getQuicPort(m *memberlist.Node) int {
	return getMeshPort(m)
}

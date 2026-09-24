package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	ErrPeerPoolClosed = errors.New("jaydb cluster: peer pool is closed")
	ErrNoLiveConn     = errors.New("jaydb cluster: no live connection available in pool")
)

const (
	DefaultPoolSize          = 8
	DefaultDialTimeout       = 3 * time.Second
	DefaultStreamOpenTimeout = 100 * time.Millisecond
)

// PeerPoolConfig specifies connection parameters for a single peer node.
type PeerPoolConfig struct {
	TargetAddr        string
	TLSConfig         *tls.Config   // Deprecated: kept for backwards compatibility
	QUICConfig        any           // Deprecated: kept for backwards compatibility
	PoolSize          int
	DialTimeout       time.Duration
	StreamOpenTimeout time.Duration // Deprecated: kept for backwards compatibility
}

// PeerPool manages a pool of persistent, reused TCP connections to a single remote peer.
type PeerPool struct {
	cfg        PeerPoolConfig
	mu         sync.RWMutex
	conns      chan net.Conn
	closed     bool
	ctx        context.Context
	cancel     context.CancelFunc
	parentNode *Node
}

// NewPeerPool creates a new PeerPool and initiates background connection warmup.
func NewPeerPool(parentCtx context.Context, cfg PeerPoolConfig, parentNode *Node) *PeerPool {
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = DefaultPoolSize
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}

	ctx, cancel := context.WithCancel(parentCtx)
	p := &PeerPool{
		cfg:        cfg,
		conns:      make(chan net.Conn, cfg.PoolSize),
		ctx:        ctx,
		cancel:     cancel,
		parentNode: parentNode,
	}

	// Proactively warm up connections in the background
	go func() {
		_ = p.EnsureConnected(ctx)
	}()

	return p
}

// EnsureConnected warms up the pool with active, pre-opened TCP connections.
func (p *PeerPool) EnsureConnected(ctx context.Context) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrPeerPoolClosed
	}
	p.mu.RUnlock()

	dialer := &net.Dialer{
		Timeout:   p.cfg.DialTimeout,
		KeepAlive: 15 * time.Second,
	}

	for i := 0; i < p.cfg.PoolSize; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Don't dial if channel is already full
		if len(p.conns) >= p.cfg.PoolSize {
			return nil
		}

		conn, err := dialer.DialContext(ctx, "tcp", p.cfg.TargetAddr)
		if err != nil {
			return err
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}

		select {
		case p.conns <- conn:
		default:
			_ = conn.Close()
			return nil
		}
	}
	return nil
}

// GetConn retrieves an idle TCP connection from the pool, or dials a new one if none are available.
func (p *PeerPool) GetConn(ctx context.Context) (net.Conn, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPeerPoolClosed
	}
	p.mu.RUnlock()

	for {
		select {
		case conn := <-p.conns:
			return conn, nil
		default:
			// No idle connection available; dial a fresh one
			dialer := &net.Dialer{
				Timeout:   p.cfg.DialTimeout,
				KeepAlive: 15 * time.Second,
			}
			conn, err := dialer.DialContext(ctx, "tcp", p.cfg.TargetAddr)
			if err != nil {
				return nil, fmt.Errorf("dial peer %s: %w", p.cfg.TargetAddr, err)
			}
			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}
			return conn, nil
		}
	}
}

// PutConn returns an active, healthy TCP connection back to the pool.
func (p *PeerPool) PutConn(conn net.Conn) {
	if conn == nil {
		return
	}

	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()

	if closed {
		_ = conn.Close()
		return
	}

	select {
	case p.conns <- conn:
	default:
		// Pool is full; close excess connection
		_ = conn.Close()
	}
}

// DropConn closes and discards a failed connection.
func (p *PeerPool) DropConn(conn net.Conn) {
	if conn != nil {
		_ = conn.Close()
	}
}

// isConnClosed probes if a socket has been closed or reset by the remote peer.
func isConnClosed(conn net.Conn) bool {
	_ = conn.SetReadDeadline(time.Now())
	var b [1]byte
	n, err := conn.Read(b[:])
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false // No data ready, connection is still alive
		}
		return true // EOF or connection reset
	}
	return n > 0 // Unexpected data waiting on idle connection
}

// Close gracefully closes all connections in the pool.
func (p *PeerPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.cancel()
	p.mu.Unlock()

	close(p.conns)
	for conn := range p.conns {
		_ = conn.Close()
	}
}

// MeshPool manages peer connection pools across the entire cluster mesh.
type MeshPool struct {
	mu         sync.RWMutex
	peers      map[string]*PeerPool
	poolSize   int
	tlsConfig  *tls.Config // Deprecated: kept for backwards compatibility
	quicConfig any         // Deprecated: kept for backwards compatibility
	dialTo     time.Duration
	streamTo   time.Duration // Deprecated: kept for backwards compatibility
	ctx        context.Context
	cancel     context.CancelFunc
	parentNode *Node
}

// NewMeshPool initializes a new MeshPool.
func NewMeshPool(parentCtx context.Context, poolSize int, tlsConfig *tls.Config, dialTo, streamTo time.Duration, parentNode *Node) *MeshPool {
	if poolSize <= 0 {
		poolSize = DefaultPoolSize
	}
	if dialTo <= 0 {
		dialTo = DefaultDialTimeout
	}
	if streamTo <= 0 {
		streamTo = DefaultStreamOpenTimeout
	}

	ctx, cancel := context.WithCancel(parentCtx)
	return &MeshPool{
		peers:      make(map[string]*PeerPool),
		poolSize:   poolSize,
		tlsConfig:  tlsConfig,
		dialTo:     dialTo,
		streamTo:   streamTo,
		ctx:        ctx,
		cancel:     cancel,
		parentNode: parentNode,
	}
}

// AddPeer creates and proactively connects a peer pool for targetAddr.
func (m *MeshPool) AddPeer(targetAddr string) {
	if targetAddr == "" {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case <-m.ctx.Done():
		return
	default:
	}

	if _, exists := m.peers[targetAddr]; exists {
		return
	}

	cfg := PeerPoolConfig{
		TargetAddr:  targetAddr,
		PoolSize:    m.poolSize,
		DialTimeout: m.dialTo,
	}

	m.peers[targetAddr] = NewPeerPool(m.ctx, cfg, m.parentNode)
}

// RemovePeer closes and removes the peer pool for targetAddr.
func (m *MeshPool) RemovePeer(targetAddr string) {
	m.mu.Lock()
	pool, exists := m.peers[targetAddr]
	if exists {
		delete(m.peers, targetAddr)
	}
	m.mu.Unlock()

	if pool != nil {
		pool.Close()
	}
}

// GetConn retrieves an active connection to targetAddr.
func (m *MeshPool) GetConn(ctx context.Context, targetAddr string) (net.Conn, error) {
	select {
	case <-m.ctx.Done():
		return nil, ErrPeerPoolClosed
	default:
	}

	m.mu.RLock()
	pool, exists := m.peers[targetAddr]
	m.mu.RUnlock()

	if !exists {
		m.AddPeer(targetAddr)
		m.mu.RLock()
		pool = m.peers[targetAddr]
		m.mu.RUnlock()
	}

	if pool == nil {
		select {
		case <-m.ctx.Done():
			return nil, ErrPeerPoolClosed
		default:
		}
		return nil, fmt.Errorf("peer pool for %s not found", targetAddr)
	}

	return pool.GetConn(ctx)
}

// GetStream is a compatibility wrapper for GetConn.
func (m *MeshPool) GetStream(ctx context.Context, targetAddr string) (net.Conn, func(), error) {
	conn, err := m.GetConn(ctx, targetAddr)
	if err != nil {
		return nil, nil, err
	}
	dropFunc := func() {
		m.DropConn(targetAddr, conn)
	}
	return conn, dropFunc, nil
}

// PeerCount returns the count of active peer pools.
func (m *MeshPool) PeerCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.peers)
}

// PutConn returns an active connection to targetAddr's pool.
func (m *MeshPool) PutConn(targetAddr string, conn net.Conn) {
	m.mu.RLock()
	pool, exists := m.peers[targetAddr]
	m.mu.RUnlock()

	if exists && pool != nil {
		pool.PutConn(conn)
	} else if conn != nil {
		_ = conn.Close()
	}
}

// DropConn closes and discards a failed connection for targetAddr.
func (m *MeshPool) DropConn(targetAddr string, conn net.Conn) {
	m.mu.RLock()
	pool, exists := m.peers[targetAddr]
	m.mu.RUnlock()

	if exists && pool != nil {
		pool.DropConn(conn)
	} else if conn != nil {
		_ = conn.Close()
	}
}

// ReconcilePeers synchronizes active peer pools with the provided slice of active cluster addresses,
// skipping selfAddr.
func (m *MeshPool) ReconcilePeers(activeAddrs []string, selfAddr string) {
	activeSet := make(map[string]struct{}, len(activeAddrs))
	for _, addr := range activeAddrs {
		if addr != "" && addr != selfAddr {
			activeSet[addr] = struct{}{}
			m.AddPeer(addr)
		}
	}

	m.mu.Lock()
	var toRemove []string
	for peerAddr := range m.peers {
		if _, keep := activeSet[peerAddr]; !keep {
			toRemove = append(toRemove, peerAddr)
		}
	}
	m.mu.Unlock()

	for _, addr := range toRemove {
		m.RemovePeer(addr)
	}
}

// TotalLiveConnections returns the total count of live idle connections across all peer pools.
func (m *MeshPool) TotalLiveConnections() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := 0
	for _, pool := range m.peers {
		if pool != nil {
			total += len(pool.conns)
		}
	}
	return total
}

// Close gracefully closes all peer connection pools in the mesh.
func (m *MeshPool) Close() {
	m.mu.Lock()
	pools := make([]*PeerPool, 0, len(m.peers))
	for _, pool := range m.peers {
		pools = append(pools, pool)
	}
	m.peers = make(map[string]*PeerPool)
	m.cancel()
	m.mu.Unlock()

	for _, pool := range pools {
		pool.Close()
	}
}

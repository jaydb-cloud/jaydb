package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
)

var (
	ErrPeerPoolClosed = errors.New("jaydb cluster: peer pool is closed")
	ErrNoLiveConn     = errors.New("jaydb cluster: no live QUIC connection available in pool")
)

const (
	DefaultPoolSize          = 8
	DefaultDialTimeout       = 3 * time.Second
	DefaultStreamOpenTimeout = 100 * time.Millisecond
)

// PeerPoolConfig specifies connection parameters for a single peer node.
type PeerPoolConfig struct {
	TargetAddr        string
	TLSConfig         *tls.Config
	QUICConfig        *quic.Config
	PoolSize          int
	DialTimeout       time.Duration
	StreamOpenTimeout time.Duration
}

// PeerPool manages a proactive pool of pre-opened QUIC connections to a single remote peer.
type PeerPool struct {
	cfg        PeerPoolConfig
	mu         sync.RWMutex
	conns      []*quic.Conn
	dialLocks  []sync.Mutex
	rrCounter  atomic.Uint64
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
	if cfg.StreamOpenTimeout <= 0 {
		cfg.StreamOpenTimeout = DefaultStreamOpenTimeout
	}

	ctx, cancel := context.WithCancel(parentCtx)
	p := &PeerPool{
		cfg:        cfg,
		conns:      make([]*quic.Conn, cfg.PoolSize),
		dialLocks:  make([]sync.Mutex, cfg.PoolSize),
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

// EnsureConnected ensures all slots in the pool have active, live QUIC connections.
func (p *PeerPool) EnsureConnected(ctx context.Context) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrPeerPoolClosed
	}
	p.mu.RUnlock()

	var firstErr error
	for i := 0; i < p.cfg.PoolSize; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.ctx.Done():
			return ErrPeerPoolClosed
		default:
		}

		if err := p.ensureSlotConnected(ctx, i); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (p *PeerPool) ensureSlotConnected(ctx context.Context, slot int) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrPeerPoolClosed
	}
	conn := p.conns[slot]
	if conn != nil && conn.Context().Err() == nil {
		p.mu.RUnlock()
		return nil // Still healthy
	}
	p.mu.RUnlock()

	// Acquire per-slot dial lock so multiple callers don't dial duplicate connections for the same slot
	p.dialLocks[slot].Lock()
	defer p.dialLocks[slot].Unlock()

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrPeerPoolClosed
	}
	conn = p.conns[slot]
	if conn != nil && conn.Context().Err() == nil {
		p.mu.RUnlock()
		return nil
	}
	p.mu.RUnlock()

	// Dial with bounded timeout
	dialCtx, cancel := context.WithTimeout(ctx, p.cfg.DialTimeout)
	defer cancel()

	newConn, err := quic.DialAddr(dialCtx, p.cfg.TargetAddr, p.cfg.TLSConfig, p.cfg.QUICConfig)
	if err != nil {
		return fmt.Errorf("quic dial %s (slot %d): %w", p.cfg.TargetAddr, slot, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = newConn.CloseWithError(0, "pool closed during dial")
		return ErrPeerPoolClosed
	}

	// If old conn existed, ensure it is closed
	if p.conns[slot] != nil {
		_ = p.conns[slot].CloseWithError(0, "replacing stale connection")
	}
	p.conns[slot] = newConn
	return nil
}

// GetStream retrieves an open bidirectional stream on a pre-opened connection from the pool.
// Returns the stream, a drop callback (to mark connection stale on transport error), and any error.
func (p *PeerPool) GetStream(ctx context.Context) (*quic.Stream, func(), error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, nil, ErrPeerPoolClosed
	}
	p.mu.RUnlock()

	// Try existing live connections first using round-robin
	startIdx := int(p.rrCounter.Add(1) % uint64(p.cfg.PoolSize))
	for attempt := 0; attempt < p.cfg.PoolSize; attempt++ {
		slot := (startIdx + attempt) % p.cfg.PoolSize

		p.mu.RLock()
		conn := p.conns[slot]
		p.mu.RUnlock()

		if conn == nil {
			continue
		}

		if conn.Context().Err() != nil {
			// Stale connection, drop and trigger background reconnect
			p.DropSlot(slot, conn)
			continue
		}

		openCtx, cancel := context.WithTimeout(ctx, p.cfg.StreamOpenTimeout)
		stream, err := conn.OpenStreamSync(openCtx)
		cancel()

		if err == nil {
			dropFunc := func() {
				if conn.Context().Err() != nil {
					p.DropSlot(slot, conn)
				}
			}
			return stream, dropFunc, nil
		}

		// Only drop the connection if the socket itself died; transient stream open
		// timeout or rate limit does NOT kill the underlying connection.
		if conn.Context().Err() != nil {
			p.DropSlot(slot, conn)
		}
	}

	// If all pooled connections were dead or empty, attempt an on-demand reconnect for startIdx
	p.mu.RLock()
	conn := p.conns[startIdx]
	p.mu.RUnlock()

	if conn == nil || conn.Context().Err() != nil {
		if err := p.ensureSlotConnected(ctx, startIdx); err != nil {
			return nil, nil, err
		}
		p.mu.RLock()
		conn = p.conns[startIdx]
		p.mu.RUnlock()
	}

	if conn == nil || conn.Context().Err() != nil {
		return nil, nil, ErrNoLiveConn
	}

	openCtx, cancel := context.WithTimeout(ctx, p.cfg.StreamOpenTimeout)
	stream, err := conn.OpenStreamSync(openCtx)
	cancel()
	if err != nil {
		if conn.Context().Err() != nil {
			p.DropSlot(startIdx, conn)
		}
		return nil, nil, fmt.Errorf("quic open stream to %s: %w", p.cfg.TargetAddr, err)
	}

	dropFunc := func() {
		if conn.Context().Err() != nil {
			p.DropSlot(startIdx, conn)
		}
	}
	return stream, dropFunc, nil
}

// DropSlot removes a connection from a slot if it matches the current entry and triggers background healing.
func (p *PeerPool) DropSlot(slot int, conn *quic.Conn) {
	p.mu.Lock()
	if p.conns[slot] != conn {
		p.mu.Unlock()
		return
	}
	p.conns[slot] = nil
	closed := p.closed
	p.mu.Unlock()

	if conn != nil {
		_ = conn.CloseWithError(0, "dropping stale connection")
	}

	if !closed {
		// Asynchronously heal the dropped slot exactly once
		go func() {
			_ = p.ensureSlotConnected(p.ctx, slot)
		}()
	}
}

// LiveCount returns the number of currently healthy connections in the pool.
func (p *PeerPool) LiveCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return 0
	}
	count := 0
	for _, c := range p.conns {
		if c != nil {
			select {
			case <-c.Context().Done():
			default:
				count++
			}
		}
	}
	return count
}

// Close gracefully closes all QUIC connections in the pool.
func (p *PeerPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.cancel()
	conns := make([]*quic.Conn, len(p.conns))
	copy(conns, p.conns)
	for i := range p.conns {
		p.conns[i] = nil
	}
	p.mu.Unlock()

	for _, c := range conns {
		if c != nil {
			_ = c.CloseWithError(0, "peer pool closed")
		}
	}
}

// MeshPool manages peer connection pools across the entire cluster mesh.
type MeshPool struct {
	mu         sync.RWMutex
	peers      map[string]*PeerPool
	poolSize   int
	tlsConfig  *tls.Config
	quicConfig *quic.Config
	dialTo     time.Duration
	streamTo   time.Duration
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
		peers:     make(map[string]*PeerPool),
		poolSize:  poolSize,
		tlsConfig: tlsConfig,
		dialTo:    dialTo,
		streamTo:  streamTo,
		quicConfig: &quic.Config{
			MaxIncomingStreams: 2000,
			MaxIdleTimeout:     30 * time.Second,
			KeepAlivePeriod:    10 * time.Second,
		},
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
		TargetAddr:        targetAddr,
		TLSConfig:         m.tlsConfig,
		QUICConfig:        m.quicConfig,
		PoolSize:          m.poolSize,
		DialTimeout:       m.dialTo,
		StreamOpenTimeout: m.streamTo,
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

// GetStream retrieves an active pre-opened stream to targetAddr.
func (m *MeshPool) GetStream(ctx context.Context, targetAddr string) (*quic.Stream, func(), error) {
	select {
	case <-m.ctx.Done():
		return nil, nil, ErrPeerPoolClosed
	default:
	}

	m.mu.RLock()
	pool, exists := m.peers[targetAddr]
	m.mu.RUnlock()

	if !exists {
		// If not registered yet, register and dial
		m.AddPeer(targetAddr)
		m.mu.RLock()
		pool = m.peers[targetAddr]
		m.mu.RUnlock()
	}

	if pool == nil {
		select {
		case <-m.ctx.Done():
			return nil, nil, ErrPeerPoolClosed
		default:
		}
		return nil, nil, fmt.Errorf("peer pool for %s not found", targetAddr)
	}

	return pool.GetStream(ctx)
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

// TotalLiveConnections returns the total count of live QUIC connections across all peer pools.
func (m *MeshPool) TotalLiveConnections() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	total := 0
	for _, p := range m.peers {
		total += p.LiveCount()
	}
	return total
}

// PeerCount returns the number of active peer pools.
func (m *MeshPool) PeerCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.peers)
}

// Close gracefully closes all peer pools in the mesh.
func (m *MeshPool) Close() {
	m.cancel()
	m.mu.Lock()
	pools := make([]*PeerPool, 0, len(m.peers))
	for _, p := range m.peers {
		pools = append(pools, p)
	}
	m.peers = make(map[string]*PeerPool)
	m.mu.Unlock()

	for _, p := range pools {
		p.Close()
	}
}

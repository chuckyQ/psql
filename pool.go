package psql

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrPoolClosed        = errors.New("psql: connection pool is closed")
	ErrInvalidPoolConfig = errors.New("psql: invalid connection pool configuration")
)

type Pool struct {
	mu         sync.Mutex
	config     *Config
	openConns  map[string]*Connection
	busyConns  map[string]*Connection
	totalConns int
	closed     bool
	notify     chan struct{}
}

func newPool(config *Config) *Pool {
	return &Pool{
		config:    config,
		openConns: make(map[string]*Connection),
		busyConns: make(map[string]*Connection),
		notify:    make(chan struct{}),
	}
}

func (p *Pool) signalLocked() {
	close(p.notify)
	p.notify = make(chan struct{})
}

func (p *Pool) acquire(ctx context.Context) (string, *Connection, bool, <-chan struct{}, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, false, nil, err
	}

	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return "", nil, false, nil, ErrPoolClosed
	}

	// Reuse an idle connection first.
	for id, conn := range p.openConns {
		delete(p.openConns, id)
		p.busyConns[id] = conn
		p.signalLocked()
		p.mu.Unlock()
		return id, conn, true, nil, nil
	}

	p.mu.Lock()
	// Reserve capacity atomically before connecting.
	if p.totalConns < int(p.config.MaxConns) {
		p.totalConns++
		p.mu.Unlock()
		// Do not lock around this because connecting
		// can be slow and bottleneck the system.
		conn, err := Connect(
			ctx,
			p.config.Host,
			p.config.Port,
			p.config.Database,
			p.config.User,
			p.config.Password,
		)

		if err != nil {
			p.mu.Lock()
			p.totalConns--
			p.signalLocked()
			p.mu.Unlock()

			return "", nil, false, nil, err
		}

		id := uuid.New().String()
		conn.poolID = id
		conn.connTimeout = p.config.ConnTimeout
		p.mu.Lock()

		// Close may have been called while Connect was running.
		if p.closed {
			p.totalConns--
			p.signalLocked()
			p.mu.Unlock()
			conn.Close()
			return "", nil, false, nil, ErrPoolClosed
		}

		p.busyConns[id] = conn
		p.signalLocked()
		p.mu.Unlock()
		return id, conn, true, nil, nil
	}

	// Capture the current notification channel while holding the lock.
	// A subsequent state change closes this channel and wakes the waiter.
	wait := p.notify
	p.mu.Unlock()

	return "", nil, false, wait, nil
}

func (p *Pool) getConn(ctx context.Context) (string, *Connection, error) {
	if ctx == nil {
		return "", nil, errors.New("psql: nil context")
	}

	if p.config == nil ||
		p.config.MaxConns <= 0 ||
		p.config.MaxIdleConns == 0 {
		return "", nil, ErrInvalidPoolConfig
	}

	for {
		id, conn, acquired, wait, err := p.acquire(ctx)
		if err != nil {
			return "", nil, err
		}

		if acquired {
			return id, conn, nil
		}

		if p.config.QueryWaitTime <= 0 {
			select {
			case <-ctx.Done():
				return "", nil, ctx.Err()
			case <-wait:
				continue
			}
		}

		timer := time.NewTimer(p.config.QueryWaitTime)

		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return "", nil, ctx.Err()

		case <-wait:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

		case <-timer.C:
		}
	}
}

// release returns a connection to the idle pool or closes it.
// Connection closing occurs outside the pool lock.
func (p *Pool) release(id string, conn *Connection) {
	if id == "" || conn == nil {
		return
	}

	p.mu.Lock()

	registered, exists := p.busyConns[id]
	if !exists || registered != conn {
		p.mu.Unlock()
		return
	}

	delete(p.busyConns, id)

	var closeConn *Connection

	if p.closed ||
		len(p.openConns) >= int(p.config.MaxIdleConns) {
		p.totalConns--
		closeConn = conn
	} else {
		p.openConns[id] = conn
	}

	p.signalLocked()
	p.mu.Unlock()

	if closeConn != nil {
		closeConn.Close()
	}
}

// Close prevents new acquisitions and closes idle connections.
// Busy connections are closed when their callers release them.
// Connections being established are closed when Connect returns.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}

	p.closed = true

	idle := make([]*Connection, 0, len(p.openConns))
	for id, conn := range p.openConns {
		idle = append(idle, conn)
		delete(p.openConns, id)
		p.totalConns--
	}

	p.signalLocked()
	p.mu.Unlock()

	for _, conn := range idle {
		conn.Close()
	}

	return nil

}

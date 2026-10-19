package psql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"uuid"
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
	defer p.mu.Unlock()

	if p.closed {
		return "", nil, false, nil, ErrPoolClosed
	}

	// Reuse an idle connection first.
	for id, conn := range p.openConns {
		delete(p.openConns, id)
		p.busyConns[id] = conn
		p.signalLocked()
		return id, conn, true, nil, nil
	}

	// Reserve capacity before opening a new connection.
	if uint64(p.totalConns) < uint64(p.config.MaxConns) {
		p.totalConns++
		conn, err := Connect(
			ctx,
			p.config.Host,
			p.config.Port,
			p.config.Database,
			p.config.User,
			p.config.Password,
		)

		if err != nil {
			p.totalConns--
			p.signalLocked()
			return "", nil, false, nil, err
		}

		id := uuid.New().String()
		conn.poolID = id
		conn.connTimeout = p.config.ConnTimeout
		// The pool might have been closed while Connect was running.
		if p.closed {
			p.totalConns--
			p.signalLocked()
			conn.Close()
			return "", nil, false, nil, ErrPoolClosed
		}

		p.busyConns[id] = conn
		p.signalLocked()
		return id, conn, true, nil, nil
	}

	wait := p.notify
	return "", nil, false, wait, nil
}

func (p *Pool) getConn(ctx context.Context) (string, *Connection, error) {
	if ctx == nil {
		return "", nil, errors.New("psql: nil context")
	}

	if p.config == nil {
		return "", nil, ErrInvalidPoolConfig
	}

	if p.config.MaxConns == 0 {
		return "", nil, fmt.Errorf(
			"%w: MaxConns must be greater than zero",
			ErrInvalidPoolConfig,
		)
	}

	for {
		id, conn, acquired, wait, err := p.acquire(ctx)
		if err != nil {
			return "", nil, err
		}

		if acquired {
			return id, conn, nil
		}

		// QueryWaitTime remains an optional retry interval. The notification
		// channel wakes the caller immediately when pool state changes.
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

func (p *Pool) release(id string, conn *Connection) {

	if id == "" || conn == nil {
		return
	}

	var closeConn *Connection

	p.mu.Lock()
	defer p.mu.Unlock()

	registered, exists := p.busyConns[id]
	if !exists || registered != conn {
		return
	}

	delete(p.busyConns, id)

	if p.closed ||
		uint64(len(p.openConns)) >= uint64(p.config.MaxIdleConns) {
		p.totalConns--
		closeConn = conn
	} else {
		p.openConns[id] = conn
	}

	p.signalLocked()
	if closeConn != nil {
		closeConn.Close()
	}
}

// Close prevents new acquisitions and closes all idle connections.
// Busy connections are closed when their callers release them.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
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
	for _, conn := range idle {
		conn.Close()
	}

	return nil
}

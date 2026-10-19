package psql

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"uuid"

	"github.com/chuckyQ/go-syncmap"
)

type Pool struct {
	mu        sync.Mutex
	cfg       *Config
	busyConns *syncmap.SyncMap[string, *Connection]
	openConns *syncmap.SyncMap[string, *Connection]
	config    *Config
	busyCount atomic.Int32
	openCount atomic.Int32
}

func (p *Pool) acquire(ctx context.Context) (string, *Connection, bool, error) {

	available := p.openCount.Load()
	busy := p.busyCount.Load()
	if available == 0 && uint(available+busy) >= p.config.MaxConns {
		return "", nil, false, nil
	} else if available > 0 {
		found := false
		var uu string
		var conn *Connection
		p.openConns.Range(func(key string, value *Connection) bool {
			uu = key
			value = conn
			found = true
			return false
		})

		if found {
			p.openConns.Delete(uu)
			p.busyConns.Store(uu, conn)
			return uu, conn, true, nil
		}

	}

	c, err := Connect(ctx, p.cfg.Host, p.cfg.Port, p.cfg.Database, p.cfg.User, p.cfg.Password)
	if err != nil {
		return "", nil, false, err
	}

	uu := uuid.New().String()
	p.busyConns.Store(uu, c)
	p.busyCount.Add(1)
	return uu, c, true, nil

}

func (p *Pool) release(uuid string, c *Connection) {
	if c != nil && uuid != "" {
		idleConns := p.openCount.Load()
		if idleConns <= int32(p.config.MaxIdleConns) {
			conn, exists := p.busyConns.LoadAndDelete(uuid)
			if exists {
				p.openConns.Store(uuid, conn)
				p.openCount.Add(1)
			} else {
				// TODO: error handle?
			}
		} else {
			p.busyConns.Delete(uuid)
			p.busyCount.Add(-1)
			c.Close()
		}
	}
}

func (p *Pool) getConn(ctx context.Context) (string, *Connection, error) {

	for {

		uu, c, acquired, err := p.acquire(ctx)
		if err != nil {
			p.release(uu, c)
			return uu, c, err
		}

		if acquired {
			return uu, c, err
		}

		time.Sleep(p.cfg.QueryWaitTime)
	}

}

func newPool(config *Config) *Pool {

	return &Pool{
		config:    config,
		openConns: syncmap.New[string, *Connection](nil),
		busyConns: syncmap.New[string, *Connection](nil),
	}

}

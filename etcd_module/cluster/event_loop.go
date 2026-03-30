package cluster

import (
	"context"
	log "log/slog"
	"sync"
	"sync/atomic"
)

// clusterEventLoop 定义clusterEventLoop类型。
type clusterEventLoop struct {
	ch        chan func()
	done      chan struct{}
	closed    atomic.Bool
	closeOnce sync.Once
}

func newClusterEventLoop(bufferSize int) *clusterEventLoop {
	if bufferSize <= 0 {
		bufferSize = 1024
	}
	return &clusterEventLoop{ch: make(chan func(), bufferSize), done: make(chan struct{})}
}

func (l *clusterEventLoop) run(ctx context.Context, logger *log.Logger, wg *sync.WaitGroup) {
	defer wg.Done()
	defer l.close()
	for {
		select {
		case <-ctx.Done():
			return
		case fn := <-l.ch:
			if fn == nil {
				continue
			}
			func() {
				defer func() {
					if r := recover(); r != nil && logger != nil {
						logger.Error("cluster event loop callback panic", "panic", r)
					}
				}()
				fn()
			}()
		}
	}
}

func (l *clusterEventLoop) post(fn func()) bool {
	if fn == nil {
		return true
	}
	if l.closed.Load() {
		return false
	}

	select {
	case <-l.done:
		return false
	case l.ch <- fn:
		return true
	}
}

func (l *clusterEventLoop) close() {
	l.closeOnce.Do(func() {
		l.closed.Store(true)
		close(l.done)
	})
}

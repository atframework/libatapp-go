package cluster

import (
	"context"
	"github.com/atframework/libatapp-go/etcd_module/events"
	"sync"
)

// EventStream 定义EventStream类型。
type EventStream struct {
	ch           chan events.Event
	handle       events.EventCallbackHandle
	eventManager events.EventManager
	closed       bool
	mu           sync.Mutex
}

// NewEventStream 创建并返回EventStream。
func (c *EtcdCluster) NewEventStream(ctx context.Context, bufferSize int, eventTypes []events.EventType) *EventStream {
	if bufferSize <= 0 {
		bufferSize = 100
	}

	stream := &EventStream{
		ch:           make(chan events.Event, bufferSize),
		eventManager: c.events.manager,
	}

	callback := func(event *events.Event) {
		select {
		case stream.ch <- *event:
		case <-ctx.Done():
			stream.Close()
		default:
			// Channel full - drop event to prevent blocking
		}
	}

	stream.handle = c.events.manager.Subscribe(eventTypes, callback)

	return stream
}

// Events 实现。
func (s *EventStream) Events() <-chan events.Event {
	return s.ch
}

// Close 关闭模块并释放底层资源。
func (s *EventStream) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	s.closed = true
	if s.handle != 0 {
		s.eventManager.Unsubscribe(s.handle)
	}
	close(s.ch)
}

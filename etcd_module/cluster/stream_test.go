package cluster

import (
	"context"
	"github.com/atframework/libatapp-go/etcd_module/events"
	"testing"
	"time"
)

// 该测试函数用于验证相关行为。
func TestNewEventStream(t *testing.T) {
	// Arrange
	mockManager := events.NewEventManager()
	defer mockManager.Close()

	cluster := &EtcdCluster{
		events: eventState{
			manager:                  mockManager,
			snapshotLoadingCallbacks: events.NewCallbackList(),
			snapshotLoadedCallbacks:  events.NewCallbackList(),
			nodeEventCallbacks:       events.NewCallbackList(),
		},
	}

	ctx := context.Background()

	// Act
	stream := cluster.NewEventStream(ctx, 10, []events.EventType{events.EventTypeNodeUp})

	// Assert
	if stream == nil {
		t.Fatal("Expected non-nil stream")
	}

	if stream.ch == nil {
		t.Fatal("Expected non-nil channel")
	}

	if cap(stream.ch) != 10 {
		t.Errorf("Expected channel buffer size 10, got %d", cap(stream.ch))
	}

	// Act
	stream.Close()
}

// 该测试函数用于验证相关行为。
func TestEventStreamDefaultBufferSize(t *testing.T) {
	// Arrange
	mockManager := events.NewEventManager()
	defer mockManager.Close()

	cluster := &EtcdCluster{
		events: eventState{
			manager:                  mockManager,
			snapshotLoadingCallbacks: events.NewCallbackList(),
			snapshotLoadedCallbacks:  events.NewCallbackList(),
			nodeEventCallbacks:       events.NewCallbackList(),
		},
	}

	ctx := context.Background()

	// Act
	stream := cluster.NewEventStream(ctx, 0, []events.EventType{events.EventTypeNodeUp})

	// Assert
	if cap(stream.ch) != 100 {
		t.Errorf("Expected default buffer size 100, got %d", cap(stream.ch))
	}

	// Act
	stream.Close()
}

// 该测试函数用于验证相关行为。
func TestEventStreamReceivesEvents(t *testing.T) {
	// Arrange
	mockManager := events.NewEventManager()
	defer mockManager.Close()

	cluster := &EtcdCluster{
		events: eventState{
			manager:                  mockManager,
			snapshotLoadingCallbacks: events.NewCallbackList(),
			snapshotLoadedCallbacks:  events.NewCallbackList(),
			nodeEventCallbacks:       events.NewCallbackList(),
		},
	}

	ctx := context.Background()
	stream := cluster.NewEventStream(ctx, 10, []events.EventType{
		events.EventTypeNodeUp,
		events.EventTypeNodeDown,
	})
	defer stream.Close()

	// Arrange
	event1 := events.NewNodeUpEvent(nil)
	event2 := events.NewNodeDownEvent(123, "test-node")

	go func() {
		time.Sleep(10 * time.Millisecond)
		mockManager.Publish(event1)
		mockManager.Publish(event2)
	}()

	// Act
	timeout := time.After(1 * time.Second)
	receivedCount := 0

	for receivedCount < 2 {
		select {
		case evt := <-stream.Events():
			receivedCount++
			if evt.Type != events.EventTypeNodeUp && evt.Type != events.EventTypeNodeDown {
				t.Errorf("Unexpected event type: %v", evt.Type)
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for events, received %d/2", receivedCount)
		}
	}
}

// 该测试函数用于验证相关行为。
func TestEventStreamFiltersEventTypes(t *testing.T) {
	// Arrange
	mockManager := events.NewEventManager()
	defer mockManager.Close()

	cluster := &EtcdCluster{
		events: eventState{
			manager:                  mockManager,
			snapshotLoadingCallbacks: events.NewCallbackList(),
			snapshotLoadedCallbacks:  events.NewCallbackList(),
			nodeEventCallbacks:       events.NewCallbackList(),
		},
	}

	ctx := context.Background()
	// Only subscribe to NodeUp
	stream := cluster.NewEventStream(ctx, 10, []events.EventType{events.EventTypeNodeUp})
	defer stream.Close()

	// Arrange
	eventUp := events.NewNodeUpEvent(nil)
	eventDown := events.NewNodeDownEvent(123, "test-node")

	go func() {
		time.Sleep(10 * time.Millisecond)
		mockManager.Publish(eventUp)
		mockManager.Publish(eventDown)
	}()

	// Act
	timeout := time.After(200 * time.Millisecond)
	receivedCount := 0

	for {
		select {
		case evt := <-stream.Events():
			receivedCount++
			if evt.Type != events.EventTypeNodeUp {
				t.Errorf("Expected NodeUp, got %v", evt.Type)
			}
		case <-timeout:
			// Assert: timeout is expected - we should only get 1 event
			if receivedCount != 1 {
				t.Errorf("Expected 1 event (NodeUp only), got %d", receivedCount)
			}
			return
		}
	}
}

// 该测试函数用于验证相关行为。
func TestEventStreamClose(t *testing.T) {
	// Arrange
	mockManager := events.NewEventManager()
	defer mockManager.Close()

	cluster := &EtcdCluster{
		events: eventState{
			manager:                  mockManager,
			snapshotLoadingCallbacks: events.NewCallbackList(),
			snapshotLoadedCallbacks:  events.NewCallbackList(),
			nodeEventCallbacks:       events.NewCallbackList(),
		},
	}

	ctx := context.Background()

	// Act
	stream := cluster.NewEventStream(ctx, 10, []events.EventType{events.EventTypeNodeUp})

	// Assert
	select {
	case <-stream.Events():
		t.Fatal("Channel should be empty")
	default:
		// Expected
	}

	// Act
	stream.Close()

	// Assert
	_, ok := <-stream.Events()
	if ok {
		t.Error("Expected channel to be closed")
	}

	// Act
	stream.Close()
}

// 该测试函数用于验证相关行为。
func TestEventStreamContextCancellation(t *testing.T) {
	// Arrange
	mockManager := events.NewEventManager()
	defer mockManager.Close()

	cluster := &EtcdCluster{
		events: eventState{
			manager:                  mockManager,
			snapshotLoadingCallbacks: events.NewCallbackList(),
			snapshotLoadedCallbacks:  events.NewCallbackList(),
			nodeEventCallbacks:       events.NewCallbackList(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream := cluster.NewEventStream(ctx, 10, []events.EventType{events.EventTypeNodeUp})
	defer stream.Close()

	// Act
	cancel()

	// Act
	time.Sleep(50 * time.Millisecond)

	// Act
	mockManager.Publish(events.NewNodeUpEvent(nil))

	// Assert
	stream.Close()
}

// 该测试函数用于验证相关行为。
func TestEventStreamBufferOverflow(t *testing.T) {
	// Arrange
	mockManager := events.NewEventManager()
	defer mockManager.Close()

	cluster := &EtcdCluster{
		events: eventState{
			manager:                  mockManager,
			snapshotLoadingCallbacks: events.NewCallbackList(),
			snapshotLoadedCallbacks:  events.NewCallbackList(),
			nodeEventCallbacks:       events.NewCallbackList(),
		},
	}

	ctx := context.Background()
	// Small buffer
	stream := cluster.NewEventStream(ctx, 2, []events.EventType{events.EventTypeNodeUp})
	defer stream.Close()

	// Act
	for i := 0; i < 10; i++ {
		mockManager.Publish(events.NewNodeUpEvent(nil))
	}

	// Act
	time.Sleep(50 * time.Millisecond)

	// Act
	receivedCount := 0
	timeout := time.After(100 * time.Millisecond)

	for {
		select {
		case <-stream.Events():
			receivedCount++
		case <-timeout:
			// Assert
			if receivedCount >= 10 {
				t.Errorf("Expected some events to be dropped, but received all %d", receivedCount)
			}
			return
		}
	}
}

// 该测试函数用于验证相关行为。
func TestEventStreamConcurrentPublish(t *testing.T) {
	// Arrange
	mockManager := events.NewEventManager()
	defer mockManager.Close()

	cluster := &EtcdCluster{
		events: eventState{
			manager:                  mockManager,
			snapshotLoadingCallbacks: events.NewCallbackList(),
			snapshotLoadedCallbacks:  events.NewCallbackList(),
			nodeEventCallbacks:       events.NewCallbackList(),
		},
	}

	ctx := context.Background()
	stream := cluster.NewEventStream(ctx, 100, []events.EventType{
		events.EventTypeNodeUp,
		events.EventTypeNodeDown,
		events.EventTypeNodeUpdate,
	})
	defer stream.Close()

	// Act
	const numGoroutines = 10
	const eventsPerGoroutine = 5

	done := make(chan struct{})
	go func() {
		for i := 0; i < numGoroutines; i++ {
			go func() {
				for j := 0; j < eventsPerGoroutine; j++ {
					mockManager.Publish(events.NewNodeUpEvent(nil))
				}
			}()
		}
		time.Sleep(200 * time.Millisecond)
		close(done)
	}()

	// Act
	receivedCount := 0
	timeout := time.After(1 * time.Second)

consumeLoop:
	for {
		select {
		case <-stream.Events():
			receivedCount++
		case <-done:
			// Drain remaining events
			time.Sleep(50 * time.Millisecond)
		drainLoop:
			for {
				select {
				case <-stream.Events():
					receivedCount++
				default:
					break drainLoop
				}
			}
			break consumeLoop
		case <-timeout:
			t.Fatal("Timeout waiting for events")
		}
	}

	// Assert
	expected := numGoroutines * eventsPerGoroutine
	if receivedCount < expected-10 { // Allow some drops due to timing
		t.Errorf("Expected around %d events, got %d", expected, receivedCount)
	}
}

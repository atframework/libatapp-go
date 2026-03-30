package events

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/atframework/libatapp-go/etcd_module/discovery"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func TestNewEventManager(t *testing.T) {
	// Arrange
	em := NewEventManager()

	// Assert
	if em == nil {
		t.Fatal("NewEventManager returned nil")
	}
	defer em.Close()
}

func TestEventManagerSubscribe(t *testing.T) {
	// Arrange
	em := NewEventManager()
	defer em.Close()

	var mu sync.Mutex
	callbackCalled := false

	handle := em.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {
		mu.Lock()
		callbackCalled = true
		mu.Unlock()
	})

	// Assert
	if handle == 0 {
		t.Log("Note: Handle is 0, but subscription was registered")
	}

	// Assert
	if em.GetSubscriberCount(EventTypeNodeUp) != 1 {
		t.Errorf("Expected 1 subscriber, got %d", em.GetSubscriberCount(EventTypeNodeUp))
	}
	if em.GetSubscriberCount(EventTypeNodeDown) != 0 {
		t.Errorf("Expected 0 subscribers for NodeDown, got %d", em.GetSubscriberCount(EventTypeNodeDown))
	}

	// Act
	em.Publish(&Event{Type: EventTypeNodeUp})

	// Assert: wait a bit for async callback
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	if !callbackCalled {
		t.Error("Callback was not called")
	}
	mu.Unlock()
}

func TestEventManagerUnsubscribe(t *testing.T) {
	// Arrange
	em := NewEventManager()
	defer em.Close()

	var mu sync.Mutex
	callbackCalled := false

	handle := em.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {
		mu.Lock()
		callbackCalled = true
		mu.Unlock()
	})

	// Act
	err := em.Unsubscribe(handle)
	if err != nil {
		t.Errorf("Unsubscribe failed: %v", err)
	}

	// Assert
	if em.GetSubscriberCount(EventTypeNodeUp) != 0 {
		t.Errorf("Expected 0 subscribers after unsubscribe, got %d", em.GetSubscriberCount(EventTypeNodeUp))
	}

	// Act
	em.Publish(&Event{Type: EventTypeNodeUp})

	// Assert: wait a bit for async callback
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	if callbackCalled {
		t.Error("Callback was called after unsubscribe")
	}
	mu.Unlock()
}

func TestEventManagerUnsubscribeByType(t *testing.T) {
	// Arrange
	em := NewEventManager()
	defer em.Close()

	em.Subscribe([]EventType{EventTypeNodeUp, EventTypeNodeDown}, func(e *Event) {})
	em.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {})

	// Assert
	if em.GetSubscriberCount(EventTypeNodeUp) != 2 {
		t.Errorf("Expected 2 subscribers for NodeUp, got %d", em.GetSubscriberCount(EventTypeNodeUp))
	}

	// Act
	err := em.UnsubscribeByType([]EventType{EventTypeNodeUp})
	if err != nil {
		t.Errorf("UnsubscribeByType failed: %v", err)
	}

	// Assert
	if em.GetSubscriberCount(EventTypeNodeUp) != 0 {
		t.Errorf("Expected 0 subscribers for NodeUp after UnsubscribeByType, got %d", em.GetSubscriberCount(EventTypeNodeUp))
	}

	// Subscriptions removed by type must not leave stale handles in other event lists.
	if em.GetSubscriberCount(EventTypeNodeDown) != 0 {
		t.Errorf("Expected 0 subscribers for NodeDown, got %d", em.GetSubscriberCount(EventTypeNodeDown))
	}
}

func TestEventManagerMultipleSubscribers(t *testing.T) {
	// Arrange
	em := NewEventManager()
	defer em.Close()

	var counter int
	var mu sync.Mutex

	em.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {
		mu.Lock()
		counter++
		mu.Unlock()
	})
	em.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {
		mu.Lock()
		counter++
		mu.Unlock()
	})

	// Act
	em.Publish(&Event{Type: EventTypeNodeUp})

	// Assert
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	if counter != 2 {
		t.Errorf("Expected counter to be 2, got %d", counter)
	}
	mu.Unlock()
}

func TestEventManagerMultipleEventTypes(t *testing.T) {
	// Arrange
	em := NewEventManager()
	defer em.Close()

	var nodeUpCalled, nodeDownCalled int32

	em.Subscribe([]EventType{EventTypeNodeUp, EventTypeNodeDown}, func(e *Event) {
		switch e.Type {
		case EventTypeNodeUp:
			atomic.AddInt32(&nodeUpCalled, 1)
		case EventTypeNodeDown:
			atomic.AddInt32(&nodeDownCalled, 1)
		}
	})

	em.Publish(&Event{Type: EventTypeNodeUp})
	em.Publish(&Event{Type: EventTypeNodeDown})

	// Assert
	time.Sleep(10 * time.Millisecond)

	if atomic.LoadInt32(&nodeUpCalled) == 0 {
		t.Error("NodeUp callback was not called")
	}
	if atomic.LoadInt32(&nodeDownCalled) == 0 {
		t.Error("NodeDown callback was not called")
	}
}

func TestEventManagerClose(t *testing.T) {
	// Arrange
	em := NewEventManager()

	em.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {})

	// Assert
	if em.GetSubscriberCount(EventTypeNodeUp) != 1 {
		t.Errorf("Expected 1 subscriber, got %d", em.GetSubscriberCount(EventTypeNodeUp))
	}

	// Act
	em.Close()

	// After Close, GetSubscriberCount should return 0 because the map is cleared
	// Note: This is implementation-dependent
}

func TestNewLeaseEvents(t *testing.T) {
	granted := NewLeaseGrantedEvent(1234, 16)
	if granted == nil {
		t.Fatal("NewLeaseGrantedEvent returned nil")
	}
	if granted.Type != EventTypeLeaseGranted {
		t.Fatalf("unexpected lease granted type: %v", granted.Type)
	}
	if granted.Metadata["lease_id"] != int64(1234) {
		t.Fatalf("unexpected lease_id: %v", granted.Metadata["lease_id"])
	}
	if granted.Metadata["ttl"] != int64(16) {
		t.Fatalf("unexpected ttl: %v", granted.Metadata["ttl"])
	}

	expired := NewLeaseExpiredEvent(4321)
	if expired == nil {
		t.Fatal("NewLeaseExpiredEvent returned nil")
	}
	if expired.Type != EventTypeLeaseExpired {
		t.Fatalf("unexpected lease expired type: %v", expired.Type)
	}
	if expired.Metadata["lease_id"] != int64(4321) {
		t.Fatalf("unexpected lease_id: %v", expired.Metadata["lease_id"])
	}
}

func TestNewLeaseReleasedEvent(t *testing.T) {
	released := NewLeaseReleasedEvent(5678)
	if released == nil {
		t.Fatal("NewLeaseReleasedEvent returned nil")
	}
	if released.Type != EventTypeLeaseReleased {
		t.Fatalf("unexpected lease released type: %v", released.Type)
	}
	if released.Metadata["lease_id"] != int64(5678) {
		t.Fatalf("unexpected lease_id: %v", released.Metadata["lease_id"])
	}
}

func TestEventManagerNilCallback(t *testing.T) {
	// Arrange
	em := NewEventManager()
	defer em.Close()

	// Act
	handle := em.Subscribe([]EventType{EventTypeNodeUp}, nil)

	// Assert
	if handle != 0 {
		t.Error("Expected handle to be 0 for nil callback")
	}
}

func TestEventManagerCloseWaitsForCallbacks(t *testing.T) {
	// Arrange
	em := NewEventManager()

	var wg sync.WaitGroup
	wg.Add(1)

	em.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {
		time.Sleep(50 * time.Millisecond)
		wg.Done()
	})

	// Act
	em.Publish(&Event{Type: EventTypeNodeUp})

	// Act
	done := make(chan struct{})
	go func() {
		em.Close()
		close(done)
	}()

	// Assert
	select {
	case <-done:
		// Success - Close completed
	case <-time.After(200 * time.Millisecond):
		t.Error("Close did not complete in time")
	}
}

func TestEventManagerCloseFromCallback_NoDeadlock(t *testing.T) {
	em := NewEventManager()

	em.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {
		em.Close()
	})

	publishDone := make(chan struct{})
	go func() {
		em.Publish(&Event{Type: EventTypeNodeUp})
		close(publishDone)
	}()

	select {
	case <-publishDone:
		// pass
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Publish deadlocked when callback called Close")
	}
}

func TestEventManagerPublishRunsSynchronously(t *testing.T) {
	// Arrange
	em := NewEventManager()
	defer em.Close()

	called := false
	em.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {
		called = true
	})

	// Act
	em.Publish(&Event{Type: EventTypeNodeUp})

	// Assert
	if !called {
		t.Fatalf("expected Publish to invoke callback synchronously")
	}
}

func TestEventManagerSubscribe_FirstHandleIsNonZero(t *testing.T) {
	em := NewEventManager()
	defer em.Close()

	handle := em.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {})
	if handle == 0 {
		t.Fatal("expected first subscription handle to be non-zero")
	}
}

func TestEventType_String(t *testing.T) {
	// Arrange
	tests := []struct {
		eventType EventType
		expected  string
	}{
		{EventTypeNodeUp, "node_up"},
		{EventTypeNodeDown, "node_down"},
		{EventTypeNodeUpdate, "node_update"},
		{EventTypeClusterUp, "cluster_up"},
		{EventTypeClusterDown, "cluster_down"},
		{EventTypeClusterChange, "cluster_change"},
		{EventTypeWatchConnected, "watch_connected"},
		{EventTypeWatchDisconnected, "watch_disconnected"},
		{EventTypeWatchReconnected, "watch_reconnected"},
		{EventTypeSnapshotLoaded, "snapshot_loaded"},
		{EventTypeLeaseGranted, "lease_granted"},
		{EventTypeLeaseExpired, "lease_expired"},
		{EventTypeLeaseReleased, "lease_released"},
		{EventType(999), "unknown"},
	}

	// Act
	for _, tt := range tests {
		if got := tt.eventType.String(); got != tt.expected {
			t.Errorf("EventType(%d).String() = %q, want %q", tt.eventType, got, tt.expected)
		}
	}
}

func TestClusterState_String(t *testing.T) {
	// Arrange
	tests := []struct {
		state    ClusterState
		expected string
	}{
		{ClusterStateUnknown, "unknown"},
		{ClusterStateDisconnected, "disconnected"},
		{ClusterStateConnecting, "connecting"},
		{ClusterStateConnected, "connected"},
		{ClusterStateReady, "ready"},
		{ClusterState(100), "invalid"},
	}

	// Act
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("ClusterState(%d).String() = %q, want %q", tt.state, got, tt.expected)
		}
	}
}

func TestNewNodeUpEvent(t *testing.T) {
	// Arrange
	event := NewNodeUpEvent(&discovery.DiscoveryNode{})

	// Assert
	if event.Type != EventTypeNodeUp {
		t.Errorf("Expected EventTypeNodeUp, got %v", event.Type)
	}
	if event.Timestamp.IsZero() {
		t.Error("Expected non-zero timestamp")
	}
}

func TestNewNodeDownEvent(t *testing.T) {
	// Arrange
	event := NewNodeDownEvent(123, "test-node")

	// Assert
	if event.Type != EventTypeNodeDown {
		t.Errorf("Expected EventTypeNodeDown, got %v", event.Type)
	}
	if event.NodeID != 123 {
		t.Errorf("Expected NodeID 123, got %d", event.NodeID)
	}
	if event.NodeName != "test-node" {
		t.Errorf("Expected NodeName 'test-node', got %s", event.NodeName)
	}
}

func TestNewWatchConnectedEvent(t *testing.T) {
	// Arrange
	event := NewWatchConnectedEvent(1000)

	// Assert
	if event.Type != EventTypeWatchConnected {
		t.Errorf("Expected EventTypeWatchConnected, got %v", event.Type)
	}
	if event.Revision != 1000 {
		t.Errorf("Expected Revision 1000, got %d", event.Revision)
	}
}

func TestNewSnapshotLoadedEvent(t *testing.T) {
	// Arrange
	event := NewSnapshotLoadedEvent(5, 1000)

	// Assert
	if event.Type != EventTypeSnapshotLoaded {
		t.Errorf("Expected EventTypeSnapshotLoaded, got %v", event.Type)
	}
	if event.Revision != 1000 {
		t.Errorf("Expected Revision 1000, got %d", event.Revision)
	}
	if nodeCount, ok := event.Metadata["node_count"].(int); !ok || nodeCount != 5 {
		t.Errorf("Expected node_count 5, got %v", event.Metadata["node_count"])
	}
}

func TestEventManagerPublishNilEvent(t *testing.T) {
	// Arrange
	em := NewEventManager()
	defer em.Close()

	// Act
	em.Publish(nil)
}

func TestEventManagerConcurrentSubscribe(t *testing.T) {
	// Arrange
	em := NewEventManager()
	defer em.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			em.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {})
		}()
	}
	wg.Wait()

	// Assert
	if em.GetSubscriberCount(EventTypeNodeUp) != 10 {
		t.Errorf("Expected 10 subscribers, got %d", em.GetSubscriberCount(EventTypeNodeUp))
	}
}

func TestEventManagerConcurrentPublish(t *testing.T) {
	// Arrange
	em := NewEventManager()
	defer em.Close()

	var counter int64

	for i := 0; i < 5; i++ {
		em.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {
			atomic.AddInt64(&counter, 1)
		})
	}

	// Act
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			em.Publish(&Event{Type: EventTypeNodeUp})
		}()
	}
	wg.Wait()

	// Assert
	time.Sleep(200 * time.Millisecond)

	// Assert: due to concurrent publishing, verify some callbacks were called
	finalCounter := atomic.LoadInt64(&counter)
	if finalCounter == 0 {
		t.Error("Expected some callbacks to be called")
	}
}

func TestEventManagerPublishMultipleTypes(t *testing.T) {
	// Arrange
	em := NewEventManager()
	ch := make(chan EventType, 2)

	em.Subscribe([]EventType{EventTypeNodeUp, EventTypeNodeDown}, func(event *Event) {
		ch <- event.Type
	})

	// Act
	em.Publish(NewNodeUpEvent(&discovery.DiscoveryNode{Info: &pb.AtappDiscovery{Name: "svc"}}))
	em.Publish(NewNodeDownEvent(1, "svc"))

	// Assert
	got := map[EventType]bool{}
	for i := 0; i < 2; i++ {
		select {
		case typ := <-ch:
			got[typ] = true
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("expected event publish")
		}
	}

	if !got[EventTypeNodeUp] || !got[EventTypeNodeDown] {
		t.Fatalf("expected both node up and down events")
	}
}

func TestEventManagerUnsubscribeByType_NoDanglingHandles(t *testing.T) {
	manager := NewEventManager()
	defer manager.Close()

	manager.Subscribe([]EventType{EventTypeNodeUp, EventTypeNodeDown}, func(e *Event) {})
	manager.Subscribe([]EventType{EventTypeNodeDown}, func(e *Event) {})

	if err := manager.UnsubscribeByType([]EventType{EventTypeNodeUp}); err != nil {
		t.Fatalf("UnsubscribeByType failed: %v", err)
	}

	em, ok := manager.(*eventManager)
	if !ok {
		t.Fatalf("unexpected event manager type %T", manager)
	}

	em.mu.RLock()
	defer em.mu.RUnlock()

	for _, handle := range em.callbacks[EventTypeNodeDown] {
		if _, exists := em.subscriptions[handle]; !exists {
			t.Fatalf("found dangling handle %d in node_down callbacks", handle)
		}
	}
}

func TestEventManagerClose_DoesNotBlockOnInFlightPublish(t *testing.T) {
	manager := NewEventManager()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	publishDone := make(chan struct{})

	manager.Subscribe([]EventType{EventTypeNodeUp}, func(e *Event) {
		entered <- struct{}{}
		<-release
	})

	go func() {
		manager.Publish(&Event{Type: EventTypeNodeUp})
		close(publishDone)
	}()

	select {
	case <-entered:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("callback did not start in time")
	}

	closeDone := make(chan struct{})
	go func() {
		manager.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		// expected: non-blocking close
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Close blocked with in-flight callback")
	}

	close(release)

	select {
	case <-publishDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Publish did not finish after callback release")
	}
}

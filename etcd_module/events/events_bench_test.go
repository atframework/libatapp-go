package events

import (
	"testing"
	"time"
)

// 该基准函数用于评估性能表现。
func BenchmarkEventPublish(b *testing.B) {
	em := NewEventManager()
	defer em.Close()

	event := &Event{
		Type:      EventTypeNodeUp,
		Timestamp: time.Now(),
		NodeName:  "test-node",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		em.Publish(event)
	}
}

// 该基准函数用于评估性能表现。
func BenchmarkEventPublishWithSubscribers(b *testing.B) {
	em := NewEventManager()
	defer em.Close()

	// Add 10 subscribers
	for i := 0; i < 10; i++ {
		em.Subscribe([]EventType{EventTypeNodeUp, EventTypeNodeDown}, func(event *Event) {
			_ = event.NodeName
		})
	}

	event := &Event{
		Type:      EventTypeNodeUp,
		Timestamp: time.Now(),
		NodeName:  "test-node",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		em.Publish(event)
	}
}

// 该基准函数用于评估性能表现。
func BenchmarkSubscribe(b *testing.B) {
	em := NewEventManager()
	defer em.Close()

	callback := func(event *Event) {
		_ = event.NodeName
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		em.Subscribe([]EventType{EventTypeNodeUp}, callback)
	}
}

// 该基准函数用于评估性能表现。
func BenchmarkUnsubscribe(b *testing.B) {
	em := NewEventManager()
	defer em.Close()

	callback := func(event *Event) {
		_ = event.NodeName
	}

	handles := make([]EventCallbackHandle, 100)
	for i := range handles {
		handles[i] = em.Subscribe([]EventType{EventTypeNodeUp}, callback)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		em.Unsubscribe(handles[i%len(handles)])
	}
}

// 该基准函数用于评估性能表现。
func BenchmarkGetSubscriberCount(b *testing.B) {
	em := NewEventManager()
	defer em.Close()

	em.Subscribe([]EventType{EventTypeNodeUp}, func(event *Event) {})
	em.Subscribe([]EventType{EventTypeNodeDown}, func(event *Event) {})
	em.Subscribe([]EventType{EventTypeNodeUp, EventTypeNodeDown}, func(event *Event) {})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		em.GetSubscriberCount(EventTypeNodeUp)
	}
}

package integration_test

// Shared test constants and helpers for embed integration tests.
// These replicate the constants/helpers from the parent module's
// module_actors_integration_test.go — _test package symbols cannot be imported
// across modules, so they are re-defined here.

import (
	"testing"
	"time"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
)

// ── path prefix constants (mirror actorInteg* in the parent module) ───────

const (
	embedByIDPrefix   = "/svc/by_id"
	embedByNamePrefix = "/svc/by_name"
	embedTopoPrefix   = "/svc/topology"
)

// subscribeEmbedEvents subscribes to all module events and returns a buffered channel.
func subscribeEmbedEvents(t *testing.T, m *modulev2.EtcdModule) <-chan modulev2.EventEnvelope {
	t.Helper()
	ch := make(chan modulev2.EventEnvelope, 256)
	handle := m.Subscribe(func(e modulev2.EventEnvelope) {
		select {
		case ch <- e:
		default:
		}
	})
	t.Cleanup(func() { m.Unsubscribe(handle) })
	return ch
}

// waitForEmbedEvent blocks until an event of wantType arrives on ch or the
// deadline passes.
func waitForEmbedEvent(
	t *testing.T,
	ch <-chan modulev2.EventEnvelope,
	wantType modulev2.EventType,
	timeout time.Duration,
) modulev2.EventEnvelope {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case env := <-ch:
			if env.Type == wantType {
				return env
			}
		case <-deadline:
			t.Fatalf("timed out waiting for module event %s", modulev2.EventTypeName(wantType))
			return modulev2.EventEnvelope{}
		}
	}
}

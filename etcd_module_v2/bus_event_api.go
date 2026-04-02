// Package modulev2 — public re-exports of EventBus types for external callers.
//
// The raw event infrastructure lives in internal/runtime and
// internal/orchestrator.  This file lifts the types and constants that
// integration layers (e.g. root-package adapter) need to subscribe to the
// EventBus without importing internal packages.
package modulev2

import (
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/orchestrator"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
)

// ── Event infrastructure types ────────────────────────────────────────────

// EventType is the event identifier string used in SubscribeType calls.
type EventType = runtime.EventType

// EventEnvelope is the carrier type passed to every EventBus handler.
type EventEnvelope = runtime.EventEnvelope

// EventHandler is the callback signature for EventBus subscriptions.
type EventHandler = runtime.EventHandler

// ── Watch event constants ─────────────────────────────────────────────────

const (
	// EventTypeUnknown is the zero value; also used as wildcard in Subscribe.
	EventTypeUnknown EventType = runtime.EventTypeUnknown

	// EventWatchNodeUp fires when a new node appears under a watched prefix.
	EventWatchNodeUp EventType = runtime.EventWatchNodeUp
	// EventWatchNodeDown fires when a node disappears under a watched prefix.
	EventWatchNodeDown EventType = runtime.EventWatchNodeDown
	// EventWatchNodeUpdate fires when an existing node's value changes.
	EventWatchNodeUpdate EventType = runtime.EventWatchNodeUpdate
	// EventWatchTopologyUp fires when a topology entry appears.
	EventWatchTopologyUp EventType = runtime.EventWatchTopologyUp
	// EventWatchTopologyDown fires when a topology entry is removed.
	EventWatchTopologyDown EventType = runtime.EventWatchTopologyDown
	// EventWatchTopologyUpdate fires when a topology entry is updated.
	EventWatchTopologyUpdate EventType = runtime.EventWatchTopologyUpdate

	// EventWatchSnapshotLoading fires just before the initial Get snapshot begins.
	EventWatchSnapshotLoading EventType = runtime.EventWatchSnapshotLoading
	// EventWatchSnapshotLoaded fires after the initial Get snapshot completes.
	EventWatchSnapshotLoaded EventType = runtime.EventWatchSnapshotLoaded

	// EventWatchTopologySnapshotLoading fires just before the initial topology Get begins.
	EventWatchTopologySnapshotLoading EventType = runtime.EventWatchTopologySnapshotLoading
	// EventWatchTopologySnapshotLoaded fires after the initial topology Get snapshot completes.
	EventWatchTopologySnapshotLoaded EventType = runtime.EventWatchTopologySnapshotLoaded

	// EventLeaseGranted fires each time a lease is successfully granted.
	EventLeaseGranted EventType = runtime.EventLeaseGranted
	// EventLeaseExpired fires when the lease keepalive channel closes unexpectedly.
	EventLeaseExpired EventType = runtime.EventLeaseExpired
	// EventLeaseReleased fires after an intentional lease revocation.
	EventLeaseReleased EventType = runtime.EventLeaseReleased

	// EventRegistrationChanged fires when the registration index is updated.
	EventRegistrationChanged EventType = runtime.EventRegistrationChanged
)

// EventTypeName returns a human-readable name for the given EventType.
// Intended for logging and metrics.
func EventTypeName(t EventType) string {
	return runtime.EventTypeName(t)
}

// ── Payload type aliases ──────────────────────────────────────────────────

// WatchNodePayload is the Payload for EventWatchNodeUp, Down, and Update.
type WatchNodePayload = orchestrator.WatchNodePayload

// WatchSnapshotLoadingPayload is the Payload for EventWatchSnapshotLoading.
type WatchSnapshotLoadingPayload = orchestrator.WatchSnapshotLoadingPayload

// WatchSnapshotLoadedPayload is the Payload for EventWatchSnapshotLoaded.
type WatchSnapshotLoadedPayload = orchestrator.WatchSnapshotLoadedPayload

// WatchTopologyPayload is the Payload for EventWatchTopologyUp, Down, and Update.
type WatchTopologyPayload = orchestrator.WatchTopologyPayload

// WatchTopologySnapshotLoadedPayload is the Payload for EventWatchTopologySnapshotLoaded.
type WatchTopologySnapshotLoadedPayload = orchestrator.WatchTopologySnapshotLoadedPayload

// RegistrationChangedPayload is the Payload for EventRegistrationChanged.
type RegistrationChangedPayload = orchestrator.RegistrationChangedPayload

// LeaseGrantedPayload is the Payload for EventLeaseGranted.
type LeaseGrantedPayload = orchestrator.LeaseGrantedPayload

// LeaseExpiredPayload is the Payload for EventLeaseExpired.
type LeaseExpiredPayload = orchestrator.LeaseExpiredPayload

// LeaseReleasedPayload is the Payload for EventLeaseReleased.
type LeaseReleasedPayload = orchestrator.LeaseReleasedPayload

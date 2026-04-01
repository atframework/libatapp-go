package runtime

import "fmt"

// ── EventType constants ───────────────────────────────────────────────────
//
// Values are stable integers — do NOT reorder or reassign.
// Append new events at the end of each group.

const (
	// EventTypeUnknown is the zero value; it acts as wildcard in EventBus.Subscribe.
	EventTypeUnknown EventType = iota // 0

	// Lease events (producer: LeaseActor)
	EventLeaseGranted  // 1
	EventLeaseExpired  // 2
	EventLeaseReleased // 3

	// Registration events (producer: RegistrationActor)
	EventRegistrationChanged // 4

	// Watch events (producer: WatchActor)
	EventWatchSnapshotLoading         // 5
	EventWatchSnapshotLoaded          // 6
	EventWatchNodeUp                  // 7
	EventWatchNodeDown                // 8
	EventWatchNodeUpdate              // 9
	EventWatchTopologyUp              // 10
	EventWatchTopologyDown            // 11
	EventWatchTopologyUpdate          // 12
	EventWatchTopologySnapshotLoading // 13
	EventWatchTopologySnapshotLoaded  // 14
)

// EventTypeName returns a human-readable label for the given EventType.
// Used for logging and metrics; not a stable serialisation format.
func EventTypeName(t EventType) string {
	switch t {
	case EventTypeUnknown:
		return "unknown"
	case EventLeaseGranted:
		return "lease.granted"
	case EventLeaseExpired:
		return "lease.expired"
	case EventLeaseReleased:
		return "lease.released"
	case EventRegistrationChanged:
		return "registration.changed"
	case EventWatchSnapshotLoading:
		return "watch.snapshot.loading"
	case EventWatchSnapshotLoaded:
		return "watch.snapshot.loaded"
	case EventWatchNodeUp:
		return "watch.node.up"
	case EventWatchNodeDown:
		return "watch.node.down"
	case EventWatchNodeUpdate:
		return "watch.node.update"
	case EventWatchTopologyUp:
		return "watch.topology.up"
	case EventWatchTopologyDown:
		return "watch.topology.down"
	case EventWatchTopologyUpdate:
		return "watch.topology.update"
	case EventWatchTopologySnapshotLoading:
		return "watch.topology.snapshot.loading"
	case EventWatchTopologySnapshotLoaded:
		return "watch.topology.snapshot.loaded"
	default:
		return fmt.Sprintf("EventType(%d)", uint32(t))
	}
}

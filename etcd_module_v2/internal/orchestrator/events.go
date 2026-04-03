package orchestrator

import (
	"fmt"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/snapshot"
)

// ── Payload types ─────────────────────────────────────────────────────────

// LeaseGrantedPayload is the Payload carried by EventLeaseGranted.
type LeaseGrantedPayload struct {
	LeaseID clientv3.LeaseID
	TTL     int64
}

// LeaseExpiredPayload is the Payload carried by EventLeaseExpired.
type LeaseExpiredPayload struct {
	LeaseID clientv3.LeaseID
}

// LeaseReleasedPayload is the Payload carried by EventLeaseReleased.
type LeaseReleasedPayload struct {
	LeaseID clientv3.LeaseID
}

// RegistrationChangedPayload is the Payload carried by EventRegistrationChanged.
// The maps are immutable after the envelope is published.
type RegistrationChangedPayload struct {
	snapshot.SelfRegistrationSnapshot
}

// WatchNodePayload is shared by EventWatchNodeUp, Down, and Update.
type WatchNodePayload struct {
	// Revision is the etcd revision at the time of the event.
	Revision int64
	// Key is the full etcd key of the affected node.
	Key string
	// Value is the decoded AtappDiscovery for Up/Update events; nil for Down.
	Value *pb.AtappDiscovery
	// RawValue is the raw protobuf bytes (for forward compatibility).
	RawValue []byte
	// ModRevision is the etcd create/mod/version metadata of the KV.
	ModRevision    int64
	Version        int64
	CreateRevision int64
}

// WatchSnapshotLoadingPayload is the Payload for EventWatchSnapshotLoading.
type WatchSnapshotLoadingPayload struct {
	// Prefix is the watch prefix that triggered the reload.
	Prefix string
}

// WatchSnapshotLoadedPayload is the Payload for EventWatchSnapshotLoaded.
type WatchSnapshotLoadedPayload struct {
	// Prefix is the watch prefix whose initial snapshot was loaded.
	Prefix string
	// Nodes is the full initial snapshot indexed by etcd key.
	Nodes map[string]*snapshot.DiscoveryNode
	// Revision is the etcd revision at which the snapshot was taken.
	Revision int64
}

// WatchTopologyPayload is shared by EventWatchTopologyUp, Down, and Update.
type WatchTopologyPayload struct {
	Revision int64
	Key      string
	Value    *pb.AtappTopologyInfo

	ModRevision    int64
	Version        int64
	CreateRevision int64
}

// WatchTopologySnapshotLoadedPayload is the Payload for EventWatchTopologySnapshotLoaded.
type WatchTopologySnapshotLoadedPayload struct {
	// Nodes is the full initial topology snapshot indexed by ID.
	Nodes map[uint64]*snapshot.TopologyNode
	// Revision is the etcd revision at which the snapshot was taken.
	Revision int64
}

// ── DedupeKey helpers ─────────────────────────────────────────────────────

// LeaseDedupeKey returns a stable idempotency key for a lease event.
func LeaseDedupeKey(epoch uint64, eventType runtime.EventType) string {
	return fmt.Sprintf("lease:%d:%d", epoch, eventType)
}

// WatchNodeDedupeKey returns a stable idempotency key for a watch node event.
func WatchNodeDedupeKey(revision int64, key string, eventType runtime.EventType) string {
	return fmt.Sprintf("watch:%d:%s:%d", revision, key, eventType)
}

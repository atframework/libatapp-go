// Package modulev2 is the public API surface of the v2 etcd module.
// It depends only on:
//   - go.etcd.io/etcd/client/v3 (clientv3 SDK)
//   - github.com/atframework/libatapp-go/protocol/atframe (proto types)
//   - the internal/ sub‑packages defined here
//
// It deliberately does NOT import any code from etcd_module/.
package modulev2

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/etcdversion"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/orchestrator"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/snapshot"
)

// ── Re-export snapshot types ──────────────────────────────────────────────

// ExportSnapshot is the unified read-model exported by this module.
// Use EtcdModule.GetSnapshot() to obtain the latest atomic copy.
type ExportSnapshot = snapshot.ExportSnapshot

// DiscoverySetSnapshot is the discovery sub-view in ExportSnapshot.
type DiscoverySetSnapshot = snapshot.DiscoverySetSnapshot

// DiscoverySnapshot is kept as a compatibility alias.
type DiscoverySnapshot = snapshot.DiscoverySetSnapshot

// TopologySnapshot is the topology sub-view in ExportSnapshot.
type TopologySnapshot = snapshot.TopologySnapshot

// DiscoveryNode is the canonical read-model node type exported by v2.
type DiscoveryNode = snapshot.DiscoveryNode

// TopologyNode is the canonical topology read-model node type exported by v2.
type TopologyNode = snapshot.TopologyNode

// DataVersion is the public etcd key version tuple used by v2 snapshots.
type DataVersion = etcdversion.DataVersion

// ── Watcher callback types ────────────────────────────────────────────────

// SnapshotCallback is invoked on the ProjectionActor's run goroutine whenever
// a new ExportSnapshot is published.  The implementation must be fast and
// non-blocking.
type SnapshotCallback = orchestrator.SnapshotCallback

// NodeEventCallback is called for individual node-level watch events.
type NodeEventCallback func(eventType runtime.EventType, node *DiscoveryNode)

// ── RegistrationHandle ────────────────────────────────────────────────────

// RegistrationHandle is a token returned by EtcdModule.RegisterService.
// It does not carry a channel; callers interact with it via context.
type RegistrationHandle struct {
	path       string
	cancelFunc context.CancelFunc
	doneCh     <-chan error
}

// Path returns the primary etcd key used for this registration.
func (h *RegistrationHandle) Path() string { return h.path }

// Wait blocks until the registration write completes or ctx is cancelled.
func (h *RegistrationHandle) Wait(ctx context.Context) error {
	select {
	case err := <-h.doneCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ── PathConfig ────────────────────────────────────────────────────────────

// PathConfig holds the etcd key prefix configuration for a module instance.
type PathConfig struct {
	// ByNamePrefix is the prefix for name-indexed service keys.
	// e.g. "/services/name/"
	ByNamePrefix string
	// ByIDPrefix is the prefix for ID-indexed service keys.
	// e.g. "/services/id/"
	ByIDPrefix string
	// ByTypeIDPrefix is the prefix for type-id-indexed service keys.
	// e.g. "/services/by_type_id/"
	ByTypeIDPrefix string
	// ByTypeNamePrefix is the prefix for type-name-indexed service keys.
	// e.g. "/services/by_type_name/"
	ByTypeNamePrefix string
	// ByTagPrefix is the prefix for tag-indexed service keys.
	// e.g. "/services/by_tag/"
	ByTagPrefix string
	// TopologyPrefix is the prefix for topology keepalive keys.
	// e.g. "/topology/"
	TopologyPrefix string
	// WatchPrefixes lists the etcd prefixes that WatchActor should monitor.
	WatchPrefixes []string
	// LeaseTTL is the default lease TTL in seconds (default: 10).
	LeaseTTL int64
}

// Validate fills in defaults and returns the config.
func (c PathConfig) Validate() PathConfig {
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 10
	}
	return c
}

// ── ServiceInfo ───────────────────────────────────────────────────────────

// ServiceInfo groups the parameters required to register a single service.
type ServiceInfo struct {
	// Discovery is the AtappDiscovery proto to write.
	Discovery *pb.AtappDiscovery
	// TopologyInfo overrides the derived topology keepalive payload when set.
	TopologyInfo *pb.AtappTopologyInfo
	// Path is the primary etcd key (bypath).
	Path string
	// TTL overrides PathConfig.LeaseTTL for this service (0 = use default).
	TTL int64
}

// ── Event handle ──────────────────────────────────────────────────────────

// EventHandle is an opaque subscription handle returned by AddXxxCallback.
type EventHandle = runtime.EventHandleHandle

// ── EtcdClient alias ──────────────────────────────────────────────────────

// EtcdClient is the etcd operations interface expected by this module.
// Any value satisfying etcd_module/client.EtcdClient will satisfy this too.
type EtcdClient = orchestrator.EtcdClient

// ── LeaseID re-export ─────────────────────────────────────────────────────

// LeaseID re-exports the clientv3 lease identifier for callers that do not
// want to import clientv3 directly.
type LeaseID = clientv3.LeaseID

// ── ModuleOptions ─────────────────────────────────────────────────────────

// ModuleOptions holds optional settings for EtcdModule initialisation.
type ModuleOptions struct {
	// OnSnapshotPublished is called on ProjectionActor's goroutine whenever a
	// new ExportSnapshot is atomically published.
	OnSnapshotPublished SnapshotCallback
	// RetryInterval is the base interval for lease-grant retries.
	RetryInterval time.Duration
	// ConsistentHashVirtualNodes controls vnode count when using
	// GetNodeByConsistentHash/GetNodesByConsistentHash.
	// <=0 uses the legacy default (80) for C++/v1 parity.
	ConsistentHashVirtualNodes int
}

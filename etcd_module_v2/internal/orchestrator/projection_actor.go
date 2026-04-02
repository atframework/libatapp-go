package orchestrator

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/etcdversion"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/snapshot"
)

// ── Message types (sealed interface) ─────────────────────────────────────

type projMsg interface{ projMsgKind() }

// ProjectionMsgType enumerates message kinds for logging and metrics.
type ProjectionMsgType uint8

const (
	ProjectionMsgApplyEvent          ProjectionMsgType = iota + 1
	ProjectionMsgRebuildFromSnapshot                   // 2
	ProjectionMsgReset                                 // 3
)

// Name returns a human-readable label.
func (t ProjectionMsgType) Name() string {
	switch t {
	case ProjectionMsgApplyEvent:
		return "ProjectionMsgApplyEvent"
	case ProjectionMsgRebuildFromSnapshot:
		return "ProjectionMsgRebuildFromSnapshot"
	case ProjectionMsgReset:
		return "ProjectionMsgReset"
	default:
		return "ProjectionMsgUnknown"
	}
}

type projMsgApply struct{ Env runtime.EventEnvelope }
type projMsgReset struct{}

func (projMsgApply) projMsgKind() {}
func (projMsgReset) projMsgKind() {}

// ── Snapshot callback ─────────────────────────────────────────────────────

// SnapshotCallback is invoked by ProjectionActor each time it publishes a new
// ExportSnapshot.  Implementations must be fast and non-blocking.
type SnapshotCallback func(snap *snapshot.ExportSnapshot)

// ── ProjectionActor ───────────────────────────────────────────────────────

// ProjectionActor is the sole publisher of ExportSnapshot.  It consumes all
// EventBus events and merges them into a unified Discovery + Topology +
// Registration view that is atomically swapped via atomic.Pointer.
//
// Invariants:
//  1. The atomic pointer is only ever stored inside the Run goroutine.
//  2. External callers load the pointer lock-free; they must not mutate the result.
//  3. snapshot.Version is strictly monotonically increasing.
//
// Mailbox capacity: 256 (watch events may burst).
type ProjectionActor struct {
	runtime.ActorBase[projMsg]

	eventBus runtime.EventBus

	// snapshot holds the latest published read model.
	// Written only in Run goroutine via atomic.Pointer.Store.
	snapshot atomic.Pointer[snapshot.ExportSnapshot]

	// onPublish is an optional external callback fired after every atomic store.
	onPublish SnapshotCallback

	// dedupeCache keeps the last-seen DedupeKey per event Source to drop
	// replay duplicates.  Keyed by EventSource; sized for 4 actors.
	dedupeCache map[string]struct{}

	// pending tracks accepted-but-not-yet-merged Registration epochs so we
	// don't regress the snapshot on out-of-order delivery.
	currentLeaseEpoch uint64

	// version monotonically incremented on each publish.
	version uint64

	// discLoadingPrefixes tracks discovery watch prefixes that are currently
	// in the loading (not-ready) state.  Discovery.Ready is true only when
	// this set is empty.  This lets multiple simultaneous streams (e.g.
	// /by_id and /by_name) each supply their own nodes without the second
	// stream overwriting nodes loaded by the first.
	discLoadingPrefixes map[string]struct{}

	// internal mutable sub-state (only touched in Run goroutine)
	discovery snapshot.DiscoverySetSnapshot
	topology  snapshot.TopologySnapshot
	reg       snapshot.RegistrationSnapshot
}

// NewProjectionActor creates a ProjectionActor.  onPublish may be nil.
func NewProjectionActor(bus runtime.EventBus, onPublish SnapshotCallback) *ProjectionActor {
	a := &ProjectionActor{
		ActorBase:           runtime.NewActorBase[projMsg](256),
		eventBus:            bus,
		onPublish:           onPublish,
		dedupeCache:         make(map[string]struct{}),
		discLoadingPrefixes: make(map[string]struct{}),
	}
	a.discovery.NodesByPath = make(map[string]*snapshot.DiscoveryNode)
	a.topology.NodesByID = make(map[uint64]*snapshot.TopologyNode)
	return a
}

// ── External API ──────────────────────────────────────────────────────────

// GetSnapshot returns the latest published snapshot.  The returned pointer is
// valid until the next call; callers must not hold it across goroutine
// boundaries without copying.
func (a *ProjectionActor) GetSnapshot() *snapshot.ExportSnapshot {
	return a.snapshot.Load()
}

// Reset requests a full state clear (e.g. on module Stop).
func (a *ProjectionActor) Reset() {
	a.Post(projMsgReset{})
}

// Run is the actor's event loop.
func (a *ProjectionActor) Run(ctx context.Context) {
	// Subscribe to ALL EventBus events.
	h := a.eventBus.Subscribe(func(env runtime.EventEnvelope) {
		// Non-blocking inject into mailbox; drop on full (level-triggered).
		a.Post(projMsgApply{Env: env})
	})
	defer a.eventBus.Unsubscribe(h)

	a.RunLoop(ctx, a.handle)
}

// ── Message handlers ──────────────────────────────────────────────────────

func (a *ProjectionActor) handle(msg projMsg) {
	switch m := msg.(type) {
	case projMsgApply:
		a.onApply(m.Env)
	case projMsgReset:
		a.onReset()
	}
}

func (a *ProjectionActor) onApply(env runtime.EventEnvelope) {
	// ── Deduplicate ────────────────────────────────────────────────────
	if env.DedupeKey != "" {
		if _, seen := a.dedupeCache[env.DedupeKey]; seen {
			return
		}
		a.dedupeCache[env.DedupeKey] = struct{}{}
		// Bound the cache to avoid unbounded growth
		if len(a.dedupeCache) > 4096 {
			a.dedupeCache = make(map[string]struct{})
		}
	}

	switch env.Type {
	// ── Lease events ────────────────────────────────────────────────────
	case runtime.EventLeaseExpired:
		a.reg = snapshot.RegistrationSnapshot{}
		a.currentLeaseEpoch = env.LeaseEpoch
		a.publishSnapshot(snapshot.SnapshotCauseRegistration)

	case runtime.EventLeaseGranted:
		a.currentLeaseEpoch = env.LeaseEpoch

	case runtime.EventLeaseReleased:
		a.reg = snapshot.RegistrationSnapshot{}
		a.publishSnapshot(snapshot.SnapshotCauseRegistration)

	// ── Registration events ─────────────────────────────────────────────
	case runtime.EventRegistrationChanged:
		// Discard stale-epoch registrations to prevent regression.
		if env.LeaseEpoch < a.currentLeaseEpoch {
			return
		}
		pl := env.Payload.(RegistrationChangedPayload)
		a.reg = *pl.RegistrationSnapshot.Clone()
		a.currentLeaseEpoch = env.LeaseEpoch
		a.publishSnapshot(snapshot.SnapshotCauseRegistration)

	// ── Watch events ─────────────────────────────────────────────────────
	case runtime.EventWatchSnapshotLoading:
		// Mark this prefix as loading and evict its stale nodes.
		// Nodes from OTHER prefixes remain intact, preserving their data while
		// this stream rebuilds.  Ready becomes false until all loading prefixes
		// have completed their initial snapshot.
		var prefix string
		if pl, ok := env.Payload.(WatchSnapshotLoadingPayload); ok {
			prefix = pl.Prefix
		}
		a.discLoadingPrefixes[prefix] = struct{}{}
		for path := range a.discovery.NodesByPath {
			if prefix == "" || strings.HasPrefix(path, prefix) {
				delete(a.discovery.NodesByPath, path)
			}
		}
		a.discovery.Ready = false
		a.publishSnapshot(snapshot.SnapshotCauseDiscovery)

	case runtime.EventWatchSnapshotLoaded:
		pl := env.Payload.(WatchSnapshotLoadedPayload)
		// Remove stale nodes for this prefix before merging the new snapshot.
		// Nodes from other concurrent prefixes are left untouched.
		if pl.Prefix != "" {
			for path := range a.discovery.NodesByPath {
				if strings.HasPrefix(path, pl.Prefix) {
					delete(a.discovery.NodesByPath, path)
				}
			}
		} else {
			a.discovery.NodesByPath = make(map[string]*snapshot.DiscoveryNode)
		}
		for path, node := range pl.Nodes {
			a.discovery.NodesByPath[path] = node
		}
		if pl.Revision > a.discovery.LastRevision {
			a.discovery.LastRevision = pl.Revision
		}
		// Mark this prefix as done loading; Ready = true when all are done.
		delete(a.discLoadingPrefixes, pl.Prefix)
		a.discovery.Ready = len(a.discLoadingPrefixes) == 0
		a.discovery.RebuildIndexes()
		a.publishSnapshot(snapshot.SnapshotCauseDiscovery)

	case runtime.EventWatchTopologySnapshotLoaded:
		pl := env.Payload.(WatchTopologySnapshotLoadedPayload)
		a.topology = snapshot.TopologySnapshot{
			Ready:        true,
			LastRevision: pl.Revision,
			NodesByID:    pl.Nodes,
		}
		// Rebuild path index from ID index
		if len(pl.Nodes) > 0 {
			a.topology.RebuildIndexes()
		}
		a.publishSnapshot(snapshot.SnapshotCauseTopology)

	case runtime.EventWatchNodeUp, runtime.EventWatchNodeUpdate:
		pl := env.Payload.(WatchNodePayload)
		a.discovery.UpsertNode(&snapshot.DiscoveryNode{
			Info:        pl.Value,
			Path:        pl.Key,
			DataVersion: etcdversion.New(pl.CreateRevision, pl.ModRevision, pl.Version),
		})
		if pl.Revision > a.discovery.LastRevision {
			a.discovery.LastRevision = pl.Revision
		}
		a.publishSnapshot(snapshot.SnapshotCauseDiscovery)

	case runtime.EventWatchNodeDown:
		pl := env.Payload.(WatchNodePayload)
		a.discovery.RemoveNodeByPath(pl.Key)
		if pl.Revision > a.discovery.LastRevision {
			a.discovery.LastRevision = pl.Revision
		}
		a.publishSnapshot(snapshot.SnapshotCauseDiscovery)

	case runtime.EventWatchTopologyUp, runtime.EventWatchTopologyUpdate:
		pl := env.Payload.(WatchTopologyPayload)
		a.topology.UpsertNode(&snapshot.TopologyNode{
			Info:        pl.Value,
			DataVersion: etcdversion.New(pl.CreateRevision, pl.ModRevision, pl.Version),
		})
		if pl.Revision > a.topology.LastRevision {
			a.topology.LastRevision = pl.Revision
		}
		a.publishSnapshot(snapshot.SnapshotCauseTopology)

	case runtime.EventWatchTopologyDown:
		pl := env.Payload.(WatchTopologyPayload)
		// Try to remove by ID (from previous value)
		if pl.Value != nil && pl.Value.GetId() != 0 {
			a.topology.RemoveNodeByID(pl.Value.GetId())
		}
		if pl.Revision > a.topology.LastRevision {
			a.topology.LastRevision = pl.Revision
		}
		a.publishSnapshot(snapshot.SnapshotCauseTopology)
	}
}

func (a *ProjectionActor) onReset() {
	a.discovery = snapshot.DiscoverySetSnapshot{NodesByPath: make(map[string]*snapshot.DiscoveryNode)}
	a.topology = snapshot.TopologySnapshot{
		NodesByID: make(map[uint64]*snapshot.TopologyNode),
	}
	a.reg = snapshot.RegistrationSnapshot{}
	a.dedupeCache = make(map[string]struct{})
	a.discLoadingPrefixes = make(map[string]struct{})
	a.publishSnapshot(snapshot.SnapshotCauseReset)
}

// ── Snapshot assembly & atomic publish ───────────────────────────────────

func (a *ProjectionActor) publishSnapshot(cause snapshot.SnapshotCause) {
	a.version++
	snap := &snapshot.ExportSnapshot{
		Version:      a.version,
		PublishedAt:  time.Now(),
		Cause:        cause,
		Discovery:    *a.discovery.Clone(),
		Topology:     *a.topology.Clone(),
		Registration: *a.reg.Clone(),
	}
	a.snapshot.Store(snap)
	if a.onPublish != nil {
		a.onPublish(snap)
	}
}

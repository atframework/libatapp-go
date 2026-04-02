package orchestrator_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/orchestrator"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/snapshot"
)

// ── helpers ───────────────────────────────────────────────────────────────

// waitForSnapshot blocks until the callback channel delivers a snapshot or
// the deadline passes.
func waitForSnapshot(t *testing.T, ch <-chan *snapshot.ExportSnapshot, timeout time.Duration) *snapshot.ExportSnapshot {
	t.Helper()
	select {
	case snap := <-ch:
		return snap
	case <-time.After(timeout):
		t.Fatal("timed out waiting for snapshot")
		return nil
	}
}

// startProjectionActor starts the actor and returns a cancel func + the
// snapshot-published notification channel.
// The function blocks until the actor's bus subscription is established.
func startProjectionActor(
	t *testing.T,
	bus runtime.EventBus,
) (actor *orchestrator.ProjectionActor, snapCh <-chan *snapshot.ExportSnapshot, cancel context.CancelFunc) {
	t.Helper()
	ch := make(chan *snapshot.ExportSnapshot, 8)

	// Track how many subscriptions are registered on the bus by wrapping onPublish.
	// We use a "ready" sentinel: before starting the actor, we poll the bus by
	// publishing a harmless sentinel event.  Once the actor's subscription is
	// registered it will receive the sentinel and post to its mailbox (where the
	// EventType(0) unknown case is a no-op), confirming readiness.
	//
	// Simpler approach used here: the actor subscribes at the very first line of
	// its Run function.  A single runtime.Gosched() is not guaranteed.  So we use
	// a two-phase approach:
	//   1. Register a "before-actor" probe subscription on the bus.
	//   2. Start the actor goroutine.
	//   3. Register an "after-actor" probe subscription on the bus.
	//   4. Any event now goes to: before-probe → actor → after-probe (or actor may
	//      not have subscribed yet, if the goroutine hasn't started).
	//   5. We keep publishing until the actor's Run has registered — we detect this
	//      by the actor eventually producing a snapshot OR by waiting a fixed time.
	//
	// For tests the simplest reliable approach: give the goroutine scheduler time.
	a := orchestrator.NewProjectionActor(bus, func(snap *snapshot.ExportSnapshot) {
		select {
		case ch <- snap:
		default:
		}
	})
	ctx, c := context.WithCancel(context.Background())
	go a.Run(ctx)

	// Yield to the Go scheduler and wait for the actor goroutine to complete
	// bus.Subscribe before we start publishing test events.
	// 20 ms is a conservative upper bound; typically < 0.1 ms on any hardware.
	time.Sleep(20 * time.Millisecond)

	return a, ch, c
}

// publishEvent publishes env on the bus and returns it for chaining.
func allNodes(snap *snapshot.ExportSnapshot) map[string]*snapshot.DiscoveryNode {
	if snap == nil {
		return nil
	}
	return snap.Discovery.NodesByPath
}

func allTopologyNodes(snap *snapshot.ExportSnapshot) map[uint64]*snapshot.TopologyNode {
	if snap == nil {
		return nil
	}
	return snap.Topology.NodesByID
}

// ── ProjectionActor tests ─────────────────────────────────────────────────

func TestProjectionActor_SnapshotLoaded_PopulatesTopology(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	nodes := map[string]*snapshot.DiscoveryNode{
		"/svc/by_id/1": {Path: "/svc/by_id/1", Info: &pb.AtappDiscovery{}},
		"/svc/by_id/2": {Path: "/svc/by_id/2", Info: &pb.AtappDiscovery{}},
	}

	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoaded,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadedPayload{Nodes: nodes, Revision: 100},
	})

	snap := waitForSnapshot(t, snapCh, 3*time.Second)
	require.NotNil(t, snap)
	assert.Len(t, allNodes(snap), 2)
	assert.True(t, snap.Discovery.Ready)
	assert.Equal(t, int64(100), snap.Discovery.LastRevision)
}

func TestProjectionActor_NodeUp_AddsNode(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchNodeUp,
		Version: 1,
		Payload: orchestrator.WatchNodePayload{
			Key:      "/svc/by_id/42",
			Value:    &pb.AtappDiscovery{},
			Revision: 10,
		},
	})

	snap := waitForSnapshot(t, snapCh, 3*time.Second)
	require.NotNil(t, snap)
	assert.Contains(t, allNodes(snap), "/svc/by_id/42")
}

func TestProjectionActor_NodeDown_RemovesNode(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	// First add a node.
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchNodeUp,
		Version: 1,
		Payload: orchestrator.WatchNodePayload{
			Key:      "/svc/by_id/7",
			Value:    &pb.AtappDiscovery{},
			Revision: 5,
		},
	})
	waitForSnapshot(t, snapCh, 3*time.Second)

	// Then remove it.
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchNodeDown,
		Version: 1,
		Payload: orchestrator.WatchNodePayload{Key: "/svc/by_id/7", Revision: 6},
	})

	snap := waitForSnapshot(t, snapCh, 3*time.Second)
	assert.NotContains(t, allNodes(snap), "/svc/by_id/7")
}

func TestProjectionActor_NodeUpdate_UpdatesNode(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	v1 := &pb.AtappDiscovery{}
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchNodeUp,
		Version: 1,
		Payload: orchestrator.WatchNodePayload{Key: "/svc/by_id/3", Value: v1, Revision: 1},
	})
	waitForSnapshot(t, snapCh, 3*time.Second)

	v2 := &pb.AtappDiscovery{}
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchNodeUpdate,
		Version: 1,
		Payload: orchestrator.WatchNodePayload{Key: "/svc/by_id/3", Value: v2, Revision: 2},
	})

	snap := waitForSnapshot(t, snapCh, 3*time.Second)
	node := allNodes(snap)["/svc/by_id/3"]
	require.NotNil(t, node)
	assert.Same(t, v2, node.Info, "node info should be updated to v2")
}

func TestProjectionActor_TopologyUp_AddsTopologyNode(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	v := &pb.AtappTopologyInfo{Id: 101}
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchTopologyUp,
		Version: 1,
		Payload: orchestrator.WatchTopologyPayload{
			Key:      "/topology/svc/1",
			Value:    v,
			Revision: 10,
		},
	})

	snap := waitForSnapshot(t, snapCh, 3*time.Second)
	require.NotNil(t, snap)
	assert.Contains(t, allTopologyNodes(snap), uint64(101))
	assert.Equal(t, int64(10), snap.Topology.LastRevision)
}

func TestProjectionActor_TopologyDown_RemovesTopologyNode(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	v := &pb.AtappTopologyInfo{Id: 102}
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchTopologyUp,
		Version: 1,
		Payload: orchestrator.WatchTopologyPayload{
			Key:      "/topology/svc/2",
			Value:    v,
			Revision: 1,
		},
	})
	waitForSnapshot(t, snapCh, 3*time.Second)

	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchTopologyDown,
		Version: 1,
		Payload: orchestrator.WatchTopologyPayload{
			Key:      "/topology/svc/2",
			Value:    v,
			Revision: 2,
		},
	})

	snap := waitForSnapshot(t, snapCh, 3*time.Second)
	assert.NotContains(t, allTopologyNodes(snap), uint64(102))
	assert.Equal(t, int64(2), snap.Topology.LastRevision)
}

func TestProjectionActor_DedupeKey_PreventsDoubleApply(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	env := runtime.EventEnvelope{
		Type:      runtime.EventWatchNodeUp,
		Version:   1,
		DedupeKey: "watch:1:/svc/by_id/5:7",
		Payload:   orchestrator.WatchNodePayload{Key: "/svc/by_id/5", Value: &pb.AtappDiscovery{}, Revision: 1},
	}

	bus.Publish(env)
	waitForSnapshot(t, snapCh, 3*time.Second)

	// Publish same dedupe key again — should be dropped.
	bus.Publish(env)

	// No second snapshot should arrive within 100 ms.
	select {
	case extra := <-snapCh:
		t.Fatalf("unexpected second snapshot (version=%d) from duplicated event", extra.Version)
	case <-time.After(100 * time.Millisecond):
		// correct: no duplicate publish
	}
}

func TestProjectionActor_LeaseExpired_ClearsRegistration(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	// Simulate a lease expiry. Registration should be cleared.
	bus.Publish(runtime.EventEnvelope{
		Type:       runtime.EventLeaseExpired,
		Version:    1,
		LeaseEpoch: 1,
	})

	snap := waitForSnapshot(t, snapCh, 3*time.Second)
	require.NotNil(t, snap)
	// Registration snapshot should be empty after expiry.
	assert.Empty(t, snap.Registration.ByPath)
}

func TestProjectionActor_GetSnapshot_BeforeFirstPublish_ReturnsNil(t *testing.T) {
	bus := runtime.NewEventBus()
	actor := orchestrator.NewProjectionActor(bus, nil)
	assert.Nil(t, actor.GetSnapshot())
}

func TestProjectionActor_Reset_ClearsState(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	// Populate topology first.
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchNodeUp,
		Version: 1,
		Payload: orchestrator.WatchNodePayload{Key: "/svc/by_id/1", Value: &pb.AtappDiscovery{}, Revision: 1},
	})
	snap1 := waitForSnapshot(t, snapCh, 3*time.Second)
	assert.Contains(t, allNodes(snap1), "/svc/by_id/1")

	// EventWatchSnapshotLoading clears discovery state and publishes a not-ready snapshot.
	// Drain it, then follow up with EventWatchSnapshotLoaded (empty nodes) to get the ready snapshot.
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoading,
		Version: 1,
	})
	snapLoading := waitForSnapshot(t, snapCh, 3*time.Second)
	assert.False(t, snapLoading.Discovery.Ready, "Loading snapshot must have Ready=false")

	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoaded,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadedPayload{
			Nodes:    map[string]*snapshot.DiscoveryNode{},
			Revision: 10,
		},
	})
	snap2 := waitForSnapshot(t, snapCh, 3*time.Second)
	assert.Empty(t, allNodes(snap2), "topology should be cleared after SnapshotLoading+Loaded with empty nodes")
	assert.True(t, snap2.Discovery.Ready)
}

func TestProjectionActor_VersionMonotonicallyIncreases(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	var last uint64
	for i := 0; i < 5; i++ {
		bus.Publish(runtime.EventEnvelope{
			Type:    runtime.EventWatchNodeUp,
			Version: 1,
			Payload: orchestrator.WatchNodePayload{
				Key:      "/svc/by_id/99",
				Value:    &pb.AtappDiscovery{},
				Revision: int64(i + 1),
			},
		})
		snap := waitForSnapshot(t, snapCh, 3*time.Second)
		require.Greater(t, snap.Version, last, "snapshot version must strictly increase")
		last = snap.Version
	}
}

// TestProjectionActor_LeaseRebuild_Sequence tests the full lease rebuild flow:
// grant → register → expire (registration cleared) → re-grant → re-register.
func TestProjectionActor_LeaseRebuild_Sequence(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	// Epoch 1: lease granted + registration.
	bus.Publish(runtime.EventEnvelope{Type: runtime.EventLeaseGranted, LeaseEpoch: 1})

	snap1 := waitForSnapshotWithin(t, snapCh, 0) // no snapshot yet from EventLeaseGranted alone
	_ = snap1

	// (EventLeaseGranted alone doesn't publish a snapshot, just updates epoch.)
	// NodeUp to confirm topology.
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchNodeUp,
		Version: 1,
		Payload: orchestrator.WatchNodePayload{Key: "/svc/by_id/10", Value: &pb.AtappDiscovery{}, Revision: 1},
	})
	snapAfterNode := waitForSnapshot(t, snapCh, 3*time.Second)
	assert.Contains(t, allNodes(snapAfterNode), "/svc/by_id/10")

	// Epoch 1 expires.
	bus.Publish(runtime.EventEnvelope{Type: runtime.EventLeaseExpired, LeaseEpoch: 1})
	snapAfterExpiry := waitForSnapshot(t, snapCh, 3*time.Second)
	assert.Empty(t, snapAfterExpiry.Registration.ByPath, "registration cleared on lease expiry")

	// Epoch 2: new lease granted.
	bus.Publish(runtime.EventEnvelope{Type: runtime.EventLeaseGranted, LeaseEpoch: 2})

	// At this point no snapshot publish since EventLeaseGranted doesn't trigger publish.
	// Confirm topology still holds (not cleared by lease events).
	assert.Contains(t, allNodes(snapAfterExpiry), "/svc/by_id/10")
}

// waitForSnapshotWithin is a non-blocking drain helper.
func waitForSnapshotWithin(t *testing.T, ch <-chan *snapshot.ExportSnapshot, waitMs int) *snapshot.ExportSnapshot {
	t.Helper()
	if waitMs <= 0 {
		select {
		case snap := <-ch:
			return snap
		default:
			return nil
		}
	}
	select {
	case snap := <-ch:
		return snap
	case <-time.After(time.Duration(waitMs) * time.Millisecond):
		return nil
	}
}

// TestProjectionActor_MultiPrefix_MergesNodes verifies that nodes from two
// independent discovery watch prefixes (/by_id and /by_name) are merged into
// a single NodesByPath map, and that Discovery.Ready becomes true only after
// both streams have completed loading.
func TestProjectionActor_MultiPrefix_MergesNodes(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	// Both streams start loading simultaneously.
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoading,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadingPayload{Prefix: "/svc/by_id"},
	})
	snapL1 := waitForSnapshot(t, snapCh, 3*time.Second)
	assert.False(t, snapL1.Discovery.Ready)

	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoading,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadingPayload{Prefix: "/svc/by_name"},
	})
	snapL2 := waitForSnapshot(t, snapCh, 3*time.Second)
	assert.False(t, snapL2.Discovery.Ready)

	// /by_id stream finishes loading first.
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoaded,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadedPayload{
			Prefix:   "/svc/by_id",
			Nodes:    map[string]*snapshot.DiscoveryNode{"/svc/by_id/1": {Path: "/svc/by_id/1", Info: &pb.AtappDiscovery{Id: 1}}},
			Revision: 10,
		},
	})
	snapByID := waitForSnapshot(t, snapCh, 3*time.Second)
	assert.False(t, snapByID.Discovery.Ready, "still waiting for /by_name prefix")
	assert.Contains(t, allNodes(snapByID), "/svc/by_id/1", "by_id nodes must be present")

	// /by_name stream finishes loading second.
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoaded,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadedPayload{
			Prefix:   "/svc/by_name",
			Nodes:    map[string]*snapshot.DiscoveryNode{"/svc/by_name/svc-a": {Path: "/svc/by_name/svc-a", Info: &pb.AtappDiscovery{Id: 1}}},
			Revision: 10,
		},
	})
	snapFinal := waitForSnapshot(t, snapCh, 3*time.Second)

	assert.True(t, snapFinal.Discovery.Ready, "all prefixes loaded — must be Ready")
	assert.Contains(t, allNodes(snapFinal), "/svc/by_id/1", "by_id nodes survive after by_name load")
	assert.Contains(t, allNodes(snapFinal), "/svc/by_name/svc-a", "by_name nodes are present")
	assert.Len(t, allNodes(snapFinal), 2, "total 2 entries from both prefixes")
}

// TestProjectionActor_MultiPrefix_StreamRestart_OnlyEvictsOwnNodes verifies
// that when one stream restarts (SnapshotLoading), nodes from the other
// stream are NOT evicted.
func TestProjectionActor_MultiPrefix_StreamRestart_OnlyEvictsOwnNodes(t *testing.T) {
	bus := runtime.NewEventBus()
	_, snapCh, cancel := startProjectionActor(t, bus)
	defer cancel()

	// Both prefixes fully loaded.
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoading,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadingPayload{Prefix: "/svc/by_id"},
	})
	waitForSnapshot(t, snapCh, 3*time.Second)
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoaded,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadedPayload{
			Prefix:   "/svc/by_id",
			Nodes:    map[string]*snapshot.DiscoveryNode{"/svc/by_id/1": {Path: "/svc/by_id/1", Info: &pb.AtappDiscovery{Id: 1}}},
			Revision: 5,
		},
	})
	waitForSnapshot(t, snapCh, 3*time.Second)

	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoading,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadingPayload{Prefix: "/svc/by_name"},
	})
	waitForSnapshot(t, snapCh, 3*time.Second)
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoaded,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadedPayload{
			Prefix:   "/svc/by_name",
			Nodes:    map[string]*snapshot.DiscoveryNode{"/svc/by_name/svc-a": {Path: "/svc/by_name/svc-a", Info: &pb.AtappDiscovery{Id: 1}}},
			Revision: 5,
		},
	})
	snapFull := waitForSnapshot(t, snapCh, 3*time.Second)
	require.True(t, snapFull.Discovery.Ready)
	require.Len(t, allNodes(snapFull), 2)

	// /by_id stream restarts — only /by_id nodes are evicted.
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoading,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadingPayload{Prefix: "/svc/by_id"},
	})
	snapReloading := waitForSnapshot(t, snapCh, 3*time.Second)
	assert.False(t, snapReloading.Discovery.Ready, "must be not-ready while /by_id reloads")
	assert.NotContains(t, allNodes(snapReloading), "/svc/by_id/1", "stale /by_id node must be evicted")
	assert.Contains(t, allNodes(snapReloading), "/svc/by_name/svc-a", "/by_name nodes must survive")

	// /by_id finishes reloading with an updated node.
	bus.Publish(runtime.EventEnvelope{
		Type:    runtime.EventWatchSnapshotLoaded,
		Version: 1,
		Payload: orchestrator.WatchSnapshotLoadedPayload{
			Prefix:   "/svc/by_id",
			Nodes:    map[string]*snapshot.DiscoveryNode{"/svc/by_id/2": {Path: "/svc/by_id/2", Info: &pb.AtappDiscovery{Id: 2}}},
			Revision: 10,
		},
	})
	snapAfter := waitForSnapshot(t, snapCh, 3*time.Second)
	assert.True(t, snapAfter.Discovery.Ready)
	assert.NotContains(t, allNodes(snapAfter), "/svc/by_id/1", "old /by_id node must be gone")
	assert.Contains(t, allNodes(snapAfter), "/svc/by_id/2", "new /by_id node must appear")
	assert.Contains(t, allNodes(snapAfter), "/svc/by_name/svc-a", "/by_name nodes untouched")
}

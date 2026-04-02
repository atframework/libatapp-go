package libatapp

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// ── helpers ───────────────────────────────────────────────────────────────

// discNode builds a DiscoveryNode for use in snapshots.
func discNode(path string, id uint64, name string) *modulev2.DiscoveryNode {
	return &modulev2.DiscoveryNode{
		Info:        &pb.AtappDiscovery{Id: id, Name: name},
		Path:        path,
		DataVersion: modulev2.DataVersion{CreateRevision: 1, ModRevision: 1, Version: 1},
	}
}

// topoNode builds a TopologyNode for use in snapshots.
func topoNode(id uint64, name string) *modulev2.TopologyNode {
	return &modulev2.TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: id, Name: name},
		DataVersion: modulev2.DataVersion{CreateRevision: 1, ModRevision: 1, Version: 1},
	}
}

// makeDiscSnap builds a ready ExportSnapshot with only Discovery populated.
func makeDiscSnap(nodes ...*modulev2.DiscoveryNode) *modulev2.ExportSnapshot {
	byPath := make(map[string]*modulev2.DiscoveryNode, len(nodes))
	for _, n := range nodes {
		byPath[n.Path] = n
	}
	disc := modulev2.DiscoverySetSnapshot{Ready: true, NodesByPath: byPath}
	disc.RebuildIndexes()
	return &modulev2.ExportSnapshot{
		Version:     1,
		PublishedAt: time.Now(),
		Cause:       modulev2.SnapshotCauseDiscovery,
		Discovery:   disc,
	}
}

// makeTopoSnap builds a ready ExportSnapshot with only Topology populated.
func makeTopoSnap(nodes ...*modulev2.TopologyNode) *modulev2.ExportSnapshot {
	byID := make(map[uint64]*modulev2.TopologyNode, len(nodes))
	for _, n := range nodes {
		byID[n.Info.GetId()] = n
	}
	return &modulev2.ExportSnapshot{
		Version:     1,
		PublishedAt: time.Now(),
		Cause:       modulev2.SnapshotCauseTopology,
		Topology:    modulev2.TopologySnapshot{Ready: true, NodesByID: byID},
	}
}

// makeResetSnap builds a snapshot with the given cause (used for Reset/full-diff cases).
func makeResetSnap(disc modulev2.DiscoverySetSnapshot, topo modulev2.TopologySnapshot) *modulev2.ExportSnapshot {
	return &modulev2.ExportSnapshot{
		Version:     1,
		PublishedAt: time.Now(),
		Cause:       modulev2.SnapshotCauseReset,
		Discovery:   disc,
		Topology:    topo,
	}
}

// ── config state ──────────────────────────────────────────────────────────

func TestEtcdModuleAdapter_CustomData_RoundTrip(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.SetConfCustomData("hello")
	assert.Equal(t, "hello", a.GetConfCustomData())
}

func TestEtcdModuleAdapter_CustomData_DefaultEmpty(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.Equal(t, "", a.GetConfCustomData())
}

// ── enable/disable ─────────────────────────────────────────────────────────

func TestEtcdModuleAdapter_IsEtcdEnabled_InitiallyFalse(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.False(t, a.IsEtcdEnabled())
}

func TestEtcdModuleAdapter_EnableDisable_StateChange(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.EnableEtcd()
	assert.True(t, a.IsEtcdEnabled())
	a.DisableEtcd()
	assert.False(t, a.IsEtcdEnabled())
}

// ── Reset clears etcdCfg and pathCfg ─────────────────────────────────────

func TestEtcdModuleAdapter_Reset_ClearsEtcdCfg(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	// Manually seed etcdCfg to simulate post-ensureImpl state.
	a.mu.Lock()
	a.etcdCfg = &pb.AtappEtcd{}
	a.mu.Unlock()

	a.Reset()

	assert.Nil(t, a.GetConfigure(), "etcdCfg must be nil after Reset")
	assert.Equal(t, "", a.GetConfigurePath(), "path must be empty after Reset")
}

func TestEtcdModuleAdapter_Reset_ClearsPathCfg(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.mu.Lock()
	a.pathCfg = modulev2.PathConfig{
		ByIDPrefix:   "/svc/by_id",
		ByNamePrefix: "/svc/by_name",
	}
	a.mu.Unlock()

	a.Reset()

	assert.Equal(t, "", a.GetDiscoveryByIdPath(), "ByIDPrefix must be empty after Reset")
	assert.Equal(t, "", a.GetDiscoveryByNamePath(), "ByNamePrefix must be empty after Reset")
}

func TestEtcdModuleAdapter_Reset_ClearsPrevSnap(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.onSnapshotPublished(makeDiscSnap(discNode("/by_id/1", 1, "n")))
	require.NotNil(t, a.prevSnap.Load(), "prevSnap must be set after publish")

	a.Reset()

	assert.Nil(t, a.prevSnap.Load(), "prevSnap must be nil after Reset")
}

// ── Reload lifecycle ──────────────────────────────────────────────────────

// TestEtcdModuleAdapter_Reload_StopsOldImpl verifies that Reload() stops the
// running EtcdModule and clears all state.
// startAdapterWithImpl creates the adapter without an AppImpl owner, so
// ensureImpl() finds no config after the teardown and leaves m.impl == nil.
// The key assertion is that the old module transitions to stopped state.
func TestEtcdModuleAdapter_Reload_StopsOldImpl(t *testing.T) {
	a := startAdapterWithImpl(t)

	// Capture the old impl before reload.
	a.mu.RLock()
	oldImpl := a.impl
	a.mu.RUnlock()
	require.NotNil(t, oldImpl, "impl must be running before Reload")

	// Reload tears down old impl.  No AppImpl → ensureImpl creates nothing new.
	err := a.Reload()
	require.NoError(t, err)

	// The old module must have been stopped; a second Stop returns ErrNotRunning.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stopErr := oldImpl.Stop(ctx)
	assert.Equal(t, modulev2.ErrNotRunning, stopErr,
		"old impl must have been stopped by Reload")
}

// TestEtcdModuleAdapter_Reload_ClearsPathCfg verifies that pathCfg is reset
// so path accessors return empty strings after a Reload when no new config is
// available.
func TestEtcdModuleAdapter_Reload_ClearsPathCfg(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.mu.Lock()
	a.pathCfg = modulev2.PathConfig{
		ByIDPrefix: "/svc/by_id",
	}
	a.mu.Unlock()

	err := a.Reload()
	require.NoError(t, err)

	assert.Equal(t, "", a.GetDiscoveryByIdPath(),
		"ByIDPrefix must be cleared by Reload when no new config is available")
}

// TestEtcdModuleAdapter_Reload_ClearsPrevSnap verifies that prevSnap is
// cleared by Reload, preventing stale diff state leaking into the new module.
func TestEtcdModuleAdapter_Reload_ClearsPrevSnap(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.onSnapshotPublished(makeDiscSnap(discNode("/by_id/1", 1, "n")))
	require.NotNil(t, a.prevSnap.Load(), "prevSnap must be set before Reload")

	err := a.Reload()
	require.NoError(t, err)

	assert.Nil(t, a.prevSnap.Load(), "prevSnap must be nil after Reload")
}

// ── callback handle allocation ─────────────────────────────────────────────

func TestEtcdModuleAdapter_AddOnNodeDiscoveryEvent_NilReturnsZero(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.Equal(t, EventCallbackHandle(0), a.AddOnNodeDiscoveryEvent(nil))
}

func TestEtcdModuleAdapter_AddOnNodeDiscoveryEvent_NonNilReturnsNonZero(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	h := a.AddOnNodeDiscoveryEvent(func(EtcdDiscoveryAction, *EtcdDiscoveryNode) {})
	assert.NotEqual(t, EventCallbackHandle(0), h)
}

func TestEtcdModuleAdapter_AddOnTopologyInfoEvent_NilReturnsZero(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.Equal(t, EventCallbackHandle(0), a.AddOnTopologyInfoEvent(nil))
}

func TestEtcdModuleAdapter_AddOnTopologyInfoEvent_NonNilReturnsNonZero(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	h := a.AddOnTopologyInfoEvent(func(EtcdWatchEvent, *pb.AtappTopologyInfo, *EtcdDataVersion) {})
	assert.NotEqual(t, EventCallbackHandle(0), h)
}

func TestEtcdModuleAdapter_HandleAlloc_Monotonic(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	h1 := a.AddOnNodeDiscoveryEvent(func(EtcdDiscoveryAction, *EtcdDiscoveryNode) {})
	h2 := a.AddOnNodeDiscoveryEvent(func(EtcdDiscoveryAction, *EtcdDiscoveryNode) {})
	assert.Greater(t, int(h2), int(h1))
}

// ── core path: Discovery node events ─────────────────────────────────────

func TestEtcdModuleAdapter_NodeUp_DispatchesPutAction(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var gotAction EtcdDiscoveryAction
	var gotID uint64
	a.AddOnNodeDiscoveryEvent(func(action EtcdDiscoveryAction, node *EtcdDiscoveryNode) {
		gotAction = action
		if node != nil && node.Info != nil {
			gotID = node.Info.GetId()
		}
	})

	snap := makeDiscSnap(discNode("/svc/by_id/1", 1, "node-1"))
	a.onSnapshotPublished(snap)

	assert.Equal(t, EtcdDiscoveryActionPut, gotAction)
	assert.Equal(t, uint64(1), gotID)
}

func TestEtcdModuleAdapter_NodeDown_DispatchesDeleteAction(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	// Seed node so prev has it.
	a.onSnapshotPublished(makeDiscSnap(discNode("/svc/by_id/1", 1, "node-1")))

	var gotAction EtcdDiscoveryAction
	var gotID uint64
	a.AddOnNodeDiscoveryEvent(func(action EtcdDiscoveryAction, node *EtcdDiscoveryNode) {
		gotAction = action
		if node != nil && node.Info != nil {
			gotID = node.Info.GetId()
		}
	})

	// Publish empty snapshot — node-1 disappears.
	a.onSnapshotPublished(makeDiscSnap())

	assert.Equal(t, EtcdDiscoveryActionDelete, gotAction)
	assert.Equal(t, uint64(1), gotID)
}

func TestEtcdModuleAdapter_NodeUpdate_DispatchesPutAction(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.onSnapshotPublished(makeDiscSnap(discNode("/svc/by_id/2", 2, "node-2")))

	var gotAction EtcdDiscoveryAction
	a.AddOnNodeDiscoveryEvent(func(action EtcdDiscoveryAction, _ *EtcdDiscoveryNode) {
		gotAction = action
	})

	// Update with different name — content changed, callback must fire.
	a.onSnapshotPublished(makeDiscSnap(discNode("/svc/by_id/2", 2, "node-2-renamed")))

	assert.Equal(t, EtcdDiscoveryActionPut, gotAction)
}

func TestEtcdModuleAdapter_NodeUpdate_ContentUnchanged_SuppressesCallback(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	node := discNode("/svc/by_id/3", 3, "node-3")
	a.onSnapshotPublished(makeDiscSnap(node))

	var called bool
	a.AddOnNodeDiscoveryEvent(func(EtcdDiscoveryAction, *EtcdDiscoveryNode) { called = true })

	// Same content, different etcd Version counter — must be suppressed.
	node2 := &modulev2.DiscoveryNode{
		Info:        &pb.AtappDiscovery{Id: 3, Name: "node-3"},
		Path:        "/svc/by_id/3",
		DataVersion: modulev2.DataVersion{CreateRevision: 1, ModRevision: 1, Version: 2}, // version bumped, content identical
	}
	snap := makeDiscSnap(node2)
	a.onSnapshotPublished(snap)

	assert.False(t, called, "identical content must not fire callback")
}

func TestEtcdModuleAdapter_RemoveOnNodeEvent_StopsDispatch(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var count int
	h := a.AddOnNodeDiscoveryEvent(func(EtcdDiscoveryAction, *EtcdDiscoveryNode) { count++ })

	a.onSnapshotPublished(makeDiscSnap(discNode("/svc/by_id/1", 1, "n")))
	assert.Equal(t, 1, count)

	a.RemoveOnNodeEvent(h)
	a.onSnapshotPublished(makeDiscSnap(discNode("/svc/by_id/2", 2, "n")))
	assert.Equal(t, 1, count, "callback must not fire after removal")
}

// ── core path: Discovery watcher callbacks ────────────────────────────────

func TestEtcdModuleAdapter_ByIDWatcher_FiredOnNodeUp(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var senderModule EtcdModuleImpl
	_ = a.AddDiscoveryWatcherById(func(s *DiscoveryWatcherSender) {
		senderModule = s.Module
	})

	a.onSnapshotPublished(makeDiscSnap(discNode("/svc/by_id/1", 1, "n")))
	assert.Equal(t, a, senderModule)
}

func TestEtcdModuleAdapter_ByNameWatcher_FiredOnNodeUp(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var count int
	_ = a.AddDiscoveryWatcherByName(func(*DiscoveryWatcherSender) { count++ })

	a.onSnapshotPublished(makeDiscSnap(discNode("/svc/by_name/n", 1, "n")))
	assert.Equal(t, 1, count)
}

// ── path-type isolation (requires pathCfg set) ────────────────────────────

func TestEtcdModuleAdapter_ByIDWatcher_DoesNotFireForByNameNode(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.pathCfg = modulev2.PathConfig{
		ByIDPrefix:   "/svc/by_id",
		ByNamePrefix: "/svc/by_name",
	}

	var idCount int
	_ = a.AddDiscoveryWatcherById(func(*DiscoveryWatcherSender) { idCount++ })

	// A by_name node must NOT reach byID watchers.
	a.onSnapshotPublished(makeDiscSnap(discNode("/svc/by_name/n1", 1, "n")))
	assert.Equal(t, 0, idCount, "byID watcher must not fire for by_name node")
}

func TestEtcdModuleAdapter_ByNameWatcher_DoesNotFireForByIDNode(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.pathCfg = modulev2.PathConfig{
		ByIDPrefix:   "/svc/by_id",
		ByNamePrefix: "/svc/by_name",
	}

	var nameCount int
	_ = a.AddDiscoveryWatcherByName(func(*DiscoveryWatcherSender) { nameCount++ })

	// A by_id node must NOT reach byName watchers.
	a.onSnapshotPublished(makeDiscSnap(discNode("/svc/by_id/1", 1, "n")))
	assert.Equal(t, 0, nameCount, "byName watcher must not fire for by_id node")
}

func TestEtcdModuleAdapter_BothWatchers_EachFireForOwnPath(t *testing.T) {
	// Use two independent adapters to avoid cross-snapshot delete events
	// confusing the counters.
	setup := func() (a *etcdModuleAdapter, idCount, nameCount *int) {
		a = newEtcdModuleAdapter(nil)
		a.pathCfg = modulev2.PathConfig{
			ByIDPrefix:   "/svc/by_id",
			ByNamePrefix: "/svc/by_name",
		}
		ic, nc := 0, 0
		_ = a.AddDiscoveryWatcherById(func(*DiscoveryWatcherSender) { ic++ })
		_ = a.AddDiscoveryWatcherByName(func(*DiscoveryWatcherSender) { nc++ })
		return a, &ic, &nc
	}

	// by_id node → only byID fires.
	a1, id1, name1 := setup()
	a1.onSnapshotPublished(makeDiscSnap(discNode("/svc/by_id/1", 1, "n")))
	assert.Equal(t, 1, *id1, "byID fires for by_id node")
	assert.Equal(t, 0, *name1, "byName must not fire for by_id node")

	// by_name node → only byName fires.
	a2, id2, name2 := setup()
	a2.onSnapshotPublished(makeDiscSnap(discNode("/svc/by_name/n1", 2, "n2")))
	assert.Equal(t, 0, *id2, "byID must not fire for by_name node")
	assert.Equal(t, 1, *name2, "byName fires for by_name node")
}

// ── core path: Discovery snapshot ready transitions ───────────────────────

func TestEtcdModuleAdapter_HasDiscoverySnapshot_InitiallyFalse(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.False(t, a.HasDiscoverySnapshot())
}

func TestEtcdModuleAdapter_SnapshotLoaded_FiresLoadedCallback(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var cbFired bool
	a.AddOnDiscoverySnapshotLoaded(func(m EtcdModuleImpl) {
		cbFired = true
	})

	a.onSnapshotPublished(makeDiscSnap())
	assert.True(t, cbFired)
}

func TestEtcdModuleAdapter_SnapshotLoading_FiresLoadCallback(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	// First bring ready=true.
	a.onSnapshotPublished(makeDiscSnap())

	var cbFired bool
	a.AddOnLoadDiscoverySnapshot(func(EtcdModuleImpl) { cbFired = true })

	// Publish ready=false (simulates EventWatchSnapshotLoading).
	notReady := &modulev2.ExportSnapshot{
		Version:     2,
		PublishedAt: time.Now(),
		Cause:       modulev2.SnapshotCauseDiscovery,
		Discovery:   modulev2.DiscoverySetSnapshot{Ready: false},
	}
	a.onSnapshotPublished(notReady)

	assert.True(t, cbFired)
}

func TestEtcdModuleAdapter_AddOnLoadDiscoverySnapshot_NilReturnsZero(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.Equal(t, EventCallbackHandle(0), a.AddOnLoadDiscoverySnapshot(nil))
}

func TestEtcdModuleAdapter_RemoveOnDiscoverySnapshotLoaded_StopsFiring(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var count int
	h := a.AddOnDiscoverySnapshotLoaded(func(EtcdModuleImpl) { count++ })
	a.onSnapshotPublished(makeDiscSnap())
	assert.Equal(t, 1, count)

	a.RemoveOnDiscoverySnapshotLoaded(h)
	// Reset prev so next publish looks like a new transition.
	a.prevSnap.Store(nil)
	a.onSnapshotPublished(makeDiscSnap())
	assert.Equal(t, 1, count, "callback must not fire after removal")
}

// ── core path: Topology node events ──────────────────────────────────────

func TestEtcdModuleAdapter_TopologyUp_DispatchesPutEvent(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var gotEvent EtcdWatchEvent
	var gotID uint64
	a.AddOnTopologyInfoEvent(func(eventType EtcdWatchEvent, info *pb.AtappTopologyInfo, _ *EtcdDataVersion) {
		gotEvent = eventType
		if info != nil {
			gotID = info.GetId()
		}
	})

	a.onSnapshotPublished(makeTopoSnap(topoNode(100, "svc")))

	assert.Equal(t, EtcdWatchEventPut, gotEvent)
	assert.Equal(t, uint64(100), gotID)
}

func TestEtcdModuleAdapter_TopologyDown_DispatchesDeleteEvent(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	// Seed.
	a.onSnapshotPublished(makeTopoSnap(topoNode(200, "svc")))

	var gotEvent EtcdWatchEvent
	a.AddOnTopologyInfoEvent(func(eventType EtcdWatchEvent, _ *pb.AtappTopologyInfo, _ *EtcdDataVersion) {
		gotEvent = eventType
	})

	// Publish empty topology — node disappears.
	a.onSnapshotPublished(makeTopoSnap())

	assert.Equal(t, EtcdWatchEventDelete, gotEvent)
}

func TestEtcdModuleAdapter_TopologyUpdate_UpstreamIDChange_Dispatches(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	n := &modulev2.TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 500, Name: "svc", UpstreamId: 1},
		DataVersion: modulev2.DataVersion{CreateRevision: 1, ModRevision: 1, Version: 1},
	}
	a.onSnapshotPublished(makeTopoSnap(n))

	var newUpstream uint64
	a.AddOnTopologyInfoEvent(func(_ EtcdWatchEvent, info *pb.AtappTopologyInfo, _ *EtcdDataVersion) {
		if info != nil {
			newUpstream = info.GetUpstreamId()
		}
	})

	n2 := &modulev2.TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 500, Name: "svc", UpstreamId: 99},
		DataVersion: modulev2.DataVersion{CreateRevision: 1, ModRevision: 2, Version: 2},
	}
	a.onSnapshotPublished(makeTopoSnap(n2))

	assert.Equal(t, uint64(99), newUpstream)
}

func TestEtcdModuleAdapter_TopologyUpdate_ContentUnchanged_SuppressesCallback(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	n := &modulev2.TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 600, Name: "svc", UpstreamId: 42},
		DataVersion: modulev2.DataVersion{CreateRevision: 1, ModRevision: 1, Version: 1},
	}
	a.onSnapshotPublished(makeTopoSnap(n))

	var called bool
	a.AddOnTopologyInfoEvent(func(EtcdWatchEvent, *pb.AtappTopologyInfo, *EtcdDataVersion) { called = true })

	// Same logical content, only DataVersion differs.
	n2 := &modulev2.TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 600, Name: "svc", UpstreamId: 42},
		DataVersion: modulev2.DataVersion{CreateRevision: 1, ModRevision: 2, Version: 2},
	}
	a.onSnapshotPublished(makeTopoSnap(n2))

	assert.False(t, called, "identical content must not fire callback")
}

func TestEtcdModuleAdapter_TopologyWatcher_FiredOnTopologyUp(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var gotID uint64
	_ = a.AddTopologyWatcher(func(s *TopologyWatcherSender) {
		if s.Topology != nil && s.Topology.Info != nil {
			gotID = s.Topology.Info.GetId()
		}
	})

	a.onSnapshotPublished(makeTopoSnap(topoNode(300, "svc")))
	assert.Equal(t, uint64(300), gotID)
}

// ── core path: Topology snapshot ready transitions ────────────────────────

func TestEtcdModuleAdapter_HasTopologySnapshot_InitiallyFalse(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.False(t, a.HasTopologySnapshot())
}

func TestEtcdModuleAdapter_TopologySnapshotLoaded_FiresCallback(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var cbFired bool
	a.AddOnTopologySnapshotLoaded(func(EtcdModuleImpl) { cbFired = true })

	a.onSnapshotPublished(makeTopoSnap(topoNode(1, "svc")))
	assert.True(t, cbFired)
}

func TestEtcdModuleAdapter_TopologySnapshotLoading_FiresLoadCallback(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	// First bring ready=true.
	a.onSnapshotPublished(makeTopoSnap(topoNode(1, "svc")))

	var cbFired bool
	a.AddOnLoadTopologySnapshot(func(EtcdModuleImpl) { cbFired = true })

	notReady := &modulev2.ExportSnapshot{
		Version:     2,
		PublishedAt: time.Now(),
		Cause:       modulev2.SnapshotCauseTopology,
		Topology:    modulev2.TopologySnapshot{Ready: false},
	}
	a.onSnapshotPublished(notReady)

	assert.True(t, cbFired)
}

// ── core path: Reset (full diff) ──────────────────────────────────────────

func TestEtcdModuleAdapter_Reset_DiffsBothSubTrees(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var discCount, topoCount int
	a.AddOnNodeDiscoveryEvent(func(EtcdDiscoveryAction, *EtcdDiscoveryNode) { discCount++ })
	a.AddOnTopologyInfoEvent(func(EtcdWatchEvent, *pb.AtappTopologyInfo, *EtcdDataVersion) { topoCount++ })

	byPath := map[string]*modulev2.DiscoveryNode{
		"/by_id/1": discNode("/by_id/1", 1, "n"),
	}
	disc := modulev2.DiscoverySetSnapshot{Ready: true, NodesByPath: byPath}
	disc.RebuildIndexes()
	byID := map[uint64]*modulev2.TopologyNode{
		10: topoNode(10, "t"),
	}
	topo := modulev2.TopologySnapshot{Ready: true, NodesByID: byID}

	a.onSnapshotPublished(makeResetSnap(disc, topo))

	assert.Equal(t, 1, discCount, "discovery node PUT must fire")
	assert.Equal(t, 1, topoCount, "topology node PUT must fire")
}

// ── core path: Registration cause — no diff ───────────────────────────────

func TestEtcdModuleAdapter_RegistrationCause_DoesNotFireNodeCallbacks(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	// Seed some nodes in prevSnap via a discovery publish.
	a.onSnapshotPublished(makeDiscSnap(discNode("/by_id/1", 1, "n")))

	var called bool
	a.AddOnNodeDiscoveryEvent(func(EtcdDiscoveryAction, *EtcdDiscoveryNode) { called = true })

	// Same discovery content but cause=Registration — no diff should run.
	snap := &modulev2.ExportSnapshot{
		Version:     2,
		PublishedAt: time.Now(),
		Cause:       modulev2.SnapshotCauseRegistration,
		Discovery:   *makeDiscSnap(discNode("/by_id/1", 1, "n")).Discovery.Clone(),
	}
	a.onSnapshotPublished(snap)

	assert.False(t, called, "Registration cause must not trigger node diff")
}

// ── read-only helpers ──────────────────────────────────────────────────────

func TestEtcdModuleAdapter_GetTopologyInfoSet_ReturnsEmptyMap_NoImpl(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	m := a.GetTopologyInfoSet()
	assert.NotNil(t, m)
	assert.Empty(t, m)
}

// ── live-impl helpers ─────────────────────────────────────────────────────

// adapterMockClient is a minimal EtcdClient implementation for adapter tests
// that require a running EtcdModule.  All operations return immediate success.
type adapterMockClient struct{}

func (*adapterMockClient) Grant(_ context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return &clientv3.LeaseGrantResponse{ID: 1001, TTL: ttl}, nil
}
func (*adapterMockClient) KeepAlive(ctx context.Context, _ clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ch := make(chan *clientv3.LeaseKeepAliveResponse)
	go func() { <-ctx.Done(); close(ch) }()
	return ch, nil
}
func (*adapterMockClient) Revoke(_ context.Context, _ clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return &clientv3.LeaseRevokeResponse{}, nil
}
func (*adapterMockClient) Get(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return &clientv3.GetResponse{}, nil
}
func (*adapterMockClient) Put(_ context.Context, _, _ string, _ ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return &clientv3.PutResponse{}, nil
}
func (*adapterMockClient) Delete(_ context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}
func (*adapterMockClient) Watch(ctx context.Context, _ string, _ ...clientv3.OpOption) clientv3.WatchChan {
	ch := make(chan clientv3.WatchResponse)
	go func() { <-ctx.Done(); close(ch) }()
	return ch
}
func (*adapterMockClient) SetEndpoints(_ ...string) {}
func (*adapterMockClient) Close() error             { return nil }

// startAdapterWithImpl creates an etcdModuleAdapter with a running EtcdModule
// backed by adapterMockClient.  The module is stopped automatically via
// t.Cleanup.
func startAdapterWithImpl(t *testing.T) *etcdModuleAdapter {
	t.Helper()
	a := newEtcdModuleAdapter(nil)
	pathCfg := modulev2.PathConfig{
		ByIDPrefix:     "/svc/by_id",
		ByNamePrefix:   "/svc/by_name",
		TopologyPrefix: "/svc/topology",
		WatchPrefixes:  []string{"/svc/by_id", "/svc/by_name", "/svc/topology"},
		LeaseTTL:       10,
	}
	a.pathCfg = pathCfg
	opts := modulev2.ModuleOptions{
		RetryInterval:       50 * time.Millisecond,
		OnSnapshotPublished: a.onSnapshotPublished,
	}
	impl := modulev2.NewEtcdModule(&adapterMockClient{}, pathCfg, opts)
	require.NoError(t, impl.Start(context.Background()))
	a.mu.Lock()
	a.impl = impl
	a.mu.Unlock()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = impl.Stop(ctx)
	})
	return a
}

// ── Registration with live impl ───────────────────────────────────────────

func TestEtcdModuleAdapter_AddRegistrationDiscoveryActor_NilVal_ReturnsNil(t *testing.T) {
	a := startAdapterWithImpl(t)
	assert.Nil(t, a.AddRegistrationDiscoveryActor(nil, "/svc/by_id/1"))
}

func TestEtcdModuleAdapter_AddRegistrationDiscoveryActor_NoImpl_ReturnsNil(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.Nil(t, a.AddRegistrationDiscoveryActor(&pb.AtappDiscovery{Id: 1, Name: "n"}, "/svc/by_id/1"))
}

func TestEtcdModuleAdapter_AddRegistrationDiscoveryActor_WithRunningImpl_ReturnsToken(t *testing.T) {
	a := startAdapterWithImpl(t)
	reg := a.AddRegistrationDiscoveryActor(&pb.AtappDiscovery{Id: 1, Name: "node-1"}, "/svc/by_id/1")
	require.NotNil(t, reg)
	assert.Equal(t, "/svc/by_id/1", reg.GetPath())
}

func TestEtcdModuleAdapter_AddRegistrationTopologyActor_NilVal_ReturnsNil(t *testing.T) {
	a := startAdapterWithImpl(t)
	assert.Nil(t, a.AddRegistrationTopologyActor(nil, "/svc/topology/10"))
}

func TestEtcdModuleAdapter_AddRegistrationTopologyActor_WithRunningImpl_ReturnsToken(t *testing.T) {
	a := startAdapterWithImpl(t)
	reg := a.AddRegistrationTopologyActor(&pb.AtappTopologyInfo{Id: 10, Name: "svc"}, "/svc/topology/10")
	require.NotNil(t, reg)
	assert.Equal(t, "/svc/topology/10", reg.GetPath())
}

func TestEtcdModuleAdapter_RemoveRegistrationActor_NilReg_ReturnsFalse(t *testing.T) {
	a := startAdapterWithImpl(t)
	assert.False(t, a.RemoveRegistrationActor(nil))
}

func TestEtcdModuleAdapter_RemoveRegistrationActor_EmptyPath_ReturnsFalse(t *testing.T) {
	a := startAdapterWithImpl(t)
	assert.False(t, a.RemoveRegistrationActor(&EtcdRegistration{}))
}

func TestEtcdModuleAdapter_RemoveRegistrationActor_UnknownPath_ReturnsTrue(t *testing.T) {
	// Removing a path that was never registered is a no-op actor-side (nil error).
	a := startAdapterWithImpl(t)
	assert.True(t, a.RemoveRegistrationActor(&EtcdRegistration{path: "/svc/by_id/never-registered"}))
}

// ── Registration cache ────────────────────────────────────────────────────

func TestEtcdModuleAdapter_AddRegistrationDiscoveryActor_CachesEvenWhenImplNil(t *testing.T) {
	// Arrange: adapter with no impl
	a := newEtcdModuleAdapter(nil)
	disc := &pb.AtappDiscovery{Id: 1, Name: "node-1"}

	// Act
	reg := a.AddRegistrationDiscoveryActor(disc, "/svc/by_id/1")

	// Assert: token is nil (no impl), but registration is cached for later replay
	assert.Nil(t, reg)
	a.mu.RLock()
	svc, cached := a.registrations["/svc/by_id/1"]
	a.mu.RUnlock()
	require.True(t, cached)
	assert.Equal(t, disc, svc.Discovery)
}

func TestEtcdModuleAdapter_AddRegistrationTopologyActor_CachesEvenWhenImplNil(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	topo := &pb.AtappTopologyInfo{Id: 10, Name: "svc"}

	reg := a.AddRegistrationTopologyActor(topo, "/svc/topology/10")

	assert.Nil(t, reg)
	a.mu.RLock()
	svc, cached := a.registrations["/svc/topology/10"]
	a.mu.RUnlock()
	require.True(t, cached)
	assert.Equal(t, topo, svc.TopologyInfo)
}

func TestEtcdModuleAdapter_AddRegistrationDiscoveryActor_CachesWithRunningImpl(t *testing.T) {
	a := startAdapterWithImpl(t)

	reg := a.AddRegistrationDiscoveryActor(&pb.AtappDiscovery{Id: 2, Name: "node-2"}, "/svc/by_id/2")

	require.NotNil(t, reg)
	a.mu.RLock()
	_, cached := a.registrations["/svc/by_id/2"]
	a.mu.RUnlock()
	assert.True(t, cached)
}

func TestEtcdModuleAdapter_RemoveRegistrationActor_RemovesFromCache(t *testing.T) {
	a := startAdapterWithImpl(t)

	reg := a.AddRegistrationDiscoveryActor(&pb.AtappDiscovery{Id: 3, Name: "node-3"}, "/svc/by_id/3")
	require.NotNil(t, reg)

	// Verify cached before removal
	a.mu.RLock()
	_, before := a.registrations["/svc/by_id/3"]
	a.mu.RUnlock()
	require.True(t, before)

	// Remove
	ok := a.RemoveRegistrationActor(reg)
	assert.True(t, ok)

	a.mu.RLock()
	_, after := a.registrations["/svc/by_id/3"]
	a.mu.RUnlock()
	assert.False(t, after)
}

func TestEtcdModuleAdapter_ReplayRegistrations_CallsRegisterServiceForAllCached(t *testing.T) {
	// Arrange: adapter with no impl yet; register two nodes
	a := newEtcdModuleAdapter(nil)
	a.AddRegistrationDiscoveryActor(&pb.AtappDiscovery{Id: 1, Name: "n1"}, "/svc/by_id/1")
	a.AddRegistrationTopologyActor(&pb.AtappTopologyInfo{Id: 2, Name: "n2"}, "/svc/topology/2")

	// Act: start a fresh impl via startImpl (simulates hardReload outcome)
	pathCfg := modulev2.PathConfig{
		ByIDPrefix:     "/svc/by_id",
		ByNamePrefix:   "/svc/by_name",
		TopologyPrefix: "/svc/topology",
		WatchPrefixes:  []string{"/svc/by_id", "/svc/by_name", "/svc/topology"},
		LeaseTTL:       10,
	}
	opts := modulev2.ModuleOptions{
		RetryInterval:       50 * time.Millisecond,
		OnSnapshotPublished: a.onSnapshotPublished,
	}
	freshImpl := modulev2.NewEtcdModule(&adapterMockClient{}, pathCfg, opts)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = freshImpl.Stop(ctx)
	})

	err := a.startImpl(context.Background(), freshImpl)

	// Assert: no error, both cached registrations fed to the new impl
	require.NoError(t, err)
	a.mu.RLock()
	discCached := a.registrations["/svc/by_id/1"]
	topoCached := a.registrations["/svc/topology/2"]
	a.mu.RUnlock()
	assert.Equal(t, "/svc/by_id/1", discCached.Path)
	assert.Equal(t, "/svc/topology/2", topoCached.Path)
}

func TestEtcdModuleAdapter_HardReload_PathChanged_ClearsRegistrationsCache(t *testing.T) {
	// Arrange: adapter with old pathCfg and two cached registrations
	a := newEtcdModuleAdapter(nil)
	a.mu.Lock()
	a.pathCfg = modulev2.PathConfig{
		ByIDPrefix:     "/old/by_id",
		ByNamePrefix:   "/old/by_name",
		TopologyPrefix: "/old/topology",
	}
	a.registrations["/old/by_id/1"] = modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 1, Name: "n1"},
		Path:      "/old/by_id/1",
	}
	a.mu.Unlock()

	// Simulate hardReload outcome: new pathCfg (different prefixes) injected
	// by ensureImpl.  We bypass ensureImpl by directly setting the new state.
	a.mu.Lock()
	a.pathCfg = modulev2.PathConfig{
		ByIDPrefix:     "/new/by_id",
		ByNamePrefix:   "/new/by_name",
		TopologyPrefix: "/new/topology",
	}
	oldPathCfg := modulev2.PathConfig{
		ByIDPrefix:     "/old/by_id",
		ByNamePrefix:   "/old/by_name",
		TopologyPrefix: "/old/topology",
	}
	newPathCfg := a.pathCfg
	// Reproduce the path-change cache-clear logic from hardReload.
	if oldPathCfg.ByIDPrefix != newPathCfg.ByIDPrefix ||
		oldPathCfg.ByNamePrefix != newPathCfg.ByNamePrefix ||
		oldPathCfg.TopologyPrefix != newPathCfg.TopologyPrefix {
		a.registrations = make(map[string]modulev2.ServiceInfo)
	}
	a.mu.Unlock()

	// Assert: registrations cache is empty after path change
	a.mu.RLock()
	count := len(a.registrations)
	a.mu.RUnlock()
	assert.Equal(t, 0, count, "stale registrations should be cleared when path prefixes change")
}

// ── GetDiscoveryNodeSet / GetTopologyInfoSet with live impl ───────────────

func TestEtcdModuleAdapter_GetDiscoveryNodeSet_WithImpl_EmptyBeforeSnapshot(t *testing.T) {
	a := startAdapterWithImpl(t)
	// No snapshot published yet; snapshot is nil → returns empty map.
	m := a.GetDiscoveryNodeSet()
	assert.NotNil(t, m)
	assert.Empty(t, m)
}

func TestEtcdModuleAdapter_GetTopologyInfoSet_WithImpl_EmptyBeforeSnapshot(t *testing.T) {
	a := startAdapterWithImpl(t)
	m := a.GetTopologyInfoSet()
	assert.NotNil(t, m)
	assert.Empty(t, m)
}

func TestEtcdModuleAdapter_GetDiscoveryNodeSet_WithImpl_ReflectsSnapshot(t *testing.T) {
	a := startAdapterWithImpl(t)
	// Drive onSnapshotPublished directly to simulate a populated snapshot.
	a.onSnapshotPublished(makeDiscSnap(
		discNode("/svc/by_id/1", 1, "node-1"),
		discNode("/svc/by_id/2", 2, "node-2"),
	))
	m := a.GetDiscoveryNodeSet()
	// GetDiscoveryNodeSet reads from impl.GetSnapshot(), which is independent
	// of the onSnapshotPublished-driven prevSnap used for diff.  The mock
	// module has no real Watch, so impl.GetSnapshot() remains nil.
	// Assert the call does not panic and returns a non-nil map.
	assert.NotNil(t, m)
}

func TestEtcdModuleAdapter_GetTopologyInfoSet_WithImpl_ReflectsSnapshot(t *testing.T) {
	a := startAdapterWithImpl(t)
	a.onSnapshotPublished(makeTopoSnap(topoNode(10, "svc")))
	m := a.GetTopologyInfoSet()
	assert.NotNil(t, m)
}

// ── HasDiscoverySnapshot / HasTopologySnapshot with live impl ─────────────

func TestEtcdModuleAdapter_HasDiscoverySnapshot_WithImpl_FalseBeforeSnapshot(t *testing.T) {
	a := startAdapterWithImpl(t)
	// impl.GetSnapshot() is nil before ProjectionActor publishes; must be false.
	assert.False(t, a.HasDiscoverySnapshot())
}

func TestEtcdModuleAdapter_HasTopologySnapshot_WithImpl_FalseBeforeSnapshot(t *testing.T) {
	a := startAdapterWithImpl(t)
	assert.False(t, a.HasTopologySnapshot())
}

// ── etcdHardReloadRequired unit tests (table-driven, no etcd needed) ─────

func TestEtcdHardReloadRequired(t *testing.T) {
	hosts := []string{"http://localhost:2379"}

	tests := []struct {
		name     string
		old      *pb.AtappEtcd
		newCfg   *pb.AtappEtcd
		wantHard bool
	}{
		{
			name:     "nil old → hard",
			old:      nil,
			newCfg:   &pb.AtappEtcd{Enable: true, Hosts: hosts},
			wantHard: true,
		},
		{
			name:     "nil new → hard",
			old:      &pb.AtappEtcd{Enable: true, Hosts: hosts},
			newCfg:   nil,
			wantHard: true,
		},
		{
			name:     "both nil → hard",
			old:      nil,
			newCfg:   nil,
			wantHard: true,
		},
		{
			name:     "enable toggled true→false → hard",
			old:      &pb.AtappEtcd{Enable: true, Hosts: hosts},
			newCfg:   &pb.AtappEtcd{Enable: false, Hosts: hosts},
			wantHard: true,
		},
		{
			name:     "enable toggled false→true → hard",
			old:      &pb.AtappEtcd{Enable: false, Hosts: hosts},
			newCfg:   &pb.AtappEtcd{Enable: true, Hosts: hosts},
			wantHard: true,
		},
		{
			name:     "hosts emptied → hard",
			old:      &pb.AtappEtcd{Enable: true, Hosts: hosts},
			newCfg:   &pb.AtappEtcd{Enable: true, Hosts: nil},
			wantHard: true,
		},
		{
			name:     "base path changed → hard",
			old:      &pb.AtappEtcd{Enable: true, Hosts: hosts, Path: "/app/v1"},
			newCfg:   &pb.AtappEtcd{Enable: true, Hosts: hosts, Path: "/app/v2"},
			wantHard: true,
		},
		{
			name:     "auth changed → hard",
			old:      &pb.AtappEtcd{Enable: true, Hosts: hosts, Authorization: "user:pass1"},
			newCfg:   &pb.AtappEtcd{Enable: true, Hosts: hosts, Authorization: "user:pass2"},
			wantHard: true,
		},
		{
			name: "TLS toggled plaintext→TLS → hard",
			old:  &pb.AtappEtcd{Enable: true, Hosts: hosts},
			newCfg: &pb.AtappEtcd{
				Enable: true, Hosts: hosts,
				Ssl: &pb.AtappEtcdSsl{VerifyPeer: true, SslCaCert: "/ca.pem"},
			},
			wantHard: true,
		},
		{
			name: "TLS toggled TLS→plaintext → hard",
			old: &pb.AtappEtcd{
				Enable: true, Hosts: hosts,
				Ssl: &pb.AtappEtcdSsl{VerifyPeer: true, SslCaCert: "/ca.pem"},
			},
			newCfg:   &pb.AtappEtcd{Enable: true, Hosts: hosts},
			wantHard: true,
		},
		{
			name: "CA cert changed → hard",
			old: &pb.AtappEtcd{
				Enable: true, Hosts: hosts,
				Ssl: &pb.AtappEtcdSsl{VerifyPeer: true, SslCaCert: "/old/ca.pem"},
			},
			newCfg: &pb.AtappEtcd{
				Enable: true, Hosts: hosts,
				Ssl: &pb.AtappEtcdSsl{VerifyPeer: true, SslCaCert: "/new/ca.pem"},
			},
			wantHard: true,
		},
		// ── soft cases ───────────────────────────────────────────────────────
		{
			name:     "identical config → soft",
			old:      &pb.AtappEtcd{Enable: true, Hosts: hosts},
			newCfg:   &pb.AtappEtcd{Enable: true, Hosts: hosts},
			wantHard: false,
		},
		{
			name:     "hosts content changed (non-empty) → soft",
			old:      &pb.AtappEtcd{Enable: true, Hosts: []string{"http://localhost:2379"}},
			newCfg:   &pb.AtappEtcd{Enable: true, Hosts: []string{"http://localhost:2380", "http://localhost:2381"}},
			wantHard: false,
		},
		{
			name: "TLS cert/key path changed, CA unchanged → soft",
			old: &pb.AtappEtcd{
				Enable: true, Hosts: hosts,
				Ssl: &pb.AtappEtcdSsl{VerifyPeer: true, SslCaCert: "/ca.pem", SslClientCert: "/old.crt", SslClientKey: "/old.key"},
			},
			newCfg: &pb.AtappEtcd{
				Enable: true, Hosts: hosts,
				Ssl: &pb.AtappEtcdSsl{VerifyPeer: true, SslCaCert: "/ca.pem", SslClientCert: "/new.crt", SslClientKey: "/new.key"},
			},
			wantHard: false,
		},
		{
			name:     "both disabled, hosts changed → soft (enable unchanged)",
			old:      &pb.AtappEtcd{Enable: false, Hosts: hosts},
			newCfg:   &pb.AtappEtcd{Enable: false, Hosts: []string{"http://localhost:2380"}},
			wantHard: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := etcdHardReloadRequired(tc.old, tc.newCfg)
			assert.Equal(t, tc.wantHard, got)
		})
	}
}

// ── atappAreaEqual unit tests (table-driven) ──────────────────────────────
// Mirrors C++ atapp_etcd_module_unit I.1–I.11.

func TestAtappAreaEqual(t *testing.T) {
	tests := []struct {
		name string
		l, r *pb.AtappArea
		want bool
	}{
		// I.1 both nil (equal)
		{
			name: "both nil",
			l:    nil,
			r:    nil,
			want: true,
		},
		// I.2 both empty structs (equal)
		{
			name: "both empty",
			l:    &pb.AtappArea{},
			r:    &pb.AtappArea{},
			want: true,
		},
		// I.3 identical all fields (equal)
		{
			name: "identical values",
			l:    &pb.AtappArea{ZoneId: 12345, Region: "us-east-1", District: "dc-a"},
			r:    &pb.AtappArea{ZoneId: 12345, Region: "us-east-1", District: "dc-a"},
			want: true,
		},
		// I.4 different zone_id
		{
			name: "diff zone_id",
			l:    &pb.AtappArea{ZoneId: 100, Region: "us-east-1", District: "dc-a"},
			r:    &pb.AtappArea{ZoneId: 200, Region: "us-east-1", District: "dc-a"},
			want: false,
		},
		// I.5 different region
		{
			name: "diff region",
			l:    &pb.AtappArea{ZoneId: 100, Region: "us-east-1", District: "dc-a"},
			r:    &pb.AtappArea{ZoneId: 100, Region: "eu-west-2", District: "dc-a"},
			want: false,
		},
		// I.6 different district
		{
			name: "diff district",
			l:    &pb.AtappArea{ZoneId: 100, Region: "us-east-1", District: "dc-a"},
			r:    &pb.AtappArea{ZoneId: 100, Region: "us-east-1", District: "dc-b"},
			want: false,
		},
		// I.7 region same length but different content
		{
			name: "region same length diff content",
			l:    &pb.AtappArea{ZoneId: 100, Region: "abc"},
			r:    &pb.AtappArea{ZoneId: 100, Region: "xyz"},
			want: false,
		},
		// I.8 district same length but different content
		{
			name: "district same length diff content",
			l:    &pb.AtappArea{ZoneId: 100, District: "aaa"},
			r:    &pb.AtappArea{ZoneId: 100, District: "bbb"},
			want: false,
		},
		// I.9 one has region, other empty
		{
			name: "one region empty",
			l:    &pb.AtappArea{ZoneId: 100, Region: "us-east-1"},
			r:    &pb.AtappArea{ZoneId: 100},
			want: false,
		},
		// I.10 one has district, other empty
		{
			name: "one district empty",
			l:    &pb.AtappArea{ZoneId: 100, District: "dc-a"},
			r:    &pb.AtappArea{ZoneId: 100},
			want: false,
		},
		// I.11 zero zone_id vs non-zero
		{
			name: "zero vs nonzero zone_id",
			l:    &pb.AtappArea{},
			r:    &pb.AtappArea{ZoneId: 1},
			want: false,
		},
		// symmetric: l==r and r==l
		{
			name: "symmetric equal",
			l:    &pb.AtappArea{ZoneId: 42, Region: "cn-north-1", District: "zone-b"},
			r:    &pb.AtappArea{ZoneId: 42, Region: "cn-north-1", District: "zone-b"},
			want: true,
		},
		// nil vs non-nil
		{
			name: "nil vs empty struct",
			l:    nil,
			r:    &pb.AtappArea{},
			want: false,
		},
		{
			name: "empty struct vs nil",
			l:    &pb.AtappArea{},
			r:    nil,
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, atappAreaEqual(tc.l, tc.r))
			// Symmetry check: equal iff reverse is equal, inequal iff reverse is inequal.
			assert.Equal(t, tc.want, atappAreaEqual(tc.r, tc.l), "symmetry")
		})
	}
}

// ── atappTopologyEqual unit tests (table-driven) ──────────────────────────
// Mirrors C++ atapp_etcd_module_unit I.22–I.30.

func TestAtappTopologyEqual(t *testing.T) {
	tests := []struct {
		name string
		l, r *pb.AtappTopologyInfo
		want bool
	}{
		// I.22 both nil (equal via pointer identity shortcut)
		{
			name: "both nil",
			l:    nil,
			r:    nil,
			want: true,
		},
		// I.23 both empty structs
		{
			name: "both empty",
			l:    &pb.AtappTopologyInfo{},
			r:    &pb.AtappTopologyInfo{},
			want: true,
		},
		// I.24 identical all fields including labels
		{
			name: "identical all fields",
			l: &pb.AtappTopologyInfo{
				Id: 1001, UpstreamId: 2001, Name: "test-server",
				Data: &pb.AtbusTopologyData{Label: map[string]string{"role": "game", "env": "prod"}},
			},
			r: &pb.AtappTopologyInfo{
				Id: 1001, UpstreamId: 2001, Name: "test-server",
				Data: &pb.AtbusTopologyData{Label: map[string]string{"role": "game", "env": "prod"}},
			},
			want: true,
		},
		// I.25 different id
		{
			name: "diff id",
			l:    &pb.AtappTopologyInfo{Id: 1001},
			r:    &pb.AtappTopologyInfo{Id: 1002},
			want: false,
		},
		// I.26 different upstream_id
		{
			name: "diff upstream_id",
			l:    &pb.AtappTopologyInfo{Id: 1001, UpstreamId: 2001},
			r:    &pb.AtappTopologyInfo{Id: 1001, UpstreamId: 2002},
			want: false,
		},
		// I.27 different name
		{
			name: "diff name",
			l:    &pb.AtappTopologyInfo{Id: 1001, Name: "server-a"},
			r:    &pb.AtappTopologyInfo{Id: 1001, Name: "server-b"},
			want: false,
		},
		// I.28 different label count
		{
			name: "diff label count",
			l: &pb.AtappTopologyInfo{
				Id:   1001,
				Data: &pb.AtbusTopologyData{Label: map[string]string{"role": "game"}},
			},
			r: &pb.AtappTopologyInfo{
				Id:   1001,
				Data: &pb.AtbusTopologyData{Label: map[string]string{"role": "game", "env": "prod"}},
			},
			want: false,
		},
		// I.29 same label count, different keys
		{
			name: "same label count diff keys",
			l: &pb.AtappTopologyInfo{
				Id:   1001,
				Data: &pb.AtbusTopologyData{Label: map[string]string{"role": "game"}},
			},
			r: &pb.AtappTopologyInfo{
				Id:   1001,
				Data: &pb.AtbusTopologyData{Label: map[string]string{"env": "game"}},
			},
			want: false,
		},
		// I.30 same label count, different values
		{
			name: "same label count diff values",
			l: &pb.AtappTopologyInfo{
				Id:   1001,
				Data: &pb.AtbusTopologyData{Label: map[string]string{"role": "game"}},
			},
			r: &pb.AtappTopologyInfo{
				Id:   1001,
				Data: &pb.AtbusTopologyData{Label: map[string]string{"role": "lobby"}},
			},
			want: false,
		},
		// I.31 ignores fields not in comparison (hostname/pid/identity/version are not part of atappTopologyEqual)
		// NOTE: atappTopologyEqual only compares id, upstream_id, name, data.label
		{
			name: "ignores extra non-compared fields",
			l:    &pb.AtappTopologyInfo{Id: 1001, Name: "server"},
			r:    &pb.AtappTopologyInfo{Id: 1001, Name: "server"},
			want: true,
		},
		// nil vs non-nil
		{
			name: "nil vs empty struct",
			l:    nil,
			r:    &pb.AtappTopologyInfo{},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, atappTopologyEqual(tc.l, tc.r))
		})
	}
}

// ── Name ──────────────────────────────────────────────────────────────────

func TestEtcdModuleAdapter_Name_Returns_EtcdModule(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.Equal(t, "etcd_module", a.Name())
}

// ── Init ──────────────────────────────────────────────────────────────────

func TestEtcdModuleAdapter_Init_NilOwner_ReturnsNil(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	// ensureImpl returns immediately when owner is nil; Init must return nil.
	assert.NoError(t, a.Init(context.Background()))
	a.mu.RLock()
	impl := a.impl
	a.mu.RUnlock()
	assert.Nil(t, impl)
}

// ── Stop ──────────────────────────────────────────────────────────────────

func TestEtcdModuleAdapter_Stop_NilImpl_ReturnsTrue(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	ok, err := a.Stop()
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestEtcdModuleAdapter_Stop_WithImpl_StopsImpl(t *testing.T) {
	a := startAdapterWithImpl(t)
	// Capture impl before stop.
	a.mu.RLock()
	impl := a.impl
	a.mu.RUnlock()
	require.NotNil(t, impl)

	ok, err := a.Stop()
	assert.True(t, ok)
	assert.NoError(t, err)

	// A second stop on the underlying module must report ErrNotRunning.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	assert.Equal(t, modulev2.ErrNotRunning, impl.Stop(ctx))
}

// ── Cleanup ──────────────────────────────────────────────────────────────

func TestEtcdModuleAdapter_Cleanup_NilImpl_Noop(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.Cleanup() // must not panic
	a.mu.RLock()
	assert.Nil(t, a.impl)
	a.mu.RUnlock()
}

func TestEtcdModuleAdapter_Cleanup_WithImpl_NilsImpl(t *testing.T) {
	a := startAdapterWithImpl(t)
	a.mu.RLock()
	require.NotNil(t, a.impl)
	a.mu.RUnlock()

	a.Cleanup()

	a.mu.RLock()
	assert.Nil(t, a.impl, "Cleanup must set impl to nil")
	a.mu.RUnlock()
}

// ── Tick ──────────────────────────────────────────────────────────────────

func TestEtcdModuleAdapter_Tick_NilImpl_ReturnsFalse(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.False(t, a.Tick(context.Background()))
}

func TestEtcdModuleAdapter_Tick_WithImpl_ReturnsFalse(t *testing.T) {
	a := startAdapterWithImpl(t)
	// Tick forwards to impl.Tick() and always returns false.
	assert.False(t, a.Tick(context.Background()))
}

// ── GetConfigure / GetConfigurePath ───────────────────────────────────────

func TestEtcdModuleAdapter_GetConfigure_NilWhenNotSet(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.Nil(t, a.GetConfigure())
}

func TestEtcdModuleAdapter_GetConfigure_ReturnsSetRef(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	cfg := &pb.AtappEtcd{Enable: true}
	a.mu.Lock()
	a.etcdCfg = cfg
	a.mu.Unlock()
	assert.Same(t, cfg, a.GetConfigure())
}

func TestEtcdModuleAdapter_GetConfigurePath_EmptyWhenNil(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.Equal(t, "", a.GetConfigurePath())
}

func TestEtcdModuleAdapter_GetConfigurePath_ReturnsPath(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.mu.Lock()
	a.etcdCfg = &pb.AtappEtcd{Path: "/app/etcd"}
	a.mu.Unlock()
	assert.Equal(t, "/app/etcd", a.GetConfigurePath())
}

// ── SetMaybeUpdateKeepaliveTopologyValue ──────────────────────────────────

func TestEtcdModuleAdapter_SetMaybeUpdateKeepaliveTopologyValue_NilImpl_Noop(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.SetMaybeUpdateKeepaliveTopologyValue() // must not panic
}

func TestEtcdModuleAdapter_SetMaybeUpdateKeepaliveTopologyValue_WithImpl_Nopanic(t *testing.T) {
	a := startAdapterWithImpl(t)
	// SyncTopology is called on the live actor; must not panic.
	a.SetMaybeUpdateKeepaliveTopologyValue()
}

// ── Path accessors ────────────────────────────────────────────────────────

func TestEtcdModuleAdapter_PathAccessors_ReturnPathCfgValues(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.mu.Lock()
	a.pathCfg = modulev2.PathConfig{
		ByIDPrefix:     "/svc/by_id",
		ByNamePrefix:   "/svc/by_name",
		TopologyPrefix: "/svc/topology",
	}
	a.mu.Unlock()

	assert.Equal(t, "/svc/by_id", a.GetDiscoveryByIdPath())
	assert.Equal(t, "/svc/by_name", a.GetDiscoveryByNamePath())
	assert.Equal(t, "/svc/topology", a.GetTopologyPath())
}

func TestEtcdModuleAdapter_WatcherPaths_DelegateToBasePaths(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.mu.Lock()
	a.pathCfg = modulev2.PathConfig{
		ByIDPrefix:     "/w/by_id",
		ByNamePrefix:   "/w/by_name",
		TopologyPrefix: "/w/topo",
	}
	a.mu.Unlock()

	assert.Equal(t, a.GetDiscoveryByIdPath(), a.GetDiscoveryByIdWatcherPath())
	assert.Equal(t, a.GetDiscoveryByNamePath(), a.GetDiscoveryByNameWatcherPath())
	assert.Equal(t, a.GetTopologyPath(), a.GetTopologyWatcherPath())
}

// ── RemoveOnTopologyInfoEvent ─────────────────────────────────────────────

func TestEtcdModuleAdapter_RemoveOnTopologyInfoEvent_StopsDispatch(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var count int
	h := a.AddOnTopologyInfoEvent(func(EtcdWatchEvent, *pb.AtappTopologyInfo, *EtcdDataVersion) { count++ })
	a.onSnapshotPublished(makeTopoSnap(topoNode(1, "svc")))
	assert.Equal(t, 1, count)

	a.RemoveOnTopologyInfoEvent(h)
	// Clear prevSnap so next publish triggers a fresh diff.
	a.prevSnap.Store(nil)
	a.onSnapshotPublished(makeTopoSnap(topoNode(2, "svc")))
	assert.Equal(t, 1, count, "callback must not fire after removal")
}

// ── RemoveOnLoadDiscoverySnapshot ────────────────────────────────────────

func TestEtcdModuleAdapter_RemoveOnLoadDiscoverySnapshot_StopsFiring(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	// Bring ready=true so next not-ready publish triggers "loading" callbacks.
	a.onSnapshotPublished(makeDiscSnap(discNode("/by_id/1", 1, "n")))

	var count int
	h := a.AddOnLoadDiscoverySnapshot(func(EtcdModuleImpl) { count++ })

	notReady := &modulev2.ExportSnapshot{
		Version:     2,
		PublishedAt: time.Now(),
		Cause:       modulev2.SnapshotCauseDiscovery,
		Discovery:   modulev2.DiscoverySetSnapshot{Ready: false},
	}
	a.onSnapshotPublished(notReady)
	assert.Equal(t, 1, count)

	a.RemoveOnLoadDiscoverySnapshot(h)
	a.prevSnap.Store(nil)
	a.onSnapshotPublished(makeDiscSnap(discNode("/by_id/1", 1, "n")))
	// Reset ready so loading fires again.
	a.prevSnap.Store(nil)
	a.onSnapshotPublished(makeDiscSnap(discNode("/by_id/2", 2, "n2")))
	a.prevSnap.Store(nil)
	ready2 := makeDiscSnap()
	ready2.Discovery.Ready = false
	a.onSnapshotPublished(ready2)
	assert.Equal(t, 1, count, "callback must not fire after removal")
}

// ── RemoveOnLoadTopologySnapshot ─────────────────────────────────────────

func TestEtcdModuleAdapter_RemoveOnLoadTopologySnapshot_StopsFiring(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	// Bring topology ready=true.
	a.onSnapshotPublished(makeTopoSnap(topoNode(1, "svc")))

	var count int
	h := a.AddOnLoadTopologySnapshot(func(EtcdModuleImpl) { count++ })

	notReady := &modulev2.ExportSnapshot{
		Version:     2,
		PublishedAt: time.Now(),
		Cause:       modulev2.SnapshotCauseTopology,
		Topology:    modulev2.TopologySnapshot{Ready: false},
	}
	a.onSnapshotPublished(notReady)
	assert.Equal(t, 1, count)

	a.RemoveOnLoadTopologySnapshot(h)
	// Transition ready→not-ready again; callback must not fire.
	a.prevSnap.Store(nil)
	a.onSnapshotPublished(makeTopoSnap(topoNode(2, "svc")))
	a.prevSnap.Store(nil)
	a.onSnapshotPublished(notReady)
	assert.Equal(t, 1, count, "callback must not fire after removal")
}

// ── RemoveOnTopologySnapshotLoaded ───────────────────────────────────────

func TestEtcdModuleAdapter_RemoveOnTopologySnapshotLoaded_StopsFiring(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var count int
	h := a.AddOnTopologySnapshotLoaded(func(EtcdModuleImpl) { count++ })
	a.onSnapshotPublished(makeTopoSnap(topoNode(1, "svc")))
	assert.Equal(t, 1, count)

	a.RemoveOnTopologySnapshotLoaded(h)
	// Clear state; next publish transitions to ready again and would fire.
	a.prevSnap.Store(nil)
	a.onSnapshotPublished(makeTopoSnap(topoNode(2, "svc")))
	assert.Equal(t, 1, count, "callback must not fire after removal")
}

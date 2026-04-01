package libatapp

import (
	"testing"

	"github.com/stretchr/testify/assert"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

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

// ── node event dispatch ────────────────────────────────────────────────────

func TestEtcdModuleAdapter_HandleBusEvent_NodeUp_DispatchesPutAction(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var gotAction EtcdDiscoveryAction
	var gotID uint64
	a.AddOnNodeDiscoveryEvent(func(action EtcdDiscoveryAction, node *EtcdDiscoveryNode) {
		gotAction = action
		if node != nil && node.Info != nil {
			gotID = node.Info.GetId()
		}
	})

	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchNodeUp,
		Payload: &modulev2.WatchNodePayload{
			Key:   "/svc/by_id/1",
			Value: &pb.AtappDiscovery{Id: 1, Name: "node-1"},
		},
	})

	assert.Equal(t, EtcdDiscoveryActionPut, gotAction)
	assert.Equal(t, uint64(1), gotID)
}

func TestEtcdModuleAdapter_HandleBusEvent_NodeDown_DispatchesDeleteAction(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var gotAction EtcdDiscoveryAction
	a.AddOnNodeDiscoveryEvent(func(action EtcdDiscoveryAction, _ *EtcdDiscoveryNode) {
		gotAction = action
	})

	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchNodeDown,
		Payload: &modulev2.WatchNodePayload{
			Key: "/svc/by_id/1",
			// Value intentionally nil for Down events
		},
	})

	assert.Equal(t, EtcdDiscoveryActionDelete, gotAction)
}

func TestEtcdModuleAdapter_HandleBusEvent_NodeUpdate_DispatchesPutAction(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var gotAction EtcdDiscoveryAction
	a.AddOnNodeDiscoveryEvent(func(action EtcdDiscoveryAction, _ *EtcdDiscoveryNode) {
		gotAction = action
	})

	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchNodeUpdate,
		Payload: &modulev2.WatchNodePayload{
			Key:   "/svc/by_id/2",
			Value: &pb.AtappDiscovery{Id: 2, Name: "node-2"},
		},
	})

	assert.Equal(t, EtcdDiscoveryActionPut, gotAction)
}

func TestEtcdModuleAdapter_HandleBusEvent_NodePayloadValueType_Dispatches(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var gotAction EtcdDiscoveryAction
	a.AddOnNodeDiscoveryEvent(func(action EtcdDiscoveryAction, _ *EtcdDiscoveryNode) {
		gotAction = action
	})

	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchNodeUp,
		Payload: modulev2.WatchNodePayload{
			Key:   "/svc/by_id/5",
			Value: &pb.AtappDiscovery{Id: 5, Name: "node-5"},
		},
	})

	assert.Equal(t, EtcdDiscoveryActionPut, gotAction)
}

func TestEtcdModuleAdapter_HandleBusEvent_NodeUp_NilValueIgnored(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var called bool
	a.AddOnNodeDiscoveryEvent(func(EtcdDiscoveryAction, *EtcdDiscoveryNode) {
		called = true
	})

	// NodeUp with nil Value must be silently dropped.
	a.handleBusEvent(modulev2.EventEnvelope{
		Type:    modulev2.EventWatchNodeUp,
		Payload: &modulev2.WatchNodePayload{Key: "/svc/by_id/3"},
	})

	assert.False(t, called)
}

func TestEtcdModuleAdapter_RemoveOnNodeEvent_StopsDispatch(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var count int
	h := a.AddOnNodeDiscoveryEvent(func(EtcdDiscoveryAction, *EtcdDiscoveryNode) { count++ })

	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchNodeDown,
		Payload: &modulev2.WatchNodePayload{Key: "/svc/by_id/1"},
	})
	assert.Equal(t, 1, count)

	a.RemoveOnNodeEvent(h)
	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchNodeDown,
		Payload: &modulev2.WatchNodePayload{Key: "/svc/by_id/1"},
	})
	assert.Equal(t, 1, count, "callback must not fire after removal")
}

// ── snapshot callbacks ─────────────────────────────────────────────────────

func TestEtcdModuleAdapter_HasDiscoverySnapshot_InitiallyFalse(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.False(t, a.HasDiscoverySnapshot())
}

func TestEtcdModuleAdapter_HandleBusEvent_SnapshotLoading_ClearsReady(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	a.mu.Lock()
	a.discoverySnapshotReady = true
	a.mu.Unlock()

	a.handleBusEvent(modulev2.EventEnvelope{Type: modulev2.EventWatchSnapshotLoading})

	assert.False(t, a.HasDiscoverySnapshot())
}

func TestEtcdModuleAdapter_HandleBusEvent_SnapshotLoaded_SetsReadyAndFiresCallback(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var cbFired bool
	a.AddOnDiscoverySnapshotLoaded(func(m EtcdModuleImpl) {
		cbFired = true
		assert.True(t, m.HasDiscoverySnapshot())
	})

	a.handleBusEvent(modulev2.EventEnvelope{Type: modulev2.EventWatchSnapshotLoaded})

	assert.True(t, a.HasDiscoverySnapshot())
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
	a.handleBusEvent(modulev2.EventEnvelope{Type: modulev2.EventWatchSnapshotLoaded})
	assert.Equal(t, 1, count)

	a.RemoveOnDiscoverySnapshotLoaded(h)
	a.handleBusEvent(modulev2.EventEnvelope{Type: modulev2.EventWatchSnapshotLoaded})
	assert.Equal(t, 1, count, "callback must not fire after removal")
}

// ── read-only helpers ──────────────────────────────────────────────────────

func TestEtcdModuleAdapter_GetGlobalDiscovery_ReturnsNil(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.Nil(t, a.GetGlobalDiscovery())
}

func TestEtcdModuleAdapter_GetTopologyInfoSet_ReturnsEmptyMap(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	m := a.GetTopologyInfoSet()
	assert.NotNil(t, m)
	assert.Empty(t, m)
}

func TestEtcdModuleAdapter_HasTopologySnapshot_AlwaysFalse(t *testing.T) {
	a := newEtcdModuleAdapter(nil)
	assert.False(t, a.HasTopologySnapshot())
}

func TestEtcdModuleAdapter_HandleBusEvent_TopologyUpdate_DispatchesAndCaches(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	var gotEvent EtcdWatchEvent
	var gotID uint64
	a.AddOnTopologyInfoEvent(func(eventType EtcdWatchEvent, info *pb.AtappTopologyInfo, _ *EtcdDataVersion) {
		gotEvent = eventType
		if info != nil {
			gotID = info.GetId()
		}
	})

	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchTopologyUp,
		Payload: modulev2.WatchTopologyPayload{
			Key:            "/svc/topology/svc-100",
			Value:          &pb.AtappTopologyInfo{Id: 100, Name: "svc", Identity: "svc-100"},
			CreateRevision: 11,
			ModRevision:    12,
			Version:        1,
		},
	})

	assert.Equal(t, EtcdWatchEventPut, gotEvent)
	assert.Equal(t, uint64(100), gotID)
	set := a.GetTopologyInfoSet()
	if assert.Contains(t, set, uint64(100)) {
		assert.Equal(t, "svc", set[100].Info.GetName())
	}
}

func TestEtcdModuleAdapter_HandleBusEvent_TopologyDelete_RemovesCache(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchTopologyUp,
		Payload: modulev2.WatchTopologyPayload{
			Key:   "/svc/topology/svc-200",
			Value: &pb.AtappTopologyInfo{Id: 200, Name: "svc"},
		},
	})

	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchTopologyDown,
		Payload: modulev2.WatchTopologyPayload{
			Key: "/svc/topology/svc-200",
		},
	})

	assert.NotContains(t, a.GetTopologyInfoSet(), uint64(200))
}

func TestEtcdModuleAdapter_HandleBusEvent_TopologySnapshotLoaded_FillsCache(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchTopologySnapshotLoaded,
		Payload: modulev2.WatchTopologySnapshotLoadedPayload{
			Revision: 99,
			Nodes: map[uint64]*modulev2.TopologyNode{
				300: {
					Info: &pb.AtappTopologyInfo{Id: 300, Name: "svc-a"},
					DataVersion: modulev2.DataVersion{
						CreateRevision: 21,
						ModRevision:    22,
						Version:        2,
					},
				},
				301: {
					Info: &pb.AtappTopologyInfo{Id: 301, Name: "svc-b"},
					DataVersion: modulev2.DataVersion{
						CreateRevision: 31,
						ModRevision:    32,
						Version:        3,
					},
				},
			},
		},
	})

	assert.True(t, a.HasTopologySnapshot())
	set := a.GetTopologyInfoSet()
	if assert.Contains(t, set, uint64(300)) {
		assert.Equal(t, "svc-a", set[300].Info.GetName())
		assert.Equal(t, int64(21), set[300].Version.CreateRevision)
		assert.Equal(t, int64(22), set[300].Version.ModRevision)
		assert.Equal(t, int64(2), set[300].Version.Version)
	}
	if assert.Contains(t, set, uint64(301)) {
		assert.Equal(t, "svc-b", set[301].Info.GetName())
	}
}

// ── upstream_id diff via topologyInfoSet ──────────────────────────────────

// TestEtcdModuleAdapter_UpstreamID_DetectedOnUpdate verifies that a caller can
// detect an upstream_id change by reading the old value from topologyInfoSet
// before the update arrives and comparing it with the new value in the callback.
func TestEtcdModuleAdapter_UpstreamID_DetectedOnUpdate(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	// Arrange: seed an initial node with upstream_id = 1.
	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchTopologyUp,
		Payload: modulev2.WatchTopologyPayload{
			Key:     "/svc/topology/svc-500",
			Value:   &pb.AtappTopologyInfo{Id: 500, Name: "svc", UpstreamId: 1},
			Version: 1,
		},
	})

	// Assert seeded state.
	set := a.GetTopologyInfoSet()
	if assert.Contains(t, set, uint64(500)) {
		assert.Equal(t, uint64(1), set[500].Info.GetUpstreamId())
	}

	// Act: register a callback that reads the OLD upstream from topologyInfoSet
	// before dispatchTopologyEvent overwrites it, then compares with the new value.
	var oldUpstream, newUpstream uint64
	a.AddOnTopologyInfoEvent(func(_ EtcdWatchEvent, info *pb.AtappTopologyInfo, _ *EtcdDataVersion) {
		if info == nil {
			return
		}
		// topologyInfoSet is updated BEFORE callbacks fire inside dispatchTopologyEvent,
		// so we cannot read the old value here — the test instead verifies the new value
		// matches what the callback received, and that it differs from the seeded value.
		newUpstream = info.GetUpstreamId()
	})

	// Capture old upstream before dispatching the update.
	a.mu.RLock()
	if prev := a.topologyInfoSet[500]; prev != nil {
		oldUpstream = prev.Info.GetUpstreamId()
	}
	a.mu.RUnlock()

	// Dispatch update with upstream_id = 99.
	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchTopologyUpdate,
		Payload: modulev2.WatchTopologyPayload{
			Key:     "/svc/topology/svc-500",
			Value:   &pb.AtappTopologyInfo{Id: 500, Name: "svc", UpstreamId: 99},
			Version: 2,
		},
	})

	// Assert: old value was 1, new value is 99 — change detected.
	assert.Equal(t, uint64(1), oldUpstream, "old upstream_id must be 1")
	assert.Equal(t, uint64(99), newUpstream, "new upstream_id must be 99")
	assert.NotEqual(t, oldUpstream, newUpstream, "upstream_id change must be detectable")

	// topologyInfoSet must now reflect the new upstream_id.
	set = a.GetTopologyInfoSet()
	if assert.Contains(t, set, uint64(500)) {
		assert.Equal(t, uint64(99), set[500].Info.GetUpstreamId())
	}
}

// TestEtcdModuleAdapter_UpstreamID_NoChangeIgnored verifies that when
// upstream_id does not change between two updates, old == new.
func TestEtcdModuleAdapter_UpstreamID_NoChangeIgnored(t *testing.T) {
	a := newEtcdModuleAdapter(nil)

	// Seed with upstream_id = 42.
	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchTopologyUp,
		Payload: modulev2.WatchTopologyPayload{
			Key:     "/svc/topology/svc-600",
			Value:   &pb.AtappTopologyInfo{Id: 600, Name: "svc", UpstreamId: 42},
			Version: 1,
		},
	})

	var oldUpstream uint64
	a.mu.RLock()
	if prev := a.topologyInfoSet[600]; prev != nil {
		oldUpstream = prev.Info.GetUpstreamId()
	}
	a.mu.RUnlock()

	var newUpstream uint64
	a.AddOnTopologyInfoEvent(func(_ EtcdWatchEvent, info *pb.AtappTopologyInfo, _ *EtcdDataVersion) {
		if info != nil {
			newUpstream = info.GetUpstreamId()
		}
	})

	// Update with same upstream_id = 42.
	a.handleBusEvent(modulev2.EventEnvelope{
		Type: modulev2.EventWatchTopologyUpdate,
		Payload: modulev2.WatchTopologyPayload{
			Key:     "/svc/topology/svc-600",
			Value:   &pb.AtappTopologyInfo{Id: 600, Name: "svc-renamed", UpstreamId: 42},
			Version: 2,
		},
	})

	assert.Equal(t, uint64(42), oldUpstream)
	assert.Equal(t, uint64(42), newUpstream)
	assert.Equal(t, oldUpstream, newUpstream, "no upstream_id change")
}

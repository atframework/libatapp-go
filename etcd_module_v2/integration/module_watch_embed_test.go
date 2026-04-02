package integration_test

// Complete watch pipeline tests for incremental events (NodeUp/Down/Update,
// TopologyUp/Down/Update) and initial snapshot pre-population.
//
// These tests require a real embed etcd because mockserver (used in
// etcd_module_v2/module_watch_integration_test.go) cannot:
//   - deliver incremental Watch events after the initial snapshot
//   - persist KV data between Put and Get calls
//   - provide real Watch streams for EventWatchNodeDown / NodeUpdate /
//     EventWatchTopologyUp / Down / Update
//
// Mapping to existing unit / mockserver tests:
//
//   Existing (mockserver)                          │ Embed tests here
//   ───────────────────────────────────────────────┼──────────────────────────────────────────────────
//   TestModule_Watch_ByID_SnapshotFlow             │ TestModule_Embed_Watch_InitialSnapshot_WithNodes
//   TestModule_Watch_Topology_SnapshotFlow         │ TestModule_Embed_Watch_InitialTopologySnapshot_WithNodes
//   TestModule_Embed_ExternalPut_TriggersWatchNodeUp│ (already in module_embed_integration_test.go)
//   (no equivalent)                                │ TestModule_Embed_Watch_NodeDown
//   (no equivalent)                                │ TestModule_Embed_Watch_NodeUpdate
//   (no equivalent)                                │ TestModule_Embed_Watch_TopologyUp
//   (no equivalent)                                │ TestModule_Embed_Watch_TopologyDown
//   (no equivalent)                                │ TestModule_Embed_Watch_TopologyUpdate

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// marshalTopologyJSON encodes info as proto-JSON using proto field names,
// matching the format written by RegistrationActor (codec.MarshalTopologyToJSON)
// and decodable by decodeTopologyInfo in WatchActor.
func marshalTopologyJSON(t *testing.T, info *pb.AtappTopologyInfo) string {
	t.Helper()
	opts := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false}
	b, err := opts.Marshal(info)
	require.NoError(t, err)
	return string(b)
}

// ── Discovery watch tests ─────────────────────────────────────────────────

// TestModule_Embed_Watch_NodeDown verifies the full Delete pipeline:
//
//  1. External PUT fires EventWatchNodeUp, node appears in snapshot.
//  2. External DELETE fires EventWatchNodeDown with nil Value.
//  3. ProjectionActor removes the node from the snapshot.
func TestModule_Embed_Watch_NodeDown(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedByIDPrefix})
	ch := subscribeEmbedEvents(t, m)

	// Wait for the initial empty snapshot so the Watch stream is established
	// from a known revision; subsequent mutations will land in the stream.
	waitForEmbedEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 15*time.Second)

	extCli := newEmbedClient(t, etcdAddr)
	nodeKey := embedByIDPrefix + "/node-down-400"
	disc := &pb.AtappDiscovery{Id: 400, Name: "node-down-400"}

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer putCancel()
	_, err := extCli.Put(putCtx, nodeKey, marshalDiscoveryJSON(t, disc))
	require.NoError(t, err, "PUT fixture node")

	// Wait for NodeUp and snapshot to reflect the new node.
	waitForEmbedEvent(t, ch, modulev2.EventWatchNodeUp, 5*time.Second)
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil && snap.Discovery.NodesByPath[nodeKey] != nil
	}, 3*time.Second, 20*time.Millisecond, "node must appear in snapshot after PUT")

	// DELETE the node.
	delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer delCancel()
	_, err = extCli.Delete(delCtx, nodeKey)
	require.NoError(t, err, "DELETE fixture node")

	// Watch stream must deliver EventWatchNodeDown.
	env := waitForEmbedEvent(t, ch, modulev2.EventWatchNodeDown, 5*time.Second)
	pl, ok := env.Payload.(modulev2.WatchNodePayload)
	require.True(t, ok, "payload must be WatchNodePayload")
	assert.Equal(t, nodeKey, pl.Key)
	assert.Nil(t, pl.Value, "WatchNodePayload.Value must be nil for Down events")

	// ProjectionActor must remove the node from the snapshot.
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil && snap.Discovery.NodesByPath[nodeKey] == nil
	}, 3*time.Second, 20*time.Millisecond, "node must be absent from snapshot after DELETE")
}

// TestModule_Embed_Watch_NodeUpdate verifies the full Update pipeline:
//
//  1. External PUT fires EventWatchNodeUp, node appears in snapshot.
//  2. Second PUT on the same key fires EventWatchNodeUpdate with updated Value.
//  3. ProjectionActor updates the snapshot to reflect the new value.
func TestModule_Embed_Watch_NodeUpdate(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedByIDPrefix})
	ch := subscribeEmbedEvents(t, m)

	waitForEmbedEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 15*time.Second)

	extCli := newEmbedClient(t, etcdAddr)
	nodeKey := embedByIDPrefix + "/node-update-500"

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer putCancel()

	// Initial PUT.
	_, err := extCli.Put(putCtx, nodeKey, marshalDiscoveryJSON(t, &pb.AtappDiscovery{Id: 500, Name: "original-name"}))
	require.NoError(t, err, "initial PUT")

	waitForEmbedEvent(t, ch, modulev2.EventWatchNodeUp, 5*time.Second)
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		n := snap.Discovery.NodesByPath[nodeKey]
		return n != nil && n.Info.GetName() == "original-name"
	}, 3*time.Second, 20*time.Millisecond, "node must appear with original name")

	// UPDATE: same key, new value.
	updCtx, updCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer updCancel()
	_, err = extCli.Put(updCtx, nodeKey, marshalDiscoveryJSON(t, &pb.AtappDiscovery{Id: 500, Name: "updated-name"}))
	require.NoError(t, err, "update PUT")

	// Watch stream must deliver EventWatchNodeUpdate with the new decoded value.
	env := waitForEmbedEvent(t, ch, modulev2.EventWatchNodeUpdate, 5*time.Second)
	pl, ok := env.Payload.(modulev2.WatchNodePayload)
	require.True(t, ok, "payload must be WatchNodePayload")
	assert.Equal(t, nodeKey, pl.Key)
	require.NotNil(t, pl.Value, "WatchNodePayload.Value must not be nil for Update events")
	assert.Equal(t, uint64(500), pl.Value.GetId())
	assert.Equal(t, "updated-name", pl.Value.GetName())

	// Snapshot must reflect the updated name.
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		n := snap.Discovery.NodesByPath[nodeKey]
		return n != nil && n.Info.GetName() == "updated-name"
	}, 3*time.Second, 20*time.Millisecond, "snapshot must reflect updated node name")
}

// TestModule_Embed_Watch_InitialSnapshot_WithNodes verifies that nodes
// already in etcd when AddWatchPrefix is called appear in the
// WatchSnapshotLoadedPayload.Nodes (initial Get snapshot), not as incremental
// NodeUp events.
func TestModule_Embed_Watch_InitialSnapshot_WithNodes(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	extCli := newEmbedClient(t, etcdAddr)

	// Pre-PUT nodes BEFORE the module starts watching this prefix.
	preCtx, preCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer preCancel()

	keys := []string{
		embedByIDPrefix + "/pre-node-601",
		embedByIDPrefix + "/pre-node-602",
	}
	_, err := extCli.Put(preCtx, keys[0], marshalDiscoveryJSON(t, &pb.AtappDiscovery{Id: 601, Name: "pre-601"}))
	require.NoError(t, err)
	_, err = extCli.Put(preCtx, keys[1], marshalDiscoveryJSON(t, &pb.AtappDiscovery{Id: 602, Name: "pre-602"}))
	require.NoError(t, err)

	// Start module WITHOUT watch prefixes so subscription is set up before
	// the initial Get is triggered.
	m := startEmbedModule(t, etcdAddr, nil)
	ch := subscribeEmbedEvents(t, m)

	// AddWatchPrefix triggers the initial Get — nodes already in etcd will
	// appear in the Loaded payload, not as incremental NodeUp events.
	require.NoError(t, m.AddWatchPrefix(embedByIDPrefix))

	loadedEnv := waitForEmbedEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 15*time.Second)
	lpl, ok := loadedEnv.Payload.(modulev2.WatchSnapshotLoadedPayload)
	require.True(t, ok, "payload must be WatchSnapshotLoadedPayload")
	assert.Equal(t, embedByIDPrefix, lpl.Prefix)

	// Both pre-existing nodes must appear in the snapshot payload.
	require.Len(t, lpl.Nodes, 2, "initial Get must capture both pre-existing nodes")
	require.Contains(t, lpl.Nodes, keys[0])
	require.Contains(t, lpl.Nodes, keys[1])
	assert.Equal(t, "pre-601", lpl.Nodes[keys[0]].Info.GetName())
	assert.Equal(t, "pre-602", lpl.Nodes[keys[1]].Info.GetName())

	// GetSnapshot must also contain both nodes after ProjectionActor applies the event.
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil &&
			snap.Discovery.NodesByPath[keys[0]] != nil &&
			snap.Discovery.NodesByPath[keys[1]] != nil
	}, 3*time.Second, 20*time.Millisecond, "snapshot must reflect both pre-existing nodes")
}

// ── Topology watch tests ──────────────────────────────────────────────────

// TestModule_Embed_Watch_TopologyUp verifies that an external PUT under the
// topology prefix fires EventWatchTopologyUp and the node appears in the topology
// sub-view of GetSnapshot.
func TestModule_Embed_Watch_TopologyUp(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedTopoPrefix})
	ch := subscribeEmbedEvents(t, m)

	waitForEmbedEvent(t, ch, modulev2.EventWatchTopologySnapshotLoaded, 15*time.Second)

	extCli := newEmbedClient(t, etcdAddr)
	topoKey := embedTopoPrefix + "/topo-700"
	topoInfo := &pb.AtappTopologyInfo{Id: 700, Name: "topo-700"}

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer putCancel()
	_, err := extCli.Put(putCtx, topoKey, marshalTopologyJSON(t, topoInfo))
	require.NoError(t, err, "PUT topology node")

	// Watch stream must deliver EventWatchTopologyUp.
	env := waitForEmbedEvent(t, ch, modulev2.EventWatchTopologyUp, 5*time.Second)
	pl, ok := env.Payload.(modulev2.WatchTopologyPayload)
	require.True(t, ok, "payload must be WatchTopologyPayload")
	assert.Equal(t, topoKey, pl.Key)
	require.NotNil(t, pl.Value, "WatchTopologyPayload.Value must not be nil for Up events")
	assert.Equal(t, uint64(700), pl.Value.GetId())
	assert.Equal(t, "topo-700", pl.Value.GetName())

	// Topology snapshot must contain the new node.
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil && snap.Topology.NodesByID[700] != nil
	}, 3*time.Second, 20*time.Millisecond, "topology snapshot must contain node 700")

	snap := m.GetSnapshot()
	require.NotNil(t, snap)
	topoNode := snap.Topology.NodesByID[700]
	require.NotNil(t, topoNode)
	assert.Equal(t, "topo-700", topoNode.Info.GetName())
}

// TestModule_Embed_Watch_TopologyDown verifies the full DELETE pipeline for
// topology:
//
//  1. External PUT fires EventWatchTopologyUp, node appears in snapshot.
//  2. External DELETE fires EventWatchTopologyDown.
//  3. ProjectionActor removes the node from the topology snapshot.
func TestModule_Embed_Watch_TopologyDown(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedTopoPrefix})
	ch := subscribeEmbedEvents(t, m)

	waitForEmbedEvent(t, ch, modulev2.EventWatchTopologySnapshotLoaded, 15*time.Second)

	extCli := newEmbedClient(t, etcdAddr)
	topoKey := embedTopoPrefix + "/topo-down-800"
	topoInfo := &pb.AtappTopologyInfo{Id: 800, Name: "topo-down-800"}

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer putCancel()
	_, err := extCli.Put(putCtx, topoKey, marshalTopologyJSON(t, topoInfo))
	require.NoError(t, err, "PUT topology node")

	waitForEmbedEvent(t, ch, modulev2.EventWatchTopologyUp, 5*time.Second)
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil && snap.Topology.NodesByID[800] != nil
	}, 3*time.Second, 20*time.Millisecond, "topology snapshot must contain node 800")

	// DELETE the topology node.
	delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer delCancel()
	_, err = extCli.Delete(delCtx, topoKey)
	require.NoError(t, err, "DELETE topology node")

	// Watch stream must deliver EventWatchTopologyDown.
	// WatchActor sets WithPrevKV() so Value carries the decoded previous value.
	env := waitForEmbedEvent(t, ch, modulev2.EventWatchTopologyDown, 5*time.Second)
	pl, ok := env.Payload.(modulev2.WatchTopologyPayload)
	require.True(t, ok, "payload must be WatchTopologyPayload")
	assert.Equal(t, topoKey, pl.Key)

	// ProjectionActor must remove the node from the topology snapshot.
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil && snap.Topology.NodesByID[800] == nil
	}, 3*time.Second, 20*time.Millisecond, "topology snapshot must not contain node 800 after DELETE")
}

// TestModule_Embed_Watch_TopologyUpdate verifies that a second PUT on an
// existing topology key fires EventWatchTopologyUpdate and the snapshot
// reflects the updated data.
func TestModule_Embed_Watch_TopologyUpdate(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedTopoPrefix})
	ch := subscribeEmbedEvents(t, m)

	waitForEmbedEvent(t, ch, modulev2.EventWatchTopologySnapshotLoaded, 15*time.Second)

	extCli := newEmbedClient(t, etcdAddr)
	topoKey := embedTopoPrefix + "/topo-update-900"

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer putCancel()

	// Initial PUT (Version becomes 1).
	_, err := extCli.Put(putCtx, topoKey, marshalTopologyJSON(t, &pb.AtappTopologyInfo{Id: 900, Name: "original-topo"}))
	require.NoError(t, err, "initial PUT")

	waitForEmbedEvent(t, ch, modulev2.EventWatchTopologyUp, 5*time.Second)
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		n := snap.Topology.NodesByID[900]
		return n != nil && n.Info.GetName() == "original-topo"
	}, 3*time.Second, 20*time.Millisecond, "topology snapshot must reflect original name")

	// UPDATE: same key, new name (Version becomes 2 → EventWatchTopologyUpdate).
	updCtx, updCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer updCancel()
	_, err = extCli.Put(updCtx, topoKey, marshalTopologyJSON(t, &pb.AtappTopologyInfo{Id: 900, Name: "updated-topo"}))
	require.NoError(t, err, "update PUT")

	// Watch stream must deliver EventWatchTopologyUpdate (Version > 1).
	env := waitForEmbedEvent(t, ch, modulev2.EventWatchTopologyUpdate, 5*time.Second)
	pl, ok := env.Payload.(modulev2.WatchTopologyPayload)
	require.True(t, ok, "payload must be WatchTopologyPayload")
	assert.Equal(t, topoKey, pl.Key)
	require.NotNil(t, pl.Value, "WatchTopologyPayload.Value must not be nil for Update events")
	assert.Equal(t, uint64(900), pl.Value.GetId())
	assert.Equal(t, "updated-topo", pl.Value.GetName())

	// Snapshot must reflect the updated name.
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		n := snap.Topology.NodesByID[900]
		return n != nil && n.Info.GetName() == "updated-topo"
	}, 3*time.Second, 20*time.Millisecond, "topology snapshot must reflect updated name")
}

// TestModule_Embed_Watch_InitialTopologySnapshot_WithNodes verifies that
// topology nodes already in etcd when the topology prefix is added appear in
// the WatchTopologySnapshotLoadedPayload.Nodes map (initial Get snapshot).
func TestModule_Embed_Watch_InitialTopologySnapshot_WithNodes(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	extCli := newEmbedClient(t, etcdAddr)

	// Pre-PUT topology nodes BEFORE the module starts watching.
	preCtx, preCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer preCancel()

	_, err := extCli.Put(preCtx, embedTopoPrefix+"/pre-topo-1001",
		marshalTopologyJSON(t, &pb.AtappTopologyInfo{Id: 1001, Name: "pre-topo-1001"}))
	require.NoError(t, err)
	_, err = extCli.Put(preCtx, embedTopoPrefix+"/pre-topo-1002",
		marshalTopologyJSON(t, &pb.AtappTopologyInfo{Id: 1002, Name: "pre-topo-1002"}))
	require.NoError(t, err)

	// Start module WITHOUT prefixes so subscription is set up before the
	// initial Get is triggered.
	m := startEmbedModule(t, etcdAddr, nil)
	ch := subscribeEmbedEvents(t, m)

	require.NoError(t, m.AddWatchPrefix(embedTopoPrefix))

	loadedEnv := waitForEmbedEvent(t, ch, modulev2.EventWatchTopologySnapshotLoaded, 15*time.Second)
	lpl, ok := loadedEnv.Payload.(modulev2.WatchTopologySnapshotLoadedPayload)
	require.True(t, ok, "payload must be WatchTopologySnapshotLoadedPayload")

	// Both pre-existing topology nodes must appear in the snapshot payload.
	require.Len(t, lpl.Nodes, 2, "initial Get must capture both pre-existing topology nodes")
	require.Contains(t, lpl.Nodes, uint64(1001))
	require.Contains(t, lpl.Nodes, uint64(1002))
	assert.Equal(t, "pre-topo-1001", lpl.Nodes[1001].Info.GetName())
	assert.Equal(t, "pre-topo-1002", lpl.Nodes[1002].Info.GetName())

	// GetSnapshot topology must also contain both nodes.
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil &&
			snap.Topology.NodesByID[1001] != nil &&
			snap.Topology.NodesByID[1002] != nil
	}, 3*time.Second, 20*time.Millisecond, "topology snapshot must reflect both pre-existing nodes")
}

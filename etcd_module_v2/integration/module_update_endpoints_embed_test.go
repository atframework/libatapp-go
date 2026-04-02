package integration_test

// Integration tests for EtcdModule.UpdateEndpoints backed by a real embedded etcd.
//
// UpdateEndpoints calls client.SetEndpoints + watchActor.ActiveAll().
// ActiveAll() cancels every active watch stream and re-opens them, which
// causes WatchActor to re-run the initial Get sweep and fire a fresh
// EventWatchSnapshotLoaded.  These tests verify:
//
//   - After UpdateEndpoints the module still receives new watch events.
//   - Nodes that existed before UpdateEndpoints are re-read by the fresh
//     sweep and remain visible in the snapshot.
//   - UpdateEndpoints returns ErrNotRunning on a stopped module.

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

// marshalDiscJSON is a local alias so this file does not rely on the helper
// defined in module_embed_integration_test.go (same package, just a reminder).
func marshalDiscJSON(t *testing.T, d *pb.AtappDiscovery) string {
	t.Helper()
	opts := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false}
	b, err := opts.Marshal(d)
	require.NoError(t, err)
	return string(b)
}

// ── UpdateEndpoints — watch pipeline continues after stream restart ───────

// TestModule_Embed_UpdateEndpoints_WatchResumesAfterReconnect verifies the
// "soft reload" code path end-to-end:
//
//  1. Module starts, receives initial empty snapshot.
//  2. External PUT → watch delivers EventWatchNodeUp, node visible in snapshot.
//  3. UpdateEndpoints(same addr) calls SetEndpoints + ActiveAll.
//     ActiveAll cancels all streams → WatchActor re-runs init Get sweep →
//     EventWatchSnapshotLoaded fires again.
//  4. New PUT after stream restart → node visible in snapshot.
//  5. Nodes from before the restart are still visible (re-read by sweep).
func TestModule_Embed_UpdateEndpoints_WatchResumesAfterReconnect(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedByIDPrefix})
	ch := subscribeEmbedEvents(t, m)

	// Wait for the initial empty snapshot.
	waitForEmbedEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 15*time.Second)

	extCli := newEmbedClient(t, etcdAddr)

	// ── Step 2: PUT a node before UpdateEndpoints ─────────────────────────
	nodeKeyBefore := embedByIDPrefix + "/ue-501"
	discBefore := &pb.AtappDiscovery{Id: 501, Name: "ue-node-before"}

	_, err := extCli.Put(context.Background(), nodeKeyBefore, marshalDiscJSON(t, discBefore))
	require.NoError(t, err, "PUT node-before")

	waitForEmbedEvent(t, ch, modulev2.EventWatchNodeUp, 5*time.Second)
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil && snap.Discovery.NodesByPath[nodeKeyBefore] != nil
	}, 3*time.Second, 20*time.Millisecond, "node-before must appear in snapshot")

	// ── Step 3: UpdateEndpoints (same server, proves the API works) ───────
	require.NoError(t, m.UpdateEndpoints([]string{etcdAddr}))

	// ActiveAll cancels streams → WatchActor re-runs init sweep →
	// a second EventWatchSnapshotLoaded should fire.
	waitForEmbedEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 10*time.Second)

	// ── Step 4: PUT a new node after stream restart ───────────────────────
	nodeKeyAfter := embedByIDPrefix + "/ue-502"
	discAfter := &pb.AtappDiscovery{Id: 502, Name: "ue-node-after"}

	_, err = extCli.Put(context.Background(), nodeKeyAfter, marshalDiscJSON(t, discAfter))
	require.NoError(t, err, "PUT node-after")

	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil && snap.Discovery.NodesByPath[nodeKeyAfter] != nil
	}, 5*time.Second, 20*time.Millisecond, "node-after must appear in snapshot after UpdateEndpoints")

	// ── Step 5: node-before must survive the stream restart ───────────────
	// The re-sync Get sweep picks up all existing keys, so the old node
	// should still be present.
	snap := m.GetSnapshot()
	require.NotNil(t, snap)
	assert.NotNil(t, snap.Discovery.NodesByPath[nodeKeyBefore],
		"node-before must survive UpdateEndpoints stream restart")
}

// ── UpdateEndpoints on a stopped module ───────────────────────────────────

// TestModule_UpdateEndpoints_NotRunning_ReturnsErrNotRunning verifies that
// calling UpdateEndpoints before Start (idle state) returns ErrNotRunning.
func TestModule_UpdateEndpoints_NotRunning_ReturnsErrNotRunning(t *testing.T) {
	// Build a module but do NOT call Start.
	etcdAddr := embedEtcdEndpoint(t)

	// We need a real client for the constructor but we won't start the module.
	// Use a disconnected config — the module is never started so no real
	// connection attempt is made.
	_ = etcdAddr

	m := modulev2.NewEtcdModule(nil, modulev2.PathConfig{
		ByIDPrefix:    embedByIDPrefix,
		WatchPrefixes: []string{embedByIDPrefix},
		LeaseTTL:      10,
	}, modulev2.ModuleOptions{RetryInterval: 100 * time.Millisecond})

	err := m.UpdateEndpoints([]string{etcdAddr})
	assert.ErrorIs(t, err, modulev2.ErrNotRunning)
}

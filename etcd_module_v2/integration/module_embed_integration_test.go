package integration_test

// Integration tests backed by a real single-node embedded etcd.
//
// These tests cover code paths that mockserver cannot exercise:
//
//   - EventLeaseReleased:  requires a non-zero LeaseID (mockserver always
//     returns LeaseID=0, so leaseActor.onStop skips the publish).
//
//   - onAddDiscovery PUT path: requires a real lease; with leaseID=0 the guard
//     in RegistrationActor.onAddDiscovery returns early before doing the PUT,
//     so EventRegistrationChanged is never published from the Add path.
//
//   - EventWatchNodeUp from a real Watch stream: mockserver's Watch gRPC
//     service is not registered, so the watch channel closes immediately
//     after the initial empty Get; no incremental events are delivered.
//
//   - Snapshot with actual node data: requires KV persistence (mockserver Put
//     does not persist; subsequent Get always returns empty).
//
// Actor/unit test → embed integration test mapping:
//
//   TestLeaseActor_Stop_CleanShutdown
//       → TestModule_Embed_LeaseReleasedOnStop
//   TestRegistrationActor_Run_AddDiscoveryAndTopology (onAddDiscovery PUT path)
//       → TestModule_Embed_RegisterService_FiresRegistrationChanged
//   (full E2E pipeline: registration write → watch read → snapshot)
//       → TestModule_Embed_RegisterService_NodeAppearsInSnapshot
//   (full E2E pipeline: external write → watch read → snapshot)
//       → TestModule_Embed_ExternalPut_TriggersWatchNodeUp

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

// marshalDiscoveryJSON encodes d as proto-JSON using proto field names, matching
// the format written by RegistrationActor (codec.MarshalDiscoveryToJSON) and
// decodable by DecodeDiscoveryValue.  Used for external-client test Puts.
func marshalDiscoveryJSON(t *testing.T, d *pb.AtappDiscovery) string {
	t.Helper()
	opts := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false}
	b, err := opts.Marshal(d)
	require.NoError(t, err)
	return string(b)
}

// ── LeaseActor embed tests ────────────────────────────────────────────────

// TestModule_Embed_LeaseReleasedOnStop mirrors TestLeaseActor_Stop_CleanShutdown
// and exercises the EventLeaseReleased path that mockserver cannot reach.
//
// With embed etcd, LeaseGrant returns a real non-zero LeaseID, so the guard
// in leaseActor.onStop proceeds to revoke the lease and publish EventLeaseReleased.
func TestModule_Embed_LeaseReleasedOnStop(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, nil)
	ch := subscribeEmbedEvents(t, m)

	// Verify that the real lease is non-zero.
	env := waitForEmbedEvent(t, ch, modulev2.EventLeaseGranted, 15*time.Second)
	pl, ok := env.Payload.(modulev2.LeaseGrantedPayload)
	require.True(t, ok)
	require.NotZero(t, pl.LeaseID, "embed etcd must return a non-zero LeaseID")

	// Explicitly stop the module; the lease revocation must publish EventLeaseReleased.
	// (t.Cleanup also stops, but the second Stop simply returns ErrNotRunning.)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = m.Stop(stopCtx)

	// EventLeaseReleased is published synchronously during actor shutdown, so
	// it must already be in the channel by the time Stop() returns.
	waitForEmbedEvent(t, ch, modulev2.EventLeaseReleased, 5*time.Second)
}

// ── RegistrationActor embed tests ────────────────────────────────────────

// TestModule_Embed_RegisterService_FiresRegistrationChanged exercises the
// real onAddDiscovery PUT path.
//
// In mockserver tests the leaseID=0 guard skips the PUT entirely; a Unregister
// workaround is used instead.  With embed etcd the lease is real, so
// onAddDiscovery proceeds to PUT the discovery value and publishes
// EventRegistrationChanged directly — no Unregister required.
func TestModule_Embed_RegisterService_FiresRegistrationChanged(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, nil)
	ch := subscribeEmbedEvents(t, m)

	// Wait for a real lease to be established before registering.
	waitForEmbedEvent(t, ch, modulev2.EventLeaseGranted, 15*time.Second)

	svc := modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 200, Name: "embed-svc-200"},
		Path:      embedByIDPrefix + "/embed-svc-200",
		TTL:       10,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handle, err := m.RegisterService(ctx, svc)
	require.NoError(t, err)
	require.NoError(t, handle.Wait(ctx), "PUT to embed etcd must succeed with real lease")

	// onAddDiscovery fires EventRegistrationChanged after the real PUT.
	env := waitForEmbedEvent(t, ch, modulev2.EventRegistrationChanged, 5*time.Second)
	_, ok := env.Payload.(modulev2.RegistrationChangedPayload)
	assert.True(t, ok, "payload must be RegistrationChangedPayload")
}

// ── Watch actor embed tests ───────────────────────────────────────────────

// TestModule_Embed_RegisterService_NodeAppearsInSnapshot is the first full
// end-to-end pipeline test:
//
//  1. RegistrationActor.putDiscoveryWithLease writes JSON-encoded node to etcd.
//  2. WatchActor Watch stream delivers the PUT as EventWatchNodeUp.
//  3. ProjectionActor updates the snapshot.
//  4. GetSnapshot().Discovery.NodesByPath contains the registered node.
//
// This test validates that the JSON encoding in putDiscoveryWithLease
// (codec.MarshalDiscoveryToJSON) produces a value that DecodeDiscoveryValue
// can successfully decode from the Watch stream.
func TestModule_Embed_RegisterService_NodeAppearsInSnapshot(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedByIDPrefix})
	ch := subscribeEmbedEvents(t, m)

	// Wait for a real lease before calling RegisterService.
	waitForEmbedEvent(t, ch, modulev2.EventLeaseGranted, 15*time.Second)

	svc := modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 201, Name: "embed-node-201"},
		Path:      embedByIDPrefix + "/embed-node-201",
		TTL:       10,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handle, err := m.RegisterService(ctx, svc)
	require.NoError(t, err)
	require.NoError(t, handle.Wait(ctx))

	// The Watch stream must deliver EventWatchNodeUp.
	waitForEmbedEvent(t, ch, modulev2.EventWatchNodeUp, 5*time.Second)

	// ProjectionActor must update the snapshot.  Poll with Eventually because
	// the snapshot update is asynchronous after the Watch event is dispatched.
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		if snap == nil {
			return false
		}
		_, ok := snap.Discovery.NodesByPath[svc.Path]
		return ok
	}, 5*time.Second, 20*time.Millisecond,
		"snapshot must contain the registered node at path %s", svc.Path)

	snap := m.GetSnapshot()
	require.NotNil(t, snap)
	node, ok := snap.Discovery.NodesByPath[svc.Path]
	require.True(t, ok, "node must be present in Discovery.NodesByPath")
	assert.Equal(t, uint64(201), node.Info.GetId())
	assert.Equal(t, "embed-node-201", node.Info.GetName())
}

// TestModule_Embed_ExternalPut_TriggersWatchNodeUp verifies that a Put made by
// an external client (not through RegisterService) is delivered as
// EventWatchNodeUp and appears in GetSnapshot.
//
// This test isolates the Watch pipeline from registration concerns:
// external client → etcd KV → Watch stream → EventWatchNodeUp → snapshot.
func TestModule_Embed_ExternalPut_TriggersWatchNodeUp(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedByIDPrefix})
	ch := subscribeEmbedEvents(t, m)

	// Separate client for external data injection (closed via t.Cleanup).
	extCli := newEmbedClient(t, etcdAddr)

	// Wait for the initial Watch snapshot so the Watch stream is established
	// from a known revision; any subsequent Put will land in the stream.
	waitForEmbedEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 15*time.Second)

	// External client puts a JSON-encoded discovery node under the watched prefix.
	disc := &pb.AtappDiscovery{Id: 300, Name: "external-node-300"}
	nodeKey := embedByIDPrefix + "/external-node-300"
	nodeVal := marshalDiscoveryJSON(t, disc)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := extCli.Put(ctx, nodeKey, nodeVal)
	require.NoError(t, err, "external Put to embed etcd must succeed")

	// Watch stream must deliver EventWatchNodeUp with correct key and value.
	env := waitForEmbedEvent(t, ch, modulev2.EventWatchNodeUp, 5*time.Second)
	pl, ok := env.Payload.(modulev2.WatchNodePayload)
	require.True(t, ok, "payload must be WatchNodePayload")
	assert.Equal(t, nodeKey, pl.Key)
	require.NotNil(t, pl.Value, "WatchNodePayload.Value must not be nil (JSON decode must succeed)")
	assert.Equal(t, uint64(300), pl.Value.GetId())
	assert.Equal(t, "external-node-300", pl.Value.GetName())

	// Snapshot must contain the external node.
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		if snap == nil {
			return false
		}
		_, ok := snap.Discovery.NodesByPath[nodeKey]
		return ok
	}, 5*time.Second, 20*time.Millisecond,
		"snapshot must contain external node at %s", nodeKey)
}

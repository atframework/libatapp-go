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
	"fmt"
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

	// Trigger lazy lease acquisition via a registration request.
	regCtx, regCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer regCancel()
	_, _ = m.RegisterService(regCtx, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 9001, Name: "lease-released-svc"},
		Path:      embedByIDPrefix + "/lease-released-9001",
		TTL:       10,
	})

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

	svc := modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 200, Name: "embed-svc-200"},
		Path:      embedByIDPrefix + "/embed-svc-200",
		TTL:       10,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// RegisterService triggers lazy lease acquisition; a real lease will be
	// established before the PUT is replayed to etcd.
	handle, err := m.RegisterService(ctx, svc)
	require.NoError(t, err)
	// handle.Wait returns nil immediately (entry queued; actual write happens via replay).
	_ = handle.Wait(ctx)
	// Wait for the lease to be granted (proves a real non-zero lease exists).
	waitForEmbedEvent(t, ch, modulev2.EventLeaseGranted, 15*time.Second)

	// EventRegistrationChanged is published only after the real lease replay PUT.
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

	svc := modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 201, Name: "embed-node-201"},
		Path:      embedByIDPrefix + "/embed-node-201",
		TTL:       10,
	}
	// Use a longer timeout to accommodate lazy lease acquisition.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Trigger lazy lease acquisition; lease will be established before the PUT.
	handle, err := m.RegisterService(ctx, svc)
	require.NoError(t, err)
	// handle.Wait returns nil immediately (entry queued; write happens via replay).
	_ = handle.Wait(ctx)
	// Wait for the lease grant to confirm a real non-zero lease is in use.
	waitForEmbedEvent(t, ch, modulev2.EventLeaseGranted, 15*time.Second)

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

// ── Lazy-lease lifecycle integration test ────────────────────────────────

// TestModule_Embed_Lease_LazyGrantAndReleaseOnLastUnregister verifies the full
// lazy-lease lifecycle against a real embedded etcd:
//
//  1. Before any RegisterService: no EventLeaseGranted fires, etcd has no leases.
//  2. First RegisterService → EventLeaseGranted with a real non-zero LeaseID.
//  3. Lease is verified alive via etcd TimeToLive.
//  4. A second service is registered — the same lease is reused.
//  5. First service is unregistered — lease still alive (refcount > 0).
//  6. Last (second) service is unregistered → EventLeaseReleased fires.
//  7. Lease is verified dead (TTL == -1) via external etcd client.
func TestModule_Embed_Lease_LazyGrantAndReleaseOnLastUnregister(t *testing.T) {
	// ── Arrange ───────────────────────────────────────────────────────────
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, nil)
	ch := subscribeEmbedEvents(t, m)
	extCli := newEmbedClient(t, etcdAddr)

	// ── Phase 1: no lease before any registration ─────────────────────────

	// EventLeaseGranted must NOT fire within 150 ms.
	require.Never(t, func() bool {
		select {
		case env := <-ch:
			return env.Type == modulev2.EventLeaseGranted
		default:
			return false
		}
	}, 150*time.Millisecond, 20*time.Millisecond,
		"no lease must be granted before the first RegisterService call")

	// etcd itself must have zero leases.
	listCtx, listCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer listCancel()
	listResp, err := extCli.Lease.Leases(listCtx)
	require.NoError(t, err, "Lease.Leases must succeed")
	require.Empty(t, listResp.Leases, "etcd must have no leases before any registration")

	// ── Phase 2: first RegisterService → lease granted ────────────────────
	const (
		svc1ID   = uint64(8001)
		svc1Name = "lazy-grant-8001"
		svc2ID   = uint64(8002)
		svc2Name = "lazy-grant-8002"
	)
	svc1Path := fmt.Sprintf("%s/%s-%d", embedByIDPrefix, svc1Name, svc1ID)
	svc2Path := fmt.Sprintf("%s/%s-%d", embedByIDPrefix, svc2Name, svc2ID)

	regCtx, regCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer regCancel()

	h1, err := m.RegisterService(regCtx, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: svc1ID, Name: svc1Name},
		Path:      svc1Path,
		TTL:       10,
	})
	require.NoError(t, err)
	_ = h1.Wait(regCtx)

	grantEnv := waitForEmbedEvent(t, ch, modulev2.EventLeaseGranted, 15*time.Second)
	grantPl, ok := grantEnv.Payload.(modulev2.LeaseGrantedPayload)
	require.True(t, ok, "payload must be LeaseGrantedPayload")
	leaseID := grantPl.LeaseID
	require.NotZero(t, leaseID, "embed etcd must return a non-zero LeaseID")

	// Verify the lease is alive with a positive TTL.
	ttlCtx1, ttlCancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer ttlCancel1()
	ttlResp1, err := extCli.Lease.TimeToLive(ttlCtx1, leaseID)
	require.NoError(t, err, "TimeToLive must not error while lease is active")
	assert.Positive(t, ttlResp1.TTL, "lease TTL must be > 0 while a service is registered")

	// Verify svc1 key actually exists in etcd KV and is attached to the lease.
	kvCtx1, kvCancel1kv := context.WithTimeout(context.Background(), 5*time.Second)
	defer kvCancel1kv()
	getResp1, err := extCli.Get(kvCtx1, svc1Path)
	require.NoError(t, err, "KV.Get for svc1 must succeed")
	require.Len(t, getResp1.Kvs, 1, "svc1 key must exist in etcd KV after registration")
	assert.Equal(t, int64(leaseID), getResp1.Kvs[0].Lease,
		"svc1 KV entry must be attached to the active lease")

	// ── Phase 3: second RegisterService — same lease, no new grant event ──
	h2, err := m.RegisterService(regCtx, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: svc2ID, Name: svc2Name},
		Path:      svc2Path,
		TTL:       10,
	})
	require.NoError(t, err)
	_ = h2.Wait(regCtx)

	// svc2 must appear in a RegistrationChanged event.
	waitForRegChangedWith(t, ch, svc2Path, true, 5*time.Second)

	// Verify svc2 key exists in etcd KV, attached to the same lease.
	kvCtx2, kvCancel2kv := context.WithTimeout(context.Background(), 5*time.Second)
	defer kvCancel2kv()
	getResp2, err := extCli.Get(kvCtx2, svc2Path)
	require.NoError(t, err, "KV.Get for svc2 must succeed")
	require.Len(t, getResp2.Kvs, 1, "svc2 key must exist in etcd KV after registration")
	assert.Equal(t, int64(leaseID), getResp2.Kvs[0].Lease,
		"svc2 KV entry must be attached to the same lease as svc1")

	// ── Phase 4: unregister svc1 — lease must still be alive ──────────────
	require.NoError(t, m.UnregisterService(regCtx, svc1Path))

	// No EventLeaseReleased may arrive while svc2 is still registered.
	require.Never(t, func() bool {
		select {
		case env := <-ch:
			return env.Type == modulev2.EventLeaseReleased
		default:
			return false
		}
	}, 300*time.Millisecond, 20*time.Millisecond,
		"lease must not be released while svc2 is still registered")

	// svc1 key must be gone from etcd KV (Delete was called by RegistrationActor).
	kvCtx3, kvCancel3kv := context.WithTimeout(context.Background(), 5*time.Second)
	defer kvCancel3kv()
	getResp3, err := extCli.Get(kvCtx3, svc1Path)
	require.NoError(t, err, "KV.Get for svc1 after unregister must succeed")
	assert.Empty(t, getResp3.Kvs, "svc1 key must be deleted from etcd KV after unregistration")

	// svc2 key must still exist.
	getResp4, err := extCli.Get(kvCtx3, svc2Path)
	require.NoError(t, err, "KV.Get for svc2 must succeed")
	require.Len(t, getResp4.Kvs, 1, "svc2 key must still exist in etcd KV")

	ttlCtx2, ttlCancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer ttlCancel2()
	ttlResp2, err := extCli.Lease.TimeToLive(ttlCtx2, leaseID)
	require.NoError(t, err)
	assert.Positive(t, ttlResp2.TTL, "lease TTL must remain > 0 while svc2 is registered")

	// ── Phase 5: unregister last service → lease released ─────────────────
	require.NoError(t, m.UnregisterService(regCtx, svc2Path))

	relEnv := waitForEmbedEvent(t, ch, modulev2.EventLeaseReleased, 10*time.Second)
	relPl, ok := relEnv.Payload.(modulev2.LeaseReleasedPayload)
	require.True(t, ok, "payload must be LeaseReleasedPayload")
	assert.Equal(t, leaseID, relPl.LeaseID, "released LeaseID must match the granted LeaseID")

	// svc2 key must also be gone from etcd KV.
	require.Eventually(t, func() bool {
		kvCtx, kvCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer kvCancel()
		resp, respErr := extCli.Get(kvCtx, svc2Path)
		return respErr == nil && len(resp.Kvs) == 0
	}, 5*time.Second, 50*time.Millisecond,
		"svc2 key must be deleted from etcd KV after unregistration")

	// Verify the lease is dead in etcd (TTL == -1 means revoked/expired).
	require.Eventually(t, func() bool {
		ttlCtx3, ttlCancel3 := context.WithTimeout(context.Background(), 2*time.Second)
		defer ttlCancel3()
		resp, respErr := extCli.Lease.TimeToLive(ttlCtx3, leaseID)
		if respErr != nil {
			return true // error also indicates the lease is gone
		}
		return resp.TTL == -1
	}, 5*time.Second, 50*time.Millisecond,
		"lease TTL must be -1 (revoked) after all services are unregistered")

	// Mirror the Phase 1 check: etcd must report zero active leases.
	require.Eventually(t, func() bool {
		listCtx2, listCancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		defer listCancel2()
		resp, respErr := extCli.Lease.Leases(listCtx2)
		return respErr == nil && len(resp.Leases) == 0
	}, 5*time.Second, 50*time.Millisecond,
		"etcd must have zero active leases after all services are unregistered")
}

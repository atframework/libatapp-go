package integration_test

// Checker integration tests backed by a real single-node embedded etcd.
//
// These tests mirror the unit tests in internal/orchestrator/registration_actor_test.go
// (TestRegistrationActor_Checker_*) but exercise the full stack: EtcdModule API →
// RegistrationActor → real etcd KV → checker predicate.
//
// Why embed etcd is required:
//   - mockserver always returns LeaseID=0 → RegistrationActor skips the PUT and
//     therefore never runs the checker.
//   - mockserver Put does not persist data → a pre-seeded etcd key cannot be
//     read back by the checker's Get call.
//
// Mapping to C++ test cases:
//
//   TestModule_Embed_Checker_Conflict          → I.2.4 keepalive_checker_conflict
//   TestModule_Embed_Checker_SameIdentity      → I.2.5 keepalive_checker_same_identity
//   TestModule_Embed_Checker_NoPreexistingKey  → (no numbered case; all three form the
//                                                complete default_checker_t coverage)

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// TestModule_Embed_Checker_Conflict mirrors C++ I.2.4 (keepalive_checker_conflict).
//
// Scenario: a different instance previously wrote the bypath key.
// After the local module has an active lease, RegisterService is called with a
// Checker that expects our own value.  Because the key already holds a foreign
// value the checker rejects the write and handle.Wait returns ErrCheckerConflict.
// The key must remain unchanged — no overwrite must occur.
//
// The test pre-establishes a lease via a warmup registration so that the
// conflicting RegisterService call hits the direct (inline) code path in
// RegistrationActor.onAddDiscovery rather than the deferred replay path.
// This mirrors the unit-test pattern (grantLeaseAndWait → AddDiscovery → err).
func TestModule_Embed_Checker_Conflict(t *testing.T) {
	// ── Arrange ───────────────────────────────────────────────────────────

	etcdAddr := embedEtcdEndpoint(t)
	extCli := newEmbedClient(t, etcdAddr)

	// Pre-seed the conflicting key with a value belonging to a different instance.
	const (
		svcID   = uint64(2401)
		svcName = "checker-conflict-2401"
	)
	svcPath := embedByIDPrefix + "/checker-conflict-2401"
	foreignValue := "foreign-owner-opaque-value"

	preCtx, preCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer preCancel()
	_, err := extCli.Put(preCtx, svcPath, foreignValue)
	require.NoError(t, err, "pre-seed foreign key")

	m := startEmbedModule(t, etcdAddr, nil)
	ch := subscribeEmbedEvents(t, m)

	// Pre-establish the lease with a harmless warmup service so the
	// RegistrationActor has leaseID != 0 before the conflict registration.
	// This exercises the inline checker path rather than the replay path.
	warmupCtx, warmupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer warmupCancel()
	_, _ = m.RegisterService(warmupCtx, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: 9901, Name: "checker-conflict-warmup"},
		Path:      embedByIDPrefix + "/checker-conflict-warmup-9901",
		TTL:       10,
	})
	waitForEmbedEvent(t, ch, modulev2.EventLeaseGranted, 15*time.Second)

	// ── Act ───────────────────────────────────────────────────────────────

	// Lease is now active.  RegisterService with a checker expecting our own
	// value hits onAddDiscovery directly; etcd holds foreignValue → conflict.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	handle, regErr := m.RegisterService(ctx, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: svcID, Name: svcName},
		Path:      svcPath,
		TTL:       10,
		// Explicit checker: pass only when key is absent or matches our own value.
		// foreignValue matches neither condition → ErrCheckerConflict.
		Checker: modulev2.DefaultRegistrationChecker("my-own-expected-value"),
		// TopologyChecker nil → auto-inject; topology key absent → passes.
	})
	require.NoError(t, regErr, "RegisterService enqueue must not fail")
	require.NotNil(t, handle)

	// ── Assert ────────────────────────────────────────────────────────────

	// handle.Wait blocks until RegistrationActor processes the write.
	// With an active lease the checker runs inline and the error is returned
	// without going through the deferred replay cycle.
	waitErr := handle.Wait(ctx)
	require.ErrorIs(t, waitErr, modulev2.ErrCheckerConflict,
		"handle.Wait must return ErrCheckerConflict when checker detects a foreign owner")

	// The key must still hold the foreign value — no overwrite.
	getCtx, getCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer getCancel()
	getResp, err := extCli.Get(getCtx, svcPath)
	require.NoError(t, err)
	require.Len(t, getResp.Kvs, 1, "key must still exist")
	assert.Equal(t, foreignValue, string(getResp.Kvs[0].Value),
		"etcd value must remain the foreign value — RegistrationActor must not overwrite it")
}

// TestModule_Embed_Checker_SameIdentity mirrors C++ I.2.5
// (keepalive_checker_same_identity).
//
// The etcd key already holds the exact JSON that the module would write (a
// restart scenario where the module died and the TTL key survived).  The
// checker recognises its own value and allows the write through.
func TestModule_Embed_Checker_SameIdentity(t *testing.T) {
	// Arrange
	etcdAddr := embedEtcdEndpoint(t)

	const (
		svcID   = uint64(2501)
		svcName = "checker-same-ident-2501"
	)
	svcPath := embedByIDPrefix + "/checker-same-ident-2501"
	disc := &pb.AtappDiscovery{Id: svcID, Name: svcName}

	// Pre-compute the JSON the RegistrationActor would write, matching
	// codec.MarshalDiscoveryToJSON exactly via marshalDiscoveryJSON.
	ownValue := marshalDiscoveryJSON(t, disc)

	extCli := newEmbedClient(t, etcdAddr)
	preCtx, preCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer preCancel()
	_, err := extCli.Put(preCtx, svcPath, ownValue)
	require.NoError(t, err, "pre-seed key with our own JSON (restart simulation)")

	m := startEmbedModule(t, etcdAddr, []string{embedByIDPrefix})
	ch := subscribeEmbedEvents(t, m)

	// Wait for initial snapshot (Watch stream established).
	waitForEmbedEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 15*time.Second)

	// Act: register with NewDiscoveryRegistrationChecker — it builds the same
	// expected value as ownValue, so checker passes on the pre-seeded key.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	handle, regErr := m.RegisterService(ctx, modulev2.ServiceInfo{
		Discovery: disc,
		Path:      svcPath,
		TTL:       10,
		Checker:   modulev2.NewDiscoveryRegistrationChecker(disc),
	})

	// Assert: no error — same-identity restart must succeed.
	require.NoError(t, regErr, "same-identity checker must allow registration")
	require.NotNil(t, handle)

	// Wait for the lease grant and the registration write to propagate.
	waitForEmbedEvent(t, ch, modulev2.EventLeaseGranted, 15*time.Second)

	// The key must be present in etcd (PUT was issued, refreshed with new lease).
	require.Eventually(t, func() bool {
		getCtx, getCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer getCancel()
		resp, err := extCli.Get(getCtx, svcPath)
		if err != nil || len(resp.Kvs) == 0 {
			return false
		}
		return string(resp.Kvs[0].Value) == ownValue
	}, 10*time.Second, 50*time.Millisecond,
		"etcd key must hold our own value after same-identity registration")
}

// TestModule_Embed_Checker_NoPreexistingKey verifies that DefaultRegistrationChecker
// allows fresh registration when the key does not yet exist in etcd.
func TestModule_Embed_Checker_NoPreexistingKey(t *testing.T) {
	// Arrange
	etcdAddr := embedEtcdEndpoint(t)

	const (
		svcID   = uint64(2601)
		svcName = "checker-fresh-2601"
	)
	svcPath := embedByIDPrefix + "/checker-fresh-2601"
	disc := &pb.AtappDiscovery{Id: svcID, Name: svcName}

	m := startEmbedModule(t, etcdAddr, []string{embedByIDPrefix})
	ch := subscribeEmbedEvents(t, m)

	waitForEmbedEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 15*time.Second)

	// Act: register with checker; key does not exist → checker must pass.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	handle, regErr := m.RegisterService(ctx, modulev2.ServiceInfo{
		Discovery: disc,
		Path:      svcPath,
		TTL:       10,
		Checker:   modulev2.NewDiscoveryRegistrationChecker(disc),
	})

	// Assert: no error, write succeeds.
	require.NoError(t, regErr, "fresh-key checker must allow registration")
	require.NotNil(t, handle)

	// Node must appear in the watch-driven snapshot via the Watch stream.
	waitForEmbedEvent(t, ch, modulev2.EventWatchNodeUp, 15*time.Second)
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		if snap == nil {
			return false
		}
		_, ok := snap.Discovery.NodesByPath[svcPath]
		return ok
	}, 5*time.Second, 20*time.Millisecond,
		"snapshot must contain the node at %s after fresh registration", svcPath)
}

package integration_test

// Embed-etcd test helpers.
//
// Use these instead of mockserver when a test requires:
//   - non-zero real LeaseIDs  (EventLeaseReleased, onAddDiscovery PUT path)
//   - real Watch streams that deliver incremental KV events
//   - KV persistence within a test (Put then Get returns the value)

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/stretchr/testify/require"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// ── Low-level embed etcd helpers ──────────────────────────────────────────

// embedFreePort allocates and immediately releases a free TCP port on loopback.
func embedFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// embedEtcdEndpoint starts a single-node embedded etcd on random loopback ports
// and returns the client endpoint URL string (e.g. "http://127.0.0.1:NNNNN").
// The server is stopped via t.Cleanup.
func embedEtcdEndpoint(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	clientPort := embedFreePort(t)
	peerPort := embedFreePort(t)

	clientURLStr := fmt.Sprintf("http://127.0.0.1:%d", clientPort)
	peerURLStr := fmt.Sprintf("http://127.0.0.1:%d", peerPort)

	clientURL, err := url.Parse(clientURLStr)
	require.NoError(t, err)
	peerURL, err := url.Parse(peerURLStr)
	require.NoError(t, err)

	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.LogLevel = "panic" // suppress graceful-shutdown noise from embed server
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = fmt.Sprintf("default=%s", peerURLStr)

	e, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	t.Cleanup(e.Close)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(20 * time.Second):
		t.Fatal("embedded etcd did not become ready within 20s")
	}

	return clientURLStr
}

// newEmbedClient creates a *clientv3.Client connected to addr.
// The client is closed via t.Cleanup.
// Use for external data-injection clients.  When passing a client to
// EtcdModule.NewEtcdModule, use startEmbedModule instead — the module's
// Stop() calls client.Close() and must not double-close.
func newEmbedClient(t *testing.T, addr string) *clientv3.Client {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// startEmbedModule creates and starts an EtcdModule backed by the embedded
// etcd at addr.  The module is stopped via t.Cleanup.
//
// The module creates and owns an internal client; do NOT create a separate
// client with t.Cleanup for the same use — that would double-close.
// For external data injection into the same etcd, use newEmbedClient(t, addr).
func startEmbedModule(t *testing.T, addr string, watchPrefixes []string) *modulev2.EtcdModule {
	t.Helper()

	// Dedicated client owned by the module; module.Stop() calls client.Close().
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)

	cfg := modulev2.PathConfig{
		ByIDPrefix:     embedByIDPrefix,
		ByNamePrefix:   embedByNamePrefix,
		TopologyPrefix: embedTopoPrefix,
		WatchPrefixes:  watchPrefixes,
		LeaseTTL:       10,
	}
	m := modulev2.NewEtcdModule(cli, cfg, modulev2.ModuleOptions{
		RetryInterval: 100 * time.Millisecond,
	})
	require.NoError(t, m.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m.Stop(ctx)
	})
	return m
}

// ── High-level fixture helper ─────────────────────────────────────────────

// DiscoveryFixture pairs a key path with the discovery value to PUT at that path.
type DiscoveryFixture struct {
	Path      string
	Discovery *pb.AtappDiscovery
}

// makeEmbedNodesModule starts a single-node embedded etcd, creates and starts
// an EtcdModule watching watchPrefix, and pre-populates the snapshot by
// PUT-ing each fixture node via an external client.
//
// The function returns only after all nodes are visible in the snapshot, so
// callers can immediately use routing/query APIs without further polling.
//
// For empty-snapshot tests pass nodes = nil.
//
// opts is merged with {RetryInterval: 100ms} if RetryInterval is zero.
func makeEmbedNodesModule(
	t *testing.T,
	watchPrefix string,
	nodes []DiscoveryFixture,
	opts modulev2.ModuleOptions,
) *modulev2.EtcdModule {
	t.Helper()

	etcdAddr := embedEtcdEndpoint(t)

	if opts.RetryInterval == 0 {
		opts.RetryInterval = 100 * time.Millisecond
	}

	// Module-owned client; Stop() closes it.
	modCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdAddr},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)

	cfg := modulev2.PathConfig{
		ByIDPrefix:    watchPrefix,
		WatchPrefixes: []string{watchPrefix},
		LeaseTTL:      10,
	}
	m := modulev2.NewEtcdModule(modCli, cfg, opts)

	require.NoError(t, m.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m.Stop(ctx)
	})

	// Wait for the initial Watch-Get snapshot to complete.
	// GetSnapshot() returns non-nil only after EventWatchSnapshotLoaded is
	// processed by the ProjectionActor, so polling it avoids any subscription
	// timing race around startup.
	require.Eventually(t, func() bool {
		return m.GetSnapshot() != nil
	}, 15*time.Second, 20*time.Millisecond, "timed out waiting for initial snapshot")

	if len(nodes) == 0 {
		return m
	}

	// Inject nodes via a separate external client (does not own the module client).
	extCli := newEmbedClient(t, etcdAddr)
	putCtx, putCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer putCancel()
	for _, n := range nodes {
		val := marshalDiscoveryJSON(t, n.Discovery)
		_, err := extCli.Put(putCtx, n.Path, val)
		require.NoError(t, err, "PUT fixture node at %s", n.Path)
	}

	// Wait for all nodes to be reflected in the snapshot.
	require.Eventually(t, func() bool {
		snap := m.GetSnapshot()
		return snap != nil && len(snap.Discovery.NodesByPath) >= len(nodes)
	}, 5*time.Second, 20*time.Millisecond,
		"snapshot must contain all %d fixture nodes", len(nodes))

	return m
}

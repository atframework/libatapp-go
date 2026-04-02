package modulev2

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/consistenthash"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/orchestrator"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/snapshot"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// EtcdModule is the Facade (Layer 4) that wires together the four actors and
// exposes a clean, context-aware API to the application layer.
//
// Lifecycle:
//
//	module := NewEtcdModule(etcdClient, pathCfg, opts)
//	module.Start(ctx)
//	...
//	module.Stop(ctx)
type EtcdModule struct {
	mu         sync.Mutex
	state      moduleState
	cancelFunc context.CancelFunc

	cfg    PathConfig
	opts   ModuleOptions
	client EtcdClient

	bus        runtime.EventBus
	rt         *runtime.ModuleActorRuntime
	leaseActor *orchestrator.LeaseActor
	regActor   *orchestrator.RegistrationActor
	watchActor *orchestrator.WatchActor
	projActor  *orchestrator.ProjectionActor

	// rrCounters holds per-filter round-robin counters.
	// sync.Map is lock-free on the read path once a key is populated.
	rrCounters sync.Map // map[string]*atomic.Uint64
}

// moduleState tracks the facade lifecycle.
type moduleState uint8

const (
	moduleStateIdle    moduleState = iota
	moduleStateRunning             // 1
	moduleStateStopped             // 2
)

// ErrNotRunning is returned when an API is called on a stopped module.
var ErrNotRunning = errors.New("etcd_module_v2: module is not running")

// ── Constructor ───────────────────────────────────────────────────────────

// NewEtcdModule creates an EtcdModule.  Call Start to activate it.
func NewEtcdModule(client EtcdClient, cfg PathConfig, opts ModuleOptions) *EtcdModule {
	return &EtcdModule{
		cfg:    cfg.Validate(),
		opts:   opts,
		client: client,
	}
}

// ── Lifecycle ─────────────────────────────────────────────────────────────

// Start wires up the actors and launches all goroutines.  ctx governs the
// entire run; cancelling it is equivalent to calling Stop.
func (m *EtcdModule) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.state != moduleStateIdle {
		return errors.New("etcd_module_v2: already started or stopped")
	}

	runCtx, cancel := context.WithCancel(ctx)
	m.cancelFunc = cancel

	m.bus = runtime.NewEventBus()
	m.rt = runtime.NewModuleRuntime()

	// Set default retry interval.
	if m.opts.RetryInterval <= 0 {
		m.opts.RetryInterval = 3 * time.Second
	}

	// ── Wire actors ───────────────────────────────────────────────────
	m.leaseActor = orchestrator.NewLeaseActor(m.client, m.bus)
	m.regActor = orchestrator.NewRegistrationActor(
		m.client, m.bus,
		m.cfg.ByNamePrefix, m.cfg.ByIDPrefix, m.cfg.TopologyPrefix,
	)
	m.watchActor = orchestrator.NewWatchActor(m.client, m.bus)
	m.projActor = orchestrator.NewProjectionActor(m.bus, m.opts.OnSnapshotPublished)

	// ── Spawn goroutines ──────────────────────────────────────────────
	m.rt.Spawn(runCtx, m.leaseActor.Run)
	m.rt.Spawn(runCtx, m.regActor.Run)
	m.rt.Spawn(runCtx, m.watchActor.Run)
	m.rt.Spawn(runCtx, m.projActor.Run)

	// ── Register watch prefixes ───────────────────────────────────────
	for _, prefix := range m.cfg.WatchPrefixes {
		m.watchActor.AddPrefix(prefix)
	}

	// ── Start lease acquisition ───────────────────────────────────────
	m.leaseActor.Start(m.cfg.LeaseTTL)

	// ── Periodic Tick goroutine (lease retry) ─────────────────────────
	m.rt.Spawn(runCtx, func(ctx context.Context) {
		ticker := time.NewTicker(m.opts.RetryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.leaseActor.Tick()
			}
		}
	})

	m.state = moduleStateRunning
	return nil
}

// Stop gracefully shuts down the module – revokes the lease and waits for all
// goroutines to return.
func (m *EtcdModule) Stop(ctx context.Context) error {
	m.mu.Lock()
	if m.state != moduleStateRunning {
		m.mu.Unlock()
		return ErrNotRunning
	}
	cancel := m.cancelFunc
	m.state = moduleStateStopped
	m.mu.Unlock() // release before blocking ops

	var stopErr error

	// Ask LeaseActor to revoke first (best-effort).
	if m.leaseActor != nil {
		select {
		case err := <-m.leaseActor.Stop(ctx):
			if err != nil {
				stopErr = fmt.Errorf("lease revoke error: %w", err)
			}
		case <-ctx.Done():
			return fmt.Errorf("stop context cancelled during lease revoke: %w", ctx.Err())
		}
	}

	// Cancel the run context so all actors exit.
	if cancel != nil {
		cancel()
	}

	// Wait for every goroutine to finish.
	done := make(chan struct{})
	go func() {
		if m.rt != nil {
			m.rt.Wait()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		// Context cancelled, but goroutine continues (acceptable for graceful shutdown)
		// Return error to signal timeout, but cleanup continues asynchronously.
		return fmt.Errorf("stop context cancelled while waiting for goroutines: %w", ctx.Err())
	}

	// Close event bus.
	if m.bus != nil {
		m.bus.Close()
	}

	if m.client != nil {
		if err := m.client.Close(); err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("close etcd client error: %w", err))
		}
	}

	return stopErr
}

// Tick delivers a retry signal to the lease actor.  Call periodically from
// the application main loop if you are not using the built-in ticker.
func (m *EtcdModule) Tick() {
	m.mu.Lock()
	running := m.state == moduleStateRunning
	m.mu.Unlock()
	if running {
		m.leaseActor.Tick()
	}
}

// ── PathConfig accessor ───────────────────────────────────────────────────

// GetPathConfig returns the path configuration this module was created with.
// Callers should use this to avoid independently re-deriving the same paths.
func (m *EtcdModule) GetPathConfig() PathConfig {
	return m.cfg
}

// ── Snapshot API ──────────────────────────────────────────────────────────

// GetSnapshot returns the latest atomically-published ExportSnapshot.
// May return nil before the first snapshot is published.
func (m *EtcdModule) GetSnapshot() *snapshot.ExportSnapshot {
	if m.projActor == nil {
		return nil
	}
	return m.projActor.GetSnapshot()
}

// ── Service registration API ──────────────────────────────────────────────

// RegisterService submits a service registration request and returns a
// RegistrationHandle.  The handle's Wait method blocks until the write
// completes.
func (m *EtcdModule) RegisterService(ctx context.Context, svc ServiceInfo) (*RegistrationHandle, error) {
	m.mu.Lock()
	running := m.state == moduleStateRunning
	m.mu.Unlock()
	if !running {
		return nil, ErrNotRunning
	}

	ttl := svc.TTL
	if ttl <= 0 {
		ttl = m.cfg.LeaseTTL
	}

	var discoveryDone <-chan error
	var topologyDone <-chan error

	// Registration order: Topology MUST be written before Discovery.
	// Remote peers identify upstream connectivity via the Topology record;
	// writing Discovery first would make the node visible before its topology
	// context is established, causing transient routing failures.
	topologyInfo := svc.TopologyInfo
	if topologyInfo == nil {
		topologyInfo = deriveTopologyInfoFromDiscovery(svc.Discovery)
	}
	if topologyInfo != nil {
		topologyDone = m.regActor.AddTopology(ctx, topologyInfo, svc.Path, ttl)
	}

	if svc.Discovery != nil {
		discoveryDone = m.regActor.AddDiscovery(ctx, svc.Discovery, svc.Path, ttl)
	}

	doneCh := make(chan error, 1)
	go func() {
		// Wait for Topology write to complete before declaring Discovery done,
		// preserving the Topology-first ordering guarantee end-to-end.
		if topologyDone != nil {
			if err := <-topologyDone; err != nil {
				doneCh <- err
				return
			}
		}
		if discoveryDone != nil {
			if err := <-discoveryDone; err != nil {
				doneCh <- err
				return
			}
		}
		doneCh <- nil
	}()

	registrationCtx, registrationCancel := context.WithCancel(ctx)
	handle := &RegistrationHandle{
		path:       svc.Path,
		cancelFunc: registrationCancel,
		doneCh:     doneCh,
	}
	// registrationCtx is held by the caller; they cancel it to signal intent
	// to unregister (to be wired in a future iteration).
	_ = registrationCtx

	return handle, nil
}

func deriveTopologyInfoFromDiscovery(info *pb.AtappDiscovery) *pb.AtappTopologyInfo {
	if info == nil || info.GetName() == "" || info.GetId() == 0 {
		return nil
	}
	return &pb.AtappTopologyInfo{
		Id:       info.GetId(),
		Name:     info.GetName(),
		Hostname: info.GetHostname(),
		Pid:      info.GetPid(),
		Identity: info.GetIdentity(),
		Version:  info.GetVersion(),
	}
}

// UnregisterService removes the service at path.
func (m *EtcdModule) UnregisterService(ctx context.Context, path string) error {
	m.mu.Lock()
	running := m.state == moduleStateRunning
	m.mu.Unlock()
	if !running {
		return ErrNotRunning
	}

	ch := m.regActor.RemoveService(ctx, path)
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ── Watch prefix API ──────────────────────────────────────────────────────

// AddWatchPrefix registers an additional watch prefix at runtime.
func (m *EtcdModule) AddWatchPrefix(prefix string) error {
	m.mu.Lock()
	running := m.state == moduleStateRunning
	m.mu.Unlock()
	if !running {
		return ErrNotRunning
	}
	m.watchActor.AddPrefix(prefix)
	return nil
}

// RemoveWatchPrefix stops watching the given prefix.
func (m *EtcdModule) RemoveWatchPrefix(prefix string) error {
	m.mu.Lock()
	running := m.state == moduleStateRunning
	m.mu.Unlock()
	if !running {
		return ErrNotRunning
	}
	m.watchActor.RemovePrefix(prefix)
	return nil
}

// ReloadWatchStreams cancels every active watch stream and restarts them from
// the latest etcd revision.  Use this after a detected network partition or a
// forced reconnect to guarantee all registered prefixes re-sync.
func (m *EtcdModule) ReloadWatchStreams() error {
	m.mu.Lock()
	running := m.state == moduleStateRunning
	m.mu.Unlock()
	if !running {
		return ErrNotRunning
	}
	m.watchActor.ActiveAll()
	return nil
}

// UpdateEndpoints hot-replaces the etcd endpoint list on the running client
// and restarts all active watch streams so they re-establish on the new
// endpoints.  The existing lease is not revoked.
// Returns ErrNotRunning if the module is not in the running state.
func (m *EtcdModule) UpdateEndpoints(endpoints []string) error {
	m.mu.Lock()
	running := m.state == moduleStateRunning
	m.mu.Unlock()
	if !running {
		return ErrNotRunning
	}
	m.client.SetEndpoints(endpoints...)
	m.watchActor.ActiveAll()
	return nil
}

// ── EventBus subscription API ─────────────────────────────────────────────

// Subscribe registers a handler for ALL events published on the internal bus.
// Returns an EventHandle that can be passed to Unsubscribe.
func (m *EtcdModule) Subscribe(handler runtime.EventHandler) EventHandle {
	if m.bus == nil {
		return 0
	}
	return m.bus.Subscribe(handler)
}

// SubscribeType registers a handler for a specific EventType only.
func (m *EtcdModule) SubscribeType(evType runtime.EventType, handler runtime.EventHandler) EventHandle {
	if m.bus == nil {
		return 0
	}
	return m.bus.SubscribeType(evType, handler)
}

// Unsubscribe removes a previously registered handler.
func (m *EtcdModule) Unsubscribe(handle EventHandle) {
	if m.bus != nil {
		m.bus.Unsubscribe(handle)
	}
}

// ── Topology SyncTopology / FlushTopology ─────────────────────────────────

// SyncTopology requests a non-urgent topology keepalive flush.
func (m *EtcdModule) SyncTopology() {
	m.mu.Lock()
	running := m.state == moduleStateRunning
	m.mu.Unlock()
	if running {
		m.regActor.SyncTopology()
	}
}

// FlushTopology flushes the topology keepalive and waits for completion.
func (m *EtcdModule) FlushTopology(ctx context.Context) error {
	m.mu.Lock()
	running := m.state == moduleStateRunning
	m.mu.Unlock()
	if !running {
		return ErrNotRunning
	}
	return m.regActor.FlushTopology(ctx)
}

// ── Consistent-hash routing API ──────────────────────────────────────────

// GetNodeByID returns one node matching id from the current topology snapshot.
// Returns nil when no node with the given id exists.
func (m *EtcdModule) GetNodeByID(id uint64) *DiscoveryNode {
	if id == 0 {
		return nil
	}
	snap := m.GetSnapshot()
	if snap == nil {
		return nil
	}
	return snap.Discovery.GetNodeByID(id)
}

// GetNodeByName returns one node matching name from the current topology
// snapshot. Returns nil when no node with the given name exists.
func (m *EtcdModule) GetNodeByName(name string) *DiscoveryNode {
	if name == "" {
		return nil
	}
	snap := m.GetSnapshot()
	if snap == nil {
		return nil
	}
	return snap.Discovery.GetNodeByName(name)
}

// GetNodeByRoundRobin returns one node by round-robin from filtered candidates.
func (m *EtcdModule) GetNodeByRoundRobin(filter map[string]string) (*DiscoveryNode, error) {
	snap := m.GetSnapshot()
	if snap == nil {
		return nil, fmt.Errorf("no service nodes available for the given filter")
	}

	candidates := m.getFilteredDiscoveryNodes(snap, filter)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no service nodes available for the given filter")
	}

	sort.Slice(candidates, func(i, j int) bool {
		return lessRoundRobinNode(candidates[i], candidates[j])
	})

	idx := m.nextRoundRobinIndex(filterCacheKey(filter), len(candidates))
	return candidates[idx], nil
}

// GetNodeByRandom returns one node randomly from filtered candidates.
func (m *EtcdModule) GetNodeByRandom(filter map[string]string) (*DiscoveryNode, error) {
	snap := m.GetSnapshot()
	candidates := m.getFilteredDiscoveryNodes(snap, filter)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no service nodes available for the given filter")
	}
	idx := rand.Intn(len(candidates))
	return candidates[idx], nil
}

// GetNodeByConsistentHash returns one node chosen by consistent hash from the
// current topology snapshot.  filter supports the same metadata semantics as
// v1 discovery set: built-in keys (api_version/kind/group/name/namespace/
// namespace_name/uid/service_subset) and metadata.labels fallback.
func (m *EtcdModule) GetNodeByConsistentHash(
	key string,
	filter map[string]string,
	mode pb.EtcdSearchMode,
) (*DiscoveryNode, error) {
	nodes, err := m.GetNodesByConsistentHash(key, 1, filter, mode)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("consistent hashing returned no members")
	}
	return nodes[0], nil
}

// GetNodesByConsistentHash returns up to n nodes chosen by consistent hash
// from the current topology snapshot.
func (m *EtcdModule) GetNodesByConsistentHash(
	key string,
	n int,
	filter map[string]string,
	mode pb.EtcdSearchMode,
) ([]*DiscoveryNode, error) {
	if n <= 0 {
		return []*DiscoveryNode{}, nil
	}

	snap := m.GetSnapshot()
	if snap == nil || len(snap.Discovery.NodesByPath) == 0 {
		return nil, fmt.Errorf("no service nodes available for the given filter")
	}

	candidates := make([]*DiscoveryNode, 0, len(snap.Discovery.NodesByPath))
	for _, node := range snap.Discovery.NodesByPath {
		if node == nil || node.Info == nil {
			continue
		}
		if !matchesDiscoveryFilter(node.Info, filter) {
			continue
		}
		candidates = append(candidates, node)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no service nodes available for the given filter")
	}

	vnodes := m.opts.ConsistentHashVirtualNodes
	ring := consistenthash.NewRing(vnodes)
	memberToNode := make(map[string]*DiscoveryNode, len(candidates))
	for _, node := range candidates {
		member := nodeMemberKey(node.Info, node.Path)
		memberToNode[member] = node
		ring.Add(member)
	}

	members, err := ring.GetN(key, n, mode)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("consistent hashing returned no members")
	}

	result := make([]*DiscoveryNode, 0, len(members))
	for _, member := range members {
		if node := memberToNode[member]; node != nil {
			result = append(result, node)
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("consistent hashing returned no members")
	}
	return result, nil
}

func nodeMemberKey(info *pb.AtappDiscovery, fallback string) string {
	if info == nil {
		return fallback
	}
	if info.GetName() != "" || info.GetId() != 0 {
		return fmt.Sprintf("%s-%d", info.GetName(), info.GetId())
	}
	return fallback
}

// nextRoundRobinIndex returns the next index for the given filter key.
// Lock-free on the hot path: sync.Map read + atomic increment.
func (m *EtcdModule) nextRoundRobinIndex(key string, size int) int {
	if size <= 1 {
		return 0
	}
	var counter *atomic.Uint64
	if v, ok := m.rrCounters.Load(key); ok {
		counter = v.(*atomic.Uint64)
	} else {
		n := new(atomic.Uint64)
		actual, _ := m.rrCounters.LoadOrStore(key, n)
		counter = actual.(*atomic.Uint64)
	}
	return int((counter.Add(1) - 1) % uint64(size))
}

func (m *EtcdModule) getFilteredDiscoveryNodes(snap *snapshot.ExportSnapshot, filter map[string]string) []*DiscoveryNode {
	if snap == nil || len(snap.Discovery.NodesByPath) == 0 {
		return nil
	}

	result := make([]*DiscoveryNode, 0, len(snap.Discovery.NodesByPath))
	for _, node := range snap.Discovery.NodesByPath {
		if node == nil || node.Info == nil {
			continue
		}
		if !matchesDiscoveryFilter(node.Info, filter) {
			continue
		}
		result = append(result, node)
	}
	return result
}

func filterCacheKey(filter map[string]string) string {
	if len(filter) == 0 {
		return "*"
	}

	keys := make([]string, 0, len(filter))
	for k := range filter {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	b := strings.Builder{}
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(filter[k])
	}
	return b.String()
}

func lessRoundRobinNode(left, right *DiscoveryNode) bool {
	if left == nil || right == nil {
		if left == right {
			return false
		}
		return left == nil
	}

	leftInfo := left.Info
	rightInfo := right.Info
	if leftInfo != nil && rightInfo != nil {
		leftPod := int32(0)
		rightPod := int32(0)
		if leftInfo.GetRuntime() != nil {
			leftPod = leftInfo.GetRuntime().GetStatefulPodIndex()
		}
		if rightInfo.GetRuntime() != nil {
			rightPod = rightInfo.GetRuntime().GetStatefulPodIndex()
		}
		if leftPod != rightPod {
			return leftPod < rightPod
		}
		if leftInfo.GetId() != rightInfo.GetId() {
			return leftInfo.GetId() < rightInfo.GetId()
		}
		if leftInfo.GetName() != rightInfo.GetName() {
			return leftInfo.GetName() < rightInfo.GetName()
		}
	}

	return left.Path < right.Path
}
func matchesDiscoveryFilter(info *pb.AtappDiscovery, filter map[string]string) bool {
	if len(filter) == 0 {
		return true
	}
	if info == nil {
		return false
	}
	metadata := info.GetMetadata()
	for k, v := range filter {
		if v == "" {
			continue
		}
		if !matchesDiscoveryFilterKey(metadata, k, v) {
			return false
		}
	}
	return true
}

func matchesDiscoveryFilterKey(metadata *pb.AtappMetadata, key string, value string) bool {
	if metadata == nil {
		return false
	}
	switch key {
	case "api_version":
		return metadata.GetApiVersion() == value
	case "kind":
		return metadata.GetKind() == value
	case "group":
		return metadata.GetGroup() == value
	case "name":
		return metadata.GetName() == value
	case "namespace", "namespace_name":
		return metadata.GetNamespaceName() == value
	case "uid":
		return metadata.GetUid() == value
	case "service_subset":
		return metadata.GetServiceSubset() == value
	default:
		labels := metadata.GetLabels()
		if labels == nil {
			return false
		}
		nodeVal, ok := labels[key]
		return ok && nodeVal == value
	}
}

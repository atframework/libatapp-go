package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"time"

	log "log/slog"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/codec"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/pathbuilder"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/snapshot"
)

// ── Message types (sealed interface) ─────────────────────────────────────

// regMsg is the sealed interface for all RegistrationActor mailbox messages.
type regMsg interface{ regMsgKind() }

// RegMsgType enumerates message kinds for logging and metrics.
type RegMsgType uint8

const (
	RegMsgLeaseGranted    RegMsgType = iota + 1
	RegMsgLeaseExpired               // 2
	RegMsgAddDiscovery               // 3
	RegMsgAddTopology                // 4
	RegMsgRemoveService              // 5
	RegMsgSyncTopology               // 6
	RegMsgFlushTopology              // 7
	RegMsgReplayDiscovery            // 8 — internal: replay stale discovery registration
	RegMsgReplayTopology             // 9 — internal: replay stale topology registration
)

// Name returns a human-readable label.
func (t RegMsgType) Name() string {
	switch t {
	case RegMsgLeaseGranted:
		return "RegMsgLeaseGranted"
	case RegMsgLeaseExpired:
		return "RegMsgLeaseExpired"
	case RegMsgAddDiscovery:
		return "RegMsgAddDiscovery"
	case RegMsgAddTopology:
		return "RegMsgAddTopology"
	case RegMsgRemoveService:
		return "RegMsgRemoveService"
	case RegMsgSyncTopology:
		return "RegMsgSyncTopology"
	case RegMsgFlushTopology:
		return "RegMsgFlushTopology"
	case RegMsgReplayDiscovery:
		return "RegMsgReplayDiscovery"
	case RegMsgReplayTopology:
		return "RegMsgReplayTopology"
	default:
		return "RegMsgUnknown"
	}
}

// Individual message structs.
type regMsgLeaseGranted struct {
	LeaseID    clientv3.LeaseID
	LeaseEpoch uint64
}
type regMsgLeaseExpired struct{}
type regMsgAddDiscovery struct {
	Info  *pb.AtappDiscovery
	Path  string
	TTL   int64
	Reply chan<- error
}
type regMsgAddTopology struct {
	TopologyInfo *pb.AtappTopologyInfo
	ServicePath  string
	TTL          int64
	Reply        chan<- error
}
type regMsgRemoveService struct {
	Path  string
	Reply chan<- error
}
type regMsgSyncTopology struct{}
type regMsgFlushTopology struct {
	Ctx   context.Context
	Reply chan<- error
}
type regMsgReplayDiscovery struct {
	ServiceKey string
}
type regMsgReplayTopology struct {
	TopologyKey string
}

func (regMsgLeaseGranted) regMsgKind()    {}
func (regMsgLeaseExpired) regMsgKind()    {}
func (regMsgAddDiscovery) regMsgKind()    {}
func (regMsgAddTopology) regMsgKind()     {}
func (regMsgRemoveService) regMsgKind()   {}
func (regMsgSyncTopology) regMsgKind()    {}
func (regMsgFlushTopology) regMsgKind()   {}
func (regMsgReplayDiscovery) regMsgKind() {}
func (regMsgReplayTopology) regMsgKind()  {}

// ── Internal state ────────────────────────────────────────────────────────

// discoveryRegistrationEntry tracks desired/actual state for discovery paths.
type discoveryRegistrationEntry struct {
	info       *pb.AtappDiscovery
	path       string // etcd write path (by-path key; by-name/id derived from info)
	ttl        int64
	desired    bool // the operator wants this service registered
	registered bool // successfully written to etcd in current lease epoch
	stale      bool // lease expired; needs replaying
	retryCount int
	lastError  error
	updatedAt  time.Time
}

// topologyRegistrationEntry tracks desired/actual state for topology keepalive keys.
type topologyRegistrationEntry struct {
	info       *pb.AtappTopologyInfo
	key        string
	ttl        int64
	desired    bool
	registered bool
	stale      bool
	retryCount int
	lastError  error
	updatedAt  time.Time
}

// registrationActorState is all mutable state owned by the RegistrationActor's
// run goroutine.  No locking required.
type registrationActorState struct {
	leaseID            clientv3.LeaseID
	leaseEpoch         uint64
	discoveryServices  map[string]discoveryRegistrationEntry // key = service path
	topologyServices   map[string]topologyRegistrationEntry  // key = topology key
	serviceTopologyKey map[string]string                     // service path -> topology key
}

// ── RegistrationActor ────────────────────────────────────────────────────

// RegistrationActor is responsible for the write model: it drives
//   - bypath / byname / byid / topology  registration to etcd
//   - lease-epoch-aware replay after every rebuild
//
// It does NOT maintain any read model (ExportSnapshot); that is
// ProjectionActor's exclusive responsibility.
//
// Mailbox capacity: 64 (service add/remove can burst briefly).
type RegistrationActor struct {
	runtime.ActorBase[regMsg]

	etcdClient EtcdClient
	eventBus   runtime.EventBus

	// pathBuilders are injected by the EtcdModule facade.
	byNamePrefix   string // e.g. "/services/name/"
	byIDPrefix     string // e.g. "/services/id/"
	discoveryBase  string // e.g. "/services"
	topologyPrefix string // e.g. "/topology/"

	st registrationActorState
}

// NewRegistrationActor constructs a RegistrationActor.
func NewRegistrationActor(
	etcdClient EtcdClient,
	bus runtime.EventBus,
	byNamePrefix, byIDPrefix, topologyPrefix string,
) *RegistrationActor {
	a := &RegistrationActor{
		ActorBase:      runtime.NewActorBase[regMsg](64),
		etcdClient:     etcdClient,
		eventBus:       bus,
		byNamePrefix:   byNamePrefix,
		byIDPrefix:     byIDPrefix,
		discoveryBase:  deriveDiscoveryBasePrefix(byNamePrefix, byIDPrefix),
		topologyPrefix: topologyPrefix,
	}
	a.st.discoveryServices = make(map[string]discoveryRegistrationEntry)
	a.st.topologyServices = make(map[string]topologyRegistrationEntry)
	a.st.serviceTopologyKey = make(map[string]string)
	return a
}

// ── External API (goroutine-safe) ─────────────────────────────────────────

// AddDiscovery requests discovery registration for the given service.
// Returns an error channel that receives nil on success or the first write
// error.  Caller must read from the channel exactly once.
func (a *RegistrationActor) AddDiscovery(
	ctx context.Context,
	info *pb.AtappDiscovery,
	path string,
	ttl int64,
) <-chan error {
	ch := make(chan error, 1)
	if info == nil {
		ch <- nil
		return ch
	}
	msg := regMsgAddDiscovery{Info: info, Path: path, TTL: ttl, Reply: ch}
	if path == "" {
		ch <- fmt.Errorf("empty discovery path")
		return ch
	}
	if err := a.PostCtx(ctx, msg); err != nil {
		ch <- err
		return ch
	}
	return ch
}

// AddTopology requests topology registration for the given service.
func (a *RegistrationActor) AddTopology(
	ctx context.Context,
	topologyInfo *pb.AtappTopologyInfo,
	servicePath string,
	ttl int64,
) <-chan error {
	ch := make(chan error, 1)
	if topologyInfo == nil {
		ch <- nil
		return ch
	}
	msg := regMsgAddTopology{TopologyInfo: topologyInfo, ServicePath: servicePath, TTL: ttl, Reply: ch}
	if err := a.PostCtx(ctx, msg); err != nil {
		ch <- err
		return ch
	}
	return ch
}

// RemoveService requests removal of the service at path.
func (a *RegistrationActor) RemoveService(ctx context.Context, path string) <-chan error {
	ch := make(chan error, 1)
	msg := regMsgRemoveService{Path: path, Reply: ch}
	if err := a.PostCtx(ctx, msg); err != nil {
		ch <- err
		return ch
	}
	return ch
}

// SyncTopology requests a non-urgent topology keepalive flush.
func (a *RegistrationActor) SyncTopology() {
	a.Post(regMsgSyncTopology{})
}

// FlushTopology requests an urgent topology flush and waits for the result via ctx.
func (a *RegistrationActor) FlushTopology(ctx context.Context) error {
	ch := make(chan error, 1)
	if err := a.PostCtx(ctx, regMsgFlushTopology{Ctx: ctx, Reply: ch}); err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run is the actor's event loop; launch via ModuleActorRuntime.Spawn.
func (a *RegistrationActor) Run(ctx context.Context) {
	// Subscribe to lease events from the EventBus.
	hLease := a.eventBus.Subscribe(func(env runtime.EventEnvelope) {
		switch env.Type {
		case runtime.EventLeaseGranted:
			pl := env.Payload.(LeaseGrantedPayload)
			a.Post(regMsgLeaseGranted{LeaseID: pl.LeaseID, LeaseEpoch: env.LeaseEpoch})
		case runtime.EventLeaseExpired:
			a.Post(regMsgLeaseExpired{})
		}
	})
	defer a.eventBus.Unsubscribe(hLease)

	a.RunLoop(ctx, a.handle)
}

// ── Message handlers ──────────────────────────────────────────────────────

func (a *RegistrationActor) handle(msg regMsg) {
	switch m := msg.(type) {
	case regMsgLeaseGranted:
		a.onLeaseGranted(m)
	case regMsgLeaseExpired:
		a.onLeaseExpired()
	case regMsgAddDiscovery:
		a.onAddDiscovery(m)
	case regMsgAddTopology:
		a.onAddTopology(m)
	case regMsgRemoveService:
		a.onRemoveService(m)
	case regMsgSyncTopology:
		a.onSyncTopology()
	case regMsgFlushTopology:
		a.onFlushTopology(m)
	case regMsgReplayDiscovery:
		a.onReplayDiscovery(m)
	case regMsgReplayTopology:
		a.onReplayTopology(m)
	}
}

func (a *RegistrationActor) onLeaseExpired() {
	// Mark all services as stale; etcd TTL will GC the keys automatically.
	for key, svc := range a.st.discoveryServices {
		svc.registered = false
		svc.stale = true
		a.st.discoveryServices[key] = svc
	}
	for key, svc := range a.st.topologyServices {
		svc.registered = false
		svc.stale = true
		a.st.topologyServices[key] = svc
	}
	a.st.leaseID = 0
}

func (a *RegistrationActor) onLeaseGranted(msg regMsgLeaseGranted) {
	a.st.leaseID = msg.LeaseID
	a.st.leaseEpoch = msg.LeaseEpoch
	// Enqueue a replay message for every desired service that is stale or
	// not yet registered under the new lease.
	for key, svc := range a.st.discoveryServices {
		if svc.desired && (svc.stale || !svc.registered) {
			a.Post(regMsgReplayDiscovery{ServiceKey: key})
		}
	}
	for key, svc := range a.st.topologyServices {
		if svc.desired && (svc.stale || !svc.registered) {
			a.Post(regMsgReplayTopology{TopologyKey: key})
		}
	}
}

func (a *RegistrationActor) onAddDiscovery(msg regMsgAddDiscovery) {
	if a.st.discoveryServices == nil {
		a.st.discoveryServices = make(map[string]discoveryRegistrationEntry)
	}

	discoveryEntry := discoveryRegistrationEntry{
		info:    cloneDiscoveryInfo(msg.Info),
		path:    msg.Path,
		ttl:     msg.TTL,
		desired: true,
		stale:   true, // needs a write
	}
	a.st.discoveryServices[msg.Path] = discoveryEntry

	if a.st.leaseID == 0 {
		// No active lease yet; reply success (write will happen after LeaseGranted).
		if msg.Reply != nil {
			msg.Reply <- nil
		}
		return
	}

	if err := a.putDiscoveryWithLease(discoveryEntry); err != nil {
		if msg.Reply != nil {
			msg.Reply <- err
		}
		discoveryEntry.stale = true
		discoveryEntry.registered = false
		discoveryEntry.lastError = err
		a.st.discoveryServices[msg.Path] = discoveryEntry
		return
	}
	discoveryEntry.registered = true
	discoveryEntry.stale = false
	discoveryEntry.lastError = nil
	discoveryEntry.updatedAt = time.Now()
	a.st.discoveryServices[msg.Path] = discoveryEntry
	a.publishRegistrationChanged()
	if msg.Reply != nil {
		msg.Reply <- nil
	}
}

func (a *RegistrationActor) onAddTopology(msg regMsgAddTopology) {
	if a.st.topologyServices == nil {
		a.st.topologyServices = make(map[string]topologyRegistrationEntry)
	}
	if a.st.serviceTopologyKey == nil {
		a.st.serviceTopologyKey = make(map[string]string)
	}

	topologyInfo := cloneTopologyInfo(msg.TopologyInfo)
	topologyKey := buildTopologyKey(topologyInfo, a.topologyPrefix)
	if topologyKey == "" {
		if msg.Reply != nil {
			msg.Reply <- fmt.Errorf("invalid topology info")
		}
		return
	}

	topologyEntry := topologyRegistrationEntry{
		info:    topologyInfo,
		key:     topologyKey,
		ttl:     msg.TTL,
		desired: true,
		stale:   true,
	}
	a.st.topologyServices[topologyKey] = topologyEntry
	if msg.ServicePath != "" {
		a.st.serviceTopologyKey[msg.ServicePath] = topologyKey
	}

	if a.st.leaseID == 0 {
		if msg.Reply != nil {
			msg.Reply <- nil
		}
		return
	}

	if err := a.putTopologyWithLease(topologyEntry); err != nil {
		topologyEntry.stale = true
		topologyEntry.registered = false
		topologyEntry.lastError = err
		topologyEntry.retryCount++
		a.st.topologyServices[topologyKey] = topologyEntry
		if msg.Reply != nil {
			msg.Reply <- err
		}
		return
	}
	topologyEntry.registered = true
	topologyEntry.stale = false
	topologyEntry.lastError = nil
	topologyEntry.updatedAt = time.Now()
	a.st.topologyServices[topologyKey] = topologyEntry

	if msg.Reply != nil {
		msg.Reply <- nil
	}
}

func (a *RegistrationActor) onRemoveService(msg regMsgRemoveService) {
	discoveryEntry, ok := a.st.discoveryServices[msg.Path]
	if !ok {
		if msg.Reply != nil {
			msg.Reply <- nil
		}
		return
	}
	discoveryEntry.desired = false
	a.st.discoveryServices[msg.Path] = discoveryEntry

	topologyKey := a.st.serviceTopologyKey[msg.Path]
	var topologyEntry topologyRegistrationEntry
	if topologyKey != "" {
		topologyEntry = a.st.topologyServices[topologyKey]
		topologyEntry.desired = false
		a.st.topologyServices[topologyKey] = topologyEntry
	}

	if a.st.leaseID != 0 {
		dCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		a.deleteDiscoveryKeys(dCtx, discoveryEntry)
		if topologyKey != "" {
			a.deleteTopologyKey(dCtx, topologyEntry)
		}
	}
	delete(a.st.discoveryServices, msg.Path)
	if topologyKey != "" {
		delete(a.st.topologyServices, topologyKey)
	}
	delete(a.st.serviceTopologyKey, msg.Path)
	a.publishRegistrationChanged()
	if msg.Reply != nil {
		msg.Reply <- nil
	}
}

func (a *RegistrationActor) onSyncTopology() {
	// Topology keepalive is a specialised flush; put it in the same lease.
	if a.st.leaseID == 0 {
		return
	}
	for key, svc := range a.st.topologyServices {
		if !svc.desired || !svc.registered {
			continue
		}
		if err := a.putTopologyWithLease(svc); err != nil {
			log.Warn("[RegistrationActor] topology sync failed",
				"key", key,
				"err", err)
			svc.stale = true
			svc.lastError = err
			svc.retryCount++
			a.st.topologyServices[key] = svc
		}
	}
}

func (a *RegistrationActor) onFlushTopology(msg regMsgFlushTopology) {
	a.onSyncTopology()
	if msg.Reply != nil {
		msg.Reply <- nil
	}
}

func (a *RegistrationActor) onReplayDiscovery(msg regMsgReplayDiscovery) {
	svc, ok := a.st.discoveryServices[msg.ServiceKey]
	if !ok || !svc.desired || a.st.leaseID == 0 {
		return
	}
	if err := a.putDiscoveryWithLease(svc); err != nil {
		log.Warn("[RegistrationActor] replay discovery write failed",
			"key", msg.ServiceKey,
			"retry", svc.retryCount,
			"err", err)
		svc.retryCount++
		svc.lastError = err
		svc.stale = true
		a.st.discoveryServices[msg.ServiceKey] = svc
		return
	}
	svc.registered = true
	svc.stale = false
	svc.retryCount = 0
	svc.lastError = nil
	svc.updatedAt = time.Now()
	a.st.discoveryServices[msg.ServiceKey] = svc
	a.publishRegistrationChanged()
}

func (a *RegistrationActor) onReplayTopology(msg regMsgReplayTopology) {
	svc, ok := a.st.topologyServices[msg.TopologyKey]
	if !ok || !svc.desired || a.st.leaseID == 0 {
		return
	}
	if err := a.putTopologyWithLease(svc); err != nil {
		log.Warn("[RegistrationActor] replay topology write failed",
			"key", msg.TopologyKey,
			"retry", svc.retryCount,
			"err", err)
		svc.retryCount++
		svc.lastError = err
		svc.stale = true
		a.st.topologyServices[msg.TopologyKey] = svc
		return
	}
	svc.registered = true
	svc.stale = false
	svc.retryCount = 0
	svc.lastError = nil
	svc.updatedAt = time.Now()
	a.st.topologyServices[msg.TopologyKey] = svc
}

// ── etcd write helpers ────────────────────────────────────────────────────

func (a *RegistrationActor) putDiscoveryWithLease(svc discoveryRegistrationEntry) error {
	if svc.info == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leaseOpt := clientv3.WithLease(a.st.leaseID)

	infoBytes, err := codec.MarshalDiscoveryToJSON(svc.info)
	if err != nil {
		return fmt.Errorf("marshal discovery info: %w", err)
	}
	serialised := string(infoBytes)

	// by-path
	if _, err := a.etcdClient.Put(ctx, svc.path, serialised, leaseOpt); err != nil {
		return fmt.Errorf("put bypath %s: %w", svc.path, err)
	}

	// by-name
	if svc.info.GetName() != "" {
		nameKey := pathbuilder.BuildByNamePath(a.discoveryBase, svc.info)
		if nameKey == "" {
			nameKey = a.byNamePrefix + svc.info.GetName()
		}
		if _, err := a.etcdClient.Put(ctx, nameKey, serialised, leaseOpt); err != nil {
			return fmt.Errorf("put byname %s: %w", nameKey, err)
		}
	}

	// by-id
	if svc.info.GetId() != 0 {
		idKey := pathbuilder.BuildByIDPath(a.discoveryBase, svc.info)
		if idKey == "" {
			idKey = fmt.Sprintf("%s%d", a.byIDPrefix, svc.info.GetId())
		}
		if _, err := a.etcdClient.Put(ctx, idKey, serialised, leaseOpt); err != nil {
			return fmt.Errorf("put byid %s: %w", idKey, err)
		}
	}

	return nil
}

func (a *RegistrationActor) putTopologyWithLease(svc topologyRegistrationEntry) error {
	if svc.info == nil || svc.key == "" {
		return nil
	}

	topologyBytes, err := codec.MarshalTopologyToJSON(svc.info)
	if err != nil {
		return fmt.Errorf("marshal topology info: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := a.etcdClient.Put(ctx, svc.key, string(topologyBytes), clientv3.WithLease(a.st.leaseID)); err != nil {
		return fmt.Errorf("put topology %s: %w", svc.key, err)
	}

	return nil
}

func (a *RegistrationActor) deleteDiscoveryKeys(ctx context.Context, svc discoveryRegistrationEntry) {
	_, _ = a.etcdClient.Delete(ctx, svc.path)
	if svc.info.GetName() != "" {
		nameKey := pathbuilder.BuildByNamePath(a.discoveryBase, svc.info)
		if nameKey == "" {
			nameKey = a.byNamePrefix + svc.info.GetName()
		}
		_, _ = a.etcdClient.Delete(ctx, nameKey)
	}
	if svc.info.GetId() != 0 {
		idKey := pathbuilder.BuildByIDPath(a.discoveryBase, svc.info)
		if idKey == "" {
			idKey = fmt.Sprintf("%s%d", a.byIDPrefix, svc.info.GetId())
		}
		_, _ = a.etcdClient.Delete(ctx, idKey)
	}
}

func deriveDiscoveryBasePrefix(byNamePrefix, byIDPrefix string) string {
	trimmedByName := strings.TrimSuffix(byNamePrefix, "/")
	if strings.HasSuffix(trimmedByName, "/"+pathbuilder.ByNameDir) {
		return strings.TrimSuffix(trimmedByName, "/"+pathbuilder.ByNameDir)
	}

	trimmedByID := strings.TrimSuffix(byIDPrefix, "/")
	if strings.HasSuffix(trimmedByID, "/"+pathbuilder.ByIDDir) {
		return strings.TrimSuffix(trimmedByID, "/"+pathbuilder.ByIDDir)
	}

	return ""
}

func (a *RegistrationActor) deleteTopologyKey(ctx context.Context, svc topologyRegistrationEntry) {
	if svc.key != "" {
		_, _ = a.etcdClient.Delete(ctx, svc.key)
	}
}

func buildTopologyKey(topologyInfo *pb.AtappTopologyInfo, topologyPrefix string) string {
	if topologyInfo == nil || topologyInfo.GetName() == "" || topologyInfo.GetId() == 0 {
		return ""
	}
	return fmt.Sprintf("%s/%s-%d", topologyPrefix, topologyInfo.GetName(), topologyInfo.GetId())
}

func cloneDiscoveryInfo(info *pb.AtappDiscovery) *pb.AtappDiscovery {
	if info == nil {
		return nil
	}
	return proto.Clone(info).(*pb.AtappDiscovery)
}

func cloneTopologyInfo(info *pb.AtappTopologyInfo) *pb.AtappTopologyInfo {
	if info == nil {
		return nil
	}
	return proto.Clone(info).(*pb.AtappTopologyInfo)
}

func buildTopologyInfoFromDiscovery(info *pb.AtappDiscovery) *pb.AtappTopologyInfo {
	if info == nil {
		return nil
	}
	if info.GetName() == "" || info.GetId() == 0 {
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

// ── Event publishing ──────────────────────────────────────────────────────

// buildRegistrationSnapshot assembles the current registration sub-view from
// the in-memory service table.
func (a *RegistrationActor) buildRegistrationSnapshot() snapshot.RegistrationSnapshot {
	s := snapshot.RegistrationSnapshot{
		LeaseID:    a.st.leaseID,
		LeaseEpoch: a.st.leaseEpoch,
		ByPath:     make(map[string]*pb.AtappDiscovery),
		ByName:     make(map[string]*pb.AtappDiscovery),
		ByID:       make(map[uint64]*pb.AtappDiscovery),
		UpdatedAt:  time.Now(),
	}
	for _, svc := range a.st.discoveryServices {
		if !svc.registered {
			continue
		}
		s.ByPath[svc.path] = svc.info
		if svc.info.GetName() != "" {
			s.ByName[svc.info.GetName()] = svc.info
		}
		if svc.info.GetId() != 0 {
			s.ByID[svc.info.GetId()] = svc.info
		}
	}
	return s
}

func (a *RegistrationActor) publishRegistrationChanged() {
	snap := a.buildRegistrationSnapshot()
	a.eventBus.Publish(runtime.EventEnvelope{
		Type:       runtime.EventRegistrationChanged,
		Version:    1,
		Source:     runtime.EventSourceRegistrationActor,
		LeaseEpoch: a.st.leaseEpoch,
		OccurredAt: time.Now(),
		Payload:    RegistrationChangedPayload{RegistrationSnapshot: snap},
	})
}

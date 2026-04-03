package orchestrator

import (
	"context"
	"errors"
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

// ErrNoLease is returned by FlushTopology (and onSyncTopology) when no active
// lease is held.  Callers may use errors.Is to distinguish this condition from
// a real etcd write failure.
var ErrNoLease = errors.New("registration: no active lease")

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
	// LeaseEpoch is the epoch at which this replay was scheduled.  The handler
	// discards the message if the current epoch has advanced (lease rebuilt).
	LeaseEpoch uint64
}
type regMsgReplayTopology struct {
	TopologyKey string
	LeaseEpoch  uint64
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

func (e *discoveryRegistrationEntry) markRegistered() {
	e.registered = true
	e.stale = false
	e.retryCount = 0
	e.lastError = nil
	e.updatedAt = time.Now()
}

func (e *discoveryRegistrationEntry) markFailed(err error) {
	e.stale = true
	e.registered = false
	e.lastError = err
	e.retryCount++
}

func (e *discoveryRegistrationEntry) markExpired() {
	e.registered = false
	e.stale = true
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

func (e *topologyRegistrationEntry) markRegistered() {
	e.registered = true
	e.stale = false
	e.retryCount = 0
	e.lastError = nil
	e.updatedAt = time.Now()
}

func (e *topologyRegistrationEntry) markFailed(err error) {
	e.stale = true
	e.registered = false
	e.lastError = err
	e.retryCount++
}

func (e *topologyRegistrationEntry) markExpired() {
	e.registered = false
	e.stale = true
}

// registrationActorState is all mutable state owned by the RegistrationActor's
// run goroutine.  No locking required.
type registrationActorState struct {
	leaseID            clientv3.LeaseID
	leaseEpoch         uint64
	discoveryServices  map[string]discoveryRegistrationEntry // key = service path
	topologyServices   map[string]topologyRegistrationEntry  // key = topology key
	serviceTopologyKey map[string]string                     // service path -> topology key
	// pendingReplayCount tracks how many ReplayDiscovery/ReplayTopology messages
	// were enqueued by the most recent onLeaseGranted.  The last one to complete
	// successfully decrements the counter to 0 and publishes a single
	// RegistrationChanged event, avoiding N burst events per lease rebuild.
	pendingReplayCount int
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
		a.onSyncTopology(context.Background())
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
		svc.markExpired()
		a.st.discoveryServices[key] = svc
	}
	for key, svc := range a.st.topologyServices {
		svc.markExpired()
		a.st.topologyServices[key] = svc
	}
	a.st.leaseID = 0
}

func (a *RegistrationActor) onLeaseGranted(msg regMsgLeaseGranted) {
	if a.st.leaseID == msg.LeaseID {
		// Duplicate event for the current lease (e.g. bus fires twice); replays
		// already enqueued — ignore to avoid resetting pendingReplayCount.
		return
	}
	a.st.leaseID = msg.LeaseID
	a.st.leaseEpoch = msg.LeaseEpoch
	// Count and enqueue replay messages for every desired service that is stale
	// or not yet registered under the new lease.  pendingReplayCount lets the
	// last successful replay publish a single RegistrationChanged event instead
	// of one per service.
	count := 0
	for key, svc := range a.st.discoveryServices {
		if svc.desired && (svc.stale || !svc.registered) {
			// Reset backoff counter so a brand-new lease epoch starts from 0.
			svc.retryCount = 0
			a.st.discoveryServices[key] = svc
			a.Post(regMsgReplayDiscovery{ServiceKey: key, LeaseEpoch: msg.LeaseEpoch})
			count++
		}
	}
	for key, svc := range a.st.topologyServices {
		if svc.desired && (svc.stale || !svc.registered) {
			svc.retryCount = 0
			a.st.topologyServices[key] = svc
			a.Post(regMsgReplayTopology{TopologyKey: key, LeaseEpoch: msg.LeaseEpoch})
			count++
		}
	}
	a.st.pendingReplayCount = count
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

	if err := a.putDiscoveryWithLease(context.Background(), discoveryEntry); err != nil {
		if msg.Reply != nil {
			msg.Reply <- err
		}
		discoveryEntry.markFailed(err)
		a.st.discoveryServices[msg.Path] = discoveryEntry
		return
	}
	discoveryEntry.markRegistered()
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

	if err := a.putTopologyWithLease(context.Background(), topologyEntry); err != nil {
		topologyEntry.markFailed(err)
		a.st.topologyServices[topologyKey] = topologyEntry
		if msg.Reply != nil {
			msg.Reply <- err
		}
		return
	}
	topologyEntry.markRegistered()
	a.st.topologyServices[topologyKey] = topologyEntry
	a.publishRegistrationChanged()

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

func (a *RegistrationActor) onSyncTopology(ctx context.Context) error {
	// Topology keepalive is a specialised flush; put it in the same lease.
	if a.st.leaseID == 0 {
		return ErrNoLease
	}
	var firstErr error
	for key, svc := range a.st.topologyServices {
		if !svc.desired || !svc.registered {
			continue
		}
		if err := a.putTopologyWithLease(ctx, svc); err != nil {
			log.Warn("[RegistrationActor] topology sync failed",
				"key", key,
				"err", err)
			svc.markFailed(err)
			a.st.topologyServices[key] = svc
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (a *RegistrationActor) onFlushTopology(msg regMsgFlushTopology) {
	err := a.onSyncTopology(msg.Ctx)
	if msg.Reply != nil {
		msg.Reply <- err
	}
}

func (a *RegistrationActor) onReplayDiscovery(msg regMsgReplayDiscovery) {
	svc, ok := a.st.discoveryServices[msg.ServiceKey]
	if !ok || !svc.desired || a.st.leaseID == 0 {
		return
	}
	// Discard replays that were scheduled for a stale lease epoch.
	if msg.LeaseEpoch != a.st.leaseEpoch {
		return
	}
	if err := a.putDiscoveryWithLease(context.Background(), svc); err != nil {
		log.Warn("[RegistrationActor] replay discovery write failed",
			"key", msg.ServiceKey,
			"retry", svc.retryCount,
			"err", err)
		svc.markFailed(err)
		a.st.discoveryServices[msg.ServiceKey] = svc
		// Schedule an exponential-backoff retry; the epoch guard above ensures
		// that delayed messages from a dead lease are silently dropped.
		delay := backoffDuration(svc.retryCount)
		leaseEpoch := a.st.leaseEpoch
		serviceKey := msg.ServiceKey
		time.AfterFunc(delay, func() {
			a.Post(regMsgReplayDiscovery{ServiceKey: serviceKey, LeaseEpoch: leaseEpoch})
		})
		return
	}
	svc.markRegistered()
	a.st.discoveryServices[msg.ServiceKey] = svc
	if a.st.pendingReplayCount > 0 {
		a.st.pendingReplayCount--
	}
	if a.st.pendingReplayCount == 0 {
		a.publishRegistrationChanged()
	}
}

func (a *RegistrationActor) onReplayTopology(msg regMsgReplayTopology) {
	svc, ok := a.st.topologyServices[msg.TopologyKey]
	if !ok || !svc.desired || a.st.leaseID == 0 {
		return
	}
	// Discard replays that were scheduled for a stale lease epoch.
	if msg.LeaseEpoch != a.st.leaseEpoch {
		return
	}
	if err := a.putTopologyWithLease(context.Background(), svc); err != nil {
		log.Warn("[RegistrationActor] replay topology write failed",
			"key", msg.TopologyKey,
			"retry", svc.retryCount,
			"err", err)
		svc.markFailed(err)
		a.st.topologyServices[msg.TopologyKey] = svc
		delay := backoffDuration(svc.retryCount)
		leaseEpoch := a.st.leaseEpoch
		topologyKey := msg.TopologyKey
		time.AfterFunc(delay, func() {
			a.Post(regMsgReplayTopology{TopologyKey: topologyKey, LeaseEpoch: leaseEpoch})
		})
		return
	}
	svc.markRegistered()
	a.st.topologyServices[msg.TopologyKey] = svc
	if a.st.pendingReplayCount > 0 {
		a.st.pendingReplayCount--
	}
	if a.st.pendingReplayCount == 0 {
		a.publishRegistrationChanged()
	}
}

// ── etcd write helpers ────────────────────────────────────────────────────

func (a *RegistrationActor) putDiscoveryWithLease(ctx context.Context, svc discoveryRegistrationEntry) error {
	if svc.info == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
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

func (a *RegistrationActor) putTopologyWithLease(ctx context.Context, svc topologyRegistrationEntry) error {
	if svc.info == nil || svc.key == "" {
		return nil
	}

	topologyBytes, err := codec.MarshalTopologyToJSON(svc.info)
	if err != nil {
		return fmt.Errorf("marshal topology info: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
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

// backoffDuration returns a capped exponential backoff: 200ms × 2^retryCount, max 30s.
func backoffDuration(retryCount int) time.Duration {
	const base = 200 * time.Millisecond
	const maxBackoff = 30 * time.Second
	if retryCount <= 0 {
		return base
	}
	shift := retryCount
	if shift > 7 { // 200ms × 128 = 25.6s; cap shift to avoid int overflow
		shift = 7
	}
	d := time.Duration(1<<uint(shift)) * base
	if d > maxBackoff {
		return maxBackoff
	}
	return d
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

// ── Event publishing ──────────────────────────────────────────────────────

// buildRegistrationSnapshot assembles the current registration sub-view from
// the in-memory service table.
func (a *RegistrationActor) buildRegistrationSnapshot() snapshot.SelfRegistrationSnapshot {
	s := snapshot.SelfRegistrationSnapshot{
		LeaseID:          a.st.leaseID,
		LeaseEpoch:       a.st.leaseEpoch,
		ByPath:           make(map[string]*pb.AtappDiscovery),
		ByName:           make(map[string]*pb.AtappDiscovery),
		ByID:             make(map[uint64]*pb.AtappDiscovery),
		TopologyServices: make(map[string]*pb.AtappTopologyInfo),
		UpdatedAt:        time.Now(),
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
	for _, svc := range a.st.topologyServices {
		if !svc.registered {
			continue
		}
		s.TopologyServices[svc.key] = svc.info
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
		Payload:    RegistrationChangedPayload{SelfRegistrationSnapshot: snap},
	})
}

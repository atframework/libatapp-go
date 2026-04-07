package orchestrator

import (
	"context"
	"sync/atomic"
	"time"

	log "log/slog"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
)

// ── Message types (sealed interface) ─────────────────────────────────────

// leaseMsg is the sealed interface for all LeaseActor mailbox messages.
type leaseMsg interface{ leaseMsgKind() }

// LeaseMsgType enumerates message kinds for logging and metrics.
type LeaseMsgType uint8

const (
	LeaseMsgStart   LeaseMsgType = iota + 1
	LeaseMsgStop                 // 2
	LeaseMsgRenewed              // 3
	LeaseMsgExpired              // 4
	LeaseMsgTick                 // 5 — periodic retry trigger
)

// Name returns a human-readable label.
func (t LeaseMsgType) Name() string {
	switch t {
	case LeaseMsgStart:
		return "LeaseMsgStart"
	case LeaseMsgStop:
		return "LeaseMsgStop"
	case LeaseMsgRenewed:
		return "LeaseMsgRenewed"
	case LeaseMsgExpired:
		return "LeaseMsgExpired"
	case LeaseMsgTick:
		return "LeaseMsgTick"
	default:
		return "LeaseMsgUnknown"
	}
}

type leaseMsgStart struct{ TTL int64 }
type leaseMsgStop struct{ Reply chan<- error }
type leaseMsgRenewed struct {
	LeaseID clientv3.LeaseID
	TTL     int64
}
type leaseMsgExpired struct{ LeaseID clientv3.LeaseID }
type leaseMsgTick struct{}

func (leaseMsgStart) leaseMsgKind()   {}
func (leaseMsgStop) leaseMsgKind()    {}
func (leaseMsgRenewed) leaseMsgKind() {}
func (leaseMsgExpired) leaseMsgKind() {}
func (leaseMsgTick) leaseMsgKind()    {}

// ── Internal state ────────────────────────────────────────────────────────

// leasePhase tracks the lifecycle state of the lease.
type leasePhase uint8

const (
	leasePhaseIdle       leasePhase = iota
	leasePhaseAcquiring             // 1: trying to Grant
	leasePhaseActive                // 2: lease held, keepalive running
	leasePhaseRebuilding            // 3: keepalive lost, about to re-acquire
	leasePhaseRevoking              // 4: deliberate Stop
)

type leaseState struct {
	phase      leasePhase
	leaseID    clientv3.LeaseID
	leaseEpoch uint64 // incremented on every successful Grant
	ttl        int64
}

// ── LeaseActor ────────────────────────────────────────────────────────────

// LeaseActor manages the etcd lease lifecycle (Grant → KeepAlive → Rebuild).
//
// Single-writer guarantee: all leaseState fields are exclusively owned by the
// Run goroutine.  No external code reads or writes them directly.
//
// Lifecycle coupling: LeaseActor subscribes to EventRegistrationRequested and
// EventRegistrationEmpty on the EventBus at construction time.  It starts
// acquiring a lease when the first registration request arrives, and revokes
// the lease when all registrations are removed.  This ensures the etcd lease
// is held only while there is at least one active service registration.
//
// Mailbox capacity: 4 (lease operations are low-frequency).
type LeaseActor struct {
	runtime.ActorBase[leaseMsg] // embedded mailbox + run loop

	etcdClient EtcdClient
	eventBus   runtime.EventBus
	leaseTTL   int64

	// kaCancel cancels the active keepalive goroutine; only set while phase==active.
	// Stored as atomic so the keepalive goroutine can read it safely.
	kaCancel atomic.Pointer[context.CancelFunc]

	// regRefCount is the number of RegistrationActor instances that currently
	// have at least one desired service.  LeaseActor starts acquiring a lease
	// when regRefCount transitions 0→1 (EventRegistrationRequested) and stops
	// when it transitions 1→0 (EventRegistrationEmpty).  Using a counter rather
	// than a bool correctly handles multiple RegistrationActor instances.
	//
	// Written by EventBus handler goroutines; read by the Run goroutine via
	// the mailbox message path (leaseMsgRefCount).  atomic is used only for the
	// level-triggered retry in onTick(), where we need a safe read outside the
	// mailbox.
	regRefCount atomic.Int32

	// hRegRequested / hRegEmpty are the EventBus subscription handles registered
	// at construction time (before any goroutines start).  They are unsubscribed
	// when Run() exits.
	hRegRequested runtime.EventHandleHandle
	hRegEmpty     runtime.EventHandleHandle

	st leaseState // owned by Run goroutine only
}

// NewLeaseActor creates a LeaseActor ready to Start.
//
// Subscriptions to EventRegistrationRequested and EventRegistrationEmpty are
// registered immediately (before any goroutine starts) so that no event is ever
// missed due to goroutine scheduling delays.
func NewLeaseActor(etcdClient EtcdClient, bus runtime.EventBus, leaseTTL int64) *LeaseActor {
	a := &LeaseActor{
		ActorBase:  runtime.NewActorBase[leaseMsg](4),
		etcdClient: etcdClient,
		eventBus:   bus,
		leaseTTL:   leaseTTL,
	}
	// Subscribe eagerly so events are never missed regardless of goroutine scheduling.
	//
	// EventRegistrationRequested: a RegistrationActor transitioned empty→non-empty.
	// Increment the reference count; if it was 0, kick off lease acquisition.
	a.hRegRequested = bus.SubscribeType(runtime.EventRegistrationRequested, func(_ runtime.EventEnvelope) {
		newVal := a.regRefCount.Add(1)
		if newVal == 1 {
			// 0→1 transition: start the lease.
			a.Post(leaseMsgStart{TTL: a.leaseTTL})
		}
	})
	// EventRegistrationEmpty: a RegistrationActor transitioned non-empty→empty.
	// Decrement the reference count; if it reaches 0, stop the lease.
	a.hRegEmpty = bus.SubscribeType(runtime.EventRegistrationEmpty, func(_ runtime.EventEnvelope) {
		newVal := a.regRefCount.Add(-1)
		if newVal <= 0 {
			// Clamp to 0 to guard against mismatched events.
			a.regRefCount.Store(0)
			a.Post(leaseMsgStop{Reply: nil})
		}
	})
	return a
}

// ── External API (goroutine-safe) ─────────────────────────────────────────

// Start requests the actor to begin acquiring a lease with the given TTL.
// Non-blocking (level-triggered drop if mailbox is full).
func (a *LeaseActor) Start(ttl int64) {
	a.Post(leaseMsgStart{TTL: ttl})
}

// Stop requests a graceful shutdown.  The returned channel is closed once the
// actor has revoked the lease and exited.
func (a *LeaseActor) Stop(ctx context.Context) <-chan error {
	ch := make(chan error, 1)
	if err := a.PostCtx(ctx, leaseMsgStop{Reply: ch}); err != nil {
		ch <- err
		close(ch)
	}
	return ch
}

// Tick retries lease acquisition when the actor is in Acquiring/Rebuilding state.
// Non-blocking.
func (a *LeaseActor) Tick() {
	a.Post(leaseMsgTick{})
}

// Run is the actor's event loop; launch as a goroutine managed by ModuleActorRuntime.
// Unregisters the EventBus subscriptions when the context is done.
func (a *LeaseActor) Run(ctx context.Context) {
	defer a.eventBus.Unsubscribe(a.hRegRequested)
	defer a.eventBus.Unsubscribe(a.hRegEmpty)
	a.RunLoop(ctx, a.handle)
}

// ── Message handlers (only called from Run goroutine) ─────────────────────

func (a *LeaseActor) handle(msg leaseMsg) {
	switch m := msg.(type) {
	case leaseMsgStart:
		a.onStart(m)
	case leaseMsgStop:
		a.onStop(m)
	case leaseMsgRenewed:
		a.onRenewed(m)
	case leaseMsgExpired:
		a.onExpired(m)
	case leaseMsgTick:
		a.onTick()
	}
}

func (a *LeaseActor) onStart(msg leaseMsgStart) {
	if a.st.phase != leasePhaseIdle {
		return
	}
	a.st.phase = leasePhaseAcquiring
	a.st.ttl = msg.TTL
	a.tryGrant()
}

func (a *LeaseActor) onStop(msg leaseMsgStop) {
	a.stopKeepalive()
	var revokeErr error
	if a.st.leaseID != 0 {
		a.st.phase = leasePhaseRevoking
		// Revoke with a short timeout; use a background-derived context since
		// the parent ctx may already be cancelled at this point.
		rCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, revokeErr = a.etcdClient.Revoke(rCtx, a.st.leaseID)
		// Publish EventLeaseReleased regardless of revoke error: the intent is
		// clear and lease TTL will eventually expire even if revoke fails.
		a.publishLeaseReleased(a.st.leaseID)
	}
	a.st = leaseState{phase: leasePhaseIdle}
	if msg.Reply != nil {
		msg.Reply <- revokeErr
		close(msg.Reply)
	}
}

func (a *LeaseActor) onRenewed(_ leaseMsgRenewed) {
	// keepalive renewed — nothing to do; state remains Active.
}

func (a *LeaseActor) onExpired(msg leaseMsgExpired) {
	if a.st.phase != leasePhaseActive || a.st.leaseID != msg.LeaseID {
		return // stale notification, ignore
	}
	a.stopKeepalive()
	a.st.phase = leasePhaseRebuilding
	a.publishLeaseExpired(msg.LeaseID)
	// Immediately retry; Tick will drive subsequent retries if this also fails.
	a.st.phase = leasePhaseAcquiring
	a.tryGrant()
}

func (a *LeaseActor) onTick() {
	// Level-triggered stop: if regRefCount reached 0 but leaseMsgStop was
	// dropped due to a full mailbox, retry the stop on the next Tick.
	if a.regRefCount.Load() == 0 && a.st.phase != leasePhaseIdle {
		a.onStop(leaseMsgStop{})
		return
	}
	if a.st.phase == leasePhaseAcquiring || a.st.phase == leasePhaseRebuilding {
		a.tryGrant()
	}
}

// ── Internal helpers ──────────────────────────────────────────────────────

func (a *LeaseActor) tryGrant() {
	// Use a short deadline so that a slow etcd doesn't stall the mailbox loop.
	gCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := a.etcdClient.Grant(gCtx, a.st.ttl)
	if err != nil {
		log.Warn("[LeaseActor] Grant failed; will retry on next Tick",
			"err", err, "phase", a.st.phase)
		return
	}

	a.st.leaseID = resp.ID
	a.st.leaseEpoch++
	a.st.phase = leasePhaseActive

	// Start keepalive before publishing the event.
	a.startKeepalive(resp.ID)
	a.publishLeaseGranted(resp.ID, resp.TTL)
}

// startKeepalive spawns a goroutine that drives the clientv3 KeepAlive channel
// and posts leaseMsgExpired back to the mailbox when the channel closes.
func (a *LeaseActor) startKeepalive(id clientv3.LeaseID) {
	kaCtx, cancel := context.WithCancel(context.Background())
	a.kaCancel.Store(&cancel)

	go func() {
		defer func() {
			// When keepalive goroutine exits (channel closed or ctx cancelled),
			// notify the actor's mailbox non-blockingly.
			a.Post(leaseMsgExpired{LeaseID: id})
		}()

		ch, err := a.etcdClient.KeepAlive(kaCtx, id)
		if err != nil {
			log.Warn("[LeaseActor] KeepAlive failed immediately", "err", err)
			return
		}
		for range ch {
			// drain — renewed; we could post leaseMsgRenewed but it's a no-op.
		}
		// Channel closed = lease expired or kaCtx cancelled.
	}()
}

func (a *LeaseActor) stopKeepalive() {
	if ptr := a.kaCancel.Swap(nil); ptr != nil {
		(*ptr)() // cancel the keepalive context
	}
}

// ── Event publishing ──────────────────────────────────────────────────────

func (a *LeaseActor) publishLeaseGranted(id clientv3.LeaseID, ttl int64) {
	epoch := a.st.leaseEpoch
	a.eventBus.Publish(runtime.EventEnvelope{
		Type:       runtime.EventLeaseGranted,
		Version:    1,
		Source:     runtime.EventSourceLeaseActor,
		LeaseEpoch: epoch,
		OccurredAt: time.Now(),
		DedupeKey:  LeaseDedupeKey(epoch, runtime.EventLeaseGranted),
		Payload:    LeaseGrantedPayload{LeaseID: id, TTL: ttl},
	})
}

func (a *LeaseActor) publishLeaseExpired(id clientv3.LeaseID) {
	epoch := a.st.leaseEpoch
	a.eventBus.Publish(runtime.EventEnvelope{
		Type:       runtime.EventLeaseExpired,
		Version:    1,
		Source:     runtime.EventSourceLeaseActor,
		LeaseEpoch: epoch,
		OccurredAt: time.Now(),
		DedupeKey:  LeaseDedupeKey(epoch, runtime.EventLeaseExpired),
		Payload:    LeaseExpiredPayload{LeaseID: id},
	})
}

func (a *LeaseActor) publishLeaseReleased(id clientv3.LeaseID) {
	epoch := a.st.leaseEpoch
	a.eventBus.Publish(runtime.EventEnvelope{
		Type:       runtime.EventLeaseReleased,
		Version:    1,
		Source:     runtime.EventSourceLeaseActor,
		LeaseEpoch: epoch,
		OccurredAt: time.Now(),
		DedupeKey:  LeaseDedupeKey(epoch, runtime.EventLeaseReleased),
		Payload:    LeaseReleasedPayload{LeaseID: id},
	})
}

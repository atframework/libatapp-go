package orchestrator

import (
	"context"
	"strings"
	"time"

	log "log/slog"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/codec"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/etcdversion"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/runtime"
	"github.com/atframework/libatapp-go/etcd_module_v2/internal/snapshot"
)

// ── Message types (sealed interface) ─────────────────────────────────────

// watchMsg is the sealed interface for all WatchActor mailbox messages.
type watchMsg interface{ watchMsgKind() }

// WatchMsgType enumerates message kinds for logging and metrics.
type WatchMsgType uint8

const (
	WatchMsgAddPrefix    WatchMsgType = iota + 1
	WatchMsgRemovePrefix              // 2
	WatchMsgActiveAll                 // 3 — force-restart all watch streams
)

// Name returns a human-readable label.
func (t WatchMsgType) Name() string {
	switch t {
	case WatchMsgAddPrefix:
		return "WatchMsgAddPrefix"
	case WatchMsgRemovePrefix:
		return "WatchMsgRemovePrefix"
	case WatchMsgActiveAll:
		return "WatchMsgActiveAll"
	default:
		return "WatchMsgUnknown"
	}
}

type watchMsgAddPrefix struct {
	Prefix string
}
type watchMsgRemovePrefix struct {
	Prefix string
}
type watchMsgActiveAll struct{}

// internal-only: injects an event from a watch goroutine back into the mailbox
type watchMsgInternalEvent struct {
	Prefix string
	Event  clientv3.WatchResponse
}

func (watchMsgAddPrefix) watchMsgKind()     {}
func (watchMsgRemovePrefix) watchMsgKind()  {}
func (watchMsgActiveAll) watchMsgKind()     {}
func (watchMsgInternalEvent) watchMsgKind() {}

// ── Internal state ────────────────────────────────────────────────────────

// watchStreamHandle holds cancellation context for a single watch stream goroutine.
type watchStreamHandle struct {
	prefix string
	cancel context.CancelFunc
}

type watchActorState struct {
	prefixes map[string]struct{}
	streams  map[string]watchStreamHandle
}

// ── WatchActor ────────────────────────────────────────────────────────────

// WatchActor manages etcd watch streams for a set of key prefixes.
// For each registered prefix it:
//  1. Does an initial Get to build a full snapshot, then
//  2. Starts a Watch stream for incremental events.
//
// All changes are published onto the EventBus as WatchNode*/WatchTopology* events.
//
// Mailbox capacity: 256 (watch events can burst on cluster topology changes).
type WatchActor struct {
	runtime.ActorBase[watchMsg]

	etcdClient EtcdClient
	eventBus   runtime.EventBus

	st watchActorState
}

// NewWatchActor constructs a WatchActor.
func NewWatchActor(etcdClient EtcdClient, bus runtime.EventBus) *WatchActor {
	a := &WatchActor{
		ActorBase:  runtime.NewActorBase[watchMsg](256),
		etcdClient: etcdClient,
		eventBus:   bus,
	}
	a.st.prefixes = make(map[string]struct{})
	a.st.streams = make(map[string]watchStreamHandle)
	return a
}

// ── External API ──────────────────────────────────────────────────────────

// AddPrefix requests watching all keys under prefix (range watch).
func (a *WatchActor) AddPrefix(prefix string) {
	a.Post(watchMsgAddPrefix{Prefix: prefix})
}

// RemovePrefix requests stopping the watch stream for prefix.
func (a *WatchActor) RemovePrefix(prefix string) {
	a.Post(watchMsgRemovePrefix{Prefix: prefix})
}

// ActiveAll requests restarting all watch streams (e.g. after reconnect).
func (a *WatchActor) ActiveAll() {
	a.Post(watchMsgActiveAll{})
}

// Run is the actor's event loop.
func (a *WatchActor) Run(ctx context.Context) {
	a.RunLoop(ctx, func(msg watchMsg) {
		a.handle(ctx, msg)
	})
	// Cancel all active streams when the actor stops.
	for _, h := range a.st.streams {
		h.cancel()
	}
}

// ── Message handlers ──────────────────────────────────────────────────────

func (a *WatchActor) handle(ctx context.Context, msg watchMsg) {
	switch m := msg.(type) {
	case watchMsgAddPrefix:
		a.onAddPrefix(ctx, m)
	case watchMsgRemovePrefix:
		a.onRemovePrefix(m)
	case watchMsgActiveAll:
		a.onActiveAll(ctx)
	case watchMsgInternalEvent:
		a.onWatchEvent(m)
	}
}

func (a *WatchActor) onAddPrefix(ctx context.Context, msg watchMsgAddPrefix) {
	if _, exists := a.st.prefixes[msg.Prefix]; exists {
		return
	}
	a.st.prefixes[msg.Prefix] = struct{}{}
	a.startStream(ctx, msg.Prefix)
}

func (a *WatchActor) onRemovePrefix(msg watchMsgRemovePrefix) {
	delete(a.st.prefixes, msg.Prefix)
	if h, ok := a.st.streams[msg.Prefix]; ok {
		h.cancel()
		delete(a.st.streams, msg.Prefix)
	}
}

func (a *WatchActor) onActiveAll(ctx context.Context) {
	for _, h := range a.st.streams {
		h.cancel()
	}
	a.st.streams = make(map[string]watchStreamHandle)
	for prefix := range a.st.prefixes {
		a.startStream(ctx, prefix)
	}
}

func (a *WatchActor) onWatchEvent(msg watchMsgInternalEvent) {
	if msg.Event.Canceled {
		log.Warn("[WatchActor] watch stream cancelled", "prefix", msg.Prefix)
		return
	}
	for _, ev := range msg.Event.Events {
		a.dispatchKVEvent(msg.Prefix, ev, msg.Event.Header.GetRevision())
	}
}

// ── Stream management ─────────────────────────────────────────────────────

// startStream does a full Get snapshot then starts a range Watch.
// Both run inside a single goroutine that posts messages back to the mailbox.
func (a *WatchActor) startStream(ctx context.Context, prefix string) {
	streamCtx, cancel := context.WithCancel(ctx)
	a.st.streams[prefix] = watchStreamHandle{prefix: prefix, cancel: cancel}

	go func() {
		defer cancel()

		isTopology := strings.Contains(prefix, "/topology")

		if isTopology {
			a.publishTopologySnapshotLoading()
		} else {
			a.publishSnapshotLoading()
		}

		getCtx, getCancel := context.WithTimeout(streamCtx, 30*time.Second)
		resp, err := a.etcdClient.Get(getCtx, prefix, clientv3.WithPrefix())
		getCancel()
		if err != nil {
			log.Warn("[WatchActor] initial Get failed", "prefix", prefix, "err", err)
			return
		}

		if isTopology {
			// Topology: collect as map, indexed by ID (primary) and path (fallback)
			topoByID := make(map[uint64]*snapshot.TopologyNode)
			for _, kv := range resp.Kvs {
				node := decodeTopologyNode(kv)
				if node != nil {
					if node.Info.GetId() != 0 {
						// Index by ID if available
						topoByID[node.Info.GetId()] = node
					}
				}
			}
			a.publishTopologySnapshotLoaded(topoByID, resp.Header.GetRevision())
		} else {
			// Discovery: collect as map[string]*DiscoveryNode indexed by path
			nodes := make(map[string]*snapshot.DiscoveryNode, len(resp.Kvs))
			for _, kv := range resp.Kvs {
				node := decodeDiscoveryNode(kv)
				if node != nil {
					nodes[string(kv.Key)] = node
				}
			}
			a.publishSnapshotLoaded(nodes, resp.Header.GetRevision())
		}

		watchCh := a.etcdClient.Watch(
			streamCtx, prefix,
			clientv3.WithPrefix(),
			clientv3.WithRev(resp.Header.GetRevision()+1),
			clientv3.WithPrevKV(),
		)
		for {
			select {
			case <-streamCtx.Done():
				return
			case wresp, ok := <-watchCh:
				if !ok {
					return
				}
				if !a.Post(watchMsgInternalEvent{Prefix: prefix, Event: wresp}) {
					log.Warn("[WatchActor] mailbox full, dropping watch response", "prefix", prefix)
				}
			}
		}
	}()
}

// ── etcd event dispatch ───────────────────────────────────────────────────

func (a *WatchActor) dispatchKVEvent(prefix string, ev *clientv3.Event, revision int64) {
	if strings.Contains(prefix, "/topology") {
		a.dispatchTopologyKVEvent(ev, revision)
		return
	}
	a.dispatchDiscoveryKVEvent(ev, revision)
}

func (a *WatchActor) dispatchDiscoveryKVEvent(ev *clientv3.Event, revision int64) {
	key := string(ev.Kv.Key)
	switch ev.Type {
	case mvccpb.PUT:
		node := decodeDiscoveryNode(ev.Kv)
		if node == nil {
			return
		}
		payload := WatchNodePayload{
			Revision:       revision,
			Key:            key,
			Value:          node.Info,
			ModRevision:    ev.Kv.ModRevision,
			Version:        ev.Kv.Version,
			CreateRevision: ev.Kv.CreateRevision,
		}
		eventType := runtime.EventWatchNodeUp
		if ev.Kv.Version > 1 {
			eventType = runtime.EventWatchNodeUpdate
		}
		a.publish(eventType, payload, WatchNodeDedupeKey(revision, key, eventType))
	case mvccpb.DELETE:
		payload := WatchNodePayload{Revision: revision, Key: key}
		a.publish(runtime.EventWatchNodeDown, payload,
			WatchNodeDedupeKey(revision, key, runtime.EventWatchNodeDown))
	}
}

func (a *WatchActor) dispatchTopologyKVEvent(ev *clientv3.Event, revision int64) {
	key := string(ev.Kv.Key)
	switch ev.Type {
	case mvccpb.PUT:
		topologyInfo := decodeTopologyInfo(ev.Kv)
		if topologyInfo == nil {
			return
		}
		payload := WatchTopologyPayload{
			Revision:       revision,
			Key:            key,
			Value:          topologyInfo,
			ModRevision:    ev.Kv.ModRevision,
			Version:        ev.Kv.Version,
			CreateRevision: ev.Kv.CreateRevision,
		}
		eventType := runtime.EventWatchTopologyUp
		if ev.Kv.Version > 1 {
			eventType = runtime.EventWatchTopologyUpdate
		}
		a.publish(eventType, payload, WatchNodeDedupeKey(revision, key, eventType))
	case mvccpb.DELETE:
		var deletedTopologyInfo *pb.AtappTopologyInfo
		if ev.PrevKv != nil {
			deletedTopologyInfo = decodeTopologyInfo(ev.PrevKv)
		}
		payload := WatchTopologyPayload{Revision: revision, Key: key, Value: deletedTopologyInfo}
		a.publish(runtime.EventWatchTopologyDown, payload,
			WatchNodeDedupeKey(revision, key, runtime.EventWatchTopologyDown))
	}
}

// ── Snapshot publishing ───────────────────────────────────────────────────

func (a *WatchActor) publishSnapshotLoading() {
	a.eventBus.Publish(runtime.EventEnvelope{
		Type:       runtime.EventWatchSnapshotLoading,
		Version:    1,
		Source:     runtime.EventSourceWatchActor,
		OccurredAt: time.Now(),
	})
}

func (a *WatchActor) publishSnapshotLoaded(nodes map[string]*snapshot.DiscoveryNode, revision int64) {
	a.eventBus.Publish(runtime.EventEnvelope{
		Type:       runtime.EventWatchSnapshotLoaded,
		Version:    1,
		Source:     runtime.EventSourceWatchActor,
		OccurredAt: time.Now(),
		Payload:    WatchSnapshotLoadedPayload{Nodes: nodes, Revision: revision},
	})
}

func (a *WatchActor) publishTopologySnapshotLoading() {
	a.eventBus.Publish(runtime.EventEnvelope{
		Type:       runtime.EventWatchTopologySnapshotLoading,
		Version:    1,
		Source:     runtime.EventSourceWatchActor,
		OccurredAt: time.Now(),
	})
}

func (a *WatchActor) publishTopologySnapshotLoaded(nodes map[uint64]*snapshot.TopologyNode, revision int64) {
	a.eventBus.Publish(runtime.EventEnvelope{
		Type:       runtime.EventWatchTopologySnapshotLoaded,
		Version:    1,
		Source:     runtime.EventSourceWatchActor,
		OccurredAt: time.Now(),
		Payload:    WatchTopologySnapshotLoadedPayload{Nodes: nodes, Revision: revision},
	})
}

func (a *WatchActor) publish(evType runtime.EventType, payload any, dedupeKey string) {
	a.eventBus.Publish(runtime.EventEnvelope{
		Type:       evType,
		Version:    1,
		Source:     runtime.EventSourceWatchActor,
		OccurredAt: time.Now(),
		DedupeKey:  dedupeKey,
		Payload:    payload,
	})
}

// ── Decode helper ─────────────────────────────────────────────────────────

func decodeDiscoveryNode(kv *mvccpb.KeyValue) *snapshot.DiscoveryNode {
	if kv == nil {
		return nil
	}
	info := codec.DecodeDiscoveryValue(kv.Value)
	if info == nil {
		log.Warn("[WatchActor] decode discovery failed", "key", string(kv.Key))
		return nil
	}
	return &snapshot.DiscoveryNode{
		Info:        info,
		Path:        string(kv.Key),
		DataVersion: etcdversion.New(kv.CreateRevision, kv.ModRevision, kv.Version),
	}
}

func decodeTopologyInfo(kv *mvccpb.KeyValue) *pb.AtappTopologyInfo {
	if kv == nil {
		return nil
	}
	topologyInfo := codec.DecodeTopologyValue(kv.Value)
	if topologyInfo == nil {
		log.Warn("[WatchActor] decode topology failed", "key", string(kv.Key))
		return nil
	}
	return topologyInfo
}

func decodeTopologyNode(kv *mvccpb.KeyValue) *snapshot.TopologyNode {
	if kv == nil {
		return nil
	}
	info := codec.DecodeTopologyValue(kv.Value)
	if info == nil {
		return nil
	}
	return &snapshot.TopologyNode{
		Info:        info,
		DataVersion: etcdversion.New(kv.CreateRevision, kv.ModRevision, kv.Version),
	}
}

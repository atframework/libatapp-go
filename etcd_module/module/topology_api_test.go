package module

import (
	"testing"

	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func TestEtcdModule_TopologyPathAPIs(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	mod.SetConfigurePath("/svc")
	if got := mod.GetTopologyPath(); got != "/svc/topology" {
		t.Fatalf("unexpected topology path: %s", got)
	}
	if got := mod.GetTopologyWatcherPath(); got != "/svc/topology" {
		t.Fatalf("unexpected topology watcher path: %s", got)
	}
}

func TestEtcdModule_TopologySenderAndInfoSet(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	event := watcher.EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:      "/svc/topology/node-a",
		Revision: 101,
		RawValue: []byte(`{"id":"node-a","zone":"az1"}`),
	}
	sender := mod.buildTopologyWatcherSender(event)
	if sender == nil {
		t.Fatalf("expected sender")
	}
	if sender.Topology == nil {
		t.Fatalf("expected parsed topology payload")
	}
	if sender.Topology.Value["id"] != "node-a" {
		t.Fatalf("unexpected topology id: %v", sender.Topology.Value["id"])
	}

	mod.applyTopologyEvent(sender)
	set := mod.GetTopologyInfoSet()
	if len(set) != 1 {
		t.Fatalf("expected one topology record, got %d", len(set))
	}
	if set[0].Path != "/svc/topology/node-a" {
		t.Fatalf("unexpected topology path: %s", set[0].Path)
	}

	head := mod.GetLastEtcdEventTopologyHeader()
	if head == nil || head.GetRevision() != 101 {
		t.Fatalf("unexpected topology header revision: %v", head)
	}

	deleteEvent := watcher.EtcdWatchEvent{
		Type:         pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE,
		Key:          "/svc/topology/node-a",
		Revision:     102,
		RawPrevValue: []byte(`{"id":"node-a","zone":"az1"}`),
	}
	mod.applyTopologyEvent(mod.buildTopologyWatcherSender(deleteEvent))
	if got := len(mod.GetTopologyInfoSet()); got != 0 {
		t.Fatalf("expected topology info removed, got %d", got)
	}
}

func TestEtcdModule_TopologySnapshotCallbacks(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	loadCount := 0
	loadedCount := 0
	loadHandle := mod.AddOnLoadTopologySnapshot(func(m *EtcdModule) {
		loadCount++
	})
	loadedHandle := mod.AddOnTopologySnapshotLoaded(func(m *EtcdModule) {
		loadedCount++
	})
	if loadHandle == 0 || loadedHandle == 0 {
		t.Fatalf("expected valid callback handles")
	}

	mod.dispatchOnLoadTopologySnapshot()
	mod.dispatchOnTopologySnapshotLoaded()
	if loadCount != 1 || loadedCount != 1 {
		t.Fatalf("unexpected callback counts: load=%d loaded=%d", loadCount, loadedCount)
	}

	mod.RemoveOnLoadTopologySnapshot(loadHandle)
	mod.RemoveOnTopologySnapshotLoaded(loadedHandle)
	mod.dispatchOnLoadTopologySnapshot()
	mod.dispatchOnTopologySnapshotLoaded()
	if loadCount != 1 || loadedCount != 1 {
		t.Fatalf("callbacks should have been removed: load=%d loaded=%d", loadCount, loadedCount)
	}
}

func TestEtcdModule_TopologyInfoEventCallbacks(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	called := 0
	lastAction := pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT
	h := mod.AddOnTopologyInfoEvent(func(sender *TopologyWatcherSender) {
		called++
		if sender == nil || sender.Event == nil {
			t.Fatalf("expected sender with event")
		}
		lastAction = sender.Event.Type
	})
	if h == 0 {
		t.Fatalf("expected valid callback handle")
	}

	putEvent := watcher.EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:      "/svc/topology/node-b",
		Revision: 201,
		RawValue: []byte(`{"id":"node-b","zone":"az2"}`),
	}
	mod.applyTopologyEvent(mod.buildTopologyWatcherSender(putEvent))
	if called != 1 {
		t.Fatalf("expected put callback once, got %d", called)
	}
	if lastAction != pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT {
		t.Fatalf("unexpected action after put: %v", lastAction)
	}

	deleteEvent := watcher.EtcdWatchEvent{
		Type:         pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE,
		Key:          "/svc/topology/node-b",
		Revision:     202,
		RawPrevValue: []byte(`{"id":"node-b","zone":"az2"}`),
	}
	mod.applyTopologyEvent(mod.buildTopologyWatcherSender(deleteEvent))
	if called != 2 {
		t.Fatalf("expected delete callback once, got %d", called)
	}
	if lastAction != pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE {
		t.Fatalf("unexpected action after delete: %v", lastAction)
	}

	mod.RemoveOnTopologyInfoEvent(h)
	mod.applyTopologyEvent(mod.buildTopologyWatcherSender(putEvent))
	if called != 2 {
		t.Fatalf("callback should have been removed, got %d", called)
	}
}

func TestEtcdModule_TopologyInfoEvent_UnchangedPutIgnored(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	called := 0
	h := mod.AddOnTopologyInfoEvent(func(sender *TopologyWatcherSender) {
		if sender == nil {
			t.Fatalf("expected non-nil sender")
		}
		called++
	})
	if h == 0 {
		t.Fatalf("expected valid callback handle")
	}

	first := watcher.EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:      "/svc/topology/node-c",
		Revision: 301,
		RawValue: []byte(`{"id":"node-c","zone":"az3"}`),
	}
	mod.applyTopologyEvent(mod.buildTopologyWatcherSender(first))

	second := watcher.EtcdWatchEvent{
		Type:        pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:         "/svc/topology/node-c",
		Revision:    301,
		RawValue:    []byte(`{"id":"node-c","zone":"az3"}`),
		DataVersion: etcdversion.DataVersion{CreateRevision: 301, ModRevision: 301, Version: 0},
	}
	mod.applyTopologyEvent(mod.buildTopologyWatcherSender(second))

	if called != 1 {
		t.Fatalf("expected unchanged put to be ignored, got callback count %d", called)
	}
}

func TestEtcdModule_TopologyInfoEvent_SamePayloadVersionChangedTriggers(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	called := 0
	h := mod.AddOnTopologyInfoEvent(func(sender *TopologyWatcherSender) {
		if sender == nil {
			t.Fatalf("expected non-nil sender")
		}
		called++
	})
	if h == 0 {
		t.Fatalf("expected valid callback handle")
	}

	first := watcher.EtcdWatchEvent{
		Type:        pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:         "/svc/topology/node-d",
		Revision:    401,
		RawValue:    []byte(`{"id":"node-d","zone":"az3"}`),
		DataVersion: etcdversion.DataVersion{CreateRevision: 390, ModRevision: 401, Version: 1},
	}
	mod.applyTopologyEvent(mod.buildTopologyWatcherSender(first))

	second := watcher.EtcdWatchEvent{
		Type:        pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:         "/svc/topology/node-d",
		Revision:    402,
		RawValue:    []byte(`{"id":"node-d","zone":"az3"}`),
		DataVersion: etcdversion.DataVersion{CreateRevision: 390, ModRevision: 402, Version: 2},
	}
	mod.applyTopologyEvent(mod.buildTopologyWatcherSender(second))

	if called != 2 {
		t.Fatalf("expected version-changed put to trigger callback, got %d", called)
	}
}

func TestEtcdModule_TopologySnapshotReset_ClearsStaleInfoSet(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	put := watcher.EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:      "/svc/topology/stale-node",
		Revision: 900,
		RawValue: []byte(`{"id":"stale-node","zone":"az9"}`),
	}
	mod.applyTopologyEvent(mod.buildTopologyWatcherSender(put))

	if got := len(mod.GetTopologyInfoSet()); got != 1 {
		t.Fatalf("expected one topology record before snapshot reset, got %d", got)
	}

	mod.mu.Lock()
	mod.topology.setSnapshotReady(false)
	mod.topology.resetInfoSet()
	mod.mu.Unlock()

	if got := len(mod.GetTopologyInfoSet()); got != 0 {
		t.Fatalf("expected topology cache cleared on snapshot reset, got %d", got)
	}
}

func TestTopologyValueToProto(t *testing.T) {
	info := TopologyValueToProto(map[string]any{
		"id":   "123",
		"name": "svc-a",
	})
	if info == nil {
		t.Fatalf("expected proto topology info")
	}
	if info.GetId() != 123 {
		t.Fatalf("unexpected topology id: %d", info.GetId())
	}
	if info.GetName() != "svc-a" {
		t.Fatalf("unexpected topology name: %s", info.GetName())
	}

	if got := TopologyValueToProto(nil); got != nil {
		t.Fatalf("expected nil for empty input")
	}
}

func TestBuildTopologyCompatEvent(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	event := watcher.EtcdWatchEvent{
		Type:        pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:         "/svc/topology/node-compat",
		Revision:    401,
		RawValue:    []byte(`{"id":4001,"name":"svc-compat","identity":"svc-compat-1"}`),
		DataVersion: etcdversion.DataVersion{CreateRevision: 310, ModRevision: 401, Version: 7},
	}
	sender := mod.buildTopologyWatcherSender(event)
	compat := BuildTopologyCompatEvent(sender)
	if compat == nil {
		t.Fatalf("expected compat event")
	}
	if compat.EtcdHeader == nil || compat.EtcdHeader.GetRevision() != 401 {
		t.Fatalf("unexpected compat header: %+v", compat.EtcdHeader)
	}
	if compat.Storage == nil || compat.Storage.Info == nil {
		t.Fatalf("expected compat storage info")
	}
	if compat.Storage.Info.GetId() != 4001 || compat.Storage.Info.GetName() != "svc-compat" {
		t.Fatalf("unexpected compat payload: %+v", compat.Storage.Info)
	}
	if compat.Storage.CreateRevision != 310 || compat.Storage.ModRevision != 401 || compat.Storage.Version != 7 {
		t.Fatalf("unexpected compat version info: %+v", compat.Storage)
	}
}

func TestEtcdModule_GetTopologyInfoSetCompat(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	put := watcher.EtcdWatchEvent{
		Type:        pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:         "/svc/topology/node-compat-set",
		Revision:    501,
		RawValue:    []byte(`{"id":5001,"name":"svc-compat-set"}`),
		DataVersion: etcdversion.DataVersion{CreateRevision: 420, ModRevision: 501, Version: 9},
	}
	mod.applyTopologyEvent(mod.buildTopologyWatcherSender(put))

	set := mod.GetTopologyInfoSetCompat()
	if len(set) != 1 {
		t.Fatalf("expected one compat topology record, got %d", len(set))
	}
	record := set[5001]
	if record == nil || record.Info == nil {
		t.Fatalf("expected compat topology record for id 5001")
	}
	if record.Info.GetName() != "svc-compat-set" {
		t.Fatalf("unexpected compat topology name: %s", record.Info.GetName())
	}
	if record.CreateRevision != 420 || record.ModRevision != 501 {
		t.Fatalf("unexpected compat revisions: %+v", record)
	}
	if record.Version != 9 {
		t.Fatalf("unexpected compat version: %+v", record)
	}
}

func TestEtcdModule_SetTopologyKeepaliveData_StageCloneAndDirty(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	info := &pb.AtappTopologyInfo{
		Id:       1001,
		Name:     "svc-a",
		Identity: "svc-a-1001",
		Version:  "1.2.3",
	}
	mod.SetTopologyKeepaliveData(info)

	mod.mu.RLock()
	pending := mod.topology.pendingKeepalive
	dirty := mod.topology.keepaliveDirty
	mod.mu.RUnlock()

	if !dirty {
		t.Fatalf("expected topology keepalive dirty")
	}
	if pending == nil {
		t.Fatalf("expected staged topology keepalive payload")
	}
	if pending.GetId() != 1001 || pending.GetName() != "svc-a" {
		t.Fatalf("unexpected staged payload: %+v", pending)
	}

	info.Name = "changed-after-stage"
	mod.mu.RLock()
	defer mod.mu.RUnlock()
	if mod.topology.pendingKeepalive.GetName() != "svc-a" {
		t.Fatalf("expected staged payload cloned from input")
	}
}

func TestEtcdModule_FlushTopologyKeepalive_WhenClusterUnavailable(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	mod.SetConfigurePath("/svc")
	mod.SetTopologyKeepaliveData(&pb.AtappTopologyInfo{Id: 2002, Name: "svc-b"})

	if err := mod.flushTopologyKeepalive(nil); err != nil {
		t.Fatalf("expected nil error when cluster unavailable, got: %v", err)
	}

	mod.mu.RLock()
	defer mod.mu.RUnlock()
	if !mod.topology.keepaliveDirty {
		t.Fatalf("expected dirty flag preserved before cluster becomes available")
	}
	if mod.topology.keepalivePath != "" {
		t.Fatalf("expected no keepalive path before successful publish")
	}
}

func TestEtcdModule_SetMaybeUpdateKeepaliveTopologyValue_UsesSource(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	called := 0
	mod.SetTopologyKeepaliveSource(func() *pb.AtappTopologyInfo {
		called++
		return &pb.AtappTopologyInfo{Id: 3003, Name: "svc-source", Identity: "svc-source-3003"}
	})

	mod.SetMaybeUpdateKeepaliveTopologyValue()

	if called != 1 {
		t.Fatalf("expected source called once, got %d", called)
	}

	mod.mu.RLock()
	defer mod.mu.RUnlock()
	if mod.topology.pendingKeepalive == nil {
		t.Fatalf("expected pending keepalive staged from source")
	}
	if mod.topology.pendingKeepalive.GetId() != 3003 || mod.topology.pendingKeepalive.GetName() != "svc-source" {
		t.Fatalf("unexpected staged source payload: %+v", mod.topology.pendingKeepalive)
	}
}

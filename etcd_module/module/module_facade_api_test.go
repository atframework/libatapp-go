package module

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	log "log/slog"

	"github.com/atframework/libatapp-go/etcd_module/client"
	"github.com/atframework/libatapp-go/etcd_module/cluster"
	"github.com/atframework/libatapp-go/etcd_module/discovery"
	"github.com/atframework/libatapp-go/etcd_module/events"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

type snapshotFakeClient struct {
	getResp *clientv3.GetResponse
	watchCh clientv3.WatchChan
}

var _ client.EtcdClient = (*snapshotFakeClient)(nil)

func (f *snapshotFakeClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return &clientv3.PutResponse{}, nil
}

func (f *snapshotFakeClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if f.getResp == nil {
		return &clientv3.GetResponse{}, nil
	}
	return f.getResp, nil
}

func (f *snapshotFakeClient) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}

func (f *snapshotFakeClient) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	if f.watchCh == nil {
		f.watchCh = make(chan clientv3.WatchResponse)
	}
	return f.watchCh
}

func (f *snapshotFakeClient) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return &clientv3.LeaseGrantResponse{ID: clientv3.LeaseID(1), TTL: ttl}, nil
}

func (f *snapshotFakeClient) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return &clientv3.LeaseRevokeResponse{}, nil
}

func (f *snapshotFakeClient) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ch := make(chan *clientv3.LeaseKeepAliveResponse)
	close(ch)
	return ch, nil
}

func (f *snapshotFakeClient) Close() error {
	return nil
}

func TestEventAPIs_NodeCallback_InvokedAndRemoved(t *testing.T) {
	mod, cl, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	ds, err := discovery.NewEtcdDiscoverySet("/svc/", nil)
	if err != nil {
		t.Fatalf("failed to create discovery set: %v", err)
	}
	if err := cl.RegisterDiscoverySet(ds); err != nil {
		t.Fatalf("failed to register discovery set: %v", err)
	}

	invoked := 0
	lastType := events.EventTypeClusterDown
	handle := mod.AddOnNodeDiscoveryEvent(func(event *events.Event) {
		invoked++
		if event != nil {
			lastType = event.Type
		}
	})
	if handle == 0 {
		t.Fatalf("expected valid node handle")
	}

	nodeInfo := &pb.AtappDiscovery{Name: "app", Id: 42}
	ds.HandleWatcherEvent(watcher.EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:      "/svc/by_id/app-42",
		Value:    nodeInfo,
		Revision: 1,
	})
	if invoked != 1 {
		t.Fatalf("expected node callback to be invoked once, got %d", invoked)
	}
	if lastType != events.EventTypeNodeUp {
		t.Fatalf("expected node up event type, got %v", lastType)
	}

	mod.RemoveOnNodeEvent(handle)
	ds.HandleWatcherEvent(watcher.EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE,
		Key:      "/svc/by_id/app-42",
		Value:    nodeInfo,
		Revision: 2,
	})
	if invoked != 1 {
		t.Fatalf("expected removed node callback to stop receiving events, got %d", invoked)
	}
}

func TestEventAPIs_SnapshotCallbacks_TriggeredByWatcherFlow(t *testing.T) {
	fakeClient := &snapshotFakeClient{
		getResp: &clientv3.GetResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: 123},
		},
		watchCh: make(chan clientv3.WatchResponse),
	}
	cl, err := cluster.NewEtcdCluster(fakeClient, log.Default())
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	mod, err := newEtcdModuleWithCluster(cl, log.Default())
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cl.Start(ctx); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	t.Cleanup(func() {
		_ = cl.Stop(context.Background())
	})

	loadingCh := make(chan *events.Event, 1)
	loadedCh := make(chan *events.Event, 1)

	loadingHandle := mod.AddOnLoadSnapshot(func(event *events.Event) {
		loadingCh <- event
	})
	if loadingHandle == 0 {
		t.Fatalf("expected valid loading handle")
	}
	loadedHandle := mod.AddOnSnapshotLoaded(func(event *events.Event) {
		loadedCh <- event
	})
	if loadedHandle == 0 {
		t.Fatalf("expected valid loaded handle")
	}

	if err := mod.AddWatcherByCustomPath(ctx, "/svc/"); err != nil {
		t.Fatalf("failed to add watcher: %v", err)
	}

	select {
	case ev := <-loadingCh:
		if ev == nil || ev.Type != events.EventTypeSnapshotLoading {
			t.Fatalf("expected snapshot loading event")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected snapshot loading callback")
	}

	select {
	case ev := <-loadedCh:
		if ev == nil || ev.Type != events.EventTypeSnapshotLoaded {
			t.Fatalf("expected snapshot loaded event")
		}
		if ev.Revision != 123 {
			t.Fatalf("expected snapshot loaded revision 123, got %d", ev.Revision)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected snapshot loaded callback")
	}

	mod.RemoveOnLoadSnapshot(loadingHandle)
	mod.RemoveOnSnapshotLoaded(loadedHandle)
}

func TestEventAPIs_AddNilCallback_ReturnsZeroHandle(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	tests := []struct {
		name string
		call func() events.EventCallbackHandle
	}{
		{name: "node", call: func() events.EventCallbackHandle { return mod.AddOnNodeDiscoveryEvent(nil) }},
		{name: "load snapshot", call: func() events.EventCallbackHandle { return mod.AddOnLoadSnapshot(nil) }},
		{name: "snapshot loaded", call: func() events.EventCallbackHandle { return mod.AddOnSnapshotLoaded(nil) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.call(); got != 0 {
				t.Fatalf("expected zero handle, got %d", got)
			}
		})
	}
}

func TestEventAPIs_RemoveUnknownHandle_NoPanic(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	tests := []struct {
		name   string
		remove func(events.EventCallbackHandle)
	}{
		{name: "node", remove: mod.RemoveOnNodeEvent},
		{name: "load snapshot", remove: mod.RemoveOnLoadSnapshot},
		{name: "snapshot loaded", remove: mod.RemoveOnSnapshotLoaded},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.remove(0)
			tc.remove(99999)
		})
	}
}

func TestEventAPIs_DelegateToUnderlyingCluster(t *testing.T) {
	mod, cl, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	tests := []struct {
		name       string
		addFromMod func() events.EventCallbackHandle
		addFromCl  func() events.EventCallbackHandle
	}{
		{
			name:       "node",
			addFromMod: func() events.EventCallbackHandle { return mod.AddOnNodeDiscoveryEvent(func(event *events.Event) {}) },
			addFromCl:  func() events.EventCallbackHandle { return cl.AddOnNodeEvent(func(event *events.Event) {}) },
		},
		{
			name:       "load snapshot",
			addFromMod: func() events.EventCallbackHandle { return mod.AddOnLoadSnapshot(func(event *events.Event) {}) },
			addFromCl:  func() events.EventCallbackHandle { return cl.AddOnSnapshotLoading(func(event *events.Event) {}) },
		},
		{
			name:       "snapshot loaded",
			addFromMod: func() events.EventCallbackHandle { return mod.AddOnSnapshotLoaded(func(event *events.Event) {}) },
			addFromCl:  func() events.EventCallbackHandle { return cl.AddOnSnapshotLoaded(func(event *events.Event) {}) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			modHandle := tc.addFromMod()
			clusterHandle := tc.addFromCl()
			if clusterHandle != modHandle+1 {
				t.Fatalf("expected handle sequence via same callback list, got mod=%d cluster=%d", modHandle, clusterHandle)
			}
		})
	}
}

func TestKeepaliveAPIs_ReturnErrorWhenClusterNotRunning(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	ctx := context.Background()
	path := "/svc/by_id/app-42"

	info := &pb.AtappDiscovery{Name: "app", Id: 42}
	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "add keepalive",
			call: func() error {
				_, err := mod.AddRegistrationActor(ctx, info, path, 5)
				return err
			},
		},
		{
			name: "remove keepalive",
			call: func() error {
				return mod.RemoveRegistrationActor(ctx, path)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); err == nil {
				t.Fatalf("expected error when cluster is not running")
			}
		})
	}
}

func TestKeepaliveAPIs_RegisterAndRemove_WhenClusterRunning(t *testing.T) {
	fakeClient := &snapshotFakeClient{}
	cl, err := cluster.NewEtcdCluster(fakeClient, log.Default())
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	mod, err := newEtcdModuleWithCluster(cl, log.Default())
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cl.Start(ctx); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	t.Cleanup(func() {
		_ = cl.Stop(context.Background())
	})

	path := "/svc/by_id/app-42"
	info := &pb.AtappDiscovery{Name: "app", Id: 42, Identity: "app-42"}

	ka, err := mod.AddRegistrationActor(ctx, info, path, 5)
	if err != nil {
		t.Fatalf("AddRegistrationActor failed: %v", err)
	}
	if ka == nil {
		t.Fatalf("expected non-nil keepalive actor")
	}

	km := cl.GetRegistrationManager()
	if km == nil {
		t.Fatalf("expected non-nil keepalive manager")
	}
	if _, ok := km.GetRegistration(path); !ok {
		t.Fatalf("expected keepalive stored in manager for path %s", path)
	}

	if err := mod.RemoveRegistrationActor(ctx, path); err != nil {
		t.Fatalf("RemoveRegistrationActor failed: %v", err)
	}
	if _, ok := km.GetRegistration(path); ok {
		t.Fatalf("expected keepalive removed from manager for path %s", path)
	}
}

func TestWatcherAPIs_ReturnErrorWhenClusterNotRunning(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	ctx := context.Background()

	tests := []struct {
		name string
		call func() error
	}{
		{name: "by id", call: func() error { return mod.AddWatcherByID(ctx) }},
		{name: "by type id", call: func() error { return mod.AddWatcherByTypeID(ctx, 7) }},
		{name: "by type name", call: func() error { return mod.AddWatcherByTypeName(ctx, "type") }},
		{name: "by name", call: func() error { return mod.AddWatcherByName(ctx) }},
		{name: "by tag", call: func() error { return mod.AddWatcherByTag(ctx, "blue") }},
		{name: "by custom path", call: func() error { return mod.AddWatcherByCustomPath(ctx, "/custom") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); err == nil {
				t.Fatalf("expected error for watcher without running cluster")
			}
		})
	}
}

func TestWatcherAPIs_AddWatcherPaths_WhenClusterRunning(t *testing.T) {
	fakeClient := &snapshotFakeClient{
		getResp: &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 1}},
		watchCh: make(chan clientv3.WatchResponse),
	}

	cl, err := cluster.NewEtcdCluster(fakeClient, log.Default())
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	mod, err := newEtcdModuleWithCluster(cl, log.Default())
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}
	mod.SetConfigurePath("/svc")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cl.Start(ctx); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	t.Cleanup(func() {
		_ = cl.Stop(context.Background())
	})

	manager := cl.GetWatcherManager()
	tests := []struct {
		name string
		path string
		call func() error
	}{
		{name: "by id", path: mod.GetByIDWatcherPath(), call: func() error { return mod.AddWatcherByID(ctx) }},
		{name: "by type id", path: mod.GetByTypeIDWatcherPath(7), call: func() error { return mod.AddWatcherByTypeID(ctx, 7) }},
		{name: "by type name", path: mod.GetByTypeNameWatcherPath("type"), call: func() error { return mod.AddWatcherByTypeName(ctx, "type") }},
		{name: "by name", path: mod.GetByNameWatcherPath(), call: func() error { return mod.AddWatcherByName(ctx) }},
		{name: "by tag", path: mod.GetByTagWatcherPath("blue"), call: func() error { return mod.AddWatcherByTag(ctx, "blue") }},
		{name: "by custom path", path: "/custom", call: func() error { return mod.AddWatcherByCustomPath(ctx, "/custom") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); err != nil {
				t.Fatalf("AddWatcher failed: %v", err)
			}
			w, ok := manager.GetWatcher(tc.path)
			if !ok || w == nil {
				t.Fatalf("expected watcher for path %s", tc.path)
			}
			if !w.IsRunning() {
				t.Fatalf("expected watcher running for path %s", tc.path)
			}
		})
	}
}

func TestEtcdModule_NameAndTimeout(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	if got := mod.Name(); got != "etcd_module" {
		t.Fatalf("unexpected name: %s", got)
	}
	if got := mod.Timeout(); got != 0 {
		t.Fatalf("unexpected timeout value: %d", got)
	}
}

func TestEtcdModule_RegisterByID_GeneratesDefaultIdentity(t *testing.T) {
	fakeClient := &snapshotFakeClient{}
	cl, err := cluster.NewEtcdCluster(fakeClient, log.Default())
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}

	mod, err := newEtcdModuleWithCluster(cl, log.Default())
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}
	mod.SetConfigurePath("/svc")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mod.GetRawEtcdCtx().Start(ctx); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	t.Cleanup(func() {
		_ = mod.GetRawEtcdCtx().Stop(context.Background())
	})

	info := &pb.AtappDiscovery{Name: "app", Id: 42}
	ka, err := mod.RegisterByID(ctx, info, 0)
	if err != nil {
		t.Fatalf("register by id failed: %v", err)
	}
	if ka == nil {
		t.Fatalf("expected keepalive instance")
	}
	if info.Identity != "" {
		t.Fatalf("expected input info not mutated, got identity=%q", info.Identity)
	}

	if err := mod.UnregisterByID(ctx, &pb.AtappDiscovery{Name: "app", Id: 42}); err != nil {
		t.Fatalf("unregister by id failed: %v", err)
	}
}

func TestEtcdModule_GetSharedCurlMultiContextMismatch(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	_, err := mod.GetSharedCurlMultiContext()
	if err == nil {
		t.Fatalf("expected semantic mismatch error")
	}
	if !errors.Is(err, ErrCPPModuleSemanticMismatch) {
		t.Fatalf("expected ErrCPPModuleSemanticMismatch, got %v", err)
	}
}

func TestEtcdModule_AddKeepaliveActorByValue(t *testing.T) {
	fakeClient := &snapshotFakeClient{}
	cl, err := cluster.NewEtcdCluster(fakeClient, log.Default())
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}

	mod, err := newEtcdModuleWithCluster(cl, log.Default())
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mod.GetRawEtcdCtx().Start(ctx); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	t.Cleanup(func() {
		_ = mod.GetRawEtcdCtx().Stop(context.Background())
	})

	path := "/svc/by_id/app-42"
	jsonVal := `{"name":"app","id":"42","identity":"app-42"}`
	ka, err := mod.AddRegistrationActorByValue(ctx, jsonVal, path, 8)
	if err != nil {
		t.Fatalf("AddRegistrationActorByValue failed: %v", err)
	}
	if ka == nil {
		t.Fatalf("expected keepalive actor")
	}

	if err := mod.RemoveRegistrationActor(ctx, ka.GetPath()); err != nil {
		t.Fatalf("expected RemoveRegistrationActor success, got: %v", err)
	}
}

func TestEtcdModule_Close_Idempotent(t *testing.T) {
	fakeClient := &snapshotFakeClient{}
	cl, err := cluster.NewEtcdCluster(fakeClient, log.Default())
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}

	mod, err := newEtcdModuleWithCluster(cl, log.Default())
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	if err := mod.Close(context.Background()); err != nil {
		t.Fatalf("first close failed: %v", err)
	}
	if err := mod.Close(context.Background()); err != nil {
		t.Fatalf("second close should be idempotent, got: %v", err)
	}
}

func TestEtcdModule_UnregisterBy_NilInfo(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	tests := []struct {
		name string
		call func() error
	}{
		{name: "by id", call: func() error { return mod.UnregisterByID(context.Background(), nil) }},
		{name: "by type id", call: func() error { return mod.UnregisterByTypeID(context.Background(), nil) }},
		{name: "by type name", call: func() error { return mod.UnregisterByTypeName(context.Background(), nil) }},
		{name: "by name", call: func() error { return mod.UnregisterByName(context.Background(), nil) }},
		{name: "by tag", call: func() error { return mod.UnregisterByTag(context.Background(), nil, "blue") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatalf("expected error")
			}
			if err.Error() != "service info is nil" {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestEtcdModule_AddKeepaliveActorByValue_InvalidJSON(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	_, err := mod.AddRegistrationActorByValue(context.Background(), "{invalid-json", "/svc/by_id/app-1", 8)
	if err == nil {
		t.Fatalf("expected json unmarshal error")
	}
}

func TestEtcdModule_AddKeepaliveActorByValueDefaultTTL(t *testing.T) {
	fakeClient := &snapshotFakeClient{}
	cl, err := cluster.NewEtcdCluster(fakeClient, log.Default())
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}

	mod, err := newEtcdModuleWithCluster(cl, log.Default())
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mod.GetRawEtcdCtx().Start(ctx); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}
	t.Cleanup(func() {
		_ = mod.GetRawEtcdCtx().Stop(context.Background())
	})

	path := "/svc/by_id/app-42"
	jsonVal := `{"name":"app","id":"42","identity":"app-42"}`
	ka, err := mod.AddRegistrationActorByValueDefaultTTL(ctx, jsonVal, path)
	if err != nil {
		t.Fatalf("AddRegistrationActorByValueDefaultTTL failed: %v", err)
	}
	if ka == nil {
		t.Fatalf("expected keepalive actor")
	}
}

func TestEtcdModule_WatcherCallbackPublicWrappers(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	ctx := context.Background()
	if err := mod.AddWatcherByIDCallback(ctx, func(sender *WatcherSenderList) {}); err == nil {
		t.Fatalf("expected error when cluster not running")
	}
	if err := mod.AddWatcherByNameCallback(ctx, func(sender *WatcherSenderList) {}); err == nil {
		t.Fatalf("expected error when cluster not running")
	}
	if err := mod.AddWatcherByTypeIDCallback(ctx, 1001, func(sender *WatcherSenderOne) {}); err == nil {
		t.Fatalf("expected error when cluster not running")
	}
	if err := mod.AddWatcherByTypeNameCallback(ctx, "type", func(sender *WatcherSenderOne) {}); err == nil {
		t.Fatalf("expected error when cluster not running")
	}
	if err := mod.AddWatcherByTagCallback(ctx, "blue", func(sender *WatcherSenderOne) {}); err == nil {
		t.Fatalf("expected error when cluster not running")
	}
	if _, err := mod.AddWatcherByCustomPathCallback(ctx, "/custom/path", func(sender *WatcherSenderOne) {}); err == nil {
		t.Fatalf("expected error when cluster not running")
	}
}

func TestEtcdModule_DiscoverySplitAliasAPIs(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	if mod.GetLastEtcdEventHeader() != nil {
		t.Fatalf("expected nil event header before any watcher event")
	}

	if mod.HasSnapshot() {
		t.Fatalf("expected no snapshot before init/watcher flow")
	}

	mod.SetMaybeUpdateKeepaliveValue()
	mod.SetMaybeUpdateKeepaliveArea()
	mod.SetMaybeUpdateKeepaliveMetadata()

	loadHandle := mod.AddOnLoadSnapshotCompat(func(mod *EtcdModule) {})
	if loadHandle == 0 {
		t.Fatalf("expected valid load discovery snapshot handle")
	}
	loadedHandle := mod.AddOnSnapshotLoadedCompat(func(mod *EtcdModule) {})
	if loadedHandle == 0 {
		t.Fatalf("expected valid discovery snapshot loaded handle")
	}

	mod.RemoveOnLoadSnapshot(loadHandle)
	mod.RemoveOnSnapshotLoaded(loadedHandle)
}


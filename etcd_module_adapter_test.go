package libatapp

import (
	"context"
	"testing"

	log "log/slog"

	"github.com/atframework/libatapp-go/etcd_module/client/mocks"
	"github.com/atframework/libatapp-go/etcd_module/cluster"
	"github.com/atframework/libatapp-go/etcd_module/discovery"

	moduleimpl "github.com/atframework/libatapp-go/etcd_module/module"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
	"github.com/golang/mock/gomock"
)

func TestEtcdModuleAdapter_GetGlobalDiscovery_ReturnsClusterSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cl, err := cluster.NewEtcdCluster(mockClient, logger)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}

	mod, err := moduleimpl.NewEtcdModuleWithCluster(cl, logger)
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	if err := mod.Init(context.Background()); err != nil {
		t.Fatalf("failed to init module: %v", err)
	}
	defer func() {
		_ = mod.Stop(context.Background())
	}()

	adapter := &etcdModuleAdapter{impl: mod}

	got := adapter.GetGlobalDiscovery()
	if got == nil {
		t.Fatalf("expected non-nil global discovery")
	}

	expected, err := mod.GetGlobalDiscovery()
	if err != nil {
		t.Fatalf("failed to get module global discovery: %v", err)
	}
	if got != expected {
		t.Fatalf("expected adapter to return module discovery set pointer")
	}
}

func TestEtcdModuleAdapter_AddOnNodeDiscoveryEvent_UsesCoreDiscoveryFlow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cl, err := cluster.NewEtcdCluster(mockClient, logger)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}

	mod, err := moduleimpl.NewEtcdModuleWithCluster(cl, logger)
	if err != nil {
		t.Fatalf("failed to create module: %v", err)
	}

	ds, err := discovery.NewEtcdDiscoverySet("/svc", logger)
	if err != nil {
		t.Fatalf("failed to create discovery set: %v", err)
	}
	if err := cl.RegisterDiscoverySet(ds); err != nil {
		t.Fatalf("failed to register discovery set: %v", err)
	}

	adapter := &etcdModuleAdapter{impl: mod}

	actions := make([]EtcdDiscoveryAction, 0, 2)
	putNodeID := uint64(0)
	h := adapter.AddOnNodeDiscoveryEvent(func(action EtcdDiscoveryAction, node *EtcdDiscoveryNode) {
		actions = append(actions, action)
		if action == EtcdDiscoveryActionPut && node != nil && node.Info != nil {
			putNodeID = node.Info.GetId()
		}
	})
	if h == 0 {
		t.Fatalf("expected non-zero callback handle")
	}

	ds.HandleWatcherEvent(watcher.EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:      "/svc/by_id/app-1",
		Revision: 1,
		Value: &pb.AtappDiscovery{
			Id:   1,
			Name: "app-1",
		},
	})
	ds.HandleWatcherEvent(watcher.EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE,
		Key:      "/svc/by_id/app-1",
		Revision: 2,
		Value: &pb.AtappDiscovery{
			Id:   1,
			Name: "app-1",
		},
	})

	if len(actions) != 2 {
		t.Fatalf("expected 2 callbacks from core discovery flow, got %d", len(actions))
	}
	if actions[0] != EtcdDiscoveryActionPut || actions[1] != EtcdDiscoveryActionDelete {
		t.Fatalf("unexpected action sequence: %v", actions)
	}
	if putNodeID != 1 {
		t.Fatalf("expected put callback node id=1, got %d", putNodeID)
	}

	adapter.RemoveOnNodeEvent(h)
	ds.HandleWatcherEvent(watcher.EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:      "/svc/by_id/app-2",
		Revision: 3,
		Value: &pb.AtappDiscovery{
			Id:   2,
			Name: "app-2",
		},
	})
	if len(actions) != 2 {
		t.Fatalf("expected callback removed, but got additional events: %v", actions)
	}
}

func TestAdapterConverters_PropagateActionAndVersion(t *testing.T) {
	nodeInfo := toNodeInfo(&moduleimpl.WatcherSenderList{
		Action: moduleimpl.DiscoveryActionDelete,
		Node: &discovery.DiscoveryNode{
			Info: &pb.AtappDiscovery{Id: 7, Name: "app-7"},
		},
	})
	if nodeInfo == nil {
		t.Fatalf("expected non-nil node info")
	}
	if nodeInfo.Action != EtcdDiscoveryActionDelete {
		t.Fatalf("expected delete action, got %v", nodeInfo.Action)
	}
	if nodeInfo.NodeDiscovery == nil || nodeInfo.NodeDiscovery.GetId() != 7 {
		t.Fatalf("expected propagated discovery node data")
	}

	storage := &moduleimpl.TopologyCompatStorage{
		Info: &pb.AtappTopologyInfo{Id: 99, Name: "svc-99"},
	}
	storage.CreateRevision = 10
	storage.ModRevision = 20
	storage.Version = 3

	topInfo := toTopologyInfo(&moduleimpl.TopologyCompatEvent{
		Action:  moduleimpl.TopologyActionDelete,
		Storage: storage,
	})
	if topInfo == nil {
		t.Fatalf("expected non-nil topology info")
	}
	if topInfo.Action != EtcdWatchEventDelete {
		t.Fatalf("expected delete topology action, got %v", topInfo.Action)
	}
	if topInfo.Storage.Info == nil || topInfo.Storage.Info.GetId() != 99 {
		t.Fatalf("expected propagated topology info")
	}
	if topInfo.Storage.Version.CreateRevision != 10 ||
		topInfo.Storage.Version.ModRevision != 20 ||
		topInfo.Storage.Version.Version != 3 {
		t.Fatalf("unexpected topology version propagation: %+v", topInfo.Storage.Version)
	}
}

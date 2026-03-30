package cluster

import (
	"context"
	"testing"

	"github.com/atframework/libatapp-go/etcd_module/client/mocks"
	"github.com/atframework/libatapp-go/etcd_module/discovery"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
	"github.com/golang/mock/gomock"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
	log "log/slog"
)

func TestWatcherSnapshotAppliesToDiscoverySet(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	ds, _ := discovery.NewEtcdDiscoverySet("/svc/", logger)
	if err := cluster.RegisterDiscoverySet(ds); err != nil {
		t.Fatalf("Failed to register discovery set: %v", err)
	}

	kv := &mvccpb.KeyValue{Key: []byte("/svc/by_name/svc-1")}
	nodeInfo := &pb.AtappDiscovery{Name: "svc", Id: 1}
	valueBytes, err := proto.Marshal(nodeInfo)
	if err != nil {
		t.Fatalf("Failed to marshal discovery info: %v", err)
	}
	kv.Value = valueBytes

	resp := &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 10},
		Kvs:    []*mvccpb.KeyValue{kv},
	}
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(resp, nil).AnyTimes()
	watchCh := make(chan clientv3.WatchResponse)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh).AnyTimes()

	// Act
	if err := cluster.AddWatcher(ctx, "/svc/by_name/", nil); err != nil {
		t.Fatalf("Failed to add watcher: %v", err)
	}
	close(watchCh)

	cluster.makeWatcherSnapshotEventsHandler("/svc/by_name/")([]*watcher.EtcdWatchEvent{
		{
			Key:      "/svc/by_name/svc-1",
			Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
			Revision: 10,
			Value:    nodeInfo,
		},
	})

	// Assert
	nodes := ds.GetAllNodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node after snapshot, got %d", len(nodes))
	}
}

func TestWatcherSnapshotEventsHandler_IgnoreNonMaster(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cluster, _ := NewEtcdCluster(mocks.NewMockEtcdClient(ctrl), log.Default())
	ds, _ := discovery.NewEtcdDiscoverySet("/svc/", log.Default())
	if err := cluster.RegisterDiscoverySet(ds); err != nil {
		t.Fatalf("register discovery set failed: %v", err)
	}

	base := &pb.AtappDiscovery{Name: "base", Id: 100}
	ds.AddNode(&discovery.DiscoveryNode{Path: "/svc/base", Info: base})

	cluster.watchers.snapshotIndexMu.Lock()
	cluster.watchers.masterWatcher = "/svc/master/"
	cluster.watchers.snapshotIndexMu.Unlock()

	nonMasterHandler := cluster.makeWatcherSnapshotEventsHandler("/svc/other/")
	nonMasterHandler([]*watcher.EtcdWatchEvent{{
		Key:      "/svc/other/new",
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Revision: 20,
		Value:    &pb.AtappDiscovery{Name: "new", Id: 200},
	}})

	nodes := ds.GetAllNodes()
	if len(nodes) != 1 {
		t.Fatalf("expected non-master snapshot to be ignored, node count=%d", len(nodes))
	}
	if nodes[0].Path != "/svc/base" {
		t.Fatalf("expected base node to remain, got %s", nodes[0].Path)
	}
}

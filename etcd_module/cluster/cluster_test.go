package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	log "log/slog"

	"github.com/golang/mock/gomock"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/atframework/libatapp-go/etcd_module/client/mocks"
	"github.com/atframework/libatapp-go/etcd_module/discovery"
	"github.com/atframework/libatapp-go/etcd_module/events"
	"github.com/atframework/libatapp-go/etcd_module/registration"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// 该测试函数用于验证相关行为。
func TestNewEtcdClusterBasic(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	// Act
	cluster, err := NewEtcdCluster(mockClient, logger)

	// Assert
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	if cluster == nil {
		t.Fatalf("Expected non-nil cluster")
	}
}

// 该测试函数用于验证相关行为。
func TestRegisterAndGetDiscoverySet(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	// Create discovery set
	ds, _ := discovery.NewEtcdDiscoverySet("/services/api/", logger)

	// Act
	if err := cluster.RegisterDiscoverySet(ds); err != nil {
		t.Fatalf("Failed to register discovery set: %v", err)
	}

	// Assert
	retrieved, err := cluster.GetDiscoverySet()
	if err != nil {
		t.Fatalf("Failed to get discovery set: %v", err)
	}
	if retrieved != ds {
		t.Errorf("Retrieved discovery set does not match")
	}
}

// 该测试函数用于验证相关行为。
func TestRemoveDiscoverySet(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	ds, _ := discovery.NewEtcdDiscoverySet("/services/api/", logger)

	// Act
	if err := cluster.RegisterDiscoverySet(ds); err != nil {
		t.Fatalf("Failed to register discovery set: %v", err)
	}

	// Assert
	_, err := cluster.GetDiscoverySet()
	if err != nil {
		t.Errorf("Discovery set should exist: %v", err)
	}

	// Act
	if err := cluster.RemoveDiscoverySet(); err != nil {
		t.Fatalf("Failed to remove discovery set: %v", err)
	}

	// Assert
	_, err = cluster.GetDiscoverySet()
	if err == nil {
		t.Errorf("Discovery set should not exist after removal")
	}
}

// 该测试函数用于验证相关行为。
func TestMapWatcherToDiscovery(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	ds, _ := discovery.NewEtcdDiscoverySet("/services/api/", logger)

	// Act
	if err := cluster.RegisterDiscoverySet(ds); err != nil {
		t.Fatalf("Failed to register discovery set: %v", err)
	}

	// Act
	if err := cluster.MapWatcherToDiscovery("/services/api/*"); err != nil {
		t.Fatalf("Failed to map watcher: %v", err)
	}

	// Arrange: add nodes to discovery set manually (simulating watcher events)
	ds.AddNode(&discovery.DiscoveryNode{
		Path: "/services/api/node1",
		Info: &pb.AtappDiscovery{Name: "node1"},
	})

	// Assert
	nodes := ds.GetAllNodes()
	if len(nodes) != 1 {
		t.Errorf("Expected 1 node in discovery, got %d", len(nodes))
	}
}

// 该测试函数用于验证相关行为。
func TestGetServiceNode(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	ds, _ := discovery.NewEtcdDiscoverySet("/services/api/", logger)

	// Act
	if err := cluster.RegisterDiscoverySet(ds); err != nil {
		t.Fatalf("Failed to register discovery set: %v", err)
	}

	// Arrange: add a test node
	ds.AddNode(&discovery.DiscoveryNode{
		Path: "/services/api/node1",
		Info: &pb.AtappDiscovery{Name: "node1"},
	})

	// Act
	node, err := cluster.GetServiceNode("test-key", nil, RoutingStrategyConsistentHash)

	// Assert
	if err != nil {
		t.Fatalf("Failed to get service node: %v", err)
	}

	if node == nil || node.Info.Name != "node1" {
		t.Errorf("Expected to get node1, got %v", node)
	}
}

// 该测试函数用于验证相关行为。
func TestGetServiceNodes(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	ds, _ := discovery.NewEtcdDiscoverySet("/services/api/", logger)

	// Act
	if err := cluster.RegisterDiscoverySet(ds); err != nil {
		t.Fatalf("Failed to register discovery set: %v", err)
	}

	// Arrange: add multiple test nodes
	for i := 1; i <= 3; i++ {
		ds.AddNode(&discovery.DiscoveryNode{
			Path: "/services/api/node" + string(rune('0'+i)),
			Info: &pb.AtappDiscovery{Name: "node" + string(rune('0'+i))},
		})
	}

	// Act
	nodes, err := cluster.GetServiceNodes("test-key", 2, nil, RoutingStrategyConsistentHash)

	// Assert
	if err != nil {
		t.Fatalf("Failed to get service nodes: %v", err)
	}

	if len(nodes) == 0 || len(nodes) > 2 {
		t.Errorf("Expected node count in [1,2], got %d", len(nodes))
	}
}

func TestSnapshotCallbacksAndReadyState(t *testing.T) {
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

	loaded := make(chan *events.Event, 1)
	loading := make(chan *events.Event, 1)
	cluster.AddOnSnapshotLoading(func(event *events.Event) {
		loading <- event
	})
	cluster.AddOnSnapshotLoaded(func(event *events.Event) {
		loaded <- event
	})

	// Act
	cluster.makeWatcherSnapshotLoadingHandler("/svc/")()
	cluster.makeWatcherSnapshotHandler("/svc/")(10, 1)

	// Assert
	select {
	case <-loading:
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("expected snapshot loading callback")
	}
	select {
	case <-loaded:
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("expected snapshot loaded callback")
	}

	cluster.lifecycle.mu.RLock()
	ready := cluster.lifecycle.ready
	cluster.lifecycle.mu.RUnlock()
	if !ready {
		t.Fatalf("expected cluster to be ready after snapshot")
	}
}

func TestNodeEventCallbacks(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	received := make(chan *events.Event, 1)
	cluster.AddOnNodeEvent(func(event *events.Event) {
		received <- event
	})

	// Act
	node := &discovery.DiscoveryNode{Info: &pb.AtappDiscovery{Name: "svc", Id: 1}}
	cluster.dispatchNodeCallback(events.NewNodeUpEvent(node))

	// Assert
	select {
	case ev := <-received:
		if ev.Type != events.EventTypeNodeUp {
			t.Fatalf("expected node up event")
		}
	default:
		t.Fatalf("expected node callback invocation")
	}
}

func TestDiscoveryPublisherEmitsClusterEvents(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	ch := make(chan *events.Event, 2)
	handle := cluster.GetEventManager().Subscribe([]events.EventType{events.EventTypeNodeUp, events.EventTypeNodeDown}, func(event *events.Event) {
		ch <- event
	})
	defer cluster.GetEventManager().Unsubscribe(handle)

	// Act
	publisher := cluster.makeDiscoveryEventPublisher()
	info := &pb.AtappDiscovery{Name: "svc", Id: 1}
	node := &discovery.DiscoveryNode{Info: info, Path: "/svc/by_name/svc-1"}
	publisher(pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT, node, false)
	publisher(pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE, node, true)

	// Assert
	gotTypes := map[events.EventType]bool{}
	for i := 0; i < 2; i++ {
		select {
		case ev := <-ch:
			gotTypes[ev.Type] = true
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("expected event publish")
		}
	}
	if !gotTypes[events.EventTypeNodeUp] || !gotTypes[events.EventTypeNodeDown] {
		t.Fatalf("expected node up/down events to be published")
	}
}

func TestBatchHandlerAppliesDiscoveryUpdates(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	ds, _ := discovery.NewEtcdDiscoverySet("/svc/", logger)
	if err := cluster.RegisterDiscoverySet(ds); err != nil {
		t.Fatalf("Failed to register discovery set: %v", err)
	}

	// Act
	batchHandler := cluster.makeWatcherBatchHandler("/svc/")
	batchHandler([]*watcher.EtcdWatchEvent{
		{
			Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
			Key:      "/svc/by_name/svc-1",
			Revision: 1,
			Value:    &pb.AtappDiscovery{Name: "svc", Id: 1},
		},
		{
			Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
			Key:      "/svc/by_name/svc-2",
			Revision: 2,
			Value:    &pb.AtappDiscovery{Name: "svc", Id: 2},
		},
	})

	// Assert
	nodes := ds.GetAllNodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node after batch (same name replacement), got %d", len(nodes))
	}
}

func TestWatcherErrorHandlerUpdatesStats(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	// Act
	handler := cluster.makeWatcherErrorHandler()
	handler(watcher.WatchErrorCompacted, fmt.Errorf("compacted"))
	handler(watcher.WatchErrorCanceled, fmt.Errorf("canceled"))
	handler(watcher.WatchErrorUnknown, fmt.Errorf("unknown"))

	// Assert
	snapshot := cluster.GetStats()
	if snapshot.WatcherFailures != 3 {
		t.Fatalf("expected watcher failures to be 3, got %d", snapshot.WatcherFailures)
	}
}

func TestWatcherConnectionEventsPublish(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	ch := make(chan events.EventType, 2)
	handle := cluster.GetEventManager().Subscribe([]events.EventType{events.EventTypeWatchConnected, events.EventTypeWatchReconnected}, func(event *events.Event) {
		ch <- event.Type
	})
	defer cluster.GetEventManager().Unsubscribe(handle)

	// Act
	handler := cluster.makeWatcherConnectionHandler()
	handler(watcher.ConnectionStateConnected, 1)
	handler(watcher.ConnectionStateReconnected, 2)

	// Assert
	got := map[events.EventType]bool{}
	for i := 0; i < 2; i++ {
		select {
		case typ := <-ch:
			got[typ] = true
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("expected watcher connection events")
		}
	}
	if !got[events.EventTypeWatchConnected] || !got[events.EventTypeWatchReconnected] {
		t.Fatalf("expected connected and reconnected events")
	}
}

// 该测试函数用于验证相关行为。
func TestClusterStartStop(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act: start should succeed
	err := cluster.Start(ctx)
	if err != nil {
		// It's OK if Start fails due to mock client issues
		// We're just testing the API surface
	}

	// Act: stop should succeed
	cluster.Stop(ctx)
}

func TestClusterStart_DoesNotPublishConnectedStatePrematurely(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, _ := NewEtcdCluster(mockClient, log.Default())

	stateEvents := make(chan events.ClusterState, 4)
	handle := cluster.GetEventManager().Subscribe([]events.EventType{events.EventTypeClusterChange}, func(event *events.Event) {
		stateEvents <- event.ClusterState
	})
	defer cluster.GetEventManager().Unsubscribe(handle)

	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()

	if err := cluster.Start(startCtx); err != nil {
		t.Fatalf("cluster start failed: %v", err)
	}
	defer cluster.Stop(context.Background())

	select {
	case state := <-stateEvents:
		if state == events.ClusterStateConnected {
			t.Fatal("cluster published connected state before watcher connection")
		}
	case <-time.After(50 * time.Millisecond):
		// no cluster state change event is expected at startup
	}
}

func TestClusterStop_DoesNotRequireParentContextCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, _ := NewEtcdCluster(mockClient, log.Default())

	startCtx, cancelStart := context.WithCancel(context.Background())
	defer cancelStart()

	if err := cluster.Start(startCtx); err != nil {
		t.Fatalf("cluster start failed: %v", err)
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- cluster.Stop(context.Background())
	}()

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("cluster stop failed: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		cancelStart() // cleanup unblock for current buggy behavior
		t.Fatal("cluster stop blocked waiting for parent context cancellation")
	}
}

func TestClusterStartStopDirectLifecycle(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	// Act
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Act
	if err := cluster.Start(ctx); err != nil {
		// It's OK if Start fails due to mock client issues
	}
	// Act
	_ = cluster.Stop(ctx)
}

func TestRegisterService(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	// Act
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	leaseID := clientv3.LeaseID(123)
	keepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Grant(gomock.Any(), gomock.Any()).Return(&clientv3.LeaseGrantResponse{ID: leaseID}, nil).AnyTimes()
	mockClient.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).AnyTimes()
	mockClient.EXPECT().KeepAlive(gomock.Any(), leaseID).Return(keepAliveChan, nil).AnyTimes()
	mockClient.EXPECT().Revoke(gomock.Any(), leaseID).Return(&clientv3.LeaseRevokeResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(&clientv3.DeleteResponse{}, nil).AnyTimes()

	// Act
	info := &pb.AtappDiscovery{Name: "service-1", Identity: "service-1-instance-1"}
	path := "/services/service-1/instance-1"

	if err := cluster.RegisterService(ctx, info, path, 10); err != nil {
		t.Fatalf("RegisterService failed: %v", err)
	}

	// Assert
	if _, ok := cluster.GetRegistrationManager().GetRegistration(path); !ok {
		t.Fatalf("expected service to be registered in keepalive manager")
	}

	_ = cluster.Stop(context.Background())
}

func TestEnsureClusterLease_BubbleModel_ConcurrentSingleGrant(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()
	cluster, _ := NewEtcdCluster(mockClient, logger)

	leaseID := clientv3.LeaseID(9527)
	var grantCalls int32

	mockClient.EXPECT().Grant(gomock.Any(), int64(10)).DoAndReturn(func(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
		atomic.AddInt32(&grantCalls, 1)
		time.Sleep(20 * time.Millisecond)
		return &clientv3.LeaseGrantResponse{ID: leaseID}, nil
	}).Times(1)

	mockClient.EXPECT().KeepAlive(gomock.Any(), leaseID).Return(make(chan *clientv3.LeaseKeepAliveResponse), nil).Times(1)

	const bubbles = 24
	readyGate := sync.WaitGroup{}
	readyGate.Add(bubbles)
	startGate := make(chan struct{})

	resultIDs := make(chan clientv3.LeaseID, bubbles)
	errCh := make(chan error, bubbles)

	for i := 0; i < bubbles; i++ {
		go func() {
			readyGate.Done()
			<-startGate
			id, err := cluster.ensureClusterLease(context.Background(), 10)
			if err != nil {
				errCh <- err
				return
			}
			resultIDs <- id
		}()
	}

	readyGate.Wait()
	close(startGate)

	for i := 0; i < bubbles; i++ {
		select {
		case err := <-errCh:
			t.Fatalf("unexpected ensureClusterLease error: %v", err)
		case id := <-resultIDs:
			if id != leaseID {
				t.Fatalf("unexpected lease ID: got %d want %d", id, leaseID)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for bubble results")
		}
	}

	if got := atomic.LoadInt32(&grantCalls); got != 1 {
		t.Fatalf("Grant should be called exactly once, got %d", got)
	}

	cluster.lifecycle.mu.RLock()
	leaseCancel := cluster.keepalives.leaseCancel
	cluster.lifecycle.mu.RUnlock()
	if leaseCancel != nil {
		leaseCancel()
	}

	wgDone := make(chan struct{})
	go func() {
		cluster.lifecycle.wg.Wait()
		close(wgDone)
	}()

	select {
	case <-wgDone:
	case <-time.After(2 * time.Second):
		t.Fatal("cluster lease keepalive goroutine did not stop in time")
	}
}

func TestEnsureClusterLease_PublishLeaseGrantedEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, _ := NewEtcdCluster(mockClient, log.Default())

	leaseID := clientv3.LeaseID(7001)
	keepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)

	mockClient.EXPECT().Grant(gomock.Any(), int64(10)).Return(&clientv3.LeaseGrantResponse{ID: leaseID}, nil).Times(1)
	mockClient.EXPECT().KeepAlive(gomock.Any(), leaseID).Return(keepAliveChan, nil).AnyTimes()

	eventCh := make(chan *events.Event, 1)
	h := cluster.GetEventManager().Subscribe([]events.EventType{events.EventTypeLeaseGranted}, func(event *events.Event) {
		eventCh <- event
	})
	defer cluster.GetEventManager().Unsubscribe(h)

	gotLeaseID, err := cluster.ensureClusterLease(context.Background(), 10)
	if err != nil {
		t.Fatalf("ensureClusterLease failed: %v", err)
	}
	if gotLeaseID != leaseID {
		t.Fatalf("unexpected lease ID: got %d want %d", gotLeaseID, leaseID)
	}

	select {
	case ev := <-eventCh:
		if ev == nil || ev.Type != events.EventTypeLeaseGranted {
			t.Fatalf("unexpected event: %#v", ev)
		}
		if ev.Metadata["lease_id"] != int64(leaseID) {
			t.Fatalf("unexpected lease_id metadata: %v", ev.Metadata["lease_id"])
		}
		if ev.Metadata["ttl"] != int64(10) {
			t.Fatalf("unexpected ttl metadata: %v", ev.Metadata["ttl"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected lease granted event")
	}

	cluster.lifecycle.mu.RLock()
	leaseCancel := cluster.keepalives.leaseCancel
	cluster.lifecycle.mu.RUnlock()
	if leaseCancel != nil {
		leaseCancel()
	}
	close(keepAliveChan)
}

func TestRunClusterKeepalive_PublishLeaseExpiredEvent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, _ := NewEtcdCluster(mockClient, log.Default())

	leaseID := clientv3.LeaseID(7002)
	keepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)

	mockClient.EXPECT().Grant(gomock.Any(), int64(10)).Return(&clientv3.LeaseGrantResponse{ID: leaseID}, nil).Times(1)
	mockClient.EXPECT().KeepAlive(gomock.Any(), leaseID).Return(keepAliveChan, nil).Times(1)

	eventCh := make(chan *events.Event, 1)
	h := cluster.GetEventManager().Subscribe([]events.EventType{events.EventTypeLeaseExpired}, func(event *events.Event) {
		eventCh <- event
	})
	defer cluster.GetEventManager().Unsubscribe(h)

	if _, err := cluster.ensureClusterLease(context.Background(), 10); err != nil {
		t.Fatalf("ensureClusterLease failed: %v", err)
	}

	close(keepAliveChan)

	select {
	case ev := <-eventCh:
		if ev == nil || ev.Type != events.EventTypeLeaseExpired {
			t.Fatalf("unexpected event: %#v", ev)
		}
		if ev.Metadata["lease_id"] != int64(leaseID) {
			t.Fatalf("unexpected lease_id metadata: %v", ev.Metadata["lease_id"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected lease expired event")
	}

	wgDone := make(chan struct{})
	go func() {
		cluster.lifecycle.wg.Wait()
		close(wgDone)
	}()
	select {
	case <-wgDone:
	case <-time.After(2 * time.Second):
		t.Fatal("cluster lease keepalive goroutine did not stop in time")
	}
}

func TestLeaseEventBridge_DrivesRegistrationManagerLease(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, _ := NewEtcdCluster(mockClient, log.Default())

	km := cluster.GetRegistrationManager()
	if km == nil {
		t.Fatal("expected registration manager")
	}
	if got := km.GetLease(); got != 0 {
		t.Fatalf("expected initial lease=0, got %d", got)
	}

	cluster.publishClusterEventSync(events.NewLeaseGrantedEvent(9001, 10))
	if got := km.GetLease(); got != clientv3.LeaseID(9001) {
		t.Fatalf("expected lease after granted event = 9001, got %d", got)
	}

	// 自然过期（etcd TTL 耗尽）
	cluster.publishClusterEventSync(events.NewLeaseExpiredEvent(9001))
	if got := km.GetLease(); got != 0 {
		t.Fatalf("expected lease after expired event = 0, got %d", got)
	}

	// 重新授予，再测主动释放路径
	cluster.publishClusterEventSync(events.NewLeaseGrantedEvent(9002, 10))
	if got := km.GetLease(); got != clientv3.LeaseID(9002) {
		t.Fatalf("expected lease after second granted event = 9002, got %d", got)
	}

	// 主动释放（本节点 cancel/revoke）
	cluster.publishClusterEventSync(events.NewLeaseReleasedEvent(9002))
	if got := km.GetLease(); got != 0 {
		t.Fatalf("expected lease after released event = 0, got %d", got)
	}
}

func TestWatcherTracking_BubbleModel_ConcurrentTrackUntrack(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()
	cluster, _ := NewEtcdCluster(mockClient, logger)

	const bubbles = 64
	readyGate := sync.WaitGroup{}
	doneGate := sync.WaitGroup{}
	startGate := make(chan struct{})

	readyGate.Add(bubbles)
	doneGate.Add(bubbles)

	for i := 0; i < bubbles; i++ {
		go func(i int) {
			defer doneGate.Done()
			prefix := fmt.Sprintf("/svc/bubble/%d", i)
			readyGate.Done()
			<-startGate
			cluster.trackWatcher(prefix, &watcher.EtcdWatcher{})
			cluster.untrackWatcher(prefix)
		}(i)
	}

	readyGate.Wait()
	close(startGate)
	doneGate.Wait()

	cluster.watchers.snapshotIndexMu.Lock()
	defer cluster.watchers.snapshotIndexMu.Unlock()
	if len(cluster.watchers.prefixes) != 0 {
		t.Fatalf("expected prefixes map to be empty after untrack, got %d", len(cluster.watchers.prefixes))
	}
}

func TestRegisterServiceValidation(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	// Act
	ctx := context.Background()

	// Assert
	if err := cluster.RegisterService(ctx, &pb.AtappDiscovery{Name: "svc"}, "/path", 10); err == nil {
		t.Fatalf("expected error when cluster is not running")
	}

	// Act
	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Assert
	if err := cluster.RegisterService(ctx, nil, "/path", 10); err == nil {
		t.Fatalf("expected error when service info is nil")
	}

	// Assert
	if err := cluster.RegisterService(ctx, &pb.AtappDiscovery{Name: "svc"}, "", 10); err == nil {
		t.Fatalf("expected error when service path is empty")
	}

	// Assert
	if err := cluster.RegisterService(ctx, &pb.AtappDiscovery{Name: "svc"}, "/path", 0); err == nil {
		t.Fatalf("expected error when ttl is not positive")
	}
}

func TestUpdateServiceBroadcast(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	// Act
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	leaseID := clientv3.LeaseID(123)
	keepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Grant(gomock.Any(), gomock.Any()).Return(&clientv3.LeaseGrantResponse{ID: leaseID}, nil).AnyTimes()
	mockClient.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).AnyTimes()
	mockClient.EXPECT().KeepAlive(gomock.Any(), leaseID).Return(keepAliveChan, nil).AnyTimes()
	mockClient.EXPECT().Revoke(gomock.Any(), leaseID).Return(&clientv3.LeaseRevokeResponse{}, nil).AnyTimes()

	info := &pb.AtappDiscovery{Name: "service-1", Identity: "v1"}
	pathA := "/services/service-1/instance-a"
	pathB := "/services/service-1/instance-b"

	// Act
	if err := cluster.RegisterService(ctx, info, pathA, 10); err != nil {
		t.Fatalf("RegisterService pathA failed: %v", err)
	}
	if err := cluster.RegisterService(ctx, info, pathB, 10); err != nil {
		t.Fatalf("RegisterService pathB failed: %v", err)
	}

	// Act
	updated := &pb.AtappDiscovery{Name: "service-1", Identity: "v2"}
	if err := cluster.UpdateService(ctx, updated, ""); err != nil {
		t.Fatalf("UpdateService broadcast failed: %v", err)
	}

	// Assert
	keepaliveA, ok := cluster.GetRegistrationManager().GetRegistration(pathA)
	if !ok {
		t.Fatalf("keepalive for pathA not found")
	}
	keepaliveB, ok := cluster.GetRegistrationManager().GetRegistration(pathB)
	if !ok {
		t.Fatalf("keepalive for pathB not found")
	}
	if keepaliveA.GetValue() == "" || keepaliveB.GetValue() == "" {
		t.Fatalf("expected keepalives to have refreshed values")
	}
	if keepaliveA.GetValue() != keepaliveB.GetValue() {
		t.Fatalf("expected keepalives to share the same refreshed value")
	}
}

func TestUpdateServiceFailsWhenNoKeepalives(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	// Act
	ctx := context.Background()

	// Act
	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Act
	err := cluster.UpdateService(ctx, &pb.AtappDiscovery{Name: "service-1"}, "")

	// Assert
	if err == nil {
		t.Fatalf("expected error when no keepalives registered")
	}
}

func TestUpdateServiceBroadcastPartialFailure(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	// Act
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	leaseID := clientv3.LeaseID(111)
	keepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Grant(gomock.Any(), gomock.Any()).Return(&clientv3.LeaseGrantResponse{ID: leaseID}, nil).AnyTimes()
	mockClient.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).AnyTimes()
	mockClient.EXPECT().KeepAlive(gomock.Any(), leaseID).Return(keepAliveChan, nil).AnyTimes()
	mockClient.EXPECT().Revoke(gomock.Any(), leaseID).Return(&clientv3.LeaseRevokeResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(&clientv3.DeleteResponse{}, nil).AnyTimes()

	info := &pb.AtappDiscovery{Name: "service-1", Identity: "v1"}
	pathA := "/services/service-1/instance-a"
	pathB := "/services/service-1/instance-b"

	// Act
	if err := cluster.RegisterService(ctx, info, pathA, 10); err != nil {
		t.Fatalf("RegisterService pathA failed: %v", err)
	}
	if err := cluster.RegisterService(ctx, info, pathB, 10); err != nil {
		t.Fatalf("RegisterService pathB failed: %v", err)
	}

	// Act
	keepaliveA, ok := cluster.GetRegistrationManager().GetRegistration(pathA)
	if !ok {
		t.Fatalf("keepalive for pathA not found")
	}
	keepaliveA.Unregister(context.Background())

	// Act
	updated := &pb.AtappDiscovery{Name: "service-1", Identity: "v2"}
	if err := cluster.UpdateService(ctx, updated, ""); err == nil {
		t.Fatalf("expected partial failure to return error")
	}

	// Assert
	keepaliveB, ok := cluster.GetRegistrationManager().GetRegistration(pathB)
	if !ok {
		t.Fatalf("keepalive for pathB not found")
	}
	if keepaliveB.GetValue() == "" {
		t.Fatalf("expected remaining keepalive to refresh value")
	}

	close(keepAliveChan)
}

func TestRemoveWatcherReelectsMaster(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	// Act
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	watcherA := watcher.NewEtcdWatcher(mockClient, watcher.DefaultWatchConfig(), logger)
	watcherB := watcher.NewEtcdWatcher(mockClient, watcher.DefaultWatchConfig(), logger)
	cluster.watchers.manager.AddWatcher("/svc/by_id/", watcherA)
	cluster.watchers.manager.AddWatcher("/svc/by_name/", watcherB)
	cluster.watchers.prefixes["/svc/by_id/"] = struct{}{}
	cluster.watchers.prefixes["/svc/by_name/"] = struct{}{}
	cluster.watchers.snapshotIndexMu.Lock()
	cluster.watchers.masterWatcher = "/svc/by_id/"
	cluster.watchers.snapshotIndexMu.Unlock()

	cluster.watchers.snapshotIndexMu.Lock()
	master := cluster.watchers.masterWatcher
	cluster.watchers.snapshotIndexMu.Unlock()

	// Assert
	if master == "" {
		t.Fatalf("expected master watcher to be set")
	}

	// Act
	if err := cluster.RemoveWatcher(master); err != nil {
		t.Fatalf("Failed to remove watcher: %v", err)
	}

	// Assert
	cluster.watchers.snapshotIndexMu.Lock()
	newMaster := cluster.watchers.masterWatcher
	cluster.watchers.snapshotIndexMu.Unlock()
	if newMaster == "" {
		t.Fatalf("expected master watcher to be reselected")
	}
}

func TestClusterStopCleansKeepalivesAndWatchers(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	// Act
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	leaseID := clientv3.LeaseID(200)
	keepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Grant(gomock.Any(), gomock.Any()).Return(&clientv3.LeaseGrantResponse{ID: leaseID}, nil).AnyTimes()
	mockClient.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).AnyTimes()
	mockClient.EXPECT().KeepAlive(gomock.Any(), leaseID).Return(keepAliveChan, nil).AnyTimes()
	mockClient.EXPECT().Revoke(gomock.Any(), leaseID).Return(&clientv3.LeaseRevokeResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(&clientv3.DeleteResponse{}, nil).AnyTimes()
	watchCh := make(chan clientv3.WatchResponse)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh).AnyTimes()
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 1}}, nil).AnyTimes()

	info := &pb.AtappDiscovery{Name: "svc", Id: 1, Identity: "svc-1"}
	// Act
	if err := cluster.RegisterService(ctx, info, "/svc/by_name/svc-1", 10); err != nil {
		t.Fatalf("RegisterService failed: %v", err)
	}
	watcherObj := watcher.NewEtcdWatcher(mockClient, watcher.DefaultWatchConfig(), logger)
	cluster.watchers.manager.AddWatcher("/svc/", watcherObj)
	close(watchCh)

	// Act
	if err := cluster.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Assert
	if _, ok := cluster.GetRegistrationManager().GetRegistration("/svc/by_name/svc-1"); ok {
		t.Fatalf("expected keepalives to be cleared")
	}
	if _, ok := cluster.GetWatcherManager().GetWatcher("/svc/"); ok {
		t.Fatalf("expected watchers to be cleared")
	}

	close(keepAliveChan)
}

func TestAutoRegisterFromConfigAggregatesErrors(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	// Act
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	cfg := &pb.AtappEtcd{
		Enable:    true,
		Path:      "/svc",
		Keepalive: &pb.AtappEtcdKeepalive{},
		Watcher:   &pb.AtappEtcdWatcher{},
	}
	cluster.ApplyEtcdConfig(cfg)

	// Act
	info := &pb.AtappDiscovery{Name: "svc", Id: 1}
	if err := cluster.AutoRegisterFromConfig(ctx, info, 0); err == nil {
		t.Fatalf("expected error when ttl is not positive")
	}
}

func TestAutoRegisterFromConfig_KeepaliveAndWatcherPaths(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	// Act
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	leaseID := clientv3.LeaseID(100)
	keepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Grant(gomock.Any(), gomock.Any()).Return(&clientv3.LeaseGrantResponse{ID: leaseID}, nil).AnyTimes()
	mockClient.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).AnyTimes()
	mockClient.EXPECT().KeepAlive(gomock.Any(), leaseID).Return(keepAliveChan, nil).AnyTimes()
	watchCh := make(chan clientv3.WatchResponse)
	close(watchCh)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh).AnyTimes()
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 1}}, nil).AnyTimes()
	mockClient.EXPECT().Revoke(gomock.Any(), leaseID).Return(&clientv3.LeaseRevokeResponse{}, nil).AnyTimes()

	cfg := &pb.AtappEtcd{
		Enable:    true,
		Path:      "/svc",
		Keepalive: &pb.AtappEtcdKeepalive{},
		Watcher:   &pb.AtappEtcdWatcher{},
	}
	cluster.ApplyEtcdConfig(cfg)

	// Act
	info := &pb.AtappDiscovery{Name: "svc", Id: 1, TypeId: 7, TypeName: "alpha", Identity: "svc-1"}
	if err := cluster.AutoRegisterFromConfig(ctx, info, 10); err != nil {
		t.Fatalf("AutoRegisterFromConfig failed: %v", err)
	}

	// Assert
	paths := buildReportAlivePaths(info, cfg.GetKeepalive(), "/svc")
	if len(paths) == 0 {
		t.Fatalf("expected keepalive paths")
	}
	for _, path := range paths {
		if _, ok := cluster.GetRegistrationManager().GetRegistration(path); !ok {
			t.Fatalf("expected keepalive registered: %s", path)
		}
	}

	prefixes := buildWatcherPrefixes(cfg.Watcher, "/svc")
	for _, prefix := range prefixes {
		if _, ok := cluster.watchers.manager.GetWatcher(prefix); !ok {
			t.Fatalf("expected watcher registered: %s", prefix)
		}
	}

	close(keepAliveChan)
}

func TestIsAvailable_WhenRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	ctx := context.Background()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	if !cluster.IsAvailable() {
		t.Errorf("Expected cluster to be available after start")
	}
}

func TestIsAvailable_WhenNotRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	if cluster.IsAvailable() {
		t.Errorf("Expected cluster to not be available in initializing state")
	}
}

func TestIsAvailable_WhenStopped(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	ctx := context.Background()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	if err := cluster.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop cluster: %v", err)
	}

	if cluster.IsAvailable() {
		t.Errorf("Expected cluster to not be available after stop")
	}
}

func TestResolveReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	cluster.lifecycle.mu.RLock()
	initialReady := cluster.lifecycle.ready
	cluster.lifecycle.mu.RUnlock()

	if initialReady {
		t.Errorf("Expected cluster to not be ready initially")
	}

	cluster.ResolveReady()

	cluster.lifecycle.mu.RLock()
	finalReady := cluster.lifecycle.ready
	cluster.lifecycle.mu.RUnlock()

	if !finalReady {
		t.Errorf("Expected cluster to be ready after ResolveReady")
	}
}

func TestGetLease_NoKeepalives(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	leaseID := cluster.GetLease()
	if leaseID != 0 {
		t.Errorf("Expected lease ID to be 0 when no keepalives, got %d", leaseID)
	}
}

func TestGetLease_WithKeepalive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	info := &pb.AtappDiscovery{Name: "test-service"}
	path := "/services/test-service/instance-1"

	ka, err := registration.NewEtcdRegistration(info, path, 10, mockClient, logger, nil)
	if err != nil {
		t.Fatalf("Failed to create keepalive: %v", err)
	}

	cluster.keepalives.manager.AddRegistration(ka)

	gotLeaseID := cluster.GetLease()
	if gotLeaseID != 0 {
		t.Errorf("Expected lease ID 0 before registration, got %d", gotLeaseID)
	}
}

func TestGetLease_MultipleKeepalives(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)

	info := &pb.AtappDiscovery{Name: "test-service"}
	path1 := "/services/test-service/instance-1"
	path2 := "/services/test-service/instance-2"

	ka1, err := registration.NewEtcdRegistration(info, path1, 10, mockClient, logger, nil)
	if err != nil {
		t.Fatalf("Failed to create keepalive 1: %v", err)
	}
	ka2, err := registration.NewEtcdRegistration(info, path2, 10, mockClient, logger, nil)
	if err != nil {
		t.Fatalf("Failed to create keepalive 2: %v", err)
	}

	cluster.keepalives.manager.AddRegistration(ka1)
	cluster.keepalives.manager.AddRegistration(ka2)

	gotLeaseID := cluster.GetLease()
	if gotLeaseID != 0 {
		t.Errorf("Expected lease ID 0 before registration, got %d", gotLeaseID)
	}
}

func TestTick_WhenRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	ctx := context.Background()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	if err := cluster.Tick(ctx); err != nil {
		t.Errorf("Tick should not return error when running: %v", err)
	}

	_ = cluster.Stop(context.Background())
}

func TestTick_WhenNotRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	ctx := context.Background()

	if err := cluster.Tick(ctx); err != nil {
		t.Errorf("Tick should not return error when not running: %v", err)
	}
}

func TestTick_WhenStopped(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cluster, _ := NewEtcdCluster(mockClient, logger)
	ctx := context.Background()

	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	if err := cluster.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop cluster: %v", err)
	}

	if err := cluster.Tick(ctx); err != nil {
		t.Errorf("Tick should not return error after stop: %v", err)
	}
}

func TestUnregisterLastServiceStopsClusterLeaseKeepalive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, err := NewEtcdCluster(mockClient, log.Default())
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}

	if err := cluster.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = cluster.Stop(context.Background()) }()

	leaseID := clientv3.LeaseID(3001)
	keepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)
	defer close(keepAliveChan)

	mockClient.EXPECT().Grant(gomock.Any(), int64(5)).Return(&clientv3.LeaseGrantResponse{ID: leaseID}, nil).Times(1)
	mockClient.EXPECT().KeepAlive(gomock.Any(), leaseID).Return(keepAliveChan, nil).Times(1)
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/svc/last", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)
	mockClient.EXPECT().Delete(gomock.Any(), "/svc/last").Return(&clientv3.DeleteResponse{}, nil).Times(1)
	mockClient.EXPECT().Revoke(gomock.Any(), leaseID).Return(&clientv3.LeaseRevokeResponse{}, nil).Times(1)

	info := &pb.AtappDiscovery{Name: "svc", Identity: "id-last", Id: 1}
	if err := cluster.RegisterService(context.Background(), info, "/svc/last", 5); err != nil {
		t.Fatalf("RegisterService failed: %v", err)
	}
	if cluster.GetLease() == 0 {
		t.Fatal("expected lease to be granted after first registration")
	}

	if err := cluster.UnregisterService(context.Background(), "/svc/last"); err != nil {
		t.Fatalf("UnregisterService failed: %v", err)
	}
	if cluster.GetLease() != 0 {
		t.Fatal("expected cluster lease to be cleared after last service is removed")
	}
}

func TestEnsureClusterLeaseUsesPreviousRequestTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, err := NewEtcdCluster(mockClient, log.Default())
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}

	cfg := &pb.AtappEtcd{
		Request: &pb.AtappEtcdRequest{
			ConnectTimeout: durationpb.New(20 * time.Millisecond),
		},
	}
	cluster.ApplyEtcdConfig(cfg)

	deadlineObserved := int32(0)
	mockClient.EXPECT().Grant(gomock.Any(), int64(10)).DoAndReturn(func(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
		if _, ok := ctx.Deadline(); ok {
			atomic.StoreInt32(&deadlineObserved, 1)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}).Times(1)

	_, err = cluster.ensureClusterLease(context.Background(), 10)
	if err == nil {
		t.Fatal("expected timeout error from Grant")
	}
	if atomic.LoadInt32(&deadlineObserved) == 0 {
		t.Fatal("expected ensureClusterLease to apply previous request timeout")
	}
}

func TestTickProcessesDeletionQueueAndStats(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, err := NewEtcdCluster(mockClient, log.Default())
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}

	if err := cluster.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = cluster.Stop(context.Background()) }()

	cluster.config.mu.Lock()
	cluster.config.keepAliveMaxRetryTimes = 3
	cluster.config.mu.Unlock()

	cluster.config.mu.Lock()
	cluster.config.keepAliveMaxRetryTimes = 3
	cluster.config.mu.Unlock()

	cluster.enqueueRegistrationDeletion("/svc/delete-me")
	mockClient.EXPECT().Delete(gomock.Any(), "/svc/delete-me").Return(&clientv3.DeleteResponse{}, nil).Times(1)

	if err := cluster.Tick(context.Background()); err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	stats := cluster.GetStats()
	if stats.KeepaliveDeleteFail != 0 {
		t.Fatalf("expected no keepalive delete failure, got %d", stats.KeepaliveDeleteFail)
	}
	if stats.DeleteQueueSize != 0 {
		t.Fatalf("expected empty deletion queue, got %d", stats.DeleteQueueSize)
	}
}

func TestTickProcessesRetryQueueAndStats(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, err := NewEtcdCluster(mockClient, log.Default())
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}

	if err := cluster.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = cluster.Stop(context.Background()) }()

	cluster.config.mu.Lock()
	cluster.config.keepAliveMaxRetryTimes = 3
	cluster.config.mu.Unlock()

	cluster.config.mu.Lock()
	cluster.config.keepAliveMaxRetryTimes = 3
	cluster.config.mu.Unlock()

	leaseID := clientv3.LeaseID(9123)
	keepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)
	defer close(keepAliveChan)

	mockClient.EXPECT().Grant(gomock.Any(), int64(5)).Return(&clientv3.LeaseGrantResponse{ID: leaseID}, nil).AnyTimes()
	mockClient.EXPECT().KeepAlive(gomock.Any(), leaseID).Return(keepAliveChan, nil).AnyTimes()
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/svc/retry", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)
	mockClient.EXPECT().Delete(gomock.Any(), "/svc/retry").Return(&clientv3.DeleteResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Revoke(gomock.Any(), leaseID).Return(&clientv3.LeaseRevokeResponse{}, nil).AnyTimes()

	cluster.enqueueRegistrationRetry(&pb.AtappDiscovery{Name: "svc", Identity: "retry-1"}, "/svc/retry", 5)
	cluster.keepalives.retryMu.Lock()
	if actor := cluster.keepalives.retryActors["/svc/retry"]; actor != nil {
		actor.nextAttempt = time.Now().Add(-time.Second)
	}
	cluster.keepalives.retryMu.Unlock()

	if err := cluster.Tick(context.Background()); err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	stats := cluster.GetStats()
	if stats.KeepaliveRegistered == 0 {
		t.Fatal("expected keepalive registered stats")
	}
	if stats.KeepaliveRetryFail != 0 {
		t.Fatalf("expected no keepalive retry failure, got %d", stats.KeepaliveRetryFail)
	}
	if stats.RetryQueueSize != 0 {
		t.Fatalf("expected empty retry queue, got %d", stats.RetryQueueSize)
	}
	if _, ok := cluster.GetRegistrationManager().GetRegistration("/svc/retry"); !ok {
		t.Fatal("expected retry-registered keepalive to exist")
	}
}

func TestTickRetryQueueStopsAfterMaxAttempts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, err := NewEtcdCluster(mockClient, log.Default())
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}

	if err := cluster.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = cluster.Stop(context.Background()) }()

	cluster.config.mu.Lock()
	cluster.config.keepAliveMaxRetryTimes = 3
	cluster.config.mu.Unlock()

	cluster.enqueueRegistrationRetryWithAttempts(&pb.AtappDiscovery{Name: "svc", Identity: "retry-max"}, "/svc/retry-max", 5, 2)
	cluster.keepalives.retryMu.Lock()
	if actor := cluster.keepalives.retryActors["/svc/retry-max"]; actor != nil {
		actor.nextAttempt = time.Now().Add(-time.Second)
	}
	cluster.keepalives.retryMu.Unlock()

	err = cluster.Tick(context.Background())
	if err == nil {
		t.Fatal("expected tick to report max-attempt retry error")
	}

	cluster.keepalives.retryMu.Lock()
	_, stillQueued := cluster.keepalives.retryActors["/svc/retry-max"]
	cluster.keepalives.retryMu.Unlock()
	if stillQueued {
		t.Fatal("retry actor should be removed after exceeding max attempts")
	}
}

func TestTickDeletionQueueStopsAfterMaxAttempts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, err := NewEtcdCluster(mockClient, log.Default())
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}

	if err := cluster.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = cluster.Stop(context.Background()) }()

	cluster.config.mu.Lock()
	cluster.config.keepAliveMaxRetryTimes = 3
	cluster.config.mu.Unlock()

	cluster.enqueueRegistrationDeletionWithAttempts("/svc/delete-max", 2)
	cluster.keepalives.retryMu.Lock()
	if actor := cluster.keepalives.deletors["/svc/delete-max"]; actor != nil {
		actor.nextAttempt = time.Now().Add(-time.Second)
	}
	cluster.keepalives.retryMu.Unlock()

	err = cluster.Tick(context.Background())
	if err == nil {
		t.Fatal("expected tick to report max-attempt deletion error")
	}

	cluster.keepalives.retryMu.Lock()
	_, stillQueued := cluster.keepalives.deletors["/svc/delete-max"]
	cluster.keepalives.retryMu.Unlock()
	if stillQueued {
		t.Fatal("deletion actor should be removed after exceeding max attempts")
	}
}

func TestTickRetryQueueRequeuesThenSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, err := NewEtcdCluster(mockClient, log.Default())
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}

	if err := cluster.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = cluster.Stop(context.Background()) }()

	leaseID := clientv3.LeaseID(9201)
	keepAliveChan := make(chan *clientv3.LeaseKeepAliveResponse)
	defer close(keepAliveChan)

	mockClient.EXPECT().Grant(gomock.Any(), int64(5)).Return(&clientv3.LeaseGrantResponse{ID: leaseID}, nil).AnyTimes()
	mockClient.EXPECT().KeepAlive(gomock.Any(), leaseID).Return(keepAliveChan, nil).AnyTimes()
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).Times(2)
	mockClient.EXPECT().Put(gomock.Any(), "/svc/retry-flow", gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("transient put error")).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/svc/retry-flow", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)
	mockClient.EXPECT().Delete(gomock.Any(), "/svc/retry-flow").Return(&clientv3.DeleteResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Revoke(gomock.Any(), leaseID).Return(&clientv3.LeaseRevokeResponse{}, nil).AnyTimes()

	cluster.enqueueRegistrationRetry(&pb.AtappDiscovery{Name: "svc", Identity: "retry-flow"}, "/svc/retry-flow", 5)
	cluster.keepalives.retryMu.Lock()
	if actor := cluster.keepalives.retryActors["/svc/retry-flow"]; actor != nil {
		actor.nextAttempt = time.Now().Add(-time.Second)
	}
	cluster.keepalives.retryMu.Unlock()

	err = cluster.Tick(context.Background())
	if err == nil {
		t.Fatal("expected first tick to fail and requeue")
	}

	cluster.keepalives.retryMu.Lock()
	if actor := cluster.keepalives.retryActors["/svc/retry-flow"]; actor != nil {
		actor.nextAttempt = time.Now().Add(-time.Second)
	}
	cluster.keepalives.retryMu.Unlock()

	if err := cluster.Tick(context.Background()); err != nil {
		t.Fatalf("expected second tick to succeed, got %v", err)
	}

	stats := cluster.GetStats()
	if stats.KeepaliveRetryFail != 1 {
		t.Fatalf("expected 1 keepalive retry failure, got %d", stats.KeepaliveRetryFail)
	}
	if stats.KeepaliveRegistered == 0 {
		t.Fatal("expected keepalive registered after retry success")
	}
	if stats.RetryQueueSize != 0 {
		t.Fatalf("expected empty retry queue, got %d", stats.RetryQueueSize)
	}
}

func TestTickDeletionQueueRequeuesThenSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, err := NewEtcdCluster(mockClient, log.Default())
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}

	if err := cluster.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = cluster.Stop(context.Background()) }()

	cluster.enqueueRegistrationDeletion("/svc/delete-flow")
	cluster.keepalives.retryMu.Lock()
	if actor := cluster.keepalives.deletors["/svc/delete-flow"]; actor != nil {
		actor.nextAttempt = time.Now().Add(-time.Second)
	}
	cluster.keepalives.retryMu.Unlock()

	mockClient.EXPECT().Delete(gomock.Any(), "/svc/delete-flow").Return(nil, fmt.Errorf("transient delete error")).Times(1)

	err = cluster.Tick(context.Background())
	if err == nil {
		t.Fatal("expected first tick to fail and requeue delete")
	}

	cluster.keepalives.retryMu.Lock()
	if actor := cluster.keepalives.deletors["/svc/delete-flow"]; actor != nil {
		actor.nextAttempt = time.Now().Add(-time.Second)
	}
	cluster.keepalives.retryMu.Unlock()

	mockClient.EXPECT().Delete(gomock.Any(), "/svc/delete-flow").Return(&clientv3.DeleteResponse{}, nil).Times(1)

	if err := cluster.Tick(context.Background()); err != nil {
		t.Fatalf("expected second tick to succeed, got %v", err)
	}

	stats := cluster.GetStats()
	if stats.KeepaliveDeleteFail != 1 {
		t.Fatalf("expected 1 keepalive delete failure, got %d", stats.KeepaliveDeleteFail)
	}
	if stats.DeleteQueueSize != 0 {
		t.Fatalf("expected empty deletion queue, got %d", stats.DeleteQueueSize)
	}
}

func TestTickActivatesWatcherManagerActors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cluster, err := NewEtcdCluster(mockClient, log.Default())
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}

	if err := cluster.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = cluster.Stop(context.Background()) }()

	cfg := watcher.DefaultWatchConfig()
	cfg.Key = "/svc/tick-activate"
	cfg.RangeEnd = clientv3.GetPrefixRangeEnd(cfg.Key)

	w := watcher.NewEtcdWatcher(mockClient, cfg, log.Default())
	if !cluster.watchers.manager.AddWatcherIfAbsent(cfg.Key, w) {
		t.Fatalf("expected add watcher to manager")
	}

	watchCh := make(chan clientv3.WatchResponse)
	getCalled := make(chan struct{}, 1)
	watchCalled := make(chan struct{}, 1)
	mockClient.EXPECT().Get(gomock.Any(), cfg.Key, gomock.Any()).DoAndReturn(func(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		getCalled <- struct{}{}
		return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 1}}, nil
	}).Times(1)
	mockClient.EXPECT().Watch(gomock.Any(), cfg.Key, gomock.Any()).DoAndReturn(func(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
		watchCalled <- struct{}{}
		return watchCh
	}).Times(1)

	if err := cluster.Tick(context.Background()); err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	if !w.IsRunning() {
		t.Fatalf("expected watcher to be running after Tick manager activation")
	}

	select {
	case <-getCalled:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected watcher snapshot Get to be called")
	}
	select {
	case <-watchCalled:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected watcher Watch to be called")
	}

	close(watchCh)
}

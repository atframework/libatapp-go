package watcher

import (
	"context"
	"testing"
	"time"

	"github.com/atframework/libatapp-go/etcd_module/client/mocks"
	pb "github.com/atframework/libatapp-go/protocol/atframe"

	log "log/slog"

	"github.com/golang/mock/gomock"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

func TestNewEtcdWatcher(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cfg := DefaultWatchConfig()
	cfg.Key = "/test/key"

	// Act
	watcher := NewEtcdWatcher(mockClient, cfg, logger)

	// Assert
	if watcher == nil {
		t.Fatal("Expected non-nil watcher")
	}

	if watcher.IsRunning() {
		t.Error("Expected watcher to not be running initially")
	}
}

func TestWatcherStartStop(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cfg := DefaultWatchConfig()
	cfg.Key = "/test/key"
	cfg.RetryInterval = 100 * time.Millisecond

	mockClient.EXPECT().Get(gomock.Any(), cfg.Key, gomock.Any()).Return(&clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 1},
	}, nil).AnyTimes()
	watchCh := make(chan clientv3.WatchResponse)
	close(watchCh)
	mockClient.EXPECT().Watch(gomock.Any(), cfg.Key, gomock.Any()).Return(watchCh).AnyTimes()

	watcher := NewEtcdWatcher(mockClient, cfg, logger)

	// Act
	ctx := context.Background()
	err := watcher.Start(ctx)

	// Assert
	if err != nil {
		t.Fatalf("Expected no error on start, got %v", err)
	}

	if !watcher.IsRunning() {
		t.Error("Expected watcher to be running")
	}

	// Act: stop should work without error
	watcher.Stop()

	// Assert
	if watcher.IsRunning() {
		t.Error("Expected watcher to not be running after stop")
	}
}

func TestWatcherDoubleStart(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cfg := DefaultWatchConfig()
	cfg.Key = "/test/key"

	// Setup mock expectations for loadSnapshot and watchLoop operations
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 1},
	}, nil).AnyTimes()
	watchCh := make(chan clientv3.WatchResponse, 1)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh).AnyTimes()

	watcher := NewEtcdWatcher(mockClient, cfg, logger)

	// Act
	ctx := context.Background()

	err := watcher.Start(ctx)
	// Assert
	if err != nil {
		t.Fatalf("Expected no error on first start, got %v", err)
	}

	// Act
	err = watcher.Start(ctx)

	// Assert
	if err == nil {
		t.Error("Expected error on second start")
	}

	watcher.Stop()
}

func TestWatcherSetHandler(t *testing.T) {
	// Arrange
	l := log.Default()
	watcher := NewEtcdWatcher(nil, DefaultWatchConfig(), l)

	handler := func(event EtcdWatchEvent) {}

	// Act
	watcher.SetHandler(handler)
	watcher.SetErrorHandler(func(err error) {})

	// Assert: verify no panic
	watcher.Stop()
}

func TestWatcherOneShotHandler(t *testing.T) {
	// Arrange
	logger := log.Default()
	watcher := NewEtcdWatcher(nil, DefaultWatchConfig(), logger)

	callCount := 0
	watcher.AddOneShotHandler(func(event EtcdWatchEvent) {
		callCount++
	})

	// Act
	event := EtcdWatchEvent{Type: pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT, Key: "/test"}
	watcher.dispatchEvent(event)
	watcher.dispatchEvent(event)

	// Assert
	if callCount != 1 {
		t.Errorf("Expected one-shot handler to fire once, got %d", callCount)
	}
}

func TestWatcherConfig(t *testing.T) {
	// Act
	cfg := DefaultWatchConfig()

	// Assert
	if cfg.Key != "" {
		t.Errorf("Expected empty key, got %s", cfg.Key)
	}

	if cfg.RetryInterval != 15*time.Second {
		t.Errorf("Expected RetryInterval 15s, got %v", cfg.RetryInterval)
	}

	if cfg.RequestTimeout != 1*time.Hour {
		t.Errorf("Expected RequestTimeout 1h, got %v", cfg.RequestTimeout)
	}

	if cfg.GetRequestTimeout != 3*time.Minute {
		t.Errorf("Expected GetRequestTimeout 3m, got %v", cfg.GetRequestTimeout)
	}
}

func TestEtcdWatchEventType(t *testing.T) {
	// Assert
	if pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT.String() != "ETCD_WATCH_EVENT_PUT" {
		t.Errorf("Expected ETCD_WATCH_EVENT_PUT, got %s", pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT.String())
	}

	if pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE.String() != "ETCD_WATCH_EVENT_DELETE" {
		t.Errorf("Expected ETCD_WATCH_EVENT_DELETE, got %s", pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE.String())
	}

	unknown := pb.EtcdWatchEventType(100)
	if unknown.String() != "100" {
		t.Errorf("Expected 100, got %s", unknown.String())
	}
}

func TestWatcherManager(t *testing.T) {
	// Act
	logger := log.Default()
	manager := NewEtcdWatcherManager(logger)

	// Assert
	if manager == nil {
		t.Fatal("Expected non-nil manager")
	}
}

func TestCreateServiceWatcher(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	handler := func(event EtcdWatchEvent) {}

	// Act
	watcher := CreateServiceWatcher(mockClient, "/services/myapp", handler, logger)

	// Assert
	if watcher == nil {
		t.Fatal("Expected non-nil watcher")
	}

	if watcher.config.Key != "/services/myapp" {
		t.Errorf("Expected key /services/myapp, got %s", watcher.config.Key)
	}

	if watcher.config.RangeEnd != "/services/myapq" {
		t.Errorf("Expected RangeEnd /services/myapq, got %s", watcher.config.RangeEnd)
	}

	watcher.Stop()
}

func TestEtcdWatchEventMarshalJSON(t *testing.T) {
	// Arrange
	event := EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:      "/test/key",
		Revision: 100,
	}

	// Act
	data, err := event.MarshalJSON()

	// Assert
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(data) == 0 {
		t.Error("Expected non-empty JSON")
	}
}

func TestWatcherStateTracking(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	watcher := NewEtcdWatcher(mockClient, DefaultWatchConfig(), logger)

	// Assert
	if watcher.IsConnected() {
		t.Error("Expected not connected initially")
	}

	if watcher.GetWatchRevision() != 0 {
		t.Errorf("Expected revision 0, got %d", watcher.GetWatchRevision())
	}
}

func TestWatcherWithProgressNotify(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cfg := DefaultWatchConfig()
	cfg.ProgressNotify = true
	cfg.StartRevision = 100

	// Act
	watcher := NewEtcdWatcher(mockClient, cfg, logger)

	// Assert
	if watcher.config.StartRevision != 100 {
		t.Errorf("Expected StartRevision 100, got %d", watcher.config.StartRevision)
	}

	watcher.Stop()
}

func TestWatcherConnectionState_ReconnectAfterPreviousSession(t *testing.T) {
	watcher := NewEtcdWatcher(nil, DefaultWatchConfig(), log.Default())

	states := make([]ConnectionState, 0, 2)
	watcher.SetConnectionHandler(func(state ConnectionState, revision int64) {
		states = append(states, state)
	})

	firstResp := clientv3.WatchResponse{
		Header: etcdserverpb.ResponseHeader{Revision: 10},
		Events: []*clientv3.Event{{Kv: &mvccpb.KeyValue{ModRevision: 10}}},
	}
	watcher.updateConnectionState(firstResp)

	watcher.resetConnectionState()

	secondResp := clientv3.WatchResponse{
		Header: etcdserverpb.ResponseHeader{Revision: 20},
		Events: []*clientv3.Event{{Kv: &mvccpb.KeyValue{ModRevision: 20}}},
	}
	watcher.updateConnectionState(secondResp)

	if len(states) != 2 {
		t.Fatalf("expected 2 connection callbacks, got %d", len(states))
	}
	if states[0] != ConnectionStateConnected {
		t.Fatalf("expected first state connected, got %v", states[0])
	}
	if states[1] != ConnectionStateReconnected {
		t.Fatalf("expected second state reconnected, got %v", states[1])
	}
}

func TestWatcherLoadSnapshot_ErrorStillFiresLoaded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	watcher := NewEtcdWatcher(mockClient, WatchConfig{Key: "/test", RequestTimeout: time.Second}, log.Default())

	loadingCalled := false
	loadedCalled := false
	watcher.SetSnapshotLoadingHandler(func() {
		loadingCalled = true
	})
	watcher.SetSnapshotLoadedHandler(func(nodes []*EtcdWatchEvent) {
		loadedCalled = true
	})

	mockClient.EXPECT().Get(gomock.Any(), "/test", gomock.Any()).Return(nil, context.DeadlineExceeded).Times(1)

	err := watcher.loadSnapshot(context.Background())
	if err == nil {
		t.Fatal("expected loadSnapshot error")
	}
	if !loadingCalled {
		t.Fatal("expected snapshot loading handler to be called")
	}
	if !loadedCalled {
		t.Fatal("expected snapshot loaded handler to be called on error path")
	}
}

func TestWatcherLoadSnapshot_DoesNotDispatchMainHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	watcher := NewEtcdWatcher(mockClient, WatchConfig{Key: "/test", RequestTimeout: time.Second}, log.Default())

	handlerCount := 0
	loadedCount := 0
	watcher.SetHandler(func(event EtcdWatchEvent) {
		handlerCount++
	})
	watcher.SetSnapshotLoadedHandler(func(nodes []*EtcdWatchEvent) {
		loadedCount = len(nodes)
	})

	info1 := &pb.AtappDiscovery{Name: "a", Id: 1}
	info2 := &pb.AtappDiscovery{Name: "b", Id: 2}
	b1, err := proto.Marshal(info1)
	if err != nil {
		t.Fatalf("marshal info1 failed: %v", err)
	}
	b2, err := proto.Marshal(info2)
	if err != nil {
		t.Fatalf("marshal info2 failed: %v", err)
	}

	resp := &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 7},
		Kvs: []*mvccpb.KeyValue{
			{Key: []byte("/test/a"), Value: b1, ModRevision: 6},
			{Key: []byte("/test/b"), Value: b2, ModRevision: 7},
		},
	}
	mockClient.EXPECT().Get(gomock.Any(), "/test", gomock.Any()).Return(resp, nil).Times(1)

	if err := watcher.loadSnapshot(context.Background()); err != nil {
		t.Fatalf("loadSnapshot failed: %v", err)
	}

	if handlerCount != 0 {
		t.Fatalf("expected main handler not dispatched during snapshot load, got %d", handlerCount)
	}
	if loadedCount != 2 {
		t.Fatalf("expected snapshotLoaded handler to receive 2 nodes, got %d", loadedCount)
	}
}

func TestWatcherWithPrevKV(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cfg := DefaultWatchConfig()
	cfg.PrevKV = true

	// Act
	watcher := NewEtcdWatcher(mockClient, cfg, logger)

	// Assert
	if !watcher.config.PrevKV {
		t.Error("Expected PrevKV to be true")
	}

	watcher.Stop()
}

func TestWatcherWithRange(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cfg := DefaultWatchConfig()
	cfg.Key = "/services/"
	cfg.RangeEnd = "/services/myaq"

	// Act
	watcher := NewEtcdWatcher(mockClient, cfg, logger)

	// Assert
	if watcher.config.RangeEnd != "/services/myaq" {
		t.Errorf("Expected RangeEnd /services/myaq, got %s", watcher.config.RangeEnd)
	}

	watcher.Stop()
}

func TestWatcherFilterOptions(t *testing.T) {
	// Arrange
	tests := []struct {
		filter   EventFilter
		expected string
	}{
		{FilterNone, "FilterNone"},
		{FilterPut, "FilterPut"},
		{FilterDelete, "FilterDelete"},
		{FilterPrevKV, "FilterPrevKV"},
	}

	for _, tt := range tests {
		// Act
		cfg := DefaultWatchConfig()
		cfg.Filter = tt.filter

		watcher := NewEtcdWatcher(nil, cfg, nil)

		// Assert
		if watcher.config.Filter != tt.filter {
			t.Errorf("Expected filter %v, got %v", tt.filter, watcher.config.Filter)
		}
	}
}

func TestEtcdWatchEventWithProtoValue(t *testing.T) {
	// Arrange
	discovery := &pb.AtappDiscovery{
		Name: "test-service",
		Id:   12345,
	}

	event := EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:      "/test/key",
		Value:    discovery,
		Revision: 100,
	}

	// Assert
	if event.Value.Name != "test-service" {
		t.Errorf("Expected name test-service, got %s", event.Value.Name)
	}
}

func TestParseEvent_MergesVersionFromPrevKV(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	watcher := NewEtcdWatcher(mocks.NewMockEtcdClient(ctrl), DefaultWatchConfig(), log.Default())

	cur := &pb.AtappDiscovery{Name: "cur", Id: 1}
	prev := &pb.AtappDiscovery{Name: "prev", Id: 1}
	curBytes, err := proto.Marshal(cur)
	if err != nil {
		t.Fatalf("marshal current failed: %v", err)
	}
	prevBytes, err := proto.Marshal(prev)
	if err != nil {
		t.Fatalf("marshal prev failed: %v", err)
	}

	e := &clientv3.Event{
		Type: mvccpb.PUT,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/svc/a"),
			Value:          curBytes,
			CreateRevision: 10,
			ModRevision:    11,
			Version:        2,
		},
		PrevKv: &mvccpb.KeyValue{
			Value:          prevBytes,
			CreateRevision: 12,
			ModRevision:    15,
			Version:        9,
		},
	}

	watchEvent, err := watcher.parseEvent(e)
	if err != nil {
		t.Fatalf("parseEvent failed: %v", err)
	}

	if watchEvent.CreateRevision != 12 {
		t.Fatalf("expected merged CreateRevision=12, got %d", watchEvent.CreateRevision)
	}
	if watchEvent.ModRevision != 15 {
		t.Fatalf("expected merged ModRevision=15, got %d", watchEvent.ModRevision)
	}
	if watchEvent.Version != 9 {
		t.Fatalf("expected merged Version=9, got %d", watchEvent.Version)
	}
	if watchEvent.Revision != 15 {
		t.Fatalf("expected Revision to follow merged ModRevision=15, got %d", watchEvent.Revision)
	}
	if string(watchEvent.RawValue) != string(curBytes) {
		t.Fatalf("expected RawValue preserved")
	}
	if string(watchEvent.RawPrevValue) != string(prevBytes) {
		t.Fatalf("expected RawPrevValue preserved")
	}
}

func TestBatchConfig(t *testing.T) {
	// Act
	cfg := DefaultBatchConfig()

	// Assert
	if !cfg.Enabled {
		t.Error("Expected batch enabled by default")
	}

	if cfg.MaxBatchSize != 100 {
		t.Errorf("Expected MaxBatchSize 100, got %d", cfg.MaxBatchSize)
	}

	if cfg.MaxBatchDelay != 500*time.Millisecond {
		t.Errorf("Expected MaxBatchDelay 500ms, got %v", cfg.MaxBatchDelay)
	}
}

func TestWatcherBatchModeConfiguration(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	watcher := NewEtcdWatcher(mockClient, DefaultWatchConfig(), logger)

	cfg := DefaultBatchConfig()
	cfg.MaxBatchSize = 50
	cfg.MaxBatchDelay = 100 * time.Millisecond

	// Act
	watcher.SetBatchMode(cfg)
	watcher.SetBatchHandler(func(events []*EtcdWatchEvent) {})

	// Assert
	watcher.mu.RLock()
	defer watcher.mu.RUnlock()
	if watcher.batchConfig.MaxBatchSize != 50 {
		t.Errorf("Expected MaxBatchSize 50, got %d", watcher.batchConfig.MaxBatchSize)
	}
	if watcher.batchConfig.MaxBatchDelay != 100*time.Millisecond {
		t.Errorf("Expected MaxBatchDelay 100ms, got %v", watcher.batchConfig.MaxBatchDelay)
	}
}

func TestWatcherBatchHandlerCallback(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	watcher := NewEtcdWatcher(mockClient, DefaultWatchConfig(), logger)

	batchHandlerCalled := false
	var receivedEvents []*EtcdWatchEvent

	batchHandler := func(events []*EtcdWatchEvent) {
		batchHandlerCalled = true
		receivedEvents = events
	}

	// Act
	watcher.SetBatchHandler(batchHandler)

	watcher.mu.RLock()
	handler := watcher.batchHandler
	watcher.mu.RUnlock()

	// Assert
	if handler == nil {
		t.Fatal("Expected batch handler to be set")
	}

	testEvent := &EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:      "/test/key",
		Revision: 100,
	}

	// Act
	handler([]*EtcdWatchEvent{testEvent})

	// Assert
	if !batchHandlerCalled {
		t.Error("Expected batch handler to be called")
	}

	if len(receivedEvents) != 1 {
		t.Errorf("Expected 1 event, got %d", len(receivedEvents))
	}

	if receivedEvents[0].Key != "/test/key" {
		t.Errorf("Expected key /test/key, got %s", receivedEvents[0].Key)
	}
}

func TestWatcherCompactionTriggersSnapshotReload(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cfg := DefaultWatchConfig()
	cfg.Key = "/svc/"
	cfg.RangeEnd = "/svc0"

	w := NewEtcdWatcher(mockClient, cfg, logger)
	errCh := make(chan WatchErrorKind, 1)
	w.SetWatchErrorHandler(func(kind WatchErrorKind, err error) {
		errCh <- kind
	})

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 10},
	}, nil).AnyTimes()
	watchCh := make(chan clientv3.WatchResponse, 1)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Act
	if err := w.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	// Act
	watchCh <- clientv3.WatchResponse{Canceled: true, CompactRevision: 5}
	close(watchCh)

	// Assert
	select {
	case kind := <-errCh:
		if kind != WatchErrorCompacted {
			t.Fatalf("expected WatchErrorCompacted, got %v", kind)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected compaction error handler")
	}

	w.Stop()
}

func TestWatcherCancelTriggersErrorHandler(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cfg := DefaultWatchConfig()
	cfg.Key = "/svc/"
	cfg.RangeEnd = "/svc0"

	w := NewEtcdWatcher(mockClient, cfg, logger)
	errCh := make(chan WatchErrorKind, 1)
	w.SetWatchErrorHandler(func(kind WatchErrorKind, err error) {
		errCh <- kind
	})

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 10},
	}, nil).AnyTimes()
	watchCh := make(chan clientv3.WatchResponse, 1)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Act
	if err := w.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	// Act
	watchCh <- clientv3.WatchResponse{Canceled: true}
	close(watchCh)

	// Assert
	select {
	case kind := <-errCh:
		if kind != WatchErrorCanceled {
			t.Fatalf("expected WatchErrorCanceled, got %v", kind)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected cancel error handler")
	}

	w.Stop()
}

func TestWatcherErrorHandlerRunsOnWatchError(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cfg := DefaultWatchConfig()
	cfg.Key = "/svc/"
	cfg.RangeEnd = "/svc0"
	cfg.RetryInterval = 10 * time.Millisecond

	w := NewEtcdWatcher(mockClient, cfg, logger)
	errCh := make(chan error, 1)
	w.SetErrorHandler(func(err error) {
		errCh <- err
	})

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 10},
	}, nil).AnyTimes()
	watchCh := make(chan clientv3.WatchResponse)
	close(watchCh)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Act
	if err := w.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	// Assert
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected error on watch failure")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected error handler to fire")
	}

	w.Stop()
}

func TestWatcherManager_AddWatcherIfAbsent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := NewEtcdWatcherManager(log.Default())
	mockClient := mocks.NewMockEtcdClient(ctrl)
	w := NewEtcdWatcher(mockClient, WatchConfig{Key: "/svc/a"}, log.Default())

	if !mgr.AddWatcherIfAbsent("/svc/a", w) {
		t.Fatalf("expected first add to succeed")
	}
	if mgr.AddWatcherIfAbsent("/svc/a", w) {
		t.Fatalf("expected duplicate add to fail")
	}
}

func TestWatcherManager_ActiveAll_StartsNotRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := NewEtcdWatcherManager(log.Default())
	mockClient := mocks.NewMockEtcdClient(ctrl)

	cfg := DefaultWatchConfig()
	cfg.Key = "/svc/a"
	cfg.RangeEnd = "/svc/b"

	w := NewEtcdWatcher(mockClient, cfg, log.Default())
	if !mgr.AddWatcherIfAbsent(cfg.Key, w) {
		t.Fatalf("expected add watcher success")
	}

	watchCh := make(chan clientv3.WatchResponse)
	mockClient.EXPECT().Get(gomock.Any(), cfg.Key, gomock.Any()).Return(&clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 1},
	}, nil).AnyTimes()
	mockClient.EXPECT().Watch(gomock.Any(), cfg.Key, gomock.Any()).Return(watchCh).AnyTimes()

	if err := mgr.ActiveAll(context.Background()); err != nil {
		t.Fatalf("ActiveAll failed: %v", err)
	}

	if !w.IsRunning() {
		t.Fatalf("expected watcher to be running after ActiveAll")
	}

	mgr.StopAll()
	if w.IsRunning() {
		t.Fatalf("expected watcher stopped after StopAll")
	}
}

func TestWatcherManager_ActiveAll_Idempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mgr := NewEtcdWatcherManager(log.Default())
	mockClient := mocks.NewMockEtcdClient(ctrl)

	cfg := DefaultWatchConfig()
	cfg.Key = "/svc/idempotent"
	cfg.RangeEnd = "/svc/idempotenu"

	w := NewEtcdWatcher(mockClient, cfg, log.Default())
	if !mgr.AddWatcherIfAbsent(cfg.Key, w) {
		t.Fatalf("expected add watcher success")
	}

	watchCh := make(chan clientv3.WatchResponse)
	getCalled := make(chan struct{}, 1)
	watchCalled := make(chan struct{}, 1)
	mockClient.EXPECT().Get(gomock.Any(), cfg.Key, gomock.Any()).Return(&clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 1},
	}, nil).DoAndReturn(func(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		getCalled <- struct{}{}
		return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 1}}, nil
	}).Times(1)
	mockClient.EXPECT().Watch(gomock.Any(), cfg.Key, gomock.Any()).DoAndReturn(func(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
		watchCalled <- struct{}{}
		return watchCh
	}).Times(1)

	if err := mgr.ActiveAll(context.Background()); err != nil {
		t.Fatalf("first ActiveAll failed: %v", err)
	}
	select {
	case <-getCalled:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected initial Get to be called")
	}
	select {
	case <-watchCalled:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected initial Watch to be called")
	}
	if err := mgr.ActiveAll(context.Background()); err != nil {
		t.Fatalf("second ActiveAll failed: %v", err)
	}

	mgr.StopAll()
	close(watchCh)
}

func TestWatcherWatchDirect_RequestTimeoutRestartsSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	cfg := DefaultWatchConfig()
	cfg.Key = "/svc/timeout"
	cfg.RangeEnd = "/svc/timeouu"
	cfg.RequestTimeout = 20 * time.Millisecond

	w := NewEtcdWatcher(mockClient, cfg, log.Default())

	watchCh := make(chan clientv3.WatchResponse)
	defer close(watchCh)

	mockClient.EXPECT().Get(gomock.Any(), cfg.Key, gomock.Any()).Return(&clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 1},
	}, nil).Times(1)
	mockClient.EXPECT().Watch(gomock.Any(), cfg.Key, gomock.Any()).Return(watchCh).Times(1)

	start := time.Now()
	err := w.watch(context.Background())
	if err != nil {
		t.Fatalf("expected nil on watch request timeout restart, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond {
		t.Fatalf("expected watch to wait for timeout window, elapsed=%v", elapsed)
	}
}

func TestWatcherUpdateConnectionState_AdvanceRevisionFromHeaderWithoutEvents(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	w := NewEtcdWatcher(mockClient, DefaultWatchConfig(), log.Default())

	w.mu.Lock()
	w.watchRevision = 3
	w.lastRevision = 2
	w.mu.Unlock()

	w.updateConnectionState(clientv3.WatchResponse{
		Header: etcdserverpb.ResponseHeader{Revision: 10},
		Events: nil,
	})

	w.mu.RLock()
	gotWatchRevision := w.watchRevision
	gotLastRevision := w.lastRevision
	w.mu.RUnlock()

	if gotWatchRevision != 11 {
		t.Fatalf("expected watchRevision to advance to 11 from header, got %d", gotWatchRevision)
	}
	if gotLastRevision != 10 {
		t.Fatalf("expected lastRevision to become 10, got %d", gotLastRevision)
	}
}

func TestWatcherChannelClosedTriggersError(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cfg := DefaultWatchConfig()
	cfg.Key = "/svc/"
	cfg.RangeEnd = "/svc0"

	w := NewEtcdWatcher(mockClient, cfg, logger)
	errCh := make(chan error, 1)
	w.SetErrorHandler(func(err error) {
		errCh <- err
	})

	mockClient.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 10},
	}, nil).AnyTimes()
	watchCh := make(chan clientv3.WatchResponse)
	close(watchCh)
	mockClient.EXPECT().Watch(gomock.Any(), gomock.Any(), gomock.Any()).Return(watchCh).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Act
	if err := w.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}

	// Assert
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected error on channel close")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected error handler")
	}

	w.Stop()
}

func TestBatchDisabledByDefault(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	watcher := NewEtcdWatcher(mockClient, DefaultWatchConfig(), logger)

	// Assert
	watcher.mu.RLock()
	defer watcher.mu.RUnlock()

	if watcher.batchConfig.Enabled {
		t.Error("Expected batch to be disabled by default for backward compatibility")
	}
}

func TestBatchBufferInitialization(t *testing.T) {
	// Arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	watcher := NewEtcdWatcher(mockClient, DefaultWatchConfig(), logger)

	// Assert
	watcher.mu.RLock()
	defer watcher.mu.RUnlock()

	if len(watcher.batchBuffer) != 0 {
		t.Errorf("Expected empty batch buffer, got %d events", len(watcher.batchBuffer))
	}

	expectedCap := DefaultBatchConfig().MaxBatchSize
	if cap(watcher.batchBuffer) != expectedCap {
		t.Errorf("Expected buffer capacity %d, got %d", expectedCap, cap(watcher.batchBuffer))
	}
}

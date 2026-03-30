package module

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	log "log/slog"

	"github.com/golang/mock/gomock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/atframework/libatapp-go/etcd_module/client"
	"github.com/atframework/libatapp-go/etcd_module/client/mocks"
	"github.com/atframework/libatapp-go/etcd_module/cluster"
	"github.com/atframework/libatapp-go/etcd_module/discovery"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func newTestModule(t *testing.T) (*EtcdModule, *cluster.EtcdCluster, *gomock.Controller) {
	t.Helper()

	ctrl := gomock.NewController(t)
	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cl, err := cluster.NewEtcdCluster(mockClient, logger)
	if err != nil {
		ctrl.Finish()
		t.Fatalf("failed to create cluster: %v", err)
	}

	mod, err := newEtcdModuleWithCluster(cl, logger)
	if err != nil {
		ctrl.Finish()
		t.Fatalf("failed to create module: %v", err)
	}

	return mod, cl, ctrl
}

func TestNewEtcdModuleFromConfig_NilConfig(t *testing.T) {
	mod, err := NewEtcdModuleFromConfig(nil, nil)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if mod != nil {
		t.Fatalf("expected nil module")
	}
}

func TestNewEtcdModule_Success(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	if mod == nil {
		t.Fatalf("expected module")
	}
	if !mod.IsEtcdEnabled() {
		t.Fatalf("expected etcd enabled by default")
	}
}

func TestIsEtcdEnabled_EnableDisable(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	if !mod.IsEtcdEnabled() {
		t.Fatalf("expected etcd enabled")
	}
	mod.DisableEtcd()
	if mod.IsEtcdEnabled() {
		t.Fatalf("expected etcd disabled")
	}
	mod.EnableEtcd()
	if !mod.IsEtcdEnabled() {
		t.Fatalf("expected etcd enabled")
	}
}

func TestGetConfigurePath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	mod.SetConfigurePath("/svc")
	if mod.GetConfigurePath() != "/svc" {
		t.Fatalf("unexpected configure path: %s", mod.GetConfigurePath())
	}
}

func TestGetSetConfCustomData(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	mod.SetConfCustomData("custom")
	if mod.GetConfCustomData() != "custom" {
		t.Fatalf("unexpected custom data: %s", mod.GetConfCustomData())
	}
}

func TestGetByIDPath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")
	info := &pb.AtappDiscovery{Name: "app", Id: 42}

	got := mod.GetByIDPath(info)
	expect := "/svc/by_id/app-42"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetDiscoveryByIDPath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")
	info := &pb.AtappDiscovery{Name: "app", Id: 42}

	got := mod.GetDiscoveryByIDPath(info)
	expect := "/svc/by_id/app-42"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetByNamePath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")
	info := &pb.AtappDiscovery{Name: "app", Id: 42}

	got := mod.GetByNamePath(info)
	expect := "/svc/by_name/app-42"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetDiscoveryByNamePath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")
	info := &pb.AtappDiscovery{Name: "app", Id: 42}

	got := mod.GetDiscoveryByNamePath(info)
	expect := "/svc/by_name/app-42"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetByTypeIDPath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")
	info := &pb.AtappDiscovery{Name: "app", Id: 42, TypeId: 7}

	got := mod.GetByTypeIDPath(info)
	expect := "/svc/by_type_id/7/app-42"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetByTypeNamePath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")
	info := &pb.AtappDiscovery{Name: "app", Id: 42, TypeName: "type"}

	got := mod.GetByTypeNamePath(info)
	expect := "/svc/by_type_name/type/app-42"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetByTagPath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")
	info := &pb.AtappDiscovery{Name: "app", Id: 42}

	got := mod.GetByTagPath(info, "blue")
	expect := "/svc/by_tag/blue/app-42"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetByIDWatcherPath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")

	got := mod.GetByIDWatcherPath()
	expect := "/svc/by_id"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetDiscoveryByIDWatcherPath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")

	got := mod.GetDiscoveryByIDWatcherPath()
	expect := "/svc/by_id"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetByNameWatcherPath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")

	got := mod.GetByNameWatcherPath()
	expect := "/svc/by_name"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetDiscoveryByNameWatcherPath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")

	got := mod.GetDiscoveryByNameWatcherPath()
	expect := "/svc/by_name"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetByTypeIDWatcherPath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")

	got := mod.GetByTypeIDWatcherPath(7)
	expect := "/svc/by_type_id/7"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetByTypeNameWatcherPath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")

	got := mod.GetByTypeNameWatcherPath("type")
	expect := "/svc/by_type_name/type"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestGetByTagWatcherPath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)
	mod.SetConfigurePath("/svc")

	got := mod.GetByTagWatcherPath("blue")
	expect := "/svc/by_tag/blue"
	if got != expect {
		t.Fatalf("unexpected path: %s", got)
	}
}

func TestPathBuilder_NilInfoReturnsEmptyPath(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	if got := mod.GetByIDPath(nil); got != "" {
		t.Fatalf("expected empty path, got %q", got)
	}
	if got := mod.GetByNamePath(nil); got != "" {
		t.Fatalf("expected empty path, got %q", got)
	}
	if got := mod.GetByTypeIDPath(nil); got != "" {
		t.Fatalf("expected empty path, got %q", got)
	}
	if got := mod.GetByTypeNamePath(nil); got != "" {
		t.Fatalf("expected empty path, got %q", got)
	}
	if got := mod.GetByTagPath(nil, "blue"); got != "" {
		t.Fatalf("expected empty path, got %q", got)
	}
}

func TestGetRawEtcdCtx(t *testing.T) {
	mod, cl, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	if mod.GetRawEtcdCtx() != cl {
		t.Fatalf("unexpected cluster reference")
	}
}

func TestGetGlobalDiscovery(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	if err := mod.Init(context.Background()); err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}
	t.Cleanup(func() {
		_ = mod.Stop(context.Background())
	})
	ds, err := mod.GetGlobalDiscovery()
	if err != nil {
		t.Fatalf("expected global discovery set: %v", err)
	}
	if ds == nil {
		t.Fatalf("expected global discovery set")
	}
}

func TestHasSnapshot_InitiallyFalse(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	if mod.HasSnapshot() {
		t.Fatalf("expected snapshot to be false")
	}
}

func TestGetConfigure(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	if mod.GetConfigure() != nil {
		t.Fatalf("expected nil config by default")
	}

	cfg := &pb.AtappEtcd{Enable: true, Path: "/svc"}
	if err := mod.Reload(context.Background(), cfg); err != nil {
		t.Fatalf("unexpected reload error: %v", err)
	}
	gotCfg := mod.GetConfigure()
	if gotCfg == nil {
		t.Fatalf("expected config to be set")
	}
	if !proto.Equal(gotCfg, cfg) {
		t.Fatalf("expected config value to match")
	}
}

func TestLifecycleMethods(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	ctx := context.Background()
	if err := mod.Init(ctx); err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}
	if err := mod.Tick(ctx); err != nil {
		t.Fatalf("unexpected tick error: %v", err)
	}
	if err := mod.Stop(ctx); err != nil {
		t.Fatalf("unexpected stop error: %v", err)
	}
}

func TestReset(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	if err := mod.Reset(context.Background()); err != nil {
		t.Fatalf("unexpected reset error: %v", err)
	}
}

func TestReload_RespectsCanceledContext(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := mod.Reload(ctx, &pb.AtappEtcd{Enable: true, Path: "/svc"})
	if err == nil {
		t.Fatalf("expected reload to fail for canceled context")
	}
	if mod.GetConfigure() != nil {
		t.Fatalf("expected config to remain unchanged on canceled reload")
	}
}

func TestConcurrentAddKeepaliveActor_SamePath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cl, err := cluster.NewEtcdCluster(mockClient, logger)
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}
	if err := cl.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = cl.Stop(context.Background()) }()

	mod, err := newEtcdModuleWithCluster(cl, logger)
	if err != nil {
		t.Fatalf("NewEtcdModule failed: %v", err)
	}

	mockClient.EXPECT().Grant(gomock.Any(), gomock.Any()).Return(&clientv3.LeaseGrantResponse{ID: clientv3.LeaseID(9001)}, nil).AnyTimes()
	keepAliveCh := make(chan *clientv3.LeaseKeepAliveResponse)
	defer close(keepAliveCh)
	mockClient.EXPECT().KeepAlive(gomock.Any(), clientv3.LeaseID(9001)).Return(keepAliveCh, nil).AnyTimes()
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).AnyTimes()
	var putCalls atomic.Int64
	mockClient.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
		putCalls.Add(1)
		return &clientv3.PutResponse{}, nil
	}).AnyTimes()
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(&clientv3.DeleteResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Revoke(gomock.Any(), clientv3.LeaseID(9001)).Return(&clientv3.LeaseRevokeResponse{}, nil).AnyTimes()

	path := "/module/concurrent/same-path"
	workers := 20
	startGate := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-startGate
			info := &pb.AtappDiscovery{Name: "svc", Id: uint64(i + 1), Identity: fmt.Sprintf("id-%d", i)}
			_, _ = mod.AddRegistrationActor(context.Background(), info, path, 5)
		}(i)
	}
	close(startGate)
	wg.Wait()

	if _, ok := cl.GetRegistrationManager().GetRegistration(path); !ok {
		t.Fatalf("expected one keepalive at path %s", path)
	}
	if putCalls.Load() == 0 {
		t.Fatalf("expected at least one etcd put")
	}
}

func TestConcurrentAddRemoveKeepaliveActor_Race(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	logger := log.Default()

	cl, err := cluster.NewEtcdCluster(mockClient, logger)
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}
	if err := cl.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = cl.Stop(context.Background()) }()

	mod, err := newEtcdModuleWithCluster(cl, logger)
	if err != nil {
		t.Fatalf("NewEtcdModule failed: %v", err)
	}

	mockClient.EXPECT().Grant(gomock.Any(), gomock.Any()).Return(&clientv3.LeaseGrantResponse{ID: clientv3.LeaseID(9101)}, nil).AnyTimes()
	keepAliveCh := make(chan *clientv3.LeaseKeepAliveResponse)
	defer close(keepAliveCh)
	mockClient.EXPECT().KeepAlive(gomock.Any(), clientv3.LeaseID(9101)).Return(keepAliveCh, nil).AnyTimes()
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(&clientv3.DeleteResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Revoke(gomock.Any(), clientv3.LeaseID(9101)).Return(&clientv3.LeaseRevokeResponse{}, nil).AnyTimes()

	paths := []string{"/module/race/0", "/module/race/1", "/module/race/2", "/module/race/3"}
	startGate := make(chan struct{})
	var wg sync.WaitGroup
	workers := 32
	for i := 0; i < workers; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate
			path := paths[idx%len(paths)]
			info := &pb.AtappDiscovery{Name: "svc", Id: uint64(idx + 1), Identity: fmt.Sprintf("rid-%d", idx)}
			_, _ = mod.AddRegistrationActor(context.Background(), info, path, 5)
			_ = mod.RemoveRegistrationActor(context.Background(), path)
		}()
	}
	close(startGate)
	wg.Wait()

	if got := len(cl.GetRegistrationManager().GetAllRegistrations()); got != 0 {
		t.Fatalf("expected keepalive manager empty after add/remove race, got %d", got)
	}
}

const etcdEnvKey = "ETCD_ENDPOINTS"

func requireEtcdEndpoints(t *testing.T) []string {
	value := strings.TrimSpace(os.Getenv(etcdEnvKey))
	if value == "" {
		t.Skipf("integration test requires %s (e.g. 127.0.0.1:2379)", etcdEnvKey)
	}
	parts := strings.Split(value, ",")
	endpoints := make([]string, 0, len(parts))
	for _, part := range parts {
		endpoint := strings.TrimSpace(part)
		if endpoint == "" {
			continue
		}
		endpoints = append(endpoints, endpoint)
	}
	if len(endpoints) == 0 {
		t.Skipf("integration test requires %s (e.g. 127.0.0.1:2379)", etcdEnvKey)
	}
	return endpoints
}

func newIntegrationModule(t *testing.T) (*EtcdModule, *cluster.EtcdCluster, *client.EtcdClusterClient, func()) {
	t.Helper()

	endpoints := requireEtcdEndpoints(t)
	logger := log.Default()
	cli, err := client.NewEtcdClusterClient(&pb.AtappEtcd{Hosts: endpoints}, logger)
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}

	cl, err := cluster.NewEtcdCluster(cli, logger)
	if err != nil {
		_ = cli.Close()
		t.Fatalf("failed to create cluster: %v", err)
	}

	mod, err := newEtcdModuleWithCluster(cl, logger)
	if err != nil {
		_ = cli.Close()
		t.Fatalf("failed to create module: %v", err)
	}

	cleanup := func() {
		_ = mod.Stop(context.Background())
		_ = cli.Close()
	}

	return mod, cl, cli, cleanup
}

func waitForNodes(t *testing.T, ds *discovery.EtcdDiscoverySet, expect int, timeout time.Duration) []*discovery.DiscoveryNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return ds.GetAllNodes()
		}
		nodes := ds.GetAllNodes()
		if len(nodes) == expect {
			return nodes
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func cleanupPrefix(t *testing.T, cli *client.EtcdClusterClient, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = cli.Delete(ctx, prefix, clientv3.WithPrefix())
}

func TestModuleIntegration_KeepaliveWatcherDiscovery(t *testing.T) {
	mod, _, cli, cleanup := newIntegrationModule(t)
	defer cleanup()

	prefix := "/test/module/keepalive-watcher/" + formatUint(uint64(time.Now().UnixNano()))
	mod.SetConfigurePath(prefix)
	cleanupPrefix(t, cli, prefix)
	defer cleanupPrefix(t, cli, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := mod.Init(ctx); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	if err := mod.AddWatcherByID(ctx); err != nil {
		t.Fatalf("add watcher by id failed: %v", err)
	}

	info := &pb.AtappDiscovery{Name: "app", Id: 42}
	path := mod.GetByIDPath(info)

	keepalive, err := mod.AddRegistrationActor(ctx, info, path, 5)
	if err != nil {
		t.Fatalf("add keepalive actor failed: %v", err)
	}
	if keepalive == nil {
		t.Fatalf("expected keepalive instance")
	}

	ds, err := mod.GetGlobalDiscovery()
	if err != nil {
		t.Fatalf("get global discovery failed: %v", err)
	}
	nodes := waitForNodes(t, ds, 1, 5*time.Second)
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].Info == nil || nodes[0].Info.Name != "app" || nodes[0].Info.Id != 42 {
		t.Fatalf("unexpected node info: %+v", nodes[0].Info)
	}

	if err := mod.RemoveRegistrationActor(ctx, path); err != nil {
		t.Fatalf("remove keepalive actor failed: %v", err)
	}
	remaining := waitForNodes(t, ds, 0, 5*time.Second)
	if len(remaining) != 0 {
		t.Fatalf("expected node removal, got %d", len(remaining))
	}
}

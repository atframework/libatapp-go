package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	clientv3 "go.etcd.io/etcd/client/v3"
	log "log/slog"

	"github.com/atframework/libatapp-go/etcd_module/client/mocks"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func TestEnsureClusterLease_ConcurrentGrantFailureThenSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	c, err := NewEtcdCluster(mockClient, log.Default())
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = c.Stop(context.Background()) }()

	var grantCalls atomic.Int64
	var first int32 = 1
	mockClient.EXPECT().Grant(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
		grantCalls.Add(1)
		if atomic.CompareAndSwapInt32(&first, 1, 0) {
			return nil, fmt.Errorf("transient grant failure")
		}
		return &clientv3.LeaseGrantResponse{ID: clientv3.LeaseID(7007)}, nil
	}).AnyTimes()

	keepAliveCh := make(chan *clientv3.LeaseKeepAliveResponse)
	defer close(keepAliveCh)
	mockClient.EXPECT().KeepAlive(gomock.Any(), clientv3.LeaseID(7007)).Return(keepAliveCh, nil).AnyTimes()
	mockClient.EXPECT().Revoke(gomock.Any(), clientv3.LeaseID(7007)).Return(&clientv3.LeaseRevokeResponse{}, nil).AnyTimes()

	workers := 24
	startGate := make(chan struct{})
	var wg sync.WaitGroup
	var successCount atomic.Int64
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate
			if _, e := c.ensureClusterLease(context.Background(), 5); e == nil {
				successCount.Add(1)
			}
		}()
	}
	close(startGate)
	wg.Wait()

	if successCount.Load() == 0 {
		if _, err := c.ensureClusterLease(context.Background(), 5); err != nil {
			t.Fatalf("expected retry call to establish lease, got error: %v", err)
		}
	}
	if c.GetLease() == 0 {
		t.Fatalf("expected lease to be established")
	}
	if grantCalls.Load() < 2 {
		t.Fatalf("expected at least two grant attempts, got %d", grantCalls.Load())
	}
}

func TestConcurrentRegisterUnregisterServices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	c, err := NewEtcdCluster(mockClient, log.Default())
	if err != nil {
		t.Fatalf("NewEtcdCluster failed: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = c.Stop(context.Background()) }()

	mockClient.EXPECT().Grant(gomock.Any(), gomock.Any()).Return(&clientv3.LeaseGrantResponse{ID: clientv3.LeaseID(8080)}, nil).AnyTimes()
	keepAliveCh := make(chan *clientv3.LeaseKeepAliveResponse)
	defer close(keepAliveCh)
	mockClient.EXPECT().KeepAlive(gomock.Any(), clientv3.LeaseID(8080)).Return(keepAliveCh, nil).AnyTimes()
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(&clientv3.DeleteResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Revoke(gomock.Any(), clientv3.LeaseID(8080)).Return(&clientv3.LeaseRevokeResponse{}, nil).AnyTimes()

	workers := 16
	startGate := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate
			path := fmt.Sprintf("/svc/%d", idx)
			info := &pb.AtappDiscovery{Name: fmt.Sprintf("svc-%d", idx), Id: uint64(idx + 1), Identity: fmt.Sprintf("id-%d", idx)}
			_ = c.RegisterService(context.Background(), info, path, 5)
			_ = c.UnregisterService(context.Background(), path)
		}()
	}
	close(startGate)
	wg.Wait()

	if got := len(c.GetRegistrationManager().GetAllRegistrations()); got != 0 {
		t.Fatalf("expected keepalive manager to be empty, got %d", got)
	}
	stats := c.GetStats()
	if stats.KeepaliveRegistered == 0 {
		t.Fatalf("expected keepalive registration stats to increase")
	}
}

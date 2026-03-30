package registration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/atframework/libatapp-go/etcd_module/client/mocks"
	"github.com/golang/mock/gomock"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestConcurrentSetLeaseID_OnlyOneRefreshPerLease(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	svc, err := NewEtcdRegistration(testDiscovery("id-v1"), "/svc/1", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	lease1 := clientv3.LeaseID(1001)
	mockClient.EXPECT().Get(gomock.Any(), "/svc/1").Return(&clientv3.GetResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Put(gomock.Any(), "/svc/1", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)
	if err := startRegistrationWithLease(svc, context.Background(), lease1); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}

	var putCalls atomic.Int64
	mockClient.EXPECT().Put(gomock.Any(), "/svc/1", gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
		putCalls.Add(1)
		return &clientv3.PutResponse{}, nil
	}).AnyTimes()

	lease2 := clientv3.LeaseID(2002)
	startGate := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate
			setRegistrationLeaseIDAndRefresh(svc, lease2)
		}()
	}
	close(startGate)
	wg.Wait()

	if got := svc.GetLeaseID(); got != lease2 {
		t.Fatalf("expected leaseID=%d, got %d", lease2, got)
	}
	if putCalls.Load() == 0 {
		t.Fatalf("expected refresh side-effects after concurrent SetLeaseID, put=%d", putCalls.Load())
	}
}

func TestConcurrentUpdateServiceInfo_RefreshSerialization(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	svc, err := NewEtcdRegistration(testDiscovery("id-v1"), "/svc/2", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	lease := clientv3.LeaseID(3003)
	mockClient.EXPECT().Get(gomock.Any(), "/svc/2").Return(&clientv3.GetResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Put(gomock.Any(), "/svc/2", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)
	if err := startRegistrationWithLease(svc, context.Background(), lease); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}

	var putCalls atomic.Int64
	mockClient.EXPECT().Put(gomock.Any(), "/svc/2", gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
		putCalls.Add(1)
		return &clientv3.PutResponse{}, nil
	}).AnyTimes()

	startGate := make(chan struct{})
	var wg sync.WaitGroup
	workers := 24
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-startGate
			info := testDiscovery(fmt.Sprintf("id-v2-%d", i))
			_ = svc.UpdateServiceInfoWithContext(context.Background(), info)
		}(i)
	}
	close(startGate)
	wg.Wait()

	if putCalls.Load() == 0 {
		t.Fatalf("expected refresh side-effects, put=%d", putCalls.Load())
	}
	if putCalls.Load() > int64(workers) {
		t.Fatalf("unexpected excessive put calls: %d > %d", putCalls.Load(), workers)
	}
}

func TestConcurrentUpdateAndUnregisterRace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	svc, err := NewEtcdRegistration(testDiscovery("id-v1"), "/svc/race", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	lease := clientv3.LeaseID(5555)
	mockClient.EXPECT().Get(gomock.Any(), "/svc/race").Return(&clientv3.GetResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Put(gomock.Any(), "/svc/race", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Delete(gomock.Any(), "/svc/race").Return(&clientv3.DeleteResponse{}, nil).AnyTimes()
	if err := startRegistrationWithLease(svc, context.Background(), lease); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}

	startGate := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate
			if idx%3 == 0 {
				_ = svc.Unregister(context.Background())
				return
			}
			_ = svc.UpdateServiceInfoWithContext(context.Background(), testDiscovery(fmt.Sprintf("id-race-%d", idx)))
		}()
	}
	close(startGate)
	wg.Wait()

	if svc.HasData() {
		t.Fatalf("expected service to be unregistered after update/unregister race")
	}
}

package registration

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/atframework/libatapp-go/etcd_module/client/mocks"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
	"github.com/golang/mock/gomock"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestStartWithLease_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	leaseID := clientv3.LeaseID(12345)
	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)

	err = startRegistrationWithLease(service, context.Background(), leaseID)
	if err != nil {
		t.Fatalf("startRegistrationWithLease() failed: %v", err)
	}
	if service.GetState() != RegistrationActive {
		t.Fatalf("expected state %v, got %v", RegistrationActive, service.GetState())
	}
	if service.GetLeaseID() != leaseID {
		t.Fatalf("expected leaseID %d, got %d", leaseID, service.GetLeaseID())
	}
}

func TestStartWithLease_PutFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	expectedErr := errors.New("put failed")
	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(nil, expectedErr).Times(1)

	err = startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345))
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error %v, got %v", expectedErr, err)
	}
	if service.GetState() != RegistrationFailed {
		t.Fatalf("expected state %v, got %v", RegistrationFailed, service.GetState())
	}
}

func TestStartWithLease_AlreadyRegistered(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)

	if err := startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345)); err != nil {
		t.Fatalf("first StartWithLease failed: %v", err)
	}
	if err := startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345)); err == nil {
		t.Fatalf("expected second StartWithLease to fail")
	}
}

func TestNewEtcdRegistration_ClonesInput(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	info := testDiscovery("test-identity")
	service, err := NewEtcdRegistration(info, "/test", 10, mocks.NewMockEtcdClient(ctrl), testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	info.Identity = "mutated"
	if service.info.Identity != "test-identity" {
		t.Fatalf("expected cloned info to stay unchanged, got %q", service.info.Identity)
	}
}

func TestSnapshotForRegister_CapturesContextAndClonesInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	service.leaseOwner = &testLeaseOwner{leaseID: clientv3.LeaseID(12345)}
	snapshot, err := service.snapshotForRegister()
	if err != nil {
		t.Fatalf("snapshotForRegister failed: %v", err)
	}
	if snapshot.etcdClient != mockClient {
		t.Fatalf("expected etcd client captured in snapshot")
	}
	if snapshot.path != "/test" {
		t.Fatalf("expected path /test, got %s", snapshot.path)
	}
	if snapshot.leaseID != clientv3.LeaseID(12345) {
		t.Fatalf("expected leaseID 12345, got %d", snapshot.leaseID)
	}
	service.info.Identity = "mutated-after-snapshot"
	if snapshot.info.Identity != "test-identity" {
		t.Fatalf("expected cloned snapshot info, got %q", snapshot.info.Identity)
	}
}

func TestStartWithLease_ZeroLeaseID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mocks.NewMockEtcdClient(ctrl), testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}
	if err := startRegistrationWithLease(service, context.Background(), 0); err == nil {
		t.Fatalf("expected StartWithLease to fail with zero leaseID")
	}
}

func TestSetLeaseID_TriggersRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(2)

	if err := startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345)); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}
	setRegistrationLeaseIDAndRefresh(service, clientv3.LeaseID(67890))
	if service.GetLeaseID() != clientv3.LeaseID(67890) {
		t.Fatalf("expected leaseID updated, got %d", service.GetLeaseID())
	}
}

func TestSetLeaseID_SameLeaseNoRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)

	leaseID := clientv3.LeaseID(12345)
	if err := startRegistrationWithLease(service, context.Background(), leaseID); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}
	setRegistrationLeaseIDAndRefresh(service, leaseID)
}

func TestUnregister_DeletesKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)
	mockClient.EXPECT().Delete(gomock.Any(), "/test").Return(&clientv3.DeleteResponse{}, nil).Times(1)

	if err := startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345)); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}
	if err := service.Unregister(context.Background()); err != nil {
		t.Fatalf("Unregister failed: %v", err)
	}
	if service.GetState() != RegistrationStopped {
		t.Fatalf("expected state %v, got %v", RegistrationStopped, service.GetState())
	}
}

func TestUnregister_AlreadyUnregistered(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)
	mockClient.EXPECT().Delete(gomock.Any(), "/test").Return(&clientv3.DeleteResponse{}, nil).Times(1)

	if err := startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345)); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}
	if err := service.Unregister(context.Background()); err != nil {
		t.Fatalf("first Unregister failed: %v", err)
	}
	if err := service.Unregister(context.Background()); err != nil {
		t.Fatalf("second Unregister should return nil, got %v", err)
	}
}

func TestUnregister_ConcurrentSingleDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)
	mockClient.EXPECT().Delete(gomock.Any(), "/test").Return(&clientv3.DeleteResponse{}, nil).Times(1)

	if err := startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345)); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = service.Unregister(context.Background())
		}()
	}
	wg.Wait()

	if service.GetState() != RegistrationStopped {
		t.Fatalf("expected state %v, got %v", RegistrationStopped, service.GetState())
	}
}

func TestUnregister_RetryAfterDeleteFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)
	if err := startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345)); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}

	deleteErr := errors.New("delete failed")
	gomock.InOrder(
		mockClient.EXPECT().Delete(gomock.Any(), "/test").Return(nil, deleteErr).Times(1),
		mockClient.EXPECT().Delete(gomock.Any(), "/test").Return(&clientv3.DeleteResponse{}, nil).Times(1),
	)

	if err := service.Unregister(context.Background()); err == nil {
		t.Fatalf("expected first Unregister to fail")
	}
	if err := service.Unregister(context.Background()); err != nil {
		t.Fatalf("expected second Unregister to succeed, got %v", err)
	}
}

func TestUpdateServiceInfo_ChangedTriggersRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("identity-v1"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(2)

	if err := startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345)); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}
	if err := service.UpdateServiceInfo(&pb.AtappDiscovery{Name: "test-service", Identity: "identity-v2"}); err != nil {
		t.Fatalf("UpdateServiceInfo failed: %v", err)
	}
}

func TestUpdateServiceInfo_UnchangedNoRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)

	if err := startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345)); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}
	if err := service.UpdateServiceInfo(testDiscovery("test-identity")); err != nil {
		t.Fatalf("UpdateServiceInfo failed: %v", err)
	}
}

func TestUpdateServiceInfo_ClonesInput(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("identity-v1"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(2)

	if err := startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345)); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}
	updated := testDiscovery("identity-v2")
	if err := service.UpdateServiceInfo(updated); err != nil {
		t.Fatalf("UpdateServiceInfo failed: %v", err)
	}
	updated.Identity = "identity-mutated-outside"
	if service.info.Identity != "identity-v2" {
		t.Fatalf("expected internal cloned info, got %q", service.info.Identity)
	}
}

func TestIdentityCollision_BlocksRegistration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("new-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}

	existingValue := `{"name":"test-service","identity":"existing-identity"}`
	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{
		Kvs: []*mvccpb.KeyValue{{Key: []byte("/test"), Value: []byte(existingValue)}},
	}, nil).Times(1)

	if err := startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345)); err == nil {
		t.Fatalf("expected registration to fail due to identity collision")
	}
	if service.GetState() != RegistrationFailed {
		t.Fatalf("expected state %v, got %v", RegistrationFailed, service.GetState())
	}
}

func TestRegistrationManager_SetLease_PropagatesAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	manager := NewRegistrationManager(testLogger())

	svc1, _ := NewEtcdRegistration(testDiscovery("id1"), "/test/svc1", 10, mockClient, testLogger(), manager)
	svc2, _ := NewEtcdRegistration(testDiscovery("id2"), "/test/svc2", 10, mockClient, testLogger(), manager)

	mockClient.EXPECT().Get(gomock.Any(), "/test/svc1").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test/svc1", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)
	mockClient.EXPECT().Get(gomock.Any(), "/test/svc2").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test/svc2", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)

	manager.SetLease(clientv3.LeaseID(111))
	if err := svc1.Start(context.Background()); err != nil {
		t.Fatalf("svc1 Start failed: %v", err)
	}
	if err := svc2.Start(context.Background()); err != nil {
		t.Fatalf("svc2 Start failed: %v", err)
	}

	manager.AddRegistration(svc1)
	manager.AddRegistration(svc2)

	mockClient.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(2)

	manager.SetLease(clientv3.LeaseID(222))
	if svc1.GetLeaseID() != clientv3.LeaseID(222) || svc2.GetLeaseID() != clientv3.LeaseID(222) {
		t.Fatalf("expected manager SetLease to propagate to all services")
	}
}

func TestGetValueAndHasDataLifecycle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	service, err := NewEtcdRegistration(testDiscovery("test-identity"), "/test", 10, mockClient, testLogger(), nil)
	if err != nil {
		t.Fatalf("NewEtcdRegistration failed: %v", err)
	}
	if service.HasData() {
		t.Fatalf("expected HasData false before registration")
	}

	mockClient.EXPECT().Get(gomock.Any(), "/test").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)
	mockClient.EXPECT().Delete(gomock.Any(), "/test").Return(&clientv3.DeleteResponse{}, nil).Times(1)

	if err := startRegistrationWithLease(service, context.Background(), clientv3.LeaseID(12345)); err != nil {
		t.Fatalf("StartWithLease failed: %v", err)
	}
	if !service.HasData() {
		t.Fatalf("expected HasData true after registration")
	}
	if service.GetValue() == "" {
		t.Fatalf("expected non-empty cached value after registration")
	}
	if err := service.Unregister(context.Background()); err != nil {
		t.Fatalf("Unregister failed: %v", err)
	}
	if service.HasData() {
		t.Fatalf("expected HasData false after unregister")
	}
}

func TestRegistrationManager_ActiveAll_StartsNotRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	manager := NewRegistrationManager(testLogger())
	service, _ := NewEtcdRegistration(testDiscovery("id1"), "/test/svc1", 10, mockClient, testLogger(), nil)
	manager.AddRegistration(service)
	manager.SetLease(clientv3.LeaseID(12345))

	mockClient.EXPECT().Get(gomock.Any(), "/test/svc1").Return(&clientv3.GetResponse{}, nil).Times(1)
	mockClient.EXPECT().Put(gomock.Any(), "/test/svc1", gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).Times(1)

	if err := manager.ActiveAll(context.Background()); err != nil {
		t.Fatalf("ActiveAll failed: %v", err)
	}
	if service.GetState() != RegistrationActive {
		t.Fatalf("expected service active after ActiveAll, got %v", service.GetState())
	}
}

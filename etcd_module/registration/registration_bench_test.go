package registration

import (
	"context"
	"fmt"
	"testing"

	"github.com/atframework/libatapp-go/etcd_module/client/mocks"
	"github.com/golang/mock/gomock"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func BenchmarkServiceRegister(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	leaseID := clientv3.LeaseID(12345)
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(&clientv3.DeleteResponse{}, nil).AnyTimes()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		service, _ := NewEtcdRegistration(testDiscovery("bench-identity"), "/test", 10, mockClient, testLogger(), nil)
		_ = startRegistrationWithLease(service, context.Background(), leaseID)
		_ = service.Unregister(context.Background())
	}
}

func BenchmarkServiceUnregister(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	mockClient := mocks.NewMockEtcdClient(ctrl)
	leaseID := clientv3.LeaseID(12345)
	mockClient.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&clientv3.PutResponse{}, nil).AnyTimes()
	mockClient.EXPECT().Delete(gomock.Any(), gomock.Any()).Return(&clientv3.DeleteResponse{}, nil).AnyTimes()

	service, _ := NewEtcdRegistration(testDiscovery("bench-identity"), "/test", 10, mockClient, testLogger(), nil)
	_ = startRegistrationWithLease(service, context.Background(), leaseID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = service.Unregister(context.Background())
	}
}

func BenchmarkRegistrationManagerAdd(b *testing.B) {
	manager := NewRegistrationManager(testLogger())
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()
	mockClient := mocks.NewMockEtcdClient(ctrl)

	services := make([]*EtcdRegistration, 100)
	for i := range services {
		s, _ := NewEtcdRegistration(testDiscovery(fmt.Sprintf("id-%d", i)), fmt.Sprintf("/test/%d", i), 10, mockClient, testLogger(), nil)
		services[i] = s
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, s := range services {
			manager.AddRegistration(s)
		}
	}
}

package registration

import (
	"context"
	log "log/slog"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type testLeaseOwner struct {
	leaseID clientv3.LeaseID
}

func (o *testLeaseOwner) GetLease() clientv3.LeaseID {
	if o == nil {
		return 0
	}
	return o.leaseID
}

func (o *testLeaseOwner) SetLease(leaseID clientv3.LeaseID) {
	if o == nil {
		return
	}
	o.leaseID = leaseID
}

func testLogger() *log.Logger {
	return log.Default()
}

func testDiscovery(identity string) *pb.AtappDiscovery {
	return &pb.AtappDiscovery{
		Name:     "test-service",
		Identity: identity,
	}
}

func startRegistrationWithLease(s *EtcdRegistration, ctx context.Context, leaseID clientv3.LeaseID) error {
	if s == nil {
		return nil
	}
	if leaseID == 0 {
		return s.Start(ctx)
	}

	s.mu.Lock()
	owner, ok := s.leaseOwner.(*testLeaseOwner)
	if !ok || owner == nil {
		owner = &testLeaseOwner{}
		s.leaseOwner = owner
	}
	owner.leaseID = leaseID
	s.mu.Unlock()

	return s.Start(ctx)
}

func setRegistrationLeaseIDAndRefresh(s *EtcdRegistration, leaseID clientv3.LeaseID) error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	owner, ok := s.leaseOwner.(*testLeaseOwner)
	if !ok || owner == nil {
		owner = &testLeaseOwner{}
		s.leaseOwner = owner
	}
	old := owner.leaseID
	owner.leaseID = leaseID
	isClosed := s.isClosed
	s.mu.Unlock()

	if old == leaseID || isClosed {
		return nil
	}
	return s.TriggerMaybeUpdate(context.Background())
}

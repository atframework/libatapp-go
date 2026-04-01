// Package snapshot defines the read-model structures for the v2 module.
// Nothing in this package writes to etcd; all mutations happen in the
// orchestrator layer and are published atomically via ProjectionActor.
package snapshot

import (
	"time"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// RegistrationSnapshot is the Registration sub-view within ExportSnapshot.
// It mirrors the current state of all services that have been successfully
// written to etcd by RegistrationActor.
//
// All maps are immutable after construction (copy-on-write semantics).
type RegistrationSnapshot struct {
	LeaseID   clientv3.LeaseID
	LeaseEpoch uint64
	ByPath    map[string]*pb.AtappDiscovery
	ByName    map[string]*pb.AtappDiscovery
	ByID      map[uint64]*pb.AtappDiscovery
	UpdatedAt time.Time
}

// Clone returns a shallow copy whose maps point to the same underlying
// *pb.AtappDiscovery values (safe because those are treated as immutable).
func (s *RegistrationSnapshot) Clone() *RegistrationSnapshot {
	if s == nil {
		return &RegistrationSnapshot{}
	}
	out := &RegistrationSnapshot{
		LeaseID:    s.LeaseID,
		LeaseEpoch: s.LeaseEpoch,
		UpdatedAt:  s.UpdatedAt,
	}
	if len(s.ByPath) > 0 {
		out.ByPath = make(map[string]*pb.AtappDiscovery, len(s.ByPath))
		for k, v := range s.ByPath {
			out.ByPath[k] = v
		}
	}
	if len(s.ByName) > 0 {
		out.ByName = make(map[string]*pb.AtappDiscovery, len(s.ByName))
		for k, v := range s.ByName {
			out.ByName[k] = v
		}
	}
	if len(s.ByID) > 0 {
		out.ByID = make(map[uint64]*pb.AtappDiscovery, len(s.ByID))
		for k, v := range s.ByID {
			out.ByID[k] = v
		}
	}
	return out
}

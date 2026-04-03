// Package snapshot defines the read-model structures for the v2 module.
// Nothing in this package writes to etcd; all mutations happen in the
// orchestrator layer and are published atomically via ProjectionActor.
package snapshot

import (
	"time"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SelfRegistrationSnapshot is the self-node registration sub-view within ExportSnapshot.
// It mirrors the write-side state: services that this node has successfully
// written to etcd via RegistrationActor (bypath/byname/byid/topology).
// It is distinct from DiscoverySetSnapshot / TopologySnapshot, which are
// read-side views of remote nodes discovered via WatchActor.
//
// All maps are immutable after construction (copy-on-write semantics).
type SelfRegistrationSnapshot struct {
	LeaseID          clientv3.LeaseID
	LeaseEpoch       uint64
	ByPath           map[string]*pb.AtappDiscovery
	ByName           map[string]*pb.AtappDiscovery
	ByID             map[uint64]*pb.AtappDiscovery
	TopologyServices map[string]*pb.AtappTopologyInfo // key = etcd topology key (topologyPrefix/name-id)
	UpdatedAt        time.Time
}

// Clone returns a shallow copy whose maps point to the same underlying
// *pb.AtappDiscovery values (safe because those are treated as immutable).
func (s *SelfRegistrationSnapshot) Clone() *SelfRegistrationSnapshot {
	if s == nil {
		return &SelfRegistrationSnapshot{}
	}
	out := &SelfRegistrationSnapshot{
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
	if len(s.TopologyServices) > 0 {
		out.TopologyServices = make(map[string]*pb.AtappTopologyInfo, len(s.TopologyServices))
		for k, v := range s.TopologyServices {
			out.TopologyServices[k] = v
		}
	}
	return out
}

package snapshot

import (
	"time"

	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/etcdversion"
)

// DiscoverySetSnapshot is the discovery read-model sub-view within ExportSnapshot.
// It captures the last-known set of remote nodes as discovered via etcd Watch.
type DiscoverySetSnapshot struct {
	// Ready is true once the initial snapshot has been loaded successfully.
	Ready bool
	// LastRevision is the etcd revision of the most recent event applied.
	LastRevision int64
	// NodesByPath is the authoritative index of all live topology nodes.
	// Keys are the full etcd paths; values are immutable after construction.
	NodesByPath map[string]*DiscoveryNode
	// NodesByID is a best-match index for fast GetNodeByID lookups.
	NodesByID map[uint64]*DiscoveryNode
	// NodesByName is a best-match index for fast GetNodeByName lookups.
	NodesByName map[string]*DiscoveryNode
}

// DiscoveryNode wraps a remote node's discovery info together with its etcd
// metadata.
//
// This is the read-model counterpart of etcd_module/discovery.DiscoveryNode,
// not a mutable runtime entity. Compared with v1 DiscoveryNode, it currently
// does NOT carry:
//   - ingress selection state/cursor (IngressIndex, IngressForListen)
//   - lifecycle callback/private slots (onDestroy, privateData)
//   - mutable helper methods (CopyFrom/CopyTo/UpdateVersion)
//   - identity/hash helpers and behavior methods (Equal, GetNameHash,
//     GetIngressSize, NextIngressGateway, CopyKeyTo, WithDiscoveryInfo)
//   - node-local mutex and mutable in-place state
type DiscoveryNode struct {
	Info *pb.AtappDiscovery
	Path string
	etcdversion.DataVersion
}

// DiscoveryNodeSnapshot is kept as a compatibility alias.
type DiscoveryNodeSnapshot = DiscoveryNode

// Clone returns a shallow copy.
func (s *DiscoverySetSnapshot) Clone() *DiscoverySetSnapshot {
	if s == nil {
		return &DiscoverySetSnapshot{}
	}
	out := &DiscoverySetSnapshot{
		Ready:        s.Ready,
		LastRevision: s.LastRevision,
	}
	if len(s.NodesByPath) > 0 {
		out.NodesByPath = make(map[string]*DiscoveryNode, len(s.NodesByPath))
		for k, v := range s.NodesByPath {
			out.NodesByPath[k] = v
		}
	}
	if len(s.NodesByID) > 0 {
		out.NodesByID = make(map[uint64]*DiscoveryNode, len(s.NodesByID))
		for k, v := range s.NodesByID {
			out.NodesByID[k] = v
		}
	}
	if len(s.NodesByName) > 0 {
		out.NodesByName = make(map[string]*DiscoveryNode, len(s.NodesByName))
		for k, v := range s.NodesByName {
			out.NodesByName[k] = v
		}
	}
	return out
}

// RebuildIndexes rebuilds NodesByID and NodesByName from NodesByPath.
func (s *DiscoverySetSnapshot) RebuildIndexes() {
	if s == nil {
		return
	}
	if len(s.NodesByPath) == 0 {
		s.NodesByID = nil
		s.NodesByName = nil
		return
	}

	s.NodesByID = make(map[uint64]*DiscoveryNode, len(s.NodesByPath))
	s.NodesByName = make(map[string]*DiscoveryNode, len(s.NodesByPath))
	for _, node := range s.NodesByPath {
		s.indexNode(node)
	}
}

// UpsertNode updates NodesByPath and incremental indexes for one node.
func (s *DiscoverySetSnapshot) UpsertNode(node *DiscoveryNode) {
	if s == nil || node == nil {
		return
	}
	if s.NodesByPath == nil {
		s.NodesByPath = make(map[string]*DiscoveryNode)
	}
	s.NodesByPath[node.Path] = node
	if s.NodesByID == nil {
		s.NodesByID = make(map[uint64]*DiscoveryNode)
	}
	if s.NodesByName == nil {
		s.NodesByName = make(map[string]*DiscoveryNode)
	}
	s.indexNode(node)
}

// RemoveNodeByPath removes one path entry and repairs indexes.
func (s *DiscoverySetSnapshot) RemoveNodeByPath(path string) {
	if s == nil || path == "" || s.NodesByPath == nil {
		return
	}
	if _, ok := s.NodesByPath[path]; !ok {
		return
	}
	delete(s.NodesByPath, path)
	// Rebuild for correctness when multiple paths share same id/name.
	s.RebuildIndexes()
}

// GetNodeByID returns indexed node by id.
func (s *DiscoverySetSnapshot) GetNodeByID(id uint64) *DiscoveryNode {
	if s == nil || id == 0 || len(s.NodesByID) == 0 {
		return nil
	}
	return s.NodesByID[id]
}

// GetNodeByName returns indexed node by name.
func (s *DiscoverySetSnapshot) GetNodeByName(name string) *DiscoveryNode {
	if s == nil || name == "" || len(s.NodesByName) == 0 {
		return nil
	}
	return s.NodesByName[name]
}

func (s *DiscoverySetSnapshot) indexNode(node *DiscoveryNode) {
	if node == nil || node.Info == nil {
		return
	}
	if id := node.Info.GetId(); id != 0 {
		cur := s.NodesByID[id]
		if betterNodeForIndex(cur, node) {
			s.NodesByID[id] = node
		}
	}
	if name := node.Info.GetName(); name != "" {
		cur := s.NodesByName[name]
		if betterNodeForIndex(cur, node) {
			s.NodesByName[name] = node
		}
	}
}

func betterNodeForIndex(current *DiscoveryNode, candidate *DiscoveryNode) bool {
	if candidate == nil {
		return false
	}
	if current == nil {
		return true
	}
	if candidate.ModRevision != current.ModRevision {
		return candidate.ModRevision > current.ModRevision
	}
	if candidate.CreateRevision != current.CreateRevision {
		return candidate.CreateRevision > current.CreateRevision
	}
	if candidate.Version != current.Version {
		return candidate.Version > current.Version
	}
	return candidate.Path < current.Path
}

// TopologyNode wraps a remote node's topology info together with its etcd
// metadata.
type TopologyNode struct {
	Info *pb.AtappTopologyInfo
	etcdversion.DataVersion
}

// TopologySnapshot is the topology read-model sub-view within ExportSnapshot.
type TopologySnapshot struct {
	Ready        bool
	LastRevision int64
	NodesByID    map[uint64]*TopologyNode // Primary index by ID for C++ parity
}

// Clone returns a shallow copy.
func (s *TopologySnapshot) Clone() *TopologySnapshot {
	if s == nil {
		return &TopologySnapshot{}
	}
	out := &TopologySnapshot{
		Ready:        s.Ready,
		LastRevision: s.LastRevision,
	}
	if len(s.NodesByID) > 0 {
		out.NodesByID = make(map[uint64]*TopologyNode, len(s.NodesByID))
		for k, v := range s.NodesByID {
			out.NodesByID[k] = v
		}
	}
	return out
}

// RebuildIndexes is a no-op; NodesByID is the only index now.
func (s *TopologySnapshot) RebuildIndexes() {
	// No-op: ID indexing is maintained incrementally
}

// UpsertNode stores a topology node by ID.
func (s *TopologySnapshot) UpsertNode(node *TopologyNode) {
	if s == nil || node == nil || node.Info == nil {
		return
	}
	if id := node.Info.GetId(); id != 0 {
		if s.NodesByID == nil {
			s.NodesByID = make(map[uint64]*TopologyNode)
		}
		cur := s.NodesByID[id]
		if betterNode(cur, node) {
			s.NodesByID[id] = node
		}
	}
}

// RemoveNodeByID removes one topology node by ID.
func (s *TopologySnapshot) RemoveNodeByID(id uint64) {
	if s == nil || id == 0 || len(s.NodesByID) == 0 {
		return
	}
	delete(s.NodesByID, id)
}

// GetNodeByID returns indexed node by id.
func (s *TopologySnapshot) GetNodeByID(id uint64) *TopologyNode {
	if s == nil || id == 0 || len(s.NodesByID) == 0 {
		return nil
	}
	return s.NodesByID[id]
}

func (s *TopologySnapshot) indexNode(node *TopologyNode) {
	// Kept for internal reference; use UpsertNode for updates
	if node == nil || node.Info == nil {
		return
	}
	if id := node.Info.GetId(); id != 0 {
		cur := s.NodesByID[id]
		if betterNode(cur, node) {
			s.NodesByID[id] = node
		}
	}
}

func betterNode(current *TopologyNode, candidate *TopologyNode) bool {
	if candidate == nil {
		return false
	}
	if current == nil {
		return true
	}
	if candidate.ModRevision != current.ModRevision {
		return candidate.ModRevision > current.ModRevision
	}
	if candidate.CreateRevision != current.CreateRevision {
		return candidate.CreateRevision > current.CreateRevision
	}
	if candidate.Version != current.Version {
		return candidate.Version > current.Version
	}
	// All DataVersion fields are equal; versions are identical
	return false
}

// ── SnapshotCause ────────────────────────────────────────────────────────

// SnapshotCause identifies which sub-tree triggered the publish.
// Consumers may use it to skip diffing the unaffected sub-tree.
type SnapshotCause uint8

const (
	// SnapshotCauseReset is the zero value: full reset or initial publish.
	SnapshotCauseReset SnapshotCause = 0
	// SnapshotCauseDiscovery means only the Discovery sub-tree changed.
	SnapshotCauseDiscovery SnapshotCause = 1
	// SnapshotCauseTopology means only the Topology sub-tree changed.
	SnapshotCauseTopology SnapshotCause = 2
)

// ── ExportSnapshot ────────────────────────────────────────────────────────

// ExportSnapshot is the single, atomically-published read-model exported by
// ProjectionActor.  Callers obtain a pointer via EtcdModule.GetSnapshot() and
// may read it lock-free; they must not mutate any field.
//
// ExportSnapshot only reflects Watch results (remote nodes seen via etcd).
// Self-node registration state is published separately via EventRegistrationChanged.
type ExportSnapshot struct {
	// Version is monotonically incremented on every atomic publish.
	Version     uint64
	PublishedAt time.Time
	// Cause identifies which sub-tree triggered this publish.
	Cause     SnapshotCause
	Discovery DiscoverySetSnapshot
	Topology  TopologySnapshot
}

// Empty returns true if the snapshot carries no meaningful data yet.
func (s *ExportSnapshot) Empty() bool {
	return s == nil || s.Version == 0
}

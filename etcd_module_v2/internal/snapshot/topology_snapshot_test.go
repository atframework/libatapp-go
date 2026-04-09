package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/etcdversion"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func TestTopologySnapshot_RebuildIndexes(t *testing.T) {
	s := &TopologySnapshot{
		NodesByID: map[uint64]*TopologyNode{
			100: {Info: &pb.AtappTopologyInfo{Id: 100}},
			200: {Info: &pb.AtappTopologyInfo{Id: 200}},
		},
	}

	// Verify NodesByID index is still correct
	nByID := s.GetNodeByID(100)
	require.NotNil(t, nByID)
	assert.Equal(t, uint64(100), nByID.Info.GetId())

	nByID2 := s.GetNodeByID(200)
	require.NotNil(t, nByID2)
	assert.Equal(t, uint64(200), nByID2.Info.GetId())
}

func TestTopologySnapshot_UpsertAndGetByID(t *testing.T) {
	s := &TopologySnapshot{}
	node := &TopologyNode{
		Info: &pb.AtappTopologyInfo{Id: 500},
	}

	s.UpsertNode(node)

	// Verify node is stored in ID index
	assert.Equal(t, node, s.GetNodeByID(500))
}

func TestTopologySnapshot_RemoveNodeByID(t *testing.T) {
	s := &TopologySnapshot{}
	node := &TopologyNode{
		Info: &pb.AtappTopologyInfo{Id: 600},
	}

	s.UpsertNode(node)
	require.NotNil(t, s.GetNodeByID(600))

	s.RemoveNodeByID(600)

	// After removal, the node should be gone
	assert.Nil(t, s.GetNodeByID(600))
}

func TestTopologySnapshot_Clone(t *testing.T) {
	s := &TopologySnapshot{
		Ready:        true,
		LastRevision: 42,
		NodesByID: map[uint64]*TopologyNode{
			700: {Info: &pb.AtappTopologyInfo{Id: 700}},
		},
	}

	cloned := s.Clone()

	// Verify clone has same fields
	assert.Equal(t, s.Ready, cloned.Ready)
	assert.Equal(t, s.LastRevision, cloned.LastRevision)
	assert.Equal(t, len(s.NodesByID), len(cloned.NodesByID))

	// Verify clone's ID index works
	node := cloned.GetNodeByID(700)
	require.NotNil(t, node)
	assert.Equal(t, uint64(700), node.Info.GetId())
}

// ── betterNode table-driven tests ────────────────────────────────────────
// Mirrors C++ topology_update_version semantics (I.12–I.21).
//
// Go betterNode implements "overall winner" selection:
//   ModRevision compared first, then CreateRevision, then Version.
// This is the read-model projection equivalent of the C++ per-field max
// upgrade.  Each case verifies which of two TopologyNode versions "wins"
// when the same node ID is upserted twice.

func node(id uint64, create, modify, version int64) *TopologyNode {
	return &TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: id},
		DataVersion: etcdversion.DataVersion{CreateRevision: create, ModRevision: modify, Version: version},
	}
}

func TestBetterNode(t *testing.T) {
	tests := []struct {
		name       string
		current    *TopologyNode // already in snapshot
		candidate  *TopologyNode // incoming node
		wantBetter bool          // should candidate replace current?
	}{
		// ── nil guards ───────────────────────────────────────────────────────
		// I.12 equivalent: candidate nil → never better
		{
			name:       "nil candidate never better",
			current:    node(1, 10, 20, 1),
			candidate:  nil,
			wantBetter: false,
		},
		// I.13 equivalent: nil current → any candidate is better
		{
			name:       "nil current always worse",
			current:    nil,
			candidate:  node(1, 1, 1, 1),
			wantBetter: true,
		},

		// ── modify_revision is the primary comparator ─────────────────────
		// I.13-style: higher modify_revision wins
		{
			name:       "higher modify_revision wins",
			current:    node(1, 10, 20, 1),
			candidate:  node(1, 10, 30, 2),
			wantBetter: true,
		},
		// lower modify_revision loses (I.15/I.16 no-change cases)
		{
			name:       "lower modify_revision loses",
			current:    node(1, 10, 20, 3),
			candidate:  node(1, 10, 15, 2),
			wantBetter: false,
		},

		// ── create_revision is the secondary comparator ───────────────────
		// I.12-style: modify tied, higher create_revision wins
		{
			name:       "tie modify, higher create_revision wins",
			current:    node(1, 10, 20, 1),
			candidate:  node(1, 15, 20, 1),
			wantBetter: true,
		},
		// modify tied, lower create_revision loses
		{
			name:       "tie modify, lower create_revision loses",
			current:    node(1, 10, 20, 1),
			candidate:  node(1, 5, 20, 1),
			wantBetter: false,
		},

		// ── version is the tertiary comparator ───────────────────────────
		// I.14-style: both revisions tied, higher version wins
		{
			name:       "tie both revisions, higher version wins",
			current:    node(1, 10, 20, 1),
			candidate:  node(1, 10, 20, 5),
			wantBetter: true,
		},
		// I.15-style: all equal → candidate not better (no change)
		{
			name:       "all equal not better",
			current:    node(1, 10, 20, 3),
			candidate:  node(1, 10, 20, 3),
			wantBetter: false,
		},
		// I.16-style: all lower → not better
		{
			name:       "all lower not better",
			current:    node(1, 10, 20, 3),
			candidate:  node(1, 5, 15, 2),
			wantBetter: false,
		},

		// ── mixed higher/lower (I.18-style) ──────────────────────────────
		// modify is the dominant field: higher modify wins even if create is lower
		{
			name:       "higher modify wins over lower create",
			current:    node(1, 15, 10, 3),
			candidate:  node(1, 5, 25, 1),
			wantBetter: false,
		},
		// higher create win if modify is lower
		{
			name:       "lower modify loses to higher create",
			current:    node(1, 5, 25, 1),
			candidate:  node(1, 15, 10, 3),
			wantBetter: true,
		},

		// ── sequential upgrade chain (I.20-style) ─────────────────────────
		// v1 → v2 (only modify increases): betterNode(v1, v2) = true
		{
			name:       "v1 to v2 modify increase",
			current:    node(1, 1, 1, 1),
			candidate:  node(1, 1, 5, 2),
			wantBetter: true,
		},
		// v2 repeated: betterNode(v2, v2) = false (idempotent)
		{
			name:       "same v2 repeated not better",
			current:    node(1, 1, 5, 2),
			candidate:  node(1, 1, 5, 2),
			wantBetter: false,
		},
		// v2 → v3 (create_revision jumps, key recreated)
		{
			name:       "key recreated higher create wins",
			current:    node(1, 1, 5, 2),
			candidate:  node(1, 10, 10, 1),
			wantBetter: true,
		},

		// ── zero-initialized current (I.21-style) ────────────────────────
		{
			name:       "zero current any nonzero candidate wins",
			current:    node(1, 0, 0, 0),
			candidate:  node(1, 1, 1, 1),
			wantBetter: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := betterNode(tc.current, tc.candidate)
			assert.Equal(t, tc.wantBetter, got)
		})
	}
}

// ── TopologySnapshot multiple-entry independence ──────────────────────────
// Mirrors C++ I.31–I.32: map-based version tracking.

// TestTopologySnapshot_MultipleEntries_Independent verifies that upserting
// a new version of one entry does not affect other entries in the same snapshot.
func TestTopologySnapshot_MultipleEntries_Independent(t *testing.T) {
	s := &TopologySnapshot{}

	// Arrange: insert three independent nodes
	for _, id := range []uint64{1, 2, 3} {
		s.UpsertNode(&TopologyNode{
			Info:        &pb.AtappTopologyInfo{Id: id, Name: "server"},
			DataVersion: etcdversion.DataVersion{CreateRevision: int64(id), ModRevision: int64(id), Version: 1},
		})
	}

	// Act: update only node 2 with a higher modify_revision
	s.UpsertNode(&TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 2, Name: "server"},
		DataVersion: etcdversion.DataVersion{CreateRevision: 2, ModRevision: 10, Version: 5},
	})

	// Assert: node 1 unchanged
	n1 := s.GetNodeByID(1)
	require.NotNil(t, n1)
	assert.Equal(t, int64(1), n1.ModRevision)
	assert.Equal(t, int64(1), n1.Version)

	// Assert: node 2 updated
	n2 := s.GetNodeByID(2)
	require.NotNil(t, n2)
	assert.Equal(t, int64(10), n2.ModRevision)
	assert.Equal(t, int64(5), n2.Version)

	// Assert: node 3 unchanged
	n3 := s.GetNodeByID(3)
	require.NotNil(t, n3)
	assert.Equal(t, int64(3), n3.ModRevision)
	assert.Equal(t, int64(1), n3.Version)
}

// TestTopologySnapshot_Upsert_LowerRevisionDoesNotOverwrite verifies that
// re-upserting a node with a lower revision preserves the existing entry
// (idempotent / no regression on the same version: I.15-style).
func TestTopologySnapshot_Upsert_LowerRevisionDoesNotOverwrite(t *testing.T) {
	s := &TopologySnapshot{}

	high := &TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 100, Name: "svc"},
		DataVersion: etcdversion.DataVersion{CreateRevision: 1, ModRevision: 5, Version: 2},
	}
	low := &TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 100, Name: "svc"},
		DataVersion: etcdversion.DataVersion{CreateRevision: 1, ModRevision: 2, Version: 1},
	}

	s.UpsertNode(high)
	s.UpsertNode(low) // should be ignored

	got := s.GetNodeByID(100)
	require.NotNil(t, got)
	assert.Equal(t, int64(5), got.ModRevision, "lower revision must not overwrite")
}

// TestTopologySnapshot_SequentialUpgrades mirrors C++ I.20 sequential version chain.
func TestTopologySnapshot_SequentialUpgrades(t *testing.T) {
	s := &TopologySnapshot{}

	s.UpsertNode(&TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 42},
		DataVersion: etcdversion.DataVersion{CreateRevision: 0, ModRevision: 0, Version: 0},
	})
	got := s.GetNodeByID(42)
	require.NotNil(t, got)

	// v1: first real write
	s.UpsertNode(&TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 42},
		DataVersion: etcdversion.DataVersion{CreateRevision: 1, ModRevision: 1, Version: 1},
	})
	got = s.GetNodeByID(42)
	assert.Equal(t, int64(1), got.CreateRevision)
	assert.Equal(t, int64(1), got.ModRevision)
	assert.Equal(t, int64(1), got.Version)

	// v2: only modify_revision increases
	s.UpsertNode(&TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 42},
		DataVersion: etcdversion.DataVersion{CreateRevision: 1, ModRevision: 5, Version: 2},
	})
	got = s.GetNodeByID(42)
	assert.Equal(t, int64(1), got.CreateRevision)
	assert.Equal(t, int64(5), got.ModRevision)
	assert.Equal(t, int64(2), got.Version)

	// v2 again: same values → no regression
	s.UpsertNode(&TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 42},
		DataVersion: etcdversion.DataVersion{CreateRevision: 1, ModRevision: 5, Version: 2},
	})
	got = s.GetNodeByID(42)
	assert.Equal(t, int64(5), got.ModRevision, "idempotent re-upsert must not regress")

	// v3: create_revision jumps (key recreated), modify resets
	s.UpsertNode(&TopologyNode{
		Info:        &pb.AtappTopologyInfo{Id: 42},
		DataVersion: etcdversion.DataVersion{CreateRevision: 10, ModRevision: 10, Version: 1},
	})
	got = s.GetNodeByID(42)
	assert.Equal(t, int64(10), got.CreateRevision)
	assert.Equal(t, int64(10), got.ModRevision)
}

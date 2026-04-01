package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

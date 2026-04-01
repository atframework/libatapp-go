package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func TestDiscoverySetSnapshot_RebuildIndexes(t *testing.T) {
	s := &DiscoverySetSnapshot{
		NodesByPath: map[string]*DiscoveryNode{
			"/svc/a": {Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 1, Name: "svc-a"}},
			"/svc/b": {Path: "/svc/b", Info: &pb.AtappDiscovery{Id: 2, Name: "svc-b"}},
		},
	}

	s.RebuildIndexes()

	nByID := s.GetNodeByID(2)
	require.NotNil(t, nByID)
	assert.Equal(t, "/svc/b", nByID.Path)

	nByName := s.GetNodeByName("svc-a")
	require.NotNil(t, nByName)
	assert.Equal(t, "/svc/a", nByName.Path)
}

func TestDiscoverySetSnapshot_UpsertAndRemove(t *testing.T) {
	s := &DiscoverySetSnapshot{}
	n := &DiscoveryNode{Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 10, Name: "svc"}}

	s.UpsertNode(n)
	require.NotNil(t, s.GetNodeByID(10))
	require.NotNil(t, s.GetNodeByName("svc"))

	s.RemoveNodeByPath("/svc/a")
	assert.Nil(t, s.GetNodeByID(10))
	assert.Nil(t, s.GetNodeByName("svc"))
}

package modulev2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/atframework/libatapp-go/etcd_module_v2/internal/snapshot"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func TestGetNodeByID(t *testing.T) {
	nodes := map[string]*snapshot.DiscoveryNode{
		"/svc/a": {Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 1, Name: "svc-a"}},
		"/svc/b": {Path: "/svc/b", Info: &pb.AtappDiscovery{Id: 2, Name: "svc-b"}},
	}
	m, cancel := makeModuleWithSnapshot(t, nodes, ModuleOptions{})
	defer cancel()

	node := m.GetNodeByID(2)
	require.NotNil(t, node)
	assert.Equal(t, "/svc/b", node.Path)
	assert.Nil(t, m.GetNodeByID(999))
}

func TestGetNodeByName(t *testing.T) {
	nodes := map[string]*snapshot.DiscoveryNode{
		"/svc/a": {Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 1, Name: "svc-a"}},
		"/svc/b": {Path: "/svc/b", Info: &pb.AtappDiscovery{Id: 2, Name: "svc-b"}},
	}
	m, cancel := makeModuleWithSnapshot(t, nodes, ModuleOptions{})
	defer cancel()

	node := m.GetNodeByName("svc-a")
	require.NotNil(t, node)
	assert.Equal(t, "/svc/a", node.Path)
	assert.Nil(t, m.GetNodeByName("svc-missing"))
}

func TestGetNodeByRoundRobin_Order(t *testing.T) {
	nodes := map[string]*snapshot.DiscoveryNode{
		"/svc/c": {
			Path: "/svc/c",
			Info: &pb.AtappDiscovery{Id: 3, Name: "svc-c", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 2}},
		},
		"/svc/a": {
			Path: "/svc/a",
			Info: &pb.AtappDiscovery{Id: 1, Name: "svc-a", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 0}},
		},
		"/svc/b": {
			Path: "/svc/b",
			Info: &pb.AtappDiscovery{Id: 2, Name: "svc-b", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 1}},
		},
	}
	m, cancel := makeModuleWithSnapshot(t, nodes, ModuleOptions{})
	defer cancel()

	n1, err := m.GetNodeByRoundRobin(nil)
	require.NoError(t, err)
	n2, err := m.GetNodeByRoundRobin(nil)
	require.NoError(t, err)
	n3, err := m.GetNodeByRoundRobin(nil)
	require.NoError(t, err)

	assert.Equal(t, "/svc/a", n1.Path)
	assert.Equal(t, "/svc/b", n2.Path)
	assert.Equal(t, "/svc/c", n3.Path)
}

func TestGetNodeByRandom_Filter(t *testing.T) {
	nodes := map[string]*snapshot.DiscoveryNode{
		"/svc/prod": {
			Path: "/svc/prod",
			Info: &pb.AtappDiscovery{Id: 1, Name: "svc", Metadata: &pb.AtappMetadata{Labels: map[string]string{"env": "prod"}}},
		},
		"/svc/test": {
			Path: "/svc/test",
			Info: &pb.AtappDiscovery{Id: 2, Name: "svc", Metadata: &pb.AtappMetadata{Labels: map[string]string{"env": "test"}}},
		},
	}
	m, cancel := makeModuleWithSnapshot(t, nodes, ModuleOptions{})
	defer cancel()

	node, err := m.GetNodeByRandom(map[string]string{"env": "prod"})
	require.NoError(t, err)
	require.NotNil(t, node)
	assert.Equal(t, "/svc/prod", node.Path)
}

func TestGetNodeByRoundRobin_Empty(t *testing.T) {
	m, cancel := makeModuleWithSnapshot(t, nil, ModuleOptions{})
	defer cancel()

	node, err := m.GetNodeByRoundRobin(nil)
	assert.Nil(t, node)
	assert.Error(t, err)
}

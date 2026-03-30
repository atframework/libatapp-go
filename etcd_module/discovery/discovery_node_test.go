package discovery_test

import (
	"sync"
	"testing"
	"unsafe"

	"github.com/atframework/libatapp-go/etcd_module/discovery"
	pb "github.com/atframework/libatapp-go/protocol/atframe"

	log "log/slog"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoveryNode_CopyFrom(t *testing.T) {
	node := &discovery.DiscoveryNode{}
	info := &pb.AtappDiscovery{
		Id:       12345,
		Name:     "test-service",
		Hostname: "test-host",
		Pid:      999,
		Version:  "v1.0.0",
		Listen:   []string{"tcp://127.0.0.1:8080"},
	}

	node.CopyFrom(info, 100, 200, 300)

	assert.NotNil(t, node.Info)
	assert.NotSame(t, info, node.Info)
	assert.Equal(t, info, node.Info)
	assert.Equal(t, int64(100), node.CreateRevision)
	assert.Equal(t, int64(200), node.ModRevision)
	assert.Equal(t, int64(300), node.Version)

	info.Name = "mutated-name"
	info.Listen[0] = "tcp://127.0.0.1:9090"
	assert.Equal(t, "test-service", node.Info.Name)
	assert.Equal(t, []string{"tcp://127.0.0.1:8080"}, node.Info.Listen)
}

func TestDiscoveryNode_CopyTo(t *testing.T) {
	srcInfo := &pb.AtappDiscovery{
		Id:         12345,
		Name:       "test-service",
		Hostname:   "test-host",
		Pid:        999,
		Version:    "v1.0.0",
		Listen:     []string{"tcp://127.0.0.1:8080"},
		CustomData: "custom",
	}
	node := &discovery.DiscoveryNode{
		Info: srcInfo,
	}

	output := &pb.AtappDiscovery{
		Id:       99999,
		Name:     "stale",
		Version:  "stale-version",
		Listen:   []string{"tcp://stale:1"},
		Identity: "stale-identity",
	}
	node.CopyTo(output)

	assert.Equal(t, srcInfo.Id, output.Id)
	assert.Equal(t, srcInfo.Name, output.Name)
	assert.Equal(t, srcInfo.Hostname, output.Hostname)
	assert.Equal(t, srcInfo.Pid, output.Pid)
	assert.Equal(t, srcInfo.Version, output.Version)
	assert.Equal(t, srcInfo.Listen, output.Listen)
	assert.Equal(t, srcInfo.CustomData, output.CustomData)
	assert.Empty(t, output.Identity)
}

func TestDiscoveryNode_CopyTo_NilOutput(t *testing.T) {
	node := &discovery.DiscoveryNode{
		Info: &pb.AtappDiscovery{Id: 123},
	}
	node.CopyTo(nil)
}

func TestDiscoveryNode_CopyTo_NilInfo(t *testing.T) {
	node := &discovery.DiscoveryNode{}
	output := &pb.AtappDiscovery{
		Id:      123,
		Name:    "should-be-cleared",
		Version: "stale",
	}
	node.CopyTo(output)
	assert.Equal(t, uint64(0), output.Id)
	assert.Empty(t, output.Name)
	assert.Empty(t, output.Version)
}

func TestDiscoveryNode_CopyKeyTo(t *testing.T) {
	srcInfo := &pb.AtappDiscovery{
		Id:         12345,
		Name:       "test-service",
		Hostname:   "test-host",
		Pid:        999,
		Identity:   "id-abc",
		HashCode:   "hash123",
		TypeName:   "game-server",
		TypeId:     42,
		Version:    "v1.0.0",
		Listen:     []string{"tcp://127.0.0.1:8080"},
		CustomData: "custom",
	}
	node := &discovery.DiscoveryNode{
		Info: srcInfo,
	}

	output := &pb.AtappDiscovery{}
	node.CopyKeyTo(output)

	assert.Equal(t, srcInfo.Id, output.Id)
	assert.Equal(t, srcInfo.Name, output.Name)
	assert.Equal(t, srcInfo.Hostname, output.Hostname)
	assert.Equal(t, srcInfo.Pid, output.Pid)
	assert.Equal(t, srcInfo.Identity, output.Identity)
	assert.Equal(t, srcInfo.HashCode, output.HashCode)
	assert.Equal(t, srcInfo.TypeName, output.TypeName)
	assert.Equal(t, srcInfo.TypeId, output.TypeId)

	assert.Empty(t, output.Version)
	assert.Empty(t, output.Listen)
	assert.Empty(t, output.CustomData)
}

func TestDiscoveryNode_CopyKeyTo_NilOutput(t *testing.T) {
	node := &discovery.DiscoveryNode{
		Info: &pb.AtappDiscovery{Id: 123},
	}
	node.CopyKeyTo(nil)
}

func TestDiscoveryNode_CopyKeyTo_NilInfo(t *testing.T) {
	node := &discovery.DiscoveryNode{}
	output := &pb.AtappDiscovery{}
	node.CopyKeyTo(output)
	assert.Equal(t, uint64(0), output.Id)
}

func TestDiscoveryNode_GetIngressSize_NilInfo(t *testing.T) {
	node := &discovery.DiscoveryNode{}
	assert.Equal(t, 0, node.GetIngressSize())
}

func TestDiscoveryNode_NextIngressGateway_NilInfo(t *testing.T) {
	node := &discovery.DiscoveryNode{}
	assert.Nil(t, node.NextIngressGateway())
}

func TestDiscoveryNode_NextIngressGateway_ListenFallback(t *testing.T) {
	node := &discovery.DiscoveryNode{
		Info: &pb.AtappDiscovery{
			Listen: []string{"127.0.0.1:8080", "127.0.0.1:8081"},
		},
	}

	first := node.NextIngressGateway()
	require.NotNil(t, first)
	assert.Equal(t, "127.0.0.1:8080", first.Address)

	second := node.NextIngressGateway()
	require.NotNil(t, second)
	assert.Equal(t, "127.0.0.1:8081", second.Address)
}

func TestDiscoveryNode_BubbleModel_UpdateVersionConcurrentAccess(t *testing.T) {
	node := &discovery.DiscoveryNode{
		Info: &pb.AtappDiscovery{Name: "svc"},
	}

	const writerBubbles = 16
	const readerBubbles = 16
	const iterations = 200

	var ready sync.WaitGroup
	var done sync.WaitGroup
	startGate := make(chan struct{})

	ready.Add(writerBubbles + readerBubbles)
	done.Add(writerBubbles + readerBubbles)

	for i := 0; i < writerBubbles; i++ {
		go func(offset int64) {
			defer done.Done()
			ready.Done()
			<-startGate
			for j := 0; j < iterations; j++ {
				v := int64(j) + offset
				node.UpdateVersion(v, v, v, true)
			}
		}(int64(i) * 1_000)
	}

	for i := 0; i < readerBubbles; i++ {
		go func() {
			defer done.Done()
			ready.Done()
			<-startGate
			for j := 0; j < iterations; j++ {
				out := &pb.AtappDiscovery{}
				node.CopyTo(out)
				_ = node.GetIngressSize()
			}
		}()

	}

	ready.Wait()
	close(startGate)
	done.Wait()

	assert.GreaterOrEqual(t, node.Version, int64(0))
}

func TestDiscoveryNode_Equal_SamePointer(t *testing.T) {
	node := &discovery.DiscoveryNode{Info: &pb.AtappDiscovery{Id: 1, Name: "svc-a"}}
	assert.True(t, node.Equal(node))
}

func TestDiscoveryNode_Equal_NilOther(t *testing.T) {
	node := &discovery.DiscoveryNode{Info: &pb.AtappDiscovery{Id: 1, Name: "svc-a"}}
	assert.False(t, node.Equal(nil))
}

func TestDiscoveryNode_Equal_ByIDPriority(t *testing.T) {
	left := &discovery.DiscoveryNode{Info: &pb.AtappDiscovery{Id: 1001, Name: "svc-left"}}
	rightSameID := &discovery.DiscoveryNode{Info: &pb.AtappDiscovery{Id: 1001, Name: "svc-right"}}
	rightDifferentID := &discovery.DiscoveryNode{Info: &pb.AtappDiscovery{Id: 1002, Name: "svc-left"}}

	assert.True(t, left.Equal(rightSameID), "when both ids are non-zero, equal should compare ids")
	assert.False(t, left.Equal(rightDifferentID), "when both ids are non-zero, different ids should be unequal")
}

func TestDiscoveryNode_Equal_ByNameFallback(t *testing.T) {
	left := &discovery.DiscoveryNode{Info: &pb.AtappDiscovery{Id: 0, Name: "svc-a"}}
	rightSameName := &discovery.DiscoveryNode{Info: &pb.AtappDiscovery{Id: 0, Name: "svc-a"}}
	rightDifferentName := &discovery.DiscoveryNode{Info: &pb.AtappDiscovery{Id: 0, Name: "svc-b"}}

	assert.True(t, left.Equal(rightSameName), "when id is zero, equal should fallback to name")
	assert.False(t, left.Equal(rightDifferentName), "different names should be unequal in fallback mode")
}

func TestDiscoveryNode_SetGetOnDestroy(t *testing.T) {
	node := &discovery.DiscoveryNode{}

	assert.Nil(t, node.GetOnDestroy())

	callbackInvoked := false
	callback := func(n *discovery.DiscoveryNode) {
		callbackInvoked = true
	}

	node.SetOnDestroy(callback)
	fn := node.GetOnDestroy()
	require.NotNil(t, fn)

	fn(node)
	assert.True(t, callbackInvoked)
}

func TestDiscoveryNode_ResetOnDestroy(t *testing.T) {
	node := &discovery.DiscoveryNode{}

	callback := func(n *discovery.DiscoveryNode) {}
	node.SetOnDestroy(callback)
	assert.NotNil(t, node.GetOnDestroy())

	node.ResetOnDestroy()
	assert.Nil(t, node.GetOnDestroy())
}

func TestDiscoveryNode_OnDestroyConcurrent(t *testing.T) {
	node := &discovery.DiscoveryNode{}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node.SetOnDestroy(func(n *discovery.DiscoveryNode) {})
			_ = node.GetOnDestroy()
			node.ResetOnDestroy()
		}()
	}
	wg.Wait()
}

func TestEtcdDiscoverySet_OnDestroyCallback(t *testing.T) {
	logger := log.Default()
	set, err := discovery.NewEtcdDiscoverySet("/test", logger)
	require.NoError(t, err)

	node := &discovery.DiscoveryNode{
		Info: &pb.AtappDiscovery{
			Id:       12345,
			Name:     "test-service",
			Hostname: "test-host",
		},
		Path: "/test/path",
	}

	var destroyedNode *discovery.DiscoveryNode
	node.SetOnDestroy(func(n *discovery.DiscoveryNode) {
		destroyedNode = n
	})

	set.AddNode(node)

	set.RemoveNode("/test/path")

	require.NotNil(t, destroyedNode)
	assert.Equal(t, node.Info.Id, destroyedNode.Info.Id)
}

func TestEtcdDiscoverySet_GetNodeByConsistentHashInt64(t *testing.T) {
	logger := log.Default()
	set, err := discovery.NewEtcdDiscoverySet("/test", logger)
	require.NoError(t, err)

	node1 := &discovery.DiscoveryNode{
		Info: &pb.AtappDiscovery{
			Id:       1,
			Name:     "node1",
			Hostname: "host1",
		},
		Path: "/node1",
	}
	node2 := &discovery.DiscoveryNode{
		Info: &pb.AtappDiscovery{
			Id:       2,
			Name:     "node2",
			Hostname: "host2",
		},
		Path: "/node2",
	}

	set.AddNode(node1)
	set.AddNode(node2)

	var hash int64 = 9876543210

	resultInt64, err := set.GetNodeByConsistentHashInt64(hash, nil)
	require.NoError(t, err)
	require.NotNil(t, resultInt64)

	resultUint64, err := set.GetNodeByConsistentHashUint64(uint64(hash), nil)
	require.NoError(t, err)
	require.NotNil(t, resultUint64)

	assert.Equal(t, resultUint64.Info.Id, resultInt64.Info.Id)
}

func TestEtcdDiscoverySet_GetNodeByConsistentHashInt64_Negative(t *testing.T) {
	logger := log.Default()
	set, err := discovery.NewEtcdDiscoverySet("/test", logger)
	require.NoError(t, err)

	node1 := &discovery.DiscoveryNode{
		Info: &pb.AtappDiscovery{
			Id:       1,
			Name:     "node1",
			Hostname: "host1",
		},
		Path: "/node1",
	}

	set.AddNode(node1)

	var negativeHash int64 = -123456

	resultInt64, err := set.GetNodeByConsistentHashInt64(negativeHash, nil)
	require.NoError(t, err)
	require.NotNil(t, resultInt64)

	resultUint64, err := set.GetNodeByConsistentHashUint64(uint64(negativeHash), nil)
	require.NoError(t, err)
	require.NotNil(t, resultUint64)

	assert.Equal(t, resultUint64.Info.Id, resultInt64.Info.Id)
}

func TestEtcdDiscoverySet_GetNodeByConsistentHashInt64_Empty(t *testing.T) {
	logger := log.Default()
	set, err := discovery.NewEtcdDiscoverySet("/test", logger)
	require.NoError(t, err)

	_, err2 := set.GetNodeByConsistentHashInt64(123, nil)
	assert.Error(t, err2)
}

func TestDiscoveryNode_GetNameHash(t *testing.T) {
	node := &discovery.DiscoveryNode{}
	info := &pb.AtappDiscovery{
		Name: "test-service-hash",
	}

	node.CopyFrom(info, 100, 200, 300)

	lo, hi := node.GetNameHash()
	assert.True(t, lo != 0 || hi != 0, "hash should not be all zeros for non-empty name")

	lo2, hi2 := node.GetNameHash()
	assert.Equal(t, lo, lo2, "hash should be consistent")
	assert.Equal(t, hi, hi2, "hash should be consistent")
}

func TestDiscoveryNode_GetNameHash_NilInfo(t *testing.T) {
	node := &discovery.DiscoveryNode{}
	node.CopyFrom(nil, 0, 0, 0)

	lo, hi := node.GetNameHash()
	assert.Equal(t, uint64(0), lo, "hash should be zero for nil info")
	assert.Equal(t, uint64(0), hi, "hash should be zero for nil info")
}

func TestDiscoveryNode_GetNameHash_Consistent(t *testing.T) {
	node1 := &discovery.DiscoveryNode{}
	node2 := &discovery.DiscoveryNode{}
	info := &pb.AtappDiscovery{
		Name: "same-service-name",
	}

	node1.CopyFrom(info, 100, 200, 300)
	node2.CopyFrom(info, 400, 500, 600)

	lo1, hi1 := node1.GetNameHash()
	lo2, hi2 := node2.GetNameHash()

	assert.Equal(t, lo1, lo2, "nodes with same name should have same hash")
	assert.Equal(t, hi1, hi2, "nodes with same name should have same hash")
}

func TestDiscoveryNode_PrivateData_U64(t *testing.T) {
	node := &discovery.DiscoveryNode{}

	node.SetPrivateData(uint64(42))
	v, ok := node.GetPrivateData().(uint64)
	assert.True(t, ok)
	assert.Equal(t, uint64(42), v)
}

func TestDiscoveryNode_PrivateData_I64(t *testing.T) {
	node := &discovery.DiscoveryNode{}

	node.SetPrivateData(int64(-99))
	v, ok := node.GetPrivateData().(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(-99), v)
}

func TestDiscoveryNode_PrivateData_Ptr(t *testing.T) {
	node := &discovery.DiscoveryNode{}
	testVar := int(42)
	testPtr := unsafe.Pointer(&testVar)

	node.SetPrivateData(testPtr)
	resultPtr, ok := node.GetPrivateData().(unsafe.Pointer)
	assert.True(t, ok)

	assert.Equal(t, testPtr, resultPtr)
	assert.Equal(t, 42, *(*int)(resultPtr))
}

func TestDiscoveryNode_PrivateData_Concurrent(t *testing.T) {
	node := &discovery.DiscoveryNode{}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(val uint64) {
			defer wg.Done()
			node.SetPrivateData(val)
			_ = node.GetPrivateData()
		}(uint64(i))
	}
	wg.Wait()
}

func TestDiscoveryNode_GetDiscoveryInfo(t *testing.T) {
	node := &discovery.DiscoveryNode{}
	info := &pb.AtappDiscovery{
		Id:       54321,
		Name:     "info-test",
		Hostname: "test-host-info",
	}

	node.Info = info
	
	var result *pb.AtappDiscovery
	node.WithDiscoveryInfo(func(info *pb.AtappDiscovery) {
		result = info
	})

	assert.Equal(t, info, result)
	assert.Equal(t, uint64(54321), result.Id)
}

func TestDiscoveryNode_GetNodeVersion(t *testing.T) {
	node := &discovery.DiscoveryNode{}
	info := &pb.AtappDiscovery{
		Name: "version-test",
	}

	node.CopyFrom(info, 111, 222, 333)

	createRev, modRev, ver := node.GetNodeVersion()
	assert.Equal(t, int64(111), createRev)
	assert.Equal(t, int64(222), modRev)
	assert.Equal(t, int64(333), ver)
}

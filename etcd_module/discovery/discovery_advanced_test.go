package discovery

import (
	pb "github.com/atframework/libatapp-go/protocol/atframe"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	log "log/slog"
)

// 该测试函数用于验证相关行为。
func TestConcurrentAccess(t *testing.T) {
	// Arrange
	logger := log.Default()
	prefix := "/test/"

	discovery, _ := NewEtcdDiscoverySet(prefix, logger)

	// Arrange: add some nodes via AddNode() instead of watcher events
	for i := 1; i <= 5; i++ {
		nodeInfo := &pb.AtappDiscovery{Name: "node" + string(rune('0'+i))}
		discovery.AddNode(&DiscoveryNode{
			Path: prefix + "node" + string(rune('0'+i)),
			Info: nodeInfo,
		})
	}

	// Act
	errorCount := atomic.Int32{}
	var wg sync.WaitGroup
	numGoroutines := 10

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				// Try different access patterns
				switch i % 4 {
				case 0:
					_, _ = discovery.GetNodeByRandom(nil)
				case 1:
					_, _ = discovery.GetNodeByRoundRobin(nil)
				case 2:
					_, _ = discovery.GetNodeByConsistentHash("key", nil)
				case 3:
					_ = discovery.GetAllNodes()
				}
			}
		}(g)
	}

	wg.Wait()

	// Assert
	if errorCount.Load() > 0 {
		t.Errorf("Concurrent access resulted in %d errors", errorCount.Load())
	}
}

// 该测试函数用于验证相关行为。
func TestEmptyDiscovery(t *testing.T) {
	// Arrange
	logger := log.Default()
	prefix := "/test/"

	discovery, _ := NewEtcdDiscoverySet(prefix, logger)

	// Arrange: give some time to simulate async behavior
	time.Sleep(20 * time.Millisecond)

	// Act
	_, err := discovery.GetNodeByConsistentHash("key", nil)

	// Assert
	if err == nil {
		t.Error("Expected error when no nodes available for consistent hash")
	}

	// Act
	_, err = discovery.GetNodeByRoundRobin(nil)

	// Assert
	if err == nil {
		t.Error("Expected error when no nodes available for round robin")
	}

	// Act
	_, err = discovery.GetNodeByRandom(nil)

	// Assert
	if err == nil {
		t.Error("Expected error when no nodes available for random")
	}

	// Act
	nodes := discovery.GetAllNodes()

	// Assert
	if len(nodes) != 0 {
		t.Errorf("Expected 0 nodes, got %d", len(nodes))
	}
}

// 该测试函数用于验证相关行为。
func TestNodeUpdate(t *testing.T) {
	// Arrange
	logger := log.Default()
	prefix := "/test/"
	discovery, _ := NewEtcdDiscoverySet(prefix, logger)

	// Arrange: add initial node
	node1 := &DiscoveryNode{
		Path: prefix + "node1",
		Info: &pb.AtappDiscovery{
			Name:     "node1",
			Metadata: &pb.AtappMetadata{Labels: map[string]string{"version": "v1"}},
		},
	}
	discovery.AddNode(node1)

	// Assert
	nodes := discovery.GetAllNodes()
	if len(nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(nodes))
	}

	// Act
	node1Updated := &DiscoveryNode{
		Path: prefix + "node1",
		Info: &pb.AtappDiscovery{
			Name:     "node1",
			Metadata: &pb.AtappMetadata{Labels: map[string]string{"version": "v2"}},
		},
	}
	discovery.AddNode(node1Updated)

	// Assert
	nodes = discovery.GetAllNodes()
	if len(nodes) != 1 {
		t.Errorf("Expected 1 node after update, got %d", len(nodes))
	}
}

// 该测试函数用于验证相关行为。
func TestLargeScaleNodes(t *testing.T) {
	// Arrange
	logger := log.Default()
	prefix := "/test/"
	discovery, _ := NewEtcdDiscoverySet(prefix, logger)

	// Arrange: add 100 nodes
	for i := 0; i < 100; i++ {
		discovery.AddNode(&DiscoveryNode{
			Path: prefix + "node" + string(rune(i)),
			Info: &pb.AtappDiscovery{Name: "node" + string(rune(i))},
		})
	}

	// Act
	for i := 0; i < 100; i++ {
		_, _ = discovery.GetNodeByRandom(nil)
		_, _ = discovery.GetNodeByRoundRobin(nil)
		_, _ = discovery.GetNodeByConsistentHash("test-key", nil)
	}

	// Assert
	nodes := discovery.GetAllNodes()
	if len(nodes) != 100 {
		t.Errorf("Expected 100 nodes, got %d", len(nodes))
	}
}

// 该测试函数用于验证相关行为。
func TestFilterCaching(t *testing.T) {
	// Arrange
	logger := log.Default()
	prefix := "/test/"
	discovery, _ := NewEtcdDiscoverySet(prefix, logger)

	// Arrange: add nodes with different metadata
	for i := 0; i < 10; i++ {
		env := "prod"
		if i%2 == 0 {
			env = "dev"
		}
		discovery.AddNode(&DiscoveryNode{
			Path: prefix + "node" + string(rune(i)),
			Info: &pb.AtappDiscovery{
				Name:     "node" + string(rune(i)),
				Metadata: &pb.AtappMetadata{Labels: map[string]string{"env": env}},
			},
		})
	}

	// Act
	filter := map[string]string{"env": "prod"}
	node1, err := discovery.GetNodeByRandom(filter)

	// Assert
	if err != nil {
		t.Errorf("Failed to get node with filter: %v", err)
	}

	// Act
	node2, err := discovery.GetNodeByRandom(filter)

	// Assert
	if err != nil {
		t.Errorf("Failed on second call: %v", err)
	}

	// Assert
	if node1.Info.Metadata == nil || node1.Info.Metadata.Labels["env"] != "prod" {
		t.Errorf("Node1 is not prod: %v", node1.Info.Metadata)
	}
	if node2.Info.Metadata == nil || node2.Info.Metadata.Labels["env"] != "prod" {
		t.Errorf("Node2 is not prod: %v", node2.Info.Metadata)
	}
}

// 该测试函数用于验证相关行为。
func TestRemoveNode(t *testing.T) {
	// Arrange
	logger := log.Default()
	prefix := "/test/"
	discovery, _ := NewEtcdDiscoverySet(prefix, logger)

	// Arrange: add 3 nodes
	for i := 1; i <= 3; i++ {
		discovery.AddNode(&DiscoveryNode{
			Path: prefix + "node" + string(rune('0'+i)),
			Info: &pb.AtappDiscovery{Name: "node" + string(rune('0'+i))},
		})
	}

	// Assert
	nodes := discovery.GetAllNodes()
	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}

	// Act
	discovery.RemoveNode(prefix + "node1")

	// Assert
	nodes = discovery.GetAllNodes()
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes after removal, got %d", len(nodes))
	}
}

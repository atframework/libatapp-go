package discovery_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/atframework/libatapp-go/etcd_module/discovery"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func makeTestNode(id uint64, name, path string) *discovery.DiscoveryNode {
	return &discovery.DiscoveryNode{
		Path: path,
		Info: &pb.AtappDiscovery{
			Id:   id,
			Name: name,
			Metadata: &pb.AtappMetadata{
				Kind:          "server",
				NamespaceName: "default",
			},
		},
	}
}

func TestEmpty(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	t.Run("empty set returns true", func(t *testing.T) {
		if !set.Empty() {
			t.Errorf("Empty() = false, want true")
		}
	})

	t.Run("after AddNode returns false", func(t *testing.T) {
		set.AddNode(makeTestNode(1, "node-1", "/test/node-1"))
		if set.Empty() {
			t.Errorf("Empty() = true after AddNode, want false")
		}
	})
}

func TestGetNodeByID(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	t.Run("returns nil for unknown ID", func(t *testing.T) {
		node := set.GetNodeByID(999)
		if node != nil {
			t.Errorf("GetNodeByID(999) = %v, want nil", node)
		}
	})

	t.Run("returns correct node after AddNode", func(t *testing.T) {
		testNode := makeTestNode(42, "node-42", "/test/node-42")
		set.AddNode(testNode)
		node := set.GetNodeByID(42)
		if node == nil {
			t.Fatalf("GetNodeByID(42) = nil, want node")
		}
		if node.Info.Id != 42 {
			t.Errorf("node.Info.Id = %d, want 42", node.Info.Id)
		}
		if node.Info.Name != "node-42" {
			t.Errorf("node.Info.Name = %s, want node-42", node.Info.Name)
		}
	})
}

func TestGetNodeByName(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	t.Run("returns nil for unknown name", func(t *testing.T) {
		node := set.GetNodeByName("unknown")
		if node != nil {
			t.Errorf("GetNodeByName(unknown) = %v, want nil", node)
		}
	})

	t.Run("returns correct node after AddNode", func(t *testing.T) {
		testNode := makeTestNode(1, "test-service", "/test/service-1")
		set.AddNode(testNode)
		node := set.GetNodeByName("test-service")
		if node == nil {
			t.Fatalf("GetNodeByName(test-service) = nil, want node")
		}
		if node.Info.Name != "test-service" {
			t.Errorf("node.Info.Name = %s, want test-service", node.Info.Name)
		}
		if node.Info.Id != 1 {
			t.Errorf("node.Info.Id = %d, want 1", node.Info.Id)
		}
	})
}

func TestGetSortedNodes_NoFilter(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	set.AddNode(makeTestNode(3, "node-c", "/test/node-3"))
	set.AddNode(makeTestNode(1, "node-a", "/test/node-1"))
	set.AddNode(makeTestNode(2, "node-b", "/test/node-2"))

	nodes := set.GetSortedNodes(nil)
	if len(nodes) != 3 {
		t.Fatalf("GetSortedNodes() returned %d nodes, want 3", len(nodes))
	}

	if nodes[0].Info.Id != 1 {
		t.Errorf("nodes[0].Info.Id = %d, want 1", nodes[0].Info.Id)
	}
	if nodes[1].Info.Id != 2 {
		t.Errorf("nodes[1].Info.Id = %d, want 2", nodes[1].Info.Id)
	}
	if nodes[2].Info.Id != 3 {
		t.Errorf("nodes[2].Info.Id = %d, want 3", nodes[2].Info.Id)
	}
}

func TestGetSortedNodes_WithFilter(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	node1 := makeTestNode(1, "api", "/test/api-1")
	node1.Info.Metadata.Kind = "api"
	set.AddNode(node1)

	node2 := makeTestNode(2, "worker", "/test/worker-1")
	node2.Info.Metadata.Kind = "worker"
	set.AddNode(node2)

	node3 := makeTestNode(3, "api", "/test/api-2")
	node3.Info.Metadata.Kind = "api"
	set.AddNode(node3)

	nodes := set.GetSortedNodes(map[string]string{"kind": "api"})
	if len(nodes) != 1 {
		t.Fatalf("GetSortedNodes(kind=api) returned %d nodes, want 1", len(nodes))
	}
	if nodes[0].Info.Id != 3 {
		t.Errorf("nodes[0].Info.Id = %d, want 3", nodes[0].Info.Id)
	}
}

func TestGetSortedNodes_ReturnsCopy(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	set.AddNode(makeTestNode(1, "node-1", "/test/node-1"))
	set.AddNode(makeTestNode(2, "node-2", "/test/node-2"))

	nodes1 := set.GetSortedNodes(nil)
	nodes1[0] = nil

	nodes2 := set.GetSortedNodes(nil)
	if nodes2[0] == nil {
		t.Errorf("mutating returned slice affected internal state")
	}
	if nodes2[0].Info.Id != 1 {
		t.Errorf("nodes2[0].Info.Id = %d, want 1", nodes2[0].Info.Id)
	}
}

func TestGetNodeByConsistentHashBytes(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	t.Run("error on empty key", func(t *testing.T) {
		set.AddNode(makeTestNode(1, "node-1", "/test/node-1"))
		_, err := set.GetNodeByConsistentHashBytes([]byte{}, nil)
		if err == nil {
			t.Errorf("GetNodeByConsistentHashBytes(empty) succeeded, want error")
		}
	})

	t.Run("returns node for valid key", func(t *testing.T) {
		set.AddNode(makeTestNode(2, "node-2", "/test/node-2"))
		set.AddNode(makeTestNode(3, "node-3", "/test/node-3"))
		node, err := set.GetNodeByConsistentHashBytes([]byte("test-key-123"), nil)
		if err != nil {
			t.Fatalf("GetNodeByConsistentHashBytes failed: %v", err)
		}
		if node == nil {
			t.Fatalf("GetNodeByConsistentHashBytes returned nil node")
		}
		if node.Info.Id != 1 && node.Info.Id != 2 && node.Info.Id != 3 {
			t.Errorf("node.Info.Id = %d, want one of [1, 2, 3]", node.Info.Id)
		}
	})
}

func TestGetNodeByConsistentHashUint64(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	set.AddNode(makeTestNode(10, "node-10", "/test/node-10"))
	set.AddNode(makeTestNode(20, "node-20", "/test/node-20"))
	set.AddNode(makeTestNode(30, "node-30", "/test/node-30"))

	node, err := set.GetNodeByConsistentHashUint64(123456789, nil)
	if err != nil {
		t.Fatalf("GetNodeByConsistentHashUint64 failed: %v", err)
	}
	if node == nil {
		t.Fatalf("GetNodeByConsistentHashUint64 returned nil node")
	}
	if node.Info.Id != 10 && node.Info.Id != 20 && node.Info.Id != 30 {
		t.Errorf("node.Info.Id = %d, want one of [10, 20, 30]", node.Info.Id)
	}
}

func TestMetadataIndexSize(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	t.Run("starts at 0", func(t *testing.T) {
		size := set.MetadataIndexSize()
		if size != 0 {
			t.Errorf("MetadataIndexSize() = %d, want 0", size)
		}
	})

	t.Run("grows as new filters are queried", func(t *testing.T) {
		node1 := makeTestNode(1, "node-1", "/test/node-1")
		node1.Info.Metadata.Kind = "api"
		set.AddNode(node1)

		node2 := makeTestNode(2, "node-2", "/test/node-2")
		node2.Info.Metadata.Kind = "worker"
		set.AddNode(node2)

		set.GetSortedNodes(nil)
		size1 := set.MetadataIndexSize()
		if size1 != 0 {
			t.Errorf("MetadataIndexSize() = %d after GetSortedNodes(nil), want 0", size1)
		}

		set.GetSortedNodes(map[string]string{"kind": "api"})
		size2 := set.MetadataIndexSize()
		if size2 != 1 {
			t.Errorf("MetadataIndexSize() = %d after GetSortedNodes(kind=api), want 1", size2)
		}

		set.GetSortedNodes(map[string]string{"kind": "worker"})
		size3 := set.MetadataIndexSize()
		if size3 != 2 {
			t.Errorf("MetadataIndexSize() = %d after GetSortedNodes(kind=worker), want 2", size3)
		}
	})
}

func TestGetNodeHashByConsistentHash(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	set.AddNode(makeTestNode(100, "node-100", "/test/node-100"))
	set.AddNode(makeTestNode(200, "node-200", "/test/node-200"))

	memberKey, node, err := set.GetNodeHashByConsistentHash("my-routing-key", nil)
	if err != nil {
		t.Fatalf("GetNodeHashByConsistentHash failed: %v", err)
	}

	if memberKey == "" {
		t.Errorf("memberKey is empty, want non-empty string")
	}
	if node == nil {
		t.Fatalf("node is nil, want valid node")
	}
	if node.Info.Id != 100 && node.Info.Id != 200 {
		t.Errorf("node.Info.Id = %d, want one of [100, 200]", node.Info.Id)
	}

	if memberKey != "id:100" && memberKey != "id:200" {
		t.Errorf("memberKey = %s, want one of [id:100, id:200]", memberKey)
	}
}

func TestSortedBounds(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	set.AddNode(makeTestNode(10, "node-10", "/test/node-10"))
	set.AddNode(makeTestNode(20, "node-20", "/test/node-20"))
	set.AddNode(makeTestNode(30, "node-30", "/test/node-30"))

	t.Run("lower bound exact id", func(t *testing.T) {
		idx := set.LowerBoundSortedNodes(20, "", nil)
		if idx != 1 {
			t.Fatalf("LowerBoundSortedNodes(20, \"\") = %d, want 1", idx)
		}
	})

	t.Run("lower bound between ids", func(t *testing.T) {
		idx := set.LowerBoundSortedNodes(25, "", nil)
		if idx != 2 {
			t.Fatalf("LowerBoundSortedNodes(25, \"\") = %d, want 2", idx)
		}
	})

	t.Run("upper bound exact id", func(t *testing.T) {
		idx := set.UpperBoundSortedNodes(20, "", nil)
		if idx != 2 {
			t.Fatalf("UpperBoundSortedNodes(20, \"\") = %d, want 2", idx)
		}
	})

	t.Run("upper bound tail", func(t *testing.T) {
		idx := set.UpperBoundSortedNodes(30, "", nil)
		if idx != 3 {
			t.Fatalf("UpperBoundSortedNodes(30, \"\") = %d, want 3", idx)
		}
	})
}

func TestRemoveNodeByID(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	t.Run("returns false for unknown ID", func(t *testing.T) {
		removed := set.RemoveNodeByID(999)
		if removed {
			t.Errorf("RemoveNodeByID(999) = true, want false")
		}
	})

	t.Run("returns true and node is gone after removal", func(t *testing.T) {
		set.AddNode(makeTestNode(50, "node-50", "/test/node-50"))
		node := set.GetNodeByID(50)
		if node == nil {
			t.Fatalf("node not found after AddNode")
		}

		removed := set.RemoveNodeByID(50)
		if !removed {
			t.Errorf("RemoveNodeByID(50) = false, want true")
		}

		node = set.GetNodeByID(50)
		if node != nil {
			t.Errorf("GetNodeByID(50) = %v after removal, want nil", node)
		}
	})
}

func TestRemoveNodeByName(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	t.Run("returns false for unknown name", func(t *testing.T) {
		removed := set.RemoveNodeByName("unknown")
		if removed {
			t.Errorf("RemoveNodeByName(unknown) = true, want false")
		}
	})

	t.Run("returns true and node is gone after removal", func(t *testing.T) {
		set.AddNode(makeTestNode(60, "removable", "/test/removable"))
		node := set.GetNodeByName("removable")
		if node == nil {
			t.Fatalf("node not found after AddNode")
		}

		removed := set.RemoveNodeByName("removable")
		if !removed {
			t.Errorf("RemoveNodeByName(removable) = false, want true")
		}

		node = set.GetNodeByName("removable")
		if node != nil {
			t.Errorf("GetNodeByName(removable) = %v after removal, want nil", node)
		}
	})
}

func TestConcurrentAccess(t *testing.T) {
	set, err := discovery.NewEtcdDiscoverySet("/test/", nil)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	for i := uint64(1); i <= 10; i++ {
		set.AddNode(makeTestNode(i, fmt.Sprintf("node-%d", i), fmt.Sprintf("/test/node-%d", i)))
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			set.Empty()
			set.GetNodeByID(uint64(i%10) + 1)
			set.GetNodeByName(fmt.Sprintf("node-%d", i%10+1))
			set.GetSortedNodes(nil)
		}(i)
	}
	wg.Wait()
}

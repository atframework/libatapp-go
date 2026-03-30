package discovery

import (
	"encoding/binary"
	"testing"

	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
	"github.com/spaolacci/murmur3"

	log "log/slog"
)

func TestRoutingOnStaticNodes(t *testing.T) {
	// Arrange
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/static/", logger)

	node1 := &DiscoveryNode{Path: "/static/node1", Info: &pb.AtappDiscovery{Name: "node1"}}
	node2 := &DiscoveryNode{Path: "/static/node2", Info: &pb.AtappDiscovery{Name: "node2"}}
	node3 := &DiscoveryNode{Path: "/static/node3", Info: &pb.AtappDiscovery{Name: "node3"}}
	discovery.AddNode(node1)
	discovery.AddNode(node2)
	discovery.AddNode(node3)

	// Act
	n, err := discovery.GetNodeByConsistentHash("my-key-1", nil)

	// Assert
	if err != nil {
		t.Fatalf("GetNodeByConsistentHash failed: %v", err)
	}
	if n.Info.Name != "node1" && n.Info.Name != "node2" && n.Info.Name != "node3" {
		t.Errorf("Expected a valid node, got %s", n.Info.Name)
	}

	// Act
	n2, _ := discovery.GetNodeByConsistentHash("my-key-1", nil)

	// Assert
	if n.Info.Name != n2.Info.Name {
		t.Errorf("Consistent hash was not consistent: got %s then %s", n.Info.Name, n2.Info.Name)
	}

	// Act
	// C++ 语义：先取当前索引再递增，因此首个节点是 index=0。
	expectedOrder := []string{"node1", "node2", "node3"}
	for i := 0; i < len(expectedOrder)*2; i++ {
		n, err := discovery.GetNodeByRoundRobin(nil)
		if err != nil {
			t.Fatalf("GetNodeByRoundRobin failed: %v", err)
		}
		expectedName := expectedOrder[i%len(expectedOrder)]
		// Assert
		if n.Info.Name != expectedName {
			t.Errorf("Round Robin out of order: expected %s, got %s", expectedName, n.Info.Name)
		}
	}
}

func TestMetadataFiltering(t *testing.T) {
	// Arrange
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/static/", logger)

	node1 := &DiscoveryNode{Path: "/static/node1", Info: &pb.AtappDiscovery{Name: "node1-v1", Metadata: &pb.AtappMetadata{Labels: map[string]string{"version": "v1", "env": "prod"}}}}
	node2 := &DiscoveryNode{Path: "/static/node2", Info: &pb.AtappDiscovery{Name: "node2-v2", Metadata: &pb.AtappMetadata{Labels: map[string]string{"version": "v2", "env": "prod"}}}}
	node3 := &DiscoveryNode{Path: "/static/node3", Info: &pb.AtappDiscovery{Name: "node3-v2", Metadata: &pb.AtappMetadata{Labels: map[string]string{"version": "v2", "env": "prod"}}}}
	discovery.AddNode(node1)
	discovery.AddNode(node2)
	discovery.AddNode(node3)

	// Act
	filterV2 := map[string]string{"version": "v2"}
	v2Node, err := discovery.GetNodeByRandom(filterV2)

	// Assert
	if err != nil {
		t.Fatalf("GetNodeByRandom with filter 'v2' failed: %v", err)
	}
	if v2Node.Info.Metadata.Labels["version"] != "v2" {
		t.Errorf("Expected a 'v2' node, but got version '%s'", v2Node.Info.Metadata.Labels["version"])
	}
	cacheV2 := discovery.getOrCreateCache(filterV2)
	if len(cacheV2.sorted) != 2 {
		t.Errorf("Expected cache for 'v2' to have 2 nodes, but got %d", len(cacheV2.sorted))
	}

	// Act
	filterV3 := map[string]string{"version": "v3"}
	_, err = discovery.GetNodeByRandom(filterV3)

	// Assert
	if err == nil {
		t.Error("Expected an error when filtering for 'v3', but got nil")
	}

	// Act
	filterMulti := map[string]string{"version": "v1", "env": "prod"}
	multiNode, err := discovery.GetNodeByRoundRobin(filterMulti)

	// Assert
	if err != nil {
		t.Fatalf("GetNodeByRoundRobin with multi-label filter failed: %v", err)
	}
	if multiNode.Info.Name != "node1-v1" {
		t.Errorf("Expected 'node1-v1' for multi-label filter, but got '%s'", multiNode.Info.Name)
	}
	cacheMulti := discovery.getOrCreateCache(filterMulti)
	if len(cacheMulti.sorted) != 1 {
		t.Errorf("Expected cache for multi-label to have 1 node, but got %d", len(cacheMulti.sorted))
	}
}

func TestParseUint64_Overflow(t *testing.T) {
	if _, err := parseUint64("18446744073709551615"); err != nil {
		t.Fatalf("expected max uint64 to parse, got err: %v", err)
	}

	if _, err := parseUint64("18446744073709551616"); err == nil {
		t.Fatal("expected overflow error for value > max uint64")
	}
}

func TestShouldReplaceNode_UsesEqualSemantics(t *testing.T) {
	set, _ := NewEtcdDiscoverySet("/svc/", log.Default())

	prev := &DiscoveryNode{
		Path:        "/svc/node-a",
		Info:        &pb.AtappDiscovery{Id: 1001, Name: "svc-a", Version: "v1"},
		DataVersion: etcdversion.DataVersion{CreateRevision: 1, ModRevision: 1, Version: 1},
	}
	nextSameIdentitySamePayload := &DiscoveryNode{
		Path:        "/svc/node-a",
		Info:        &pb.AtappDiscovery{Id: 1001, Name: "svc-a", Version: "v1"},
		DataVersion: etcdversion.DataVersion{CreateRevision: 2, ModRevision: 2, Version: 2},
	}
	nextSameIdentity := &DiscoveryNode{
		Path:        "/svc/node-b",
		Info:        &pb.AtappDiscovery{Id: 1001, Name: "svc-b", Version: "v2"},
		DataVersion: etcdversion.DataVersion{CreateRevision: 2, ModRevision: 2, Version: 2},
	}
	nextDifferentIdentity := &DiscoveryNode{
		Path:        "/svc/node-c",
		Info:        &pb.AtappDiscovery{Id: 1002, Name: "svc-a", Version: "v2"},
		DataVersion: etcdversion.DataVersion{CreateRevision: 2, ModRevision: 2, Version: 2},
	}

	if set.shouldReplaceNode(prev, nextSameIdentitySamePayload) {
		t.Fatalf("expected same identity + same payload to be treated as no-op update")
	}
	if !set.shouldReplaceNode(prev, nextSameIdentity) {
		t.Fatalf("expected same identity + changed payload to be replaceable")
	}
	if !set.shouldReplaceNode(prev, nextDifferentIdentity) {
		t.Fatalf("expected different identity node to be replaceable")
	}
}

func TestGetOrCreateCache_UsesFilterSnapshot(t *testing.T) {
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/snapshot/", logger)

	v1Node := &DiscoveryNode{Path: "/snapshot/v1", Info: &pb.AtappDiscovery{Name: "v1", Metadata: &pb.AtappMetadata{Labels: map[string]string{"version": "v1"}}}}
	v2Node := &DiscoveryNode{Path: "/snapshot/v2", Info: &pb.AtappDiscovery{Name: "v2", Metadata: &pb.AtappMetadata{Labels: map[string]string{"version": "v2"}}}}
	discovery.AddNode(v1Node)
	discovery.AddNode(v2Node)

	filter := map[string]string{"version": "v1"}
	entryV1 := discovery.getOrCreateCache(filter)
	if len(entryV1.sorted) != 1 || entryV1.sorted[0].Info.GetName() != "v1" {
		t.Fatalf("expected v1 cache entry")
	}

	filter["version"] = "v2"
	entryV2 := discovery.getOrCreateCache(filter)
	if len(entryV2.sorted) != 1 || entryV2.sorted[0].Info.GetName() != "v2" {
		t.Fatalf("expected v2 cache entry")
	}

	entryV1Again := discovery.getOrCreateCache(map[string]string{"version": "v1"})
	if entryV1Again != entryV1 {
		t.Fatalf("expected original v1 cache entry to remain stable")
	}
}

func TestDiscoveryEventPublisherTriggersRoutingUpdate(t *testing.T) {
	// Arrange
	logger := log.Default()
	set, err := NewEtcdDiscoverySet("/svc/", logger)
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	nodeInfo := &pb.AtappDiscovery{Name: "svc", Id: 1}
	node := &DiscoveryNode{Info: nodeInfo, Path: "/svc/by_name/svc-1", DataVersion: etcdversion.DataVersion{ModRevision: 1, CreateRevision: 1, Version: 1}}
	set.AddNode(node)

	// Act
	selected, err := set.GetNodeByRandom(nil)

	// Assert
	if err != nil {
		t.Fatalf("GetNodeByRandom failed: %v", err)
	}
	if selected.Path != node.Path {
		t.Fatalf("expected node %s, got %s", node.Path, selected.Path)
	}
}

func TestAdvancedConsistentHashModes(t *testing.T) {
	// Arrange
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/static/", logger)

	node1 := &DiscoveryNode{Path: "/static/node1", Info: &pb.AtappDiscovery{Name: "node1"}}
	node2 := &DiscoveryNode{Path: "/static/node2", Info: &pb.AtappDiscovery{Name: "node2"}}
	node3 := &DiscoveryNode{Path: "/static/node3", Info: &pb.AtappDiscovery{Name: "node3"}}
	discovery.AddNode(node1)
	discovery.AddNode(node2)
	discovery.AddNode(node3)

	t.Run("pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL", func(t *testing.T) {
		// Act
		nodes, err := discovery.GetNodesByConsistentHash("test-key", 10, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)

		// Assert
		if err != nil {
			t.Fatalf("GetNodesByConsistentHash failed: %v", err)
		}
		if len(nodes) == 0 || len(nodes) > 3 {
			t.Errorf("Expected node count in [1,3], got %d", len(nodes))
		}
	})

	t.Run("pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE", func(t *testing.T) {
		// Act
		nodes, err := discovery.GetNodesByConsistentHash("test-key", 10, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE)

		// Assert
		if err != nil {
			t.Fatalf("GetNodesByConsistentHash failed: %v", err)
		}
		if len(nodes) != 3 {
			t.Errorf("Expected 3 unique nodes, got %d", len(nodes))
		}
	})

	t.Run("pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE", func(t *testing.T) {
		// Act
		node, err := discovery.GetNodeByConsistentHash("test-key", nil)
		if err != nil {
			t.Fatalf("GetNodeByConsistentHash failed: %v", err)
		}
		nextNode, err := discovery.GetNodeByConsistentHashMode("test-key", nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE)

		// Assert
		if err != nil {
			t.Fatalf("GetNodeByConsistentHashMode failed: %v", err)
		}
		if nextNode == nil {
			t.Fatalf("pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE returned nil node")
		}
		t.Logf("pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL returned: %s, pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE returned: %s", node.Path, nextNode.Path)
	})

	t.Run("pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE", func(t *testing.T) {
		// Act
		nodes, err := discovery.GetNodesByConsistentHash("test-key", 10, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE)

		// Assert
		if err != nil {
			t.Fatalf("GetNodesByConsistentHash failed: %v", err)
		}
		if len(nodes) != 3 {
			t.Errorf("Expected 3 unique nodes, got %d", len(nodes))
		}
		seen := make(map[string]bool)
		for _, node := range nodes {
			if seen[node.Path] {
				t.Errorf("Found duplicate node: %s", node.Path)
			}
			seen[node.Path] = true
		}
	})

	t.Run("pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT", func(t *testing.T) {
		// Act
		nodes, err := discovery.GetNodesByConsistentHash("test-key", 10, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT)

		// Assert
		if err != nil {
			t.Fatalf("GetNodesByConsistentHash failed: %v", err)
		}
		if len(nodes) == 0 {
			t.Errorf("Expected at least some nodes, got %d", len(nodes))
		}
	})
}

func TestHandleBatchSkipsNilEvents(t *testing.T) {
	logger := log.Default()
	prefix := "/test/"
	set, _ := NewEtcdDiscoverySet(prefix, logger)

	events := []*watcher.EtcdWatchEvent{
		nil,
		&watcher.EtcdWatchEvent{
			Type:  pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
			Key:   prefix + "node1",
			Value: &pb.AtappDiscovery{Name: "node1"},
		},
	}

	set.HandleBatch(events)

	nodes := set.GetAllNodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node after batch with nil event, got %d", len(nodes))
	}
}

func TestLowerBoundNodeHashByConsistentHash_NextUniqueExcludeSelf(t *testing.T) {
	logger := log.Default()
	ds, _ := NewEtcdDiscoverySet("/svc/", logger)

	nodeA := &DiscoveryNode{Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 1001, Name: "svc-a"}}
	nodeB := &DiscoveryNode{Path: "/svc/b", Info: &pb.AtappDiscovery{Id: 1002, Name: "svc-b"}}
	ds.AddNode(nodeA)
	ds.AddNode(nodeB)

	cache := ds.getOrCreateCache(nil)
	ds.rebuildCache(cache)
	if len(cache.normalRing) == 0 {
		t.Fatalf("expected non-empty hashing ring")
	}

	key := cache.normalRing[0]
	out := make([]nodeHashType, 2)
	count := ds.lowerBoundNodeHashByConsistentHash(out, nodeHashType{node: key.node, hashLo: key.hashLo, hashHi: key.hashHi}, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE)
	if count <= 0 {
		t.Fatalf("expected lower bound to return at least one node")
	}
	if out[0].node == nil {
		t.Fatalf("expected first output node to be non-nil")
	}
	if out[0].node.Equal(key.node) {
		t.Fatalf("expected NEXT_UNIQUE_NODE to exclude self-equivalent node when key.node is provided")
	}
}

func TestGetNodeByConsistentHashMode_NextWithoutSelfContext(t *testing.T) {
	logger := log.Default()
	ds, _ := NewEtcdDiscoverySet("/svc/", logger)

	ds.AddNode(&DiscoveryNode{Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 1001, Name: "svc-a"}})
	ds.AddNode(&DiscoveryNode{Path: "/svc/b", Info: &pb.AtappDiscovery{Id: 1002, Name: "svc-b"}})
	ds.AddNode(&DiscoveryNode{Path: "/svc/c", Info: &pb.AtappDiscovery{Id: 1003, Name: "svc-c"}})

	key := "stable-key"
	allNode, err := ds.GetNodeByConsistentHashMode(key, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	if err != nil {
		t.Fatalf("ALL mode failed: %v", err)
	}
	nextNode, err := ds.GetNodeByConsistentHashMode(key, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE)
	if err != nil {
		t.Fatalf("NEXT_NODE mode failed: %v", err)
	}
	if allNode == nil || nextNode == nil {
		t.Fatalf("expected non-nil nodes")
	}
	if !allNode.Equal(nextNode) {
		t.Fatalf("without self context, NEXT_NODE should align with C++ lower_bound behavior and return same identity as ALL")
	}
}

func TestGetSortedNodes_PrioritizesStatefulPodIndex(t *testing.T) {
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/pod/", logger)

	nodeA := &DiscoveryNode{Path: "/pod/a", Info: &pb.AtappDiscovery{Id: 3, Name: "a", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 2}}}
	nodeB := &DiscoveryNode{Path: "/pod/b", Info: &pb.AtappDiscovery{Id: 1, Name: "b", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 0}}}
	nodeC := &DiscoveryNode{Path: "/pod/c", Info: &pb.AtappDiscovery{Id: 2, Name: "c", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 1}}}

	discovery.AddNode(nodeA)
	discovery.AddNode(nodeB)
	discovery.AddNode(nodeC)

	sorted := discovery.GetSortedNodes(nil)
	if len(sorted) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(sorted))
	}

	if sorted[0].Path != nodeB.Path || sorted[1].Path != nodeC.Path || sorted[2].Path != nodeA.Path {
		t.Fatalf("unexpected pod-index order: [%s, %s, %s]", sorted[0].Path, sorted[1].Path, sorted[2].Path)
	}
}

func TestRoundRobin_PrioritizesStatefulPodIndexOrder(t *testing.T) {
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/pod-rr/", logger)

	nodeHigh := &DiscoveryNode{Path: "/pod-rr/high", Info: &pb.AtappDiscovery{Id: 10, Name: "high", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 10}}}
	nodeLow := &DiscoveryNode{Path: "/pod-rr/low", Info: &pb.AtappDiscovery{Id: 20, Name: "low", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 1}}}
	nodeMid := &DiscoveryNode{Path: "/pod-rr/mid", Info: &pb.AtappDiscovery{Id: 30, Name: "mid", Runtime: &pb.AtappDiscoveryRuntime{StatefulPodIndex: 5}}}

	discovery.AddNode(nodeHigh)
	discovery.AddNode(nodeLow)
	discovery.AddNode(nodeMid)

	first, err := discovery.GetNodeByRoundRobin(nil)
	if err != nil {
		t.Fatalf("GetNodeByRoundRobin failed: %v", err)
	}
	second, err := discovery.GetNodeByRoundRobin(nil)
	if err != nil {
		t.Fatalf("GetNodeByRoundRobin failed: %v", err)
	}
	third, err := discovery.GetNodeByRoundRobin(nil)
	if err != nil {
		t.Fatalf("GetNodeByRoundRobin failed: %v", err)
	}

	if first.Path != nodeLow.Path || second.Path != nodeMid.Path || third.Path != nodeHigh.Path {
		t.Fatalf("unexpected round-robin order with pod index: [%s, %s, %s]", first.Path, second.Path, third.Path)
	}
}

func TestNodeFromWatchEvent_UsesVersionTuple(t *testing.T) {
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/tuple/", logger)

	discovery.HandleWatcherEvent(watcher.EtcdWatchEvent{
		Type:        pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:         "/tuple/svc-1",
		Value:       &pb.AtappDiscovery{Name: "svc", Id: 1},
		Revision:    100,
		DataVersion: etcdversion.DataVersion{CreateRevision: 11, ModRevision: 17, Version: 23},
	})

	nodes := discovery.GetAllNodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}

	n := nodes[0]
	if n.CreateRevision != 11 || n.ModRevision != 17 || n.Version != 23 {
		t.Fatalf("unexpected version tuple (%d,%d,%d)", n.CreateRevision, n.ModRevision, n.Version)
	}
}

func TestConsistentHashKey_UsesMurmur3(t *testing.T) {
	data := []byte("alignment-check")
	seed := consistentHashMagicSeed

	gotLo, gotHi := consistentHashKey(data, seed)
	expectLo, expectHi := murmur3.Sum128WithSeed(data, seed)

	if gotLo != expectLo || gotHi != expectHi {
		t.Fatalf("consistentHashKey mismatch: got (%d,%d), expect (%d,%d)", gotLo, gotHi, expectLo, expectHi)
	}
}

func TestConsistentHashUint64_UsesRawBytes(t *testing.T) {
	logger := log.Default()
	ds, _ := NewEtcdDiscoverySet("/svc/", logger)

	ds.AddNode(&DiscoveryNode{Path: "/svc/a", Info: &pb.AtappDiscovery{Id: 1001, Name: "svc-a"}})
	ds.AddNode(&DiscoveryNode{Path: "/svc/b", Info: &pb.AtappDiscovery{Id: 1002, Name: "svc-b"}})
	ds.AddNode(&DiscoveryNode{Path: "/svc/c", Info: &pb.AtappDiscovery{Id: 1003, Name: "svc-c"}})

	var keyNum uint64 = 9876543210
	fromUint, err := ds.GetNodeByConsistentHashUint64(keyNum, nil)
	if err != nil {
		t.Fatalf("GetNodeByConsistentHashUint64 failed: %v", err)
	}

	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], keyNum)
	fromBytes, err := ds.GetNodeByConsistentHashBytes(raw[:], nil)
	if err != nil {
		t.Fatalf("GetNodeByConsistentHashBytes failed: %v", err)
	}

	if fromUint == nil || fromBytes == nil {
		t.Fatalf("expected non-nil nodes")
	}
	if !fromUint.Equal(fromBytes) {
		t.Fatalf("uint64 hash path must be raw-bytes equivalent")
	}
}

func TestNodeIndexingByIDAndName(t *testing.T) {
	// Arrange
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/static/", logger)

	node := &DiscoveryNode{
		Path:        "/static/node1",
		Info:        &pb.AtappDiscovery{Name: "node1", Id: 100},
		DataVersion: etcdversion.DataVersion{CreateRevision: 1, ModRevision: 1, Version: 1},
	}
	discovery.AddNode(node)

	// Act
	cache := discovery.getOrCreateCache(nil)

	// Assert
	if len(cache.sorted) != 1 {
		t.Fatalf("Expected 1 node in cache, got %d", len(cache.sorted))
	}

	memberByID := discovery.memberKey(&pb.AtappDiscovery{Id: 100})
	memberByName := discovery.memberKey(&pb.AtappDiscovery{Name: "node1"})

	// Act
	nodeByID := discovery.lookupMember(memberByID)
	nodeByName := discovery.lookupMember(memberByName)

	// Assert
	if nodeByID != node {
		t.Fatalf("Expected lookup by id to return node")
	}
	if nodeByName != node {
		t.Fatalf("Expected lookup by name to return node")
	}
}

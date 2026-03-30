package discovery

import (
	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
	"testing"
	"time"

	log "log/slog"
)

// 该测试函数用于验证相关行为。
func TestCacheInvalidationOnNodeChange(t *testing.T) {
	// Arrange
	logger := log.Default()
	prefix := "/test/"
	discovery, _ := NewEtcdDiscoverySet(prefix, logger)

	// Arrange: add prod nodes
	discovery.AddNode(&DiscoveryNode{
		Path: prefix + "node1",
		Info: &pb.AtappDiscovery{
			Name:     "node1",
			Metadata: &pb.AtappMetadata{Labels: map[string]string{"env": "prod"}},
		},
	})

	// Act
	filter := map[string]string{"env": "prod"}
	node, _ := discovery.GetNodeByRandom(filter)

	// Assert
	if node == nil {
		t.Error("Expected 1 prod node initially")
	}

	// Act
	discovery.AddNode(&DiscoveryNode{
		Path: prefix + "node2",
		Info: &pb.AtappDiscovery{
			Name:     "node2",
			Metadata: &pb.AtappMetadata{Labels: map[string]string{"env": "dev"}},
		},
	})

	// Act
	node, _ = discovery.GetNodeByRandom(filter)

	// Assert
	if node == nil {
		t.Error("Expected to still find prod node after cache invalidation")
	}

	// Act
	discovery.RemoveNode(prefix + "node1")

	// Act
	_, err := discovery.GetNodeByRandom(filter)

	// Assert
	if err == nil {
		t.Error("Expected error when no prod nodes available after removal")
	}
}

// 该测试函数用于验证相关行为。
func TestConcurrentNodeAdditionAndRemoval(t *testing.T) {
	// Arrange
	logger := log.Default()
	prefix := "/test/"
	discovery, _ := NewEtcdDiscoverySet(prefix, logger)

	done := make(chan bool, 1)

	// Act
	go func() {
		for i := 0; i < 10; i++ {
			discovery.AddNode(&DiscoveryNode{
				Path: prefix + "node" + string(rune('0'+i)),
				Info: &pb.AtappDiscovery{Name: "node" + string(rune('0'+i))},
			})
			time.Sleep(10 * time.Millisecond)
		}
		done <- true
	}()

	// Act
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				discovery.GetAllNodes()
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	// Act
	<-done

	// Assert
	nodes := discovery.GetAllNodes()
	if len(nodes) != 10 {
		t.Errorf("Expected 10 nodes, got %d", len(nodes))
	}

	// Act
	for i := 0; i < 5; i++ {
		discovery.RemoveNode(prefix + "node" + string(rune('0'+i)))
	}

	// Assert
	nodes = discovery.GetAllNodes()
	if len(nodes) != 5 {
		t.Errorf("Expected 5 nodes after removal, got %d", len(nodes))
	}
}

// 该测试函数用于验证相关行为。
func TestHandleBatchEvent(t *testing.T) {
	// Arrange
	logger := log.Default()
	prefix := "/test/"
	discovery, _ := NewEtcdDiscoverySet(prefix, logger)

	// Arrange: create batch of events
	events := make([]*watcher.EtcdWatchEvent, 5)
	for i := 0; i < 5; i++ {
		nodeInfo := &pb.AtappDiscovery{Name: "node" + string(rune('0'+i))}
		events[i] = &watcher.EtcdWatchEvent{
			Type:  pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
			Key:   prefix + "node" + string(rune('0'+i)),
			Value: nodeInfo,
		}
	}

	// Act
	discovery.HandleBatch(events)

	// Assert
	nodes := discovery.GetAllNodes()
	if len(nodes) != 5 {
		t.Errorf("Expected 5 nodes after batch, got %d", len(nodes))
	}
}

// 该测试函数用于验证相关行为。
func TestHandleWatcherEvent(t *testing.T) {
	// Arrange
	logger := log.Default()
	prefix := "/test/"
	discovery, _ := NewEtcdDiscoverySet(prefix, logger)

	// Arrange: create PUT event
	nodeInfo := &pb.AtappDiscovery{Name: "node1"}
	putEvent := watcher.EtcdWatchEvent{
		Type:  pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:   prefix + "node1",
		Value: nodeInfo,
	}

	// Act
	discovery.HandleWatcherEvent(putEvent)

	// Assert
	nodes := discovery.GetAllNodes()
	if len(nodes) != 1 {
		t.Errorf("Expected 1 node after PUT, got %d", len(nodes))
	}

	// Arrange: create DELETE event
	deleteEvent := watcher.EtcdWatchEvent{
		Type: pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE,
		Key:  prefix + "node1",
	}

	// Act
	discovery.HandleWatcherEvent(deleteEvent)

	// Assert
	nodes = discovery.GetAllNodes()
	if len(nodes) != 0 {
		t.Errorf("Expected 0 nodes after DELETE, got %d", len(nodes))
	}
}

func TestHandleWatcherDeleteByName(t *testing.T) {
	// Arrange
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/svc/", logger)

	node := &DiscoveryNode{
		Path: "/svc/by_name/svc-1",
		Info: &pb.AtappDiscovery{Name: "svc", Id: 1},
	}
	discovery.AddNode(node)

	// Act
	deleteEvent := watcher.EtcdWatchEvent{
		Type:  pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE,
		Key:   node.Path,
		Value: &pb.AtappDiscovery{Name: "svc"},
	}
	discovery.HandleWatcherEvent(deleteEvent)

	// Assert
	if len(discovery.GetAllNodes()) != 0 {
		t.Fatalf("expected node removed by delete event")
	}
}

func TestHandleWatcherEventIgnoresOlderRevision(t *testing.T) {
	// Arrange
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/svc/", logger)

	path := "/svc/by_id/1"
	first := watcher.EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:      path,
		Revision: 10,
		Value:    &pb.AtappDiscovery{Name: "svc", Id: 1, Identity: "v1"},
	}
	older := watcher.EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:      path,
		Revision: 5,
		Value:    &pb.AtappDiscovery{Name: "svc", Id: 1, Identity: "v0"},
	}

	// Act
	discovery.HandleWatcherEvent(first)
	discovery.HandleWatcherEvent(older)

	// Assert
	nodes := discovery.GetAllNodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node after updates, got %d", len(nodes))
	}
	if nodes[0].Info == nil || nodes[0].Info.Identity != "v1" {
		t.Fatalf("expected newer revision to win, got %+v", nodes[0].Info)
	}
}

func TestApplySnapshotRemovesMissingNodes(t *testing.T) {
	// Arrange
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/svc/", logger)

	oldNode := &DiscoveryNode{Path: "/svc/by_name/old", Info: &pb.AtappDiscovery{Name: "old", Id: 1}}
	discovery.AddNode(oldNode)

	snapshot := []*watcher.EtcdWatchEvent{
		{
			Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
			Key:      "/svc/by_name/new",
			Revision: 10,
			Value:    &pb.AtappDiscovery{Name: "new", Id: 2},
		},
	}

	// Act
	discovery.ApplySnapshot(snapshot)

	// Assert
	if len(discovery.GetAllNodes()) != 1 {
		t.Fatalf("expected snapshot to replace nodes")
	}
}

func TestHandleBatchInvalidatesCache(t *testing.T) {
	// Arrange
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/svc/", logger)

	node := &DiscoveryNode{Path: "/svc/by_name/svc-1", Info: &pb.AtappDiscovery{Name: "svc", Id: 1}}
	discovery.AddNode(node)

	// Act
	if _, err := discovery.GetNodeByRandom(nil); err != nil {
		t.Fatalf("expected initial node selection")
	}

	events := []*watcher.EtcdWatchEvent{
		{
			Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE,
			Key:      node.Path,
			Revision: 2,
			Value:    &pb.AtappDiscovery{Name: "svc", Id: 1},
		},
	}

	// Act
	discovery.HandleBatch(events)

	// Assert
	if _, err := discovery.GetNodeByRandom(nil); err == nil {
		t.Fatalf("expected empty selection after batch delete")
	}
}

func TestApplySnapshotEmptyClearsNodes(t *testing.T) {
	// Arrange
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/svc/", logger)

	// Arrange: add node
	discovery.AddNode(&DiscoveryNode{Path: "/svc/by_name/svc-1", Info: &pb.AtappDiscovery{Name: "svc"}})

	// Assert
	if len(discovery.GetAllNodes()) != 1 {
		t.Fatalf("expected initial node")
	}

	// Act
	discovery.ApplySnapshot(nil)

	// Assert
	post := discovery.GetAllNodes()
	if len(post) != 0 {
		t.Fatalf("expected empty snapshot to clear nodes")
	}
}

// 该测试函数用于验证相关行为。
func TestEmptyFilterResults(t *testing.T) {
	// Arrange
	logger := log.Default()
	prefix := "/test/"
	discovery, _ := NewEtcdDiscoverySet(prefix, logger)

	// Arrange: add dev nodes
	discovery.AddNode(&DiscoveryNode{
		Path: prefix + "node1",
		Info: &pb.AtappDiscovery{
			Name:     "node1",
			Metadata: &pb.AtappMetadata{Labels: map[string]string{"env": "dev"}},
		},
	})

	// Act
	filter := map[string]string{"env": "prod"}
	_, err := discovery.GetNodeByRandom(filter)

	// Assert
	if err == nil {
		t.Error("Expected error when no nodes match filter")
	}
}

func TestRejectsStaleNodeUpdate(t *testing.T) {
	// Arrange
	logger := log.Default()
	prefix := "/test/"
	discovery, _ := NewEtcdDiscoverySet(prefix, logger)

	node := &DiscoveryNode{
		Path: prefix + "node1",
		Info: &pb.AtappDiscovery{Name: "node1", Id: 1},
		DataVersion: etcdversion.DataVersion{CreateRevision: 2, ModRevision: 2, Version: 2},
	}

	discovery.AddNode(node)

	// Assert
	if len(discovery.GetAllNodes()) != 1 {
		t.Fatalf("Expected 1 node after add")
	}

	stale := &DiscoveryNode{
		Path: prefix + "node1",
		Info: &pb.AtappDiscovery{Name: "node1", Id: 1, Identity: "stale"},
		DataVersion: etcdversion.DataVersion{CreateRevision: 1, ModRevision: 1, Version: 1},
	}

	discovery.AddNode(stale)

	// Assert
	nodes := discovery.GetAllNodes()
	if len(nodes) != 1 {
		t.Fatalf("Expected 1 node after stale update, got %d", len(nodes))
	}
	if nodes[0].Info.Identity == "stale" {
		t.Fatalf("Expected stale update to be ignored")
	}
}

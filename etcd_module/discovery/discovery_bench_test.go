package discovery

import (
	"testing"

	log "log/slog"

	"github.com/atframework/libatapp-go/etcd_module/internal/consistenthash"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func benchLogger() *log.Logger {
	return log.Default()
}

// 该基准函数用于评估性能表现。
func BenchmarkConsistentHashGetNode(b *testing.B) {
	d, _ := NewEtcdDiscoverySet("/benchmark/", benchLogger())

	// Add 100 nodes
	for i := 0; i < 100; i++ {
		d.AddNode(&DiscoveryNode{
			Path: "/benchmark/node-" + string(rune(i)),
			Info: &pb.AtappDiscovery{Name: "node-" + string(rune(i))},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.GetNodeByConsistentHash("user-key-12345", nil)
	}
}

// 该基准函数用于评估性能表现。
func BenchmarkConsistentHashGetNodes(b *testing.B) {
	d, _ := NewEtcdDiscoverySet("/benchmark/", benchLogger())

	// Add 100 nodes
	for i := 0; i < 100; i++ {
		d.AddNode(&DiscoveryNode{
			Path: "/benchmark/node-" + string(rune(i)),
			Info: &pb.AtappDiscovery{Name: "node-" + string(rune(i))},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.GetNodesByConsistentHash("user-key-12345", 10, nil, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	}
}

// 该基准函数用于评估性能表现。
func BenchmarkRoundRobin(b *testing.B) {
	d, _ := NewEtcdDiscoverySet("/benchmark/", benchLogger())

	// Add 50 nodes
	for i := 0; i < 50; i++ {
		d.AddNode(&DiscoveryNode{
			Path: "/benchmark/node-" + string(rune(i)),
			Info: &pb.AtappDiscovery{Name: "node-" + string(rune(i))},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.GetNodeByRoundRobin(nil)
	}
}

// 该基准函数用于评估性能表现。
func BenchmarkRandom(b *testing.B) {
	d, _ := NewEtcdDiscoverySet("/benchmark/", benchLogger())

	// Add 50 nodes
	for i := 0; i < 50; i++ {
		d.AddNode(&DiscoveryNode{
			Path: "/benchmark/node-" + string(rune(i)),
			Info: &pb.AtappDiscovery{Name: "node-" + string(rune(i))},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.GetNodeByRandom(nil)
	}
}

// 该基准函数用于评估性能表现。
func BenchmarkMetadataFiltering(b *testing.B) {
	d, _ := NewEtcdDiscoverySet("/benchmark/", benchLogger())

	// Add 100 nodes with different metadata
	for i := 0; i < 100; i++ {
		region := "us-east"
		if i%3 == 0 {
			region = "us-west"
		} else if i%3 == 1 {
			region = "eu-west"
		}
		d.AddNode(&DiscoveryNode{
			Path: "/benchmark/node-" + string(rune(i)),
			Info: &pb.AtappDiscovery{
				Name:     "node-" + string(rune(i)),
				Metadata: &pb.AtappMetadata{Labels: map[string]string{"region": region}},
			},
		})
	}

	filter := map[string]string{"region": "us-east"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.GetNodeByRandom(filter)
	}
}

// 该基准函数用于评估性能表现。
func BenchmarkGetAllNodes(b *testing.B) {
	d, _ := NewEtcdDiscoverySet("/benchmark/", benchLogger())

	// Add 100 nodes
	for i := 0; i < 100; i++ {
		d.AddNode(&DiscoveryNode{
			Path: "/benchmark/node-" + string(rune(i)),
			Info: &pb.AtappDiscovery{Name: "node-" + string(rune(i))},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.GetAllNodes()
	}
}

// 该基准函数用于评估性能表现。
func BenchmarkCacheLookup(b *testing.B) {
	d, _ := NewEtcdDiscoverySet("/benchmark/", benchLogger())

	// Add 100 nodes
	for i := 0; i < 100; i++ {
		d.AddNode(&DiscoveryNode{
			Path: "/benchmark/node-" + string(rune(i)),
			Info: &pb.AtappDiscovery{
				Name:     "node-" + string(rune(i)),
				Metadata: &pb.AtappMetadata{Labels: map[string]string{"version": "v1", "env": "prod"}},
			},
		})
	}

	// Prime the cache
	d.GetNodeByRandom(map[string]string{"version": "v1"})
	d.GetNodeByRandom(map[string]string{"env": "prod"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.GetNodeByRandom(map[string]string{"version": "v1"})
	}
}

// 该基准函数用于评估性能表现。
func BenchmarkConsistentHashRing(b *testing.B) {
	ring := consistenthash.NewRing(80)

	// Add 100 members
	members := make([]string, 100)
	for i := 0; i < 100; i++ {
		members[i] = "/benchmark/node-" + string(rune(i))
	}
	ring.Set(members)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.GetN("user-key-12345", 10, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	}
}

// 该基准函数用于评估性能表现。
func BenchmarkCacheInvalidationWithBatch(b *testing.B) {
	d, _ := NewEtcdDiscoverySet("/benchmark/", benchLogger())

	// Add initial nodes
	for i := 0; i < 100; i++ {
		d.AddNode(&DiscoveryNode{
			Path: "/benchmark/node-" + string(rune(i)),
			Info: &pb.AtappDiscovery{Name: "node-" + string(rune(i))},
		})
	}

	// Prime cache with a few filters
	d.GetNodeByRandom(nil)
	d.GetNodeByRandom(map[string]string{"region": "us"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Batch invalidation through handleBatch (simulates incoming batch of events)
		events := make([]*watcher.EtcdWatchEvent, 10)
		for j := 0; j < 10; j++ {
			events[j] = &watcher.EtcdWatchEvent{
				Type: pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
				Key:  "/benchmark/node-new-" + string(rune(j)),
			}
		}
		d.HandleBatch(events)
	}
}

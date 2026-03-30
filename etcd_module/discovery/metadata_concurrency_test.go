package discovery

import (
	"fmt"
	"sync"
	"testing"

	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"

	log "log/slog"
)

func TestHighConcurrencyMetadataUpdatesAndFilterLookups(t *testing.T) {
	ds, err := NewEtcdDiscoverySet("/stress/", log.Default())
	if err != nil {
		t.Fatalf("NewEtcdDiscoverySet failed: %v", err)
	}

	for i := 0; i < 40; i++ {
		env := "prod"
		if i%2 == 0 {
			env = "dev"
		}
		ds.AddNode(&DiscoveryNode{
			Path: fmt.Sprintf("/stress/svc-%d", i),
			Info: &pb.AtappDiscovery{
				Id:       uint64(i + 1),
				Name:     fmt.Sprintf("svc-%d", i),
				Metadata: &pb.AtappMetadata{Labels: map[string]string{"env": env, "version": "v1"}},
			},
			DataVersion: etcdversion.DataVersion{CreateRevision: int64(i + 1), ModRevision: int64(i + 1), Version: int64(i + 1)},
		})
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		id := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 120; i++ {
				n := (i + id) % 40
				env := "prod"
				if (i+id)%2 == 0 {
					env = "dev"
				}
				ds.HandleWatcherEvent(watcher.EtcdWatchEvent{
					Type: pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
					Key:  fmt.Sprintf("/stress/svc-%d", n),
					Value: &pb.AtappDiscovery{
						Id:       uint64(n + 1),
						Name:     fmt.Sprintf("svc-%d", n),
						Metadata: &pb.AtappMetadata{Labels: map[string]string{"env": env, "version": fmt.Sprintf("v%d", (i%3)+1)}},
					},
					Revision: int64(1000 + i + id),
				})
			}
		}()
	}

	for q := 0; q < 16; q++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 300; i++ {
				if _, err := ds.GetNodeByRandom(map[string]string{"env": "prod"}); err != nil && len(ds.GetAllNodes()) > 0 {
					t.Fatalf("unexpected lookup error: %v", err)
				}
				_, _ = ds.GetNodeByRoundRobin(map[string]string{"env": "dev"})
				_, _ = ds.GetNodeByConsistentHash("key", nil)
			}
		}()
	}

	close(start)
	wg.Wait()

	nodes := ds.GetAllNodes()
	if len(nodes) != 40 {
		t.Fatalf("expected 40 nodes after stress updates, got %d", len(nodes))
	}
}

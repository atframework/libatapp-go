package watcher

import (
	"sync"
	"testing"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func TestConcurrentAddRemoveHandlers_DuringDispatch(t *testing.T) {
	w := NewEtcdWatcher(nil, DefaultWatchConfig(), nil)

	const workers = 32
	const loops = 100

	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < loops; j++ {
				w.AddHandler(func(event EtcdWatchEvent) {})
				w.AddOneShotHandler(func(event EtcdWatchEvent) {})
				w.dispatchEvent(EtcdWatchEvent{Type: pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT, Key: "/svc"})
			}
		}()
	}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < loops*4; j++ {
				w.dispatchEvent(EtcdWatchEvent{Type: pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT, Key: "/svc"})
			}
		}()
	}

	close(start)
	wg.Wait()

	w.dispatchEvent(EtcdWatchEvent{Type: pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE, Key: "/svc"})
}

func TestConcurrentBatchHandlerAndStopRace(t *testing.T) {
	w := NewEtcdWatcher(nil, DefaultWatchConfig(), nil)

	cfg := DefaultBatchConfig()
	cfg.Enabled = true
	cfg.MaxBatchSize = 8
	w.SetBatchMode(cfg)
	w.SetBatchHandler(func(events []*EtcdWatchEvent) {})

	start := make(chan struct{})
	var wg sync.WaitGroup
	workers := 20
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				if (i+j)%5 == 0 {
					w.Stop()
					continue
				}
				w.dispatchEvent(EtcdWatchEvent{Type: pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT, Key: "/svc"})
				w.AddHandler(func(event EtcdWatchEvent) {})
			}
		}(i)
	}

	close(start)
	wg.Wait()
	w.Stop()
}

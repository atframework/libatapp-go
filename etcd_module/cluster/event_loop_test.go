package cluster

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestClusterEventLoopPostAfterClose(t *testing.T) {
	loop := newClusterEventLoop(1)
	loop.close()

	if ok := loop.post(func() {}); ok {
		t.Fatalf("expected post to return false after close")
	}
}

func TestClusterEventLoopPostReturnsQuicklyWhenClosedWhileQueueFull(t *testing.T) {
	loop := newClusterEventLoop(1)

	if ok := loop.post(func() {}); !ok {
		t.Fatalf("expected first post to succeed")
	}

	resultCh := make(chan bool, 1)
	start := time.Now()
	go func() {
		resultCh <- loop.post(func() {})
	}()

	time.Sleep(20 * time.Millisecond)
	loop.close()

	select {
	case ok := <-resultCh:
		if ok {
			t.Fatalf("expected post to fail when loop is closed")
		}
		if elapsed := time.Since(start); elapsed > 80*time.Millisecond {
			t.Fatalf("post returned too slowly after close: %v", elapsed)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("post did not return in time")
	}
}

func TestClusterEventLoopPostReturnsAfterRunContextCanceledAndQueueFull(t *testing.T) {
	loop := newClusterEventLoop(1)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go loop.run(ctx, nil, &wg)

	cancel()
	wg.Wait()

	loop.ch <- func() {}

	resultCh := make(chan bool, 1)
	start := time.Now()
	go func() {
		resultCh <- loop.post(func() {})
	}()

	select {
	case ok := <-resultCh:
		if ok {
			t.Fatalf("expected post to fail after run context canceled and queue full")
		}
		if elapsed := time.Since(start); elapsed > 80*time.Millisecond {
			t.Fatalf("post returned too slowly after run context canceled: %v", elapsed)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatalf("post blocked after run context canceled with full queue")
	}
}

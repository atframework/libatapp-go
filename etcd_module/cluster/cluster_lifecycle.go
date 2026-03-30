package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/atframework/libatapp-go/etcd_module/events"
)

var ErrClusterAlreadyStopped = errors.New("cluster already stopped")

func (c *EtcdCluster) setReady(v bool) {
	c.lifecycle.mu.Lock()
	c.lifecycle.ready = v
	c.lifecycle.mu.Unlock()
}

func (c *EtcdCluster) Start(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("cluster is nil")
	}
	return c.startInternal(ctx)
}

func (c *EtcdCluster) startInternal(ctx context.Context) error {
	c.lifecycle.mu.Lock()
	if c.lifecycle.state != ClusterStateInitializing {
		c.lifecycle.mu.Unlock()
		return fmt.Errorf("cluster already started or stopped")
	}

	c.lifecycle.state = ClusterStateRunning
	c.lifecycle.closed = false
	em := c.events.manager

	runCtx, cancelFunc := context.WithCancel(context.Background())
	if ctx != nil {
		c.startParentContextBridge(ctx, runCtx, cancelFunc)
	}
	c.lifecycle.runCtx = runCtx
	c.lifecycle.cancelFunc = cancelFunc
	c.events.loop = newClusterEventLoop(1024)
	c.startEventLoop(runCtx)
	c.lifecycle.mu.Unlock()

	c.logger.Info("EtcdCluster started")
	if em != nil {
		em.Publish(events.NewClusterUpEvent())
	}
	c.setReady(false)
	return nil
}

func (c *EtcdCluster) startParentContextBridge(parent context.Context, runCtx context.Context, cancel context.CancelFunc) {
	c.lifecycle.wg.Add(1)
	go func() {
		defer c.lifecycle.wg.Done()
		select {
		case <-parent.Done():
			cancel()
		case <-runCtx.Done():
		}
	}()
}

func (c *EtcdCluster) startEventLoop(runCtx context.Context) {
	c.lifecycle.wg.Add(1)
	go c.events.loop.run(runCtx, c.logger, &c.lifecycle.wg)
}

func (c *EtcdCluster) Stop(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("cluster is nil")
	}
	return c.stopInternal(ctx)
}

func (c *EtcdCluster) stopInternal(ctx context.Context) error {
	c.lifecycle.mu.Lock()
	if c.lifecycle.state == ClusterStateStopped || c.lifecycle.closed {
		c.lifecycle.mu.Unlock()
		return ErrClusterAlreadyStopped
	}

	c.lifecycle.state = ClusterStateStopping
	cancelFunc := c.lifecycle.cancelFunc
	loop := c.events.loop
	c.events.loop = nil
	c.lifecycle.mu.Unlock()

	if loop != nil {
		loop.close()
	}

	if cancelFunc != nil {
		cancelFunc()
	}

	if err := c.keepalives.manager.UnregisterAll(ctx); err != nil {
		c.logger.Warn("Failed to unregister keepalives during stop", "error", err)
	}
	c.stopClusterLease(ctx, true)

	c.watchers.manager.StopAll()

	c.keepalives.retryMu.Lock()
	clear(c.keepalives.retryActors)
	clear(c.keepalives.deletors)
	c.keepalives.retryMu.Unlock()
	if c.stats != nil {
		c.stats.SetRetryQueueSize(0)
		c.stats.SetDeleteQueueSize(0)
	}

	c.lifecycle.wg.Wait()

	c.lifecycle.mu.Lock()
	c.lifecycle.state = ClusterStateStopped
	c.lifecycle.runCtx = nil
	c.lifecycle.closed = true
	em := c.events.manager
	c.lifecycle.mu.Unlock()

	c.logger.Info("EtcdCluster stopped")
	if em != nil {
		em.Publish(events.NewClusterDownEvent())
		em.Publish(events.NewClusterStateChangeEvent(events.ClusterStateDisconnected, 0))
	}
	c.setReady(false)
	return nil
}

func (c *EtcdCluster) GetState() ClusterState {
	c.lifecycle.mu.RLock()
	defer c.lifecycle.mu.RUnlock()
	return c.lifecycle.state
}

func (c *EtcdCluster) IsClosed() bool {
	c.lifecycle.mu.RLock()
	defer c.lifecycle.mu.RUnlock()
	return c.lifecycle.closed
}

func (c *EtcdCluster) Close() error {
	return c.Stop(context.Background())
}

// CloseEtcdClient 关闭EtcdClient。
func (c *EtcdCluster) CloseEtcdClient() error {
	c.lifecycle.mu.RLock()
	etcdClient := c.etcdClient
	c.lifecycle.mu.RUnlock()
	if etcdClient == nil {
		return nil
	}
	return etcdClient.Close()
}

// IsAvailable 判断是否满足Available条件。
func (c *EtcdCluster) IsAvailable() bool {
	c.lifecycle.mu.RLock()
	defer c.lifecycle.mu.RUnlock()
	return c.lifecycle.state == ClusterStateRunning && !c.lifecycle.closed
}

// ResolveReady 实现。
func (c *EtcdCluster) ResolveReady() {
	c.setReady(true)
}

// GetLease 获取Lease。
func (c *EtcdCluster) GetLease() int64 {
	c.lifecycle.mu.RLock()
	defer c.lifecycle.mu.RUnlock()
	return int64(c.keepalives.leaseID)
}

// Tick 实现。
func (c *EtcdCluster) Tick(ctx context.Context) error {
	if !c.IsAvailable() {
		return nil
	}
	if err := c.processRegistrationDeletionQueue(ctx); err != nil {
		return err
	}
	if err := c.processRegistrationRetryQueue(ctx); err != nil {
		return err
	}

	c.lifecycle.mu.RLock()
	if c.lifecycle.state != ClusterStateRunning || c.lifecycle.closed {
		c.lifecycle.mu.RUnlock()
		return nil
	}
	wm := c.watchers.manager
	runCtx := c.lifecycle.runCtx
	if wm != nil {
		watchCtx := runCtx
		if watchCtx == nil {
			watchCtx = ctx
		}
		if watchCtx == nil {
			watchCtx = context.Background()
		}
		err := wm.ActiveAll(watchCtx)
		c.lifecycle.mu.RUnlock()
		if err != nil {
			return err
		}
		return nil
	}
	c.lifecycle.mu.RUnlock()

	return nil
}

// Reset provides a C++-style reset entrypoint for strong-structure parity.
func (c *EtcdCluster) Reset() {
	_ = c.Stop(context.Background())

	c.config.mu.Lock()
	c.config.config = nil
	c.config.previousRequestTimeout = defaultPreviousRequestTimeout
	c.config.retryInterval = defaultRetryInterval
	c.config.keepAliveMaxRetryTimes = defaultKeepaliveMaxRetryTimes
	c.config.leaseTTL = defaultClusterLeaseTTL
	c.config.mu.Unlock()

	c.lifecycle.mu.Lock()
	c.lifecycle.state = ClusterStateInitializing
	c.lifecycle.runCtx = nil
	c.lifecycle.cancelFunc = nil
	c.lifecycle.closed = false
	c.keepalives.leaseCancel = nil
	c.keepalives.leaseID = 0
	c.lifecycle.mu.Unlock()

	c.setReady(false)

	c.keepalives.retryMu.Lock()
	clear(c.keepalives.retryActors)
	clear(c.keepalives.deletors)
	c.keepalives.retryMu.Unlock()

	c.watchers.snapshotIndexMu.Lock()
	clear(c.watchers.prefixes)
	c.watchers.snapshotIndex = 0
	c.watchers.masterWatcher = ""
	c.watchers.snapshotIndexMu.Unlock()

	if c.stats != nil {
		c.stats.Reset()
	}
}

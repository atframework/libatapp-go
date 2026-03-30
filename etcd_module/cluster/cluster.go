// Package cluster provides the core etcd-backed service discovery orchestration.
package cluster

import (
	"context"
	"fmt"
	log "log/slog"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"

	"github.com/atframework/libatapp-go/etcd_module/client"
	"github.com/atframework/libatapp-go/etcd_module/discovery"
	"github.com/atframework/libatapp-go/etcd_module/events"
	"github.com/atframework/libatapp-go/etcd_module/registration"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

const (
	defaultClusterLeaseTTL        int64 = 16
	defaultPreviousRequestTimeout       = 5 * time.Second
	defaultRetryInterval                = 3 * time.Second
	defaultKeepaliveTimeout             = 31 * time.Second // from proto default
	defaultKeepaliveMaxRetryTimes int32 = 8
)

type keepaliveRetryActor struct {
	info        *pb.AtappDiscovery
	path        string
	ttl         int64
	nextAttempt time.Time
	attempts    int32
}

type keepaliveDeleteActor struct {
	path        string
	nextAttempt time.Time
	attempts    int32
}

type discoveryState struct {
	set *discovery.EtcdDiscoverySet
	mu  sync.RWMutex
}

type watcherState struct {
	manager         *watcher.EtcdWatcherManager
	prefixes        map[string]struct{}
	snapshotIndexMu sync.Mutex
	snapshotIndex   int64
	masterWatcher   string
}

type keepaliveState struct {
	manager     *registration.RegistrationManager
	leaseID     clientv3.LeaseID   // 全局租约，用于所有参与者
	leaseCancel context.CancelFunc // cancel func for the cluster keepalive goroutine
	grantGroup  singleflight.Group // 去重并发的 Grant 调用
	retryMu     sync.Mutex
	retryActors map[string]*keepaliveRetryActor
	deletors    map[string]*keepaliveDeleteActor
}

type eventState struct {
	manager                  events.EventManager
	snapshotLoadingCallbacks *events.CallbackList
	snapshotLoadedCallbacks  *events.CallbackList
	nodeEventCallbacks       *events.CallbackList
	leaseEventBridgeHandle   events.EventCallbackHandle
	loop                     *clusterEventLoop
}

type configState struct {
	mu                     sync.RWMutex
	config                 *pb.AtappEtcd
	leaseTTL               int64
	previousRequestTimeout time.Duration
	retryInterval          time.Duration
	keepAliveTimeout       time.Duration
	keepAliveMaxRetryTimes int32
}

type lifecycleState struct {
	state      ClusterState
	mu         sync.RWMutex
	wg         sync.WaitGroup
	runCtx     context.Context
	cancelFunc context.CancelFunc
	closed     bool
	ready      bool
}

type EtcdCluster struct {
	etcdClient client.EtcdClient
	logger     *log.Logger

	keepalives keepaliveState
	watchers   watcherState
	discovery  discoveryState
	events     eventState
	config     configState
	stats      *ClusterStats
	lifecycle  lifecycleState
}

func NewEtcdCluster(etcdClient client.EtcdClient, logger *log.Logger) (*EtcdCluster, error) {
	if etcdClient == nil {
		return nil, fmt.Errorf("etcd client is nil")
	}
	logger = normalizeClusterLogger(logger)
	km := registration.NewRegistrationManager(logger)
	wm := watcher.NewEtcdWatcherManager(logger)
	em := events.NewEventManager()
	cluster := &EtcdCluster{
		etcdClient: etcdClient,
		logger:     logger,
		keepalives: keepaliveState{
			manager:     km,
			retryActors: make(map[string]*keepaliveRetryActor),
			deletors:    make(map[string]*keepaliveDeleteActor),
		},
		watchers: watcherState{
			manager:  wm,
			prefixes: make(map[string]struct{}),
		},
		discovery: discoveryState{
			set: nil,
		},
		events: eventState{
			manager:                  em,
			snapshotLoadingCallbacks: events.NewCallbackList(),
			snapshotLoadedCallbacks:  events.NewCallbackList(),
			nodeEventCallbacks:       events.NewCallbackList(),
			loop:                     nil,
		},
		stats: &ClusterStats{},
		config: configState{
			leaseTTL:               defaultClusterLeaseTTL,
			previousRequestTimeout: defaultPreviousRequestTimeout,
			retryInterval:          defaultRetryInterval,
			keepAliveTimeout:       defaultKeepaliveTimeout,
			keepAliveMaxRetryTimes: defaultKeepaliveMaxRetryTimes,
		},
		lifecycle: lifecycleState{
			state: ClusterStateInitializing,
		},
	}
	cluster.bindLeaseEventBridge()
	return cluster, nil
}

func (c *EtcdCluster) bindLeaseEventBridge() {
	em := c.getEventManager()
	if em == nil {
		return
	}

	c.lifecycle.mu.Lock()
	if c.events.leaseEventBridgeHandle != 0 {
		c.lifecycle.mu.Unlock()
		return
	}
	handle := em.Subscribe([]events.EventType{events.EventTypeLeaseGranted, events.EventTypeLeaseExpired, events.EventTypeLeaseReleased}, func(event *events.Event) {
		if event == nil {
			return
		}
		km := c.GetRegistrationManager()
		if km == nil {
			return
		}
		switch event.Type {
		case events.EventTypeLeaseGranted:
			leaseID, ok := leaseIDFromEvent(event)
			if !ok {
				return
			}
			km.SetLease(clientv3.LeaseID(leaseID))
		case events.EventTypeLeaseExpired, events.EventTypeLeaseReleased:
			km.SetLease(0)
		}
	})
	c.events.leaseEventBridgeHandle = handle
	c.lifecycle.mu.Unlock()
}

func leaseIDFromEvent(event *events.Event) (int64, bool) {
	if event == nil || event.Metadata == nil {
		return 0, false
	}
	value, ok := event.Metadata["lease_id"]
	if !ok {
		return 0, false
	}
	switch v := value.(type) {
	case int64:
		return v, true
	case clientv3.LeaseID:
		return int64(v), true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

func normalizeClusterLogger(logger *log.Logger) *log.Logger {
	if logger == nil {
		return log.Default()
	}
	return logger
}

func (c *EtcdCluster) RegisterDiscoverySet(ds *discovery.EtcdDiscoverySet) error {
	c.discovery.mu.Lock()
	defer c.discovery.mu.Unlock()

	if c.discovery.set != nil {
		return fmt.Errorf("discovery set already registered")
	}

	c.discovery.set = ds
	ds.SetEventPublisher(c.makeDiscoveryEventPublisher())
	c.logger.Info("Discovery set registered")
	return nil
}

func (c *EtcdCluster) GetDiscoverySet() (*discovery.EtcdDiscoverySet, error) {
	c.discovery.mu.RLock()
	defer c.discovery.mu.RUnlock()

	if c.discovery.set == nil {
		return nil, fmt.Errorf("discovery set not found")
	}
	return c.discovery.set, nil
}

func (c *EtcdCluster) RemoveDiscoverySet() error {
	c.discovery.mu.Lock()
	defer c.discovery.mu.Unlock()

	if c.discovery.set == nil {
		return fmt.Errorf("discovery set not found")
	}

	c.discovery.set.SetEventPublisher(nil)
	c.discovery.set = nil
	c.logger.Info("Discovery set removed")
	return nil
}

func (c *EtcdCluster) MapWatcherToDiscovery(watcherPrefix string) error {
	c.discovery.mu.Lock()
	defer c.discovery.mu.Unlock()

	if c.discovery.set == nil {
		return fmt.Errorf("discovery set not found")
	}

	c.logger.Info("Watcher mapped to discovery set", "watcher_prefix", watcherPrefix)
	return nil
}

// GetPreviousRequestTimeout 从proto读取请求超时。
func (c *EtcdCluster) GetPreviousRequestTimeout() time.Duration {
	c.config.mu.RLock()
	defer c.config.mu.RUnlock()
	if c.config.config != nil && c.config.config.Request != nil && c.config.config.Request.ConnectTimeout != nil {
		return c.config.config.Request.ConnectTimeout.AsDuration()
	}
	return c.config.previousRequestTimeout
}

// GetRetryInterval 从proto读取重试间隔。
func (c *EtcdCluster) GetRetryInterval() time.Duration {
	c.config.mu.RLock()
	defer c.config.mu.RUnlock()
	if c.config.config != nil && c.config.config.Keepalive != nil && c.config.config.Keepalive.RetryInterval != nil {
		return c.config.config.Keepalive.RetryInterval.AsDuration()
	}
	return c.config.retryInterval
}

// GetKeepaliveMaxRetryTimes 返回keepalive最大重试次数。
func (c *EtcdCluster) GetKeepaliveMaxRetryTimes() int32 {
	c.config.mu.RLock()
	defer c.config.mu.RUnlock()
	if c.config.keepAliveMaxRetryTimes > 0 {
		return c.config.keepAliveMaxRetryTimes
	}
	return defaultKeepaliveMaxRetryTimes
}

// isLeaseEnabled 检查lease是否启用。
func (c *EtcdCluster) isLeaseEnabled() bool {
	c.config.mu.RLock()
	defer c.config.mu.RUnlock()
	return c.config.leaseTTL > 0
}

func (c *EtcdCluster) getLeaseTTL() int64 {
	c.config.mu.RLock()
	defer c.config.mu.RUnlock()
	return c.config.leaseTTL
}

func (c *EtcdCluster) stopClusterLease(ctx context.Context, revoke bool) {
	c.lifecycle.mu.Lock()
	leaseCancel := c.keepalives.leaseCancel
	leaseID := c.keepalives.leaseID
	c.keepalives.leaseCancel = nil
	c.keepalives.leaseID = 0
	etcdClient := c.etcdClient
	c.lifecycle.mu.Unlock()

	if leaseCancel != nil {
		leaseCancel()
	}
	if leaseID != 0 {
		c.publishClusterEventSync(events.NewLeaseReleasedEvent(int64(leaseID)))
	}
	if !revoke || leaseID == 0 || etcdClient == nil {
		return
	}
	if _, err := etcdClient.Revoke(ctx, leaseID); err != nil {
		c.logger.Warn("Failed to revoke cluster lease", "error", err)
	}
}

// runClusterKeepalive 实现。
func (c *EtcdCluster) runClusterKeepalive(ctx context.Context, leaseID clientv3.LeaseID) {
	defer c.lifecycle.wg.Done()

	ch, err := c.etcdClient.KeepAlive(ctx, leaseID)
	if err != nil {
		c.logger.Error("Failed to start cluster lease keepalive", "error", err, "leaseID", int64(leaseID))
		return
	}

	for {
		select {
		case ka, ok := <-ch:
			if !ok {
				c.logger.Warn("Cluster lease keepalive channel closed, lease may have expired",
					"leaseID", int64(leaseID))
				c.publishClusterEvent(events.NewLeaseExpiredEvent(int64(leaseID)))
				return
			}
			c.logger.Debug("Cluster lease renewed", "leaseID", int64(leaseID), "ttl", ka.TTL)
			c.publishClusterEvent(events.NewLeaseGrantedEvent(int64(leaseID), ka.TTL))

		case <-ctx.Done():
			c.logger.Info("Cluster lease keepalive stopping", "leaseID", int64(leaseID))
			return
		}
	}
}

func (c *EtcdCluster) ensureClusterLease(ctx context.Context, ttl int64) (clientv3.LeaseID, error) {
	c.lifecycle.mu.RLock()
	existing := c.keepalives.leaseID
	c.lifecycle.mu.RUnlock()
	if existing != 0 {
		return existing, nil
	}

	if ttl <= 0 {
		ttl = defaultClusterLeaseTTL
	}

	if !c.isLeaseEnabled() {
		return 0, fmt.Errorf("cluster lease is disabled")
	}

	type grantResult struct {
		id clientv3.LeaseID
	}

	v, err, _ := c.keepalives.grantGroup.Do("grant", func() (interface{}, error) {
		grantCtx, cancel := c.withPreviousRequestTimeout(ctx)
		defer cancel()

		c.lifecycle.mu.RLock()
		existingID := c.keepalives.leaseID
		c.lifecycle.mu.RUnlock()
		if existingID != 0 {
			return &grantResult{id: existingID}, nil
		}

		resp, err := c.etcdClient.Grant(grantCtx, ttl)
		if err != nil {
			c.logger.Error("Failed to grant cluster lease", "error", err, "ttl", ttl)
			return nil, err
		}

		kaCtx, kaCancel := context.WithCancel(context.Background())

		c.lifecycle.mu.Lock()
		c.keepalives.leaseID = resp.ID
		c.keepalives.leaseCancel = kaCancel
		c.lifecycle.wg.Add(1)
		c.lifecycle.mu.Unlock()

		c.logger.Info("Cluster lease granted", "leaseID", int64(resp.ID), "ttl", ttl)

		c.publishClusterEventSync(events.NewLeaseGrantedEvent(int64(resp.ID), ttl))

		c.startClusterKeepalive(kaCtx, resp.ID)

		return &grantResult{id: resp.ID}, nil
	})
	if err != nil {
		return 0, err
	}
	return v.(*grantResult).id, nil
}

func (c *EtcdCluster) startClusterKeepalive(ctx context.Context, leaseID clientv3.LeaseID) {
	go c.runClusterKeepalive(ctx, leaseID)
}

func (c *EtcdCluster) withPreviousRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	timeout := c.GetPreviousRequestTimeout()
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func cloneDiscoveryInfo(info *pb.AtappDiscovery) *pb.AtappDiscovery {
	if info == nil {
		return nil
	}
	cloned, ok := proto.Clone(info).(*pb.AtappDiscovery)
	if !ok {
		return info
	}
	return cloned
}

func (c *EtcdCluster) enqueueRegistrationRetry(info *pb.AtappDiscovery, path string, ttl int64) {
	c.enqueueRegistrationRetryWithAttempts(info, path, ttl, 0)
}

func (c *EtcdCluster) enqueueRegistrationRetryWithAttempts(info *pb.AtappDiscovery, path string, ttl int64, attempts int32) {
	if path == "" {
		return
	}
	if ttl <= 0 {
		ttl = defaultClusterLeaseTTL
	}
	interval := c.GetRetryInterval()
	if interval <= 0 {
		interval = defaultRetryInterval
	}

	c.keepalives.retryMu.Lock()
	defer c.keepalives.retryMu.Unlock()

	actor, ok := c.keepalives.retryActors[path]
	if !ok {
		actor = &keepaliveRetryActor{path: path}
		c.keepalives.retryActors[path] = actor
		if c.stats != nil {
			c.stats.IncKeepaliveRetryQueued()
		}
	}
	actor.info = cloneDiscoveryInfo(info)
	actor.ttl = ttl
	if attempts > actor.attempts {
		actor.attempts = attempts
	}
	actor.attempts++
	actor.nextAttempt = time.Now().Add(interval)
	if c.stats != nil {
		c.stats.SetRetryQueueSize(int64(len(c.keepalives.retryActors)))
	}
}

func (c *EtcdCluster) removeRegistrationRetry(path string) {
	c.keepalives.retryMu.Lock()
	delete(c.keepalives.retryActors, path)
	if c.stats != nil {
		c.stats.SetRetryQueueSize(int64(len(c.keepalives.retryActors)))
	}
	c.keepalives.retryMu.Unlock()
}

func (c *EtcdCluster) popDueRegistrationRetries(now time.Time) []*keepaliveRetryActor {
	c.keepalives.retryMu.Lock()
	defer c.keepalives.retryMu.Unlock()

	if len(c.keepalives.retryActors) == 0 {
		return nil
	}

	var due []*keepaliveRetryActor
	for path, actor := range c.keepalives.retryActors {
		if actor == nil {
			delete(c.keepalives.retryActors, path)
			continue
		}
		if actor.nextAttempt.After(now) {
			continue
		}
		due = append(due, actor)
		delete(c.keepalives.retryActors, path)
	}

	if c.stats != nil {
		c.stats.SetRetryQueueSize(int64(len(c.keepalives.retryActors)))
	}
	return due
}

func (c *EtcdCluster) enqueueRegistrationDeletion(path string) {
	c.enqueueRegistrationDeletionWithAttempts(path, 0)
}

func (c *EtcdCluster) enqueueRegistrationDeletionWithAttempts(path string, attempts int32) {
	if path == "" {
		return
	}
	interval := c.GetRetryInterval()
	if interval <= 0 {
		interval = defaultRetryInterval
	}

	c.keepalives.retryMu.Lock()
	actor, ok := c.keepalives.deletors[path]
	if !ok {
		actor = &keepaliveDeleteActor{path: path}
		c.keepalives.deletors[path] = actor
		if c.stats != nil {
			c.stats.IncKeepaliveDeleteQueued()
		}
	}
	if attempts > actor.attempts {
		actor.attempts = attempts
	}
	actor.attempts++
	if actor.attempts <= 1 {
		actor.nextAttempt = time.Now()
	} else {
		actor.nextAttempt = time.Now().Add(interval)
	}
	if c.stats != nil {
		c.stats.SetDeleteQueueSize(int64(len(c.keepalives.deletors)))
	}
	c.keepalives.retryMu.Unlock()
}

func (c *EtcdCluster) popDueRegistrationDeletors(now time.Time) []*keepaliveDeleteActor {
	c.keepalives.retryMu.Lock()
	defer c.keepalives.retryMu.Unlock()
	if len(c.keepalives.deletors) == 0 {
		return nil
	}

	actors := make([]*keepaliveDeleteActor, 0, len(c.keepalives.deletors))
	for path, actor := range c.keepalives.deletors {
		if actor == nil {
			delete(c.keepalives.deletors, path)
			continue
		}
		if actor.nextAttempt.After(now) {
			continue
		}
		actors = append(actors, actor)
		delete(c.keepalives.deletors, path)
	}
	if c.stats != nil {
		c.stats.SetDeleteQueueSize(int64(len(c.keepalives.deletors)))
	}
	return actors
}

func (c *EtcdCluster) processRegistrationRetryQueue(ctx context.Context) error {
	actors := c.popDueRegistrationRetries(time.Now())
	if len(actors) == 0 {
		return nil
	}

	var errs []error
	for _, actor := range actors {
		if actor == nil || actor.path == "" {
			continue
		}
		maxRetries := c.GetKeepaliveMaxRetryTimes()
		if actor.attempts >= maxRetries {
			c.logger.Error("retry keepalive exceeded max attempts", "path", actor.path, "attempts", actor.attempts, "max", maxRetries)
			if c.stats != nil {
				c.stats.IncKeepaliveRetryFailure()
				c.stats.RecordKeepaliveError(time.Now().Unix())
			}
			errs = append(errs, fmt.Errorf("retry keepalive %s exceeded max attempts", actor.path))
			continue
		}
		err := c.registerRegistration(ctx, actor.info, actor.path, actor.ttl)
		if err != nil {
			if actor.attempts >= maxRetries {
				c.logger.Error("retry keepalive exceeded max attempts", "path", actor.path, "attempts", actor.attempts, "max", maxRetries)
				if c.stats != nil {
					c.stats.IncKeepaliveRetryFailure()
					c.stats.RecordKeepaliveError(time.Now().Unix())
				}
				errs = append(errs, fmt.Errorf("retry keepalive %s exceeded max attempts", actor.path))
				continue
			}
			c.enqueueRegistrationRetryWithAttempts(actor.info, actor.path, actor.ttl, actor.attempts)
			if c.stats != nil {
				c.stats.IncKeepaliveRetryFailure()
			}
			errs = append(errs, fmt.Errorf("retry keepalive %s: %w", actor.path, err))
			continue
		}
		if c.stats != nil {
			c.stats.IncKeepaliveRetrySuccess()
		}
	}

	return joinErrors(errs)
}

func (c *EtcdCluster) processRegistrationDeletionQueue(ctx context.Context) error {
	actors := c.popDueRegistrationDeletors(time.Now())
	if len(actors) == 0 {
		return nil
	}

	var errs []error
	for _, actor := range actors {
		if actor == nil || actor.path == "" {
			continue
		}
		path := actor.path
		maxRetries := c.GetKeepaliveMaxRetryTimes()
		if actor.attempts >= maxRetries {
			c.logger.Error("delete keepalive exceeded max attempts", "path", path, "attempts", actor.attempts, "max", maxRetries)
			if c.stats != nil {
				c.stats.IncKeepaliveDeleteFailure()
			}
			errs = append(errs, fmt.Errorf("delete keepalive %s exceeded max attempts", path))
			continue
		}
		err := c.DeleteRawValue(ctx, path)
		if err != nil {
			if actor.attempts >= maxRetries {
				c.logger.Error("delete keepalive exceeded max attempts", "path", path, "attempts", actor.attempts, "max", maxRetries)
				if c.stats != nil {
					c.stats.IncKeepaliveDeleteFailure()
				}
				errs = append(errs, fmt.Errorf("delete keepalive %s exceeded max attempts", path))
				continue
			}
			c.enqueueRegistrationDeletionWithAttempts(path, actor.attempts)
			if c.stats != nil {
				c.stats.IncKeepaliveDeleteFailure()
			}
			errs = append(errs, fmt.Errorf("delete keepalive %s: %w", path, err))
			continue
		}
		if c.stats != nil {
			c.stats.IncKeepaliveDeleteSuccess()
		}
	}

	return joinErrors(errs)
}

func (c *EtcdCluster) GetRegistrationManager() *registration.RegistrationManager {
	c.lifecycle.mu.RLock()
	defer c.lifecycle.mu.RUnlock()
	return c.keepalives.manager
}

// TriggerMaybeUpdateRegistrations 实现。
func (c *EtcdCluster) TriggerMaybeUpdateRegistrations(ctx context.Context) error {
	if err := c.ensureRunning(); err != nil {
		return err
	}
	if err := c.processRegistrationDeletionQueue(ctx); err != nil {
		c.logger.Warn("process keepalive deletion queue failed", "error", err)
	}
	if err := c.processRegistrationRetryQueue(ctx); err != nil {
		c.logger.Warn("process keepalive retry queue failed", "error", err)
	}

	c.lifecycle.mu.RLock()
	km := c.keepalives.manager
	c.lifecycle.mu.RUnlock()
	if km == nil {
		return fmt.Errorf("keepalive manager is not initialized")
	}

	return km.TriggerMaybeUpdateAll(ctx)
}

func (c *EtcdCluster) GetWatcherManager() *watcher.EtcdWatcherManager {
	c.lifecycle.mu.RLock()
	defer c.lifecycle.mu.RUnlock()
	return c.watchers.manager
}

func (c *EtcdCluster) GetEventManager() events.EventManager {
	c.lifecycle.mu.RLock()
	defer c.lifecycle.mu.RUnlock()
	return c.events.manager
}

func (c *EtcdCluster) AddOnSnapshotLoading(callback events.EventCallback) events.EventCallbackHandle {
	c.lifecycle.mu.Lock()
	defer c.lifecycle.mu.Unlock()
	if c.events.snapshotLoadingCallbacks == nil {
		c.events.snapshotLoadingCallbacks = events.NewCallbackList()
	}
	return c.events.snapshotLoadingCallbacks.Add(callback)
}

func (c *EtcdCluster) RemoveOnSnapshotLoading(handle events.EventCallbackHandle) {
	c.lifecycle.mu.Lock()
	defer c.lifecycle.mu.Unlock()
	if c.events.snapshotLoadingCallbacks == nil {
		return
	}
	c.events.snapshotLoadingCallbacks.Remove(handle)
}

func (c *EtcdCluster) AddOnSnapshotLoaded(callback events.EventCallback) events.EventCallbackHandle {
	c.lifecycle.mu.Lock()
	defer c.lifecycle.mu.Unlock()
	if c.events.snapshotLoadedCallbacks == nil {
		c.events.snapshotLoadedCallbacks = events.NewCallbackList()
	}
	return c.events.snapshotLoadedCallbacks.Add(callback)
}

func (c *EtcdCluster) RemoveOnSnapshotLoaded(handle events.EventCallbackHandle) {
	c.lifecycle.mu.Lock()
	defer c.lifecycle.mu.Unlock()
	if c.events.snapshotLoadedCallbacks == nil {
		return
	}
	c.events.snapshotLoadedCallbacks.Remove(handle)
}

func (c *EtcdCluster) AddOnNodeEvent(callback events.EventCallback) events.EventCallbackHandle {
	c.lifecycle.mu.Lock()
	defer c.lifecycle.mu.Unlock()
	if c.events.nodeEventCallbacks == nil {
		c.events.nodeEventCallbacks = events.NewCallbackList()
	}
	return c.events.nodeEventCallbacks.Add(callback)
}

func (c *EtcdCluster) RemoveOnNodeEvent(handle events.EventCallbackHandle) {
	c.lifecycle.mu.Lock()
	defer c.lifecycle.mu.Unlock()
	if c.events.nodeEventCallbacks == nil {
		return
	}
	c.events.nodeEventCallbacks.Remove(handle)
}

// GetStats 获取Stats。
func (c *EtcdCluster) GetStats() ClusterStatsSnapshot {
	if c.stats == nil {
		return ClusterStatsSnapshot{}
	}
	return c.stats.Snapshot()
}

// ApplyEtcdConfig 应用EtcdConfig。
func (c *EtcdCluster) ApplyEtcdConfig(cfg *pb.AtappEtcd) {
	c.config.mu.Lock()
	defer c.config.mu.Unlock()
	c.config.config = cfg

	// 从proto读取配置值
	if cfg == nil {
		return
	}

	// 基于 keepalive.ttl 设置 leaseTTL
	if cfg.Keepalive != nil && cfg.Keepalive.Ttl != nil {
		ttl := cfg.Keepalive.Ttl.AsDuration()
		c.config.leaseTTL = int64(ttl / time.Second)
	}

	// 基于 keepalive.retry_interval 设置 retryInterval
	if cfg.Keepalive != nil && cfg.Keepalive.RetryInterval != nil {
		c.config.retryInterval = cfg.Keepalive.RetryInterval.AsDuration()
	}

	// 基于 request.connect_timeout 设置 previousRequestTimeout
	if cfg.Request != nil && cfg.Request.ConnectTimeout != nil {
		c.config.previousRequestTimeout = cfg.Request.ConnectTimeout.AsDuration()
	}
}

func (c *EtcdCluster) RegisterService(ctx context.Context, info *pb.AtappDiscovery, path string, ttl int64) error {
	if err := c.ensureRunning(); err != nil {
		return err
	}
	if err := validateRegistrationInputs(info, path, ttl); err != nil {
		return err
	}

	if err := c.registerRegistration(ctx, info, path, ttl); err != nil {
		return err
	}
	c.removeRegistrationRetry(path)
	c.logger.Info("Service registered successfully", "path", path, "name", info.Name)
	return nil
}

// PutRawValueWithClusterLease 实现。
func (c *EtcdCluster) PutRawValueWithClusterLease(ctx context.Context, path string, value string) error {
	if err := c.ensureRunning(); err != nil {
		return err
	}
	if path == "" {
		return fmt.Errorf("service path is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	ttl := c.getLeaseTTL()
	c.lifecycle.mu.RLock()
	etcdClient := c.etcdClient
	c.lifecycle.mu.RUnlock()
	leaseID, err := c.ensureClusterLease(ctx, ttl)
	if err != nil {
		return err
	}
	if etcdClient == nil {
		return fmt.Errorf("etcd client is nil")
	}

	_, err = etcdClient.Put(ctx, path, value, clientv3.WithLease(leaseID))
	if err != nil {
		return err
	}
	return nil
}

// DeleteRawValue 实现。
func (c *EtcdCluster) DeleteRawValue(ctx context.Context, path string) error {
	if err := c.ensureRunning(); err != nil {
		return err
	}
	if path == "" {
		return fmt.Errorf("service path is empty")
	}
	c.lifecycle.mu.RLock()
	etcdClient := c.etcdClient
	c.lifecycle.mu.RUnlock()
	if etcdClient == nil {
		return fmt.Errorf("etcd client is nil")
	}
	_, err := etcdClient.Delete(ctx, path)
	return err
}

func (c *EtcdCluster) UnregisterService(ctx context.Context, path string) error {
	c.lifecycle.mu.RLock()
	km := c.keepalives.manager
	c.lifecycle.mu.RUnlock()

	svc, ok := km.GetRegistration(path)
	if !ok {
		return fmt.Errorf("service not found: %s", path)
	}

	if err := svc.Unregister(ctx); err != nil {
		c.logger.Error("Failed to unregister service", "error", err, "path", path)
		c.enqueueRegistrationDeletion(path)
		if c.stats != nil {
			c.stats.RecordKeepaliveError(time.Now().Unix())
		}
		return err
	}

	if km.RemoveRegistrationAndIsEmpty(path) {
		c.stopClusterLease(ctx, true)
	}
	c.logger.Info("Service unregistered successfully", "path", path)
	return nil
}

// UpdateService 更新Service。
func (c *EtcdCluster) UpdateService(ctx context.Context, info *pb.AtappDiscovery, path string) error {
	c.lifecycle.mu.RLock()
	km := c.keepalives.manager
	c.lifecycle.mu.RUnlock()

	if path != "" {
		return c.updateSingleRegistration(ctx, km, info, path)
	}

	updated, errs := c.updateAllRegistrations(ctx, km, info)
	if updated == 0 {
		if len(errs) > 0 {
			return fmt.Errorf("failed to update keepalives: %w", errs[0])
		}
		return fmt.Errorf("no keepalives registered")
	}
	if len(errs) > 0 {
		return joinErrors(errs)
	}

	c.logger.Info("Service updated successfully", "count", updated)
	return nil
}

func (c *EtcdCluster) updateSingleRegistration(ctx context.Context, km *registration.RegistrationManager, info *pb.AtappDiscovery, path string) error {
	svc, ok := km.GetRegistration(path)
	if !ok {
		return fmt.Errorf("service not found: %s", path)
	}
	if err := svc.UpdateServiceInfoWithContext(ctx, info); err != nil {
		c.logger.Error("Failed to update service info", "error", err, "path", path)
		return err
	}
	c.logger.Info("Service updated successfully", "path", path)
	return nil
}

func (c *EtcdCluster) updateAllRegistrations(ctx context.Context, km *registration.RegistrationManager, info *pb.AtappDiscovery) (int, []error) {
	updated := 0
	var errs []error
	for _, svc := range km.GetAllRegistrations() {
		if err := svc.UpdateServiceInfoWithContext(ctx, info); err != nil {
			c.logger.Error("Failed to update keepalive info", "error", err, "path", svc.GetPath())
			errs = append(errs, err)
			continue
		}
		updated++
	}
	return updated, errs
}

// AutoRegisterFromConfig 实现。
func (c *EtcdCluster) AutoRegisterFromConfig(ctx context.Context, info *pb.AtappDiscovery, ttl int64) error {
	if err := validateAutoRegisterInputs(info, ttl); err != nil {
		return err
	}
	basePath, cfg, err := c.requireEtcdConfigBasePath()
	if err != nil {
		return err
	}

	var errs []error
	for _, path := range buildReportAlivePaths(info, cfg.GetKeepalive(), basePath) {
		if err := c.registerRegistration(ctx, info, path, ttl); err != nil {
			errs = append(errs, fmt.Errorf("keepalive %s: %w", path, err))
		} else {
			c.logger.Info("Keepalive registered successfully", "path", path)
		}
	}

	for _, prefix := range buildWatcherPrefixes(cfg.Watcher, basePath) {
		if err := c.AddWatcher(ctx, prefix, nil); err != nil {
			errs = append(errs, fmt.Errorf("watcher %s: %w", prefix, err))
		}
	}

	return joinErrors(errs)
}

func validateRegistrationInputs(info *pb.AtappDiscovery, path string, ttl int64) error {
	if info == nil {
		return fmt.Errorf("service info is nil")
	}
	if path == "" {
		return fmt.Errorf("service path is empty")
	}
	if ttl <= 0 {
		return fmt.Errorf("ttl must be positive")
	}
	return nil
}

func validateAutoRegisterInputs(info *pb.AtappDiscovery, ttl int64) error {
	if info == nil {
		return fmt.Errorf("service info is nil")
	}
	if ttl <= 0 {
		return fmt.Errorf("ttl must be positive")
	}
	return nil
}

func (c *EtcdCluster) ensureRunning() error {
	c.lifecycle.mu.RLock()
	defer c.lifecycle.mu.RUnlock()
	if c.lifecycle.state != ClusterStateRunning {
		return fmt.Errorf("cluster is not running")
	}
	return nil
}

func (c *EtcdCluster) requireEtcdConfigBasePath() (string, *pb.AtappEtcd, error) {
	c.config.mu.RLock()
	cfg := c.config.config
	c.config.mu.RUnlock()
	if cfg == nil {
		return "", nil, fmt.Errorf("etcd config not set")
	}
	if !cfg.Enable {
		return "", nil, fmt.Errorf("etcd disabled")
	}

	basePath := strings.TrimRight(cfg.Path, "/")
	if basePath == "" {
		return "", nil, fmt.Errorf("etcd path is empty")
	}
	return basePath, cfg, nil
}

func (c *EtcdCluster) registerRegistration(ctx context.Context, info *pb.AtappDiscovery, path string, ttl int64) error {
	if err := validateRegistrationInputs(info, path, ttl); err != nil {
		return err
	}

	svc, err := registration.NewEtcdRegistration(info, path, ttl, c.etcdClient, c.logger, c.keepalives.manager)
	if err != nil {
		c.logger.Error("Failed to create keepalive", "error", err, "path", path)
		return err
	}

	if _, err := c.ensureClusterLease(ctx, ttl); err != nil {
		return err
	}

	c.keepalives.manager.AddRegistration(svc)
	if err := svc.Start(ctx); err != nil {
		c.keepalives.manager.RemoveRegistration(path)
		c.enqueueRegistrationRetry(info, path, ttl)
		c.logger.Error("Failed to register keepalive", "error", err, "path", path)
		if c.stats != nil {
			c.stats.IncKeepaliveFailed()
			c.stats.RecordKeepaliveError(time.Now().Unix())
		}
		return err
	}
	if c.stats != nil {
		c.stats.IncKeepaliveRegistered()
	}
	return nil
}

func joinErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	if len(errs) == 1 {
		return errs[0]
	}
	var builder strings.Builder
	builder.WriteString("multiple errors:")
	for _, err := range errs {
		builder.WriteString(" ")
		builder.WriteString(err.Error())
		builder.WriteString(";")
	}
	return fmt.Errorf("%s", builder.String())
}

func (c *EtcdCluster) AddWatcher(ctx context.Context, watcherPrefix string, batchConfig *watcher.BatchConfig) error {
	if err := c.ensureRunning(); err != nil {
		return err
	}

	if existing, ok := c.watchers.manager.GetWatcher(watcherPrefix); ok {
		if existing != nil && existing.IsRunning() {
			c.logger.Debug("Watcher already exists and running", "prefix", watcherPrefix)
			return nil
		}
		if existing != nil {
			existing.Stop()
		}
		c.untrackWatcher(watcherPrefix)
	}

	c.lifecycle.mu.RLock()
	runCtx := c.lifecycle.runCtx
	c.lifecycle.mu.RUnlock()
	if runCtx == nil {
		return fmt.Errorf("cluster runtime context is not initialized")
	}

	w, err := c.buildWatcher(watcherPrefix, batchConfig)
	if err != nil {
		return err
	}

	if err := w.Start(runCtx); err != nil {
		c.logger.Error("Failed to start watcher", "error", err, "prefix", watcherPrefix)
		if c.stats != nil {
			c.stats.IncWatcherFailure()
		}
		return err
	}
	if c.stats != nil {
		c.stats.IncWatcherStart()
	}

	c.trackWatcher(watcherPrefix, w)

	c.logger.Info("Watcher started", "prefix", watcherPrefix)
	return nil
}

func (c *EtcdCluster) RemoveWatcher(watcherPrefix string) error {
	w, ok := c.watchers.manager.GetWatcher(watcherPrefix)
	if !ok {
		return fmt.Errorf("watcher not found: %s", watcherPrefix)
	}

	w.Stop()
	c.untrackWatcher(watcherPrefix)
	c.logger.Info("Watcher removed", "prefix", watcherPrefix)
	return nil
}

func (c *EtcdCluster) buildWatcher(watcherPrefix string, batchConfig *watcher.BatchConfig) (*watcher.EtcdWatcher, error) {
	cfg := watcher.DefaultWatchConfig()
	cfg.Key = watcherPrefix
	cfg.RangeEnd = clientv3.GetPrefixRangeEnd(watcherPrefix)
	cfg.StartRevision = 0

	w := watcher.NewEtcdWatcher(c.etcdClient, cfg, c.logger)
	if batchConfig != nil && batchConfig.Enabled {
		w.SetBatchMode(*batchConfig)
		w.SetBatchHandler(c.makeWatcherBatchHandler(watcherPrefix))
	} else {
		w.SetHandler(c.makeWatcherEventHandler(watcherPrefix))
	}
	w.SetConnectionHandler(c.makeWatcherConnectionHandler())
	w.SetWatchErrorHandler(c.makeWatcherErrorHandler())
	w.SetSnapshotHandler(c.makeWatcherSnapshotHandler(watcherPrefix))
	w.SetSnapshotLoadedHandler(c.makeWatcherSnapshotEventsHandler(watcherPrefix))
	w.SetSnapshotLoadingHandler(c.makeWatcherSnapshotLoadingHandler(watcherPrefix))
	return w, nil
}

func (c *EtcdCluster) trackWatcher(watcherPrefix string, w *watcher.EtcdWatcher) {
	c.watchers.manager.AddWatcher(watcherPrefix, w)
	c.watchers.snapshotIndexMu.Lock()
	c.watchers.prefixes[watcherPrefix] = struct{}{}
	c.watchers.snapshotIndex++
	if c.watchers.masterWatcher == "" {
		c.watchers.masterWatcher = watcherPrefix
	}
	c.watchers.snapshotIndexMu.Unlock()
}

func (c *EtcdCluster) untrackWatcher(watcherPrefix string) {
	c.watchers.manager.RemoveWatcher(watcherPrefix)
	c.watchers.snapshotIndexMu.Lock()
	delete(c.watchers.prefixes, watcherPrefix)
	if c.watchers.masterWatcher == watcherPrefix {
		c.watchers.masterWatcher = ""
		for prefix := range c.watchers.prefixes {
			c.watchers.masterWatcher = prefix
			break
		}
	}
	c.watchers.snapshotIndexMu.Unlock()
}

func (c *EtcdCluster) makeWatcherEventHandler(watcherPrefix string) watcher.EventHandler {
	return func(event watcher.EtcdWatchEvent) {
		ds := c.getDiscoverySetForEvent(watcherPrefix)
		if ds == nil {
			return
		}
		ds.HandleWatcherEvent(event)
	}
}

func (c *EtcdCluster) makeWatcherBatchHandler(watcherPrefix string) watcher.BatchEventHandler {
	return func(events []*watcher.EtcdWatchEvent) {
		ds := c.getDiscoverySetForEvent(watcherPrefix)
		if ds == nil {
			return
		}
		ds.HandleBatch(events)
	}
}

func (c *EtcdCluster) makeWatcherSnapshotHandler(watcherPrefix string) func(revision int64, nodeCount int) {
	return func(revision int64, nodeCount int) {
		c.watchers.snapshotIndexMu.Lock()
		isMaster := c.watchers.masterWatcher == "" || c.watchers.masterWatcher == watcherPrefix
		c.watchers.snapshotIndexMu.Unlock()
		if !isMaster {
			return
		}
		em, loadedCallbacks := c.snapshotLoadedTargets()
		if em == nil {
			return
		}
		c.dispatchOnEventLoop(func() {
			snapshotEvent := events.NewSnapshotLoadedEvent(nodeCount, revision)
			em.Publish(snapshotEvent)
			if loadedCallbacks != nil {
				loadedCallbacks.Publish(snapshotEvent)
			}
			c.markReadyIfNeeded(em, revision)
		})
	}
}

func (c *EtcdCluster) makeWatcherSnapshotEventsHandler(watcherPrefix string) func(nodes []*watcher.EtcdWatchEvent) {
	return func(nodes []*watcher.EtcdWatchEvent) {
		c.watchers.snapshotIndexMu.Lock()
		isMaster := c.watchers.masterWatcher == "" || c.watchers.masterWatcher == watcherPrefix
		c.watchers.snapshotIndexMu.Unlock()
		if !isMaster {
			return
		}
		ds := c.getDiscoverySetForSnapshot(nodes)
		if ds == nil {
			return
		}
		ds.ApplySnapshot(nodes)
	}
}

func (c *EtcdCluster) makeWatcherSnapshotLoadingHandler(watcherPrefix string) func() {
	return func() {
		c.watchers.snapshotIndexMu.Lock()
		isMaster := c.watchers.masterWatcher == "" || c.watchers.masterWatcher == watcherPrefix
		c.watchers.snapshotIndexMu.Unlock()
		if !isMaster {
			return
		}
		em, loadingCallbacks := c.snapshotLoadingTargets()
		if em == nil {
			return
		}
		c.dispatchOnEventLoop(func() {
			event := events.NewSnapshotLoadingEvent()
			em.Publish(event)
			if loadingCallbacks != nil {
				loadingCallbacks.Publish(event)
			}
		})
	}
}

func (c *EtcdCluster) makeWatcherConnectionHandler() func(state watcher.ConnectionState, revision int64) {
	return func(state watcher.ConnectionState, revision int64) {
		em := c.getEventManager()
		if em == nil {
			return
		}
		c.dispatchOnEventLoop(func() {
			switch state {
			case watcher.ConnectionStateConnected:
				em.Publish(events.NewWatchConnectedEvent(revision))
			case watcher.ConnectionStateDisconnected:
				c.lifecycle.mu.Lock()
				c.lifecycle.ready = false
				c.lifecycle.mu.Unlock()
				em.Publish(events.NewWatchDisconnectedEvent())
			case watcher.ConnectionStateReconnected:
				em.Publish(events.NewWatchReconnectedEvent(revision))
			}
		})
	}
}

func (c *EtcdCluster) makeWatcherErrorHandler() func(kind watcher.WatchErrorKind, err error) {
	return func(kind watcher.WatchErrorKind, err error) {
		c.recordWatcherErrorStats(kind)
		c.handleWatcherReadyState(kind)
		c.logWatcherError(kind, err)
	}
}

func (c *EtcdCluster) recordWatcherErrorStats(kind watcher.WatchErrorKind) {
	if c.stats == nil {
		return
	}
	c.stats.RecordWatcherError(int32(kind), time.Now().Unix())
	c.stats.IncWatcherFailure()
	if kind == watcher.WatchErrorCompacted {
		c.stats.IncWatcherCompaction()
		return
	}
	if kind == watcher.WatchErrorCanceled {
		c.stats.IncWatcherCanceled()
		return
	}
}

func (c *EtcdCluster) handleWatcherReadyState(kind watcher.WatchErrorKind) {
	if kind != watcher.WatchErrorCompacted && kind != watcher.WatchErrorCanceled {
		return
	}
	c.lifecycle.mu.Lock()
	c.lifecycle.ready = false
	c.lifecycle.mu.Unlock()
}

func (c *EtcdCluster) logWatcherError(kind watcher.WatchErrorKind, err error) {
	if err == nil {
		return
	}
	c.logger.Warn("watcher error", "error", err, "kind", int(kind))
}

func (c *EtcdCluster) makeDiscoveryEventPublisher() func(eventType pb.EtcdWatchEventType, node *discovery.DiscoveryNode, existed bool) {
	return func(eventType pb.EtcdWatchEventType, node *discovery.DiscoveryNode, existed bool) {
		em := c.getEventManager()
		if em == nil {
			return
		}
		switch eventType {
		case pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT:
			if existed {
				c.publishNodeEvent(em, events.NewNodeUpdateEvent(node))
			} else {
				c.publishNodeEvent(em, events.NewNodeUpEvent(node))
			}
			// connector hooks removed; keep event manager only
		case pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE:
			if node != nil && node.Info != nil {
				c.publishNodeEvent(em, events.NewNodeDownEvent(node.Info.Id, node.Info.Name))
			}
		}
	}
}

func (c *EtcdCluster) dispatchNodeCallback(event *events.Event) {
	c.lifecycle.mu.RLock()
	callbacks := c.events.nodeEventCallbacks
	c.lifecycle.mu.RUnlock()
	if callbacks == nil || event == nil {
		return
	}
	callbacks.Publish(event)
}

func (c *EtcdCluster) getEventManager() events.EventManager {
	c.lifecycle.mu.RLock()
	em := c.events.manager
	c.lifecycle.mu.RUnlock()
	return em
}

func (c *EtcdCluster) snapshotLoadedTargets() (events.EventManager, *events.CallbackList) {
	c.lifecycle.mu.RLock()
	em := c.events.manager
	loadedCallbacks := c.events.snapshotLoadedCallbacks
	c.lifecycle.mu.RUnlock()
	return em, loadedCallbacks
}

func (c *EtcdCluster) snapshotLoadingTargets() (events.EventManager, *events.CallbackList) {
	c.lifecycle.mu.RLock()
	em := c.events.manager
	loadingCallbacks := c.events.snapshotLoadingCallbacks
	c.lifecycle.mu.RUnlock()
	return em, loadingCallbacks
}

func (c *EtcdCluster) markReadyIfNeeded(em events.EventManager, revision int64) {
	c.lifecycle.mu.Lock()
	if !c.lifecycle.ready {
		c.lifecycle.ready = true
		c.lifecycle.mu.Unlock()
		em.Publish(events.NewClusterStateChangeEvent(events.ClusterStateReady, revision))
		return
	}
	c.lifecycle.mu.Unlock()
}

func (c *EtcdCluster) publishNodeEvent(em events.EventManager, event *events.Event) {
	if em == nil || event == nil {
		return
	}
	c.dispatchOnEventLoop(func() {
		em.Publish(event)
		c.dispatchNodeCallback(event)
	})
}

func (c *EtcdCluster) publishClusterEvent(event *events.Event) {
	if event == nil {
		return
	}
	em := c.getEventManager()
	if em == nil {
		return
	}
	c.dispatchOnEventLoop(func() {
		em.Publish(event)
	})
}

func (c *EtcdCluster) publishClusterEventSync(event *events.Event) {
	if event == nil {
		return
	}
	em := c.getEventManager()
	if em == nil {
		return
	}
	// 保护同步事件发布：callback panic 不应该导致整个进程崩溃
	defer func() {
		if r := recover(); r != nil {
			log.Error("EventManager callback panic during sync publish", log.Any("panic", r))
		}
	}()
	em.Publish(event)
}

func (c *EtcdCluster) dispatchOnEventLoop(fn func()) {
	if fn == nil {
		return
	}
	c.lifecycle.mu.RLock()
	loop := c.events.loop
	c.lifecycle.mu.RUnlock()
	if loop == nil {
		fn()
		return
	}
	_ = loop.post(fn)
}

func (c *EtcdCluster) getDiscoverySetForEvent(watcherPrefix string) *discovery.EtcdDiscoverySet {
	c.discovery.mu.RLock()
	ds := c.discovery.set
	c.discovery.mu.RUnlock()
	if ds == nil {
		c.logger.Warn("No discovery set registered for watcher", "prefix", watcherPrefix)
		return nil
	}
	return ds
}

func (c *EtcdCluster) getDiscoverySetForSnapshot(nodes []*watcher.EtcdWatchEvent) *discovery.EtcdDiscoverySet {
	if len(nodes) == 0 {
		return nil
	}
	c.discovery.mu.RLock()
	ds := c.discovery.set
	c.discovery.mu.RUnlock()
	return ds
}

func (c *EtcdCluster) GetServiceNode(key string, filter map[string]string, strategy RoutingStrategy) (*discovery.DiscoveryNode, error) {
	ds, err := c.GetDiscoverySet()
	if err != nil {
		return nil, err
	}

	switch strategy {
	case RoutingStrategyConsistentHash:
		return ds.GetNodeByConsistentHash(key, filter)
	case RoutingStrategyRoundRobin:
		return ds.GetNodeByRoundRobin(filter)
	case RoutingStrategyRandom:
		return ds.GetNodeByRandom(filter)
	default:
		return nil, fmt.Errorf("unknown routing strategy: %v", strategy)
	}
}

func (c *EtcdCluster) GetServiceNodes(key string, count int, filter map[string]string, strategy RoutingStrategy) ([]*discovery.DiscoveryNode, error) {
	ds, err := c.GetDiscoverySet()
	if err != nil {
		return nil, err
	}

	switch strategy {
	case RoutingStrategyConsistentHash:
		return ds.GetNodesByConsistentHash(key, count, filter, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	default:
		return nil, fmt.Errorf("unknown routing strategy for multiple nodes: %v", strategy)
	}
}

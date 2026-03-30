// Package watcher provides etcd watch functionality for service discovery.
// It supports watching single keys or key ranges with various options.
package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	log "log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/atframework/libatapp-go/etcd_module/client"
	internalcodec "github.com/atframework/libatapp-go/etcd_module/internal/codec"
	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
	pb "github.com/atframework/libatapp-go/protocol/atframe"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EventType represents the type of watch event.

// EtcdWatchEvent 定义EtcdWatchEvent类型。
type EtcdWatchEvent struct {
	Type     pb.EtcdWatchEventType
	Key      string
	Value    *pb.AtappDiscovery
	RawValue []byte
	Revision int64
	// Node version tuple aligned with etcd KV revision semantics.
	etcdversion.DataVersion
	PrevValue    *pb.AtappDiscovery
	RawPrevValue []byte
}

// WatchConfig 定义WatchConfig配置结构。
type WatchConfig struct {
	// Key to watch
	Key string
	// RangeEnd for range watch (empty for single key)
	RangeEnd string
	// StartRevision is the revision to watch from (0 = now)
	StartRevision int64
	// PrevKV if true, the watch will return the previous key-value pair
	PrevKV bool
	// ProgressNotify if true, the server will periodically send progress notifications
	ProgressNotify bool
	// Filter filters the events to receive
	Filter EventFilter
	// RetryInterval is the interval between retry attempts
	RetryInterval time.Duration
	// RequestTimeout is the timeout for watch requests
	RequestTimeout time.Duration
	// GetRequestTimeout is the timeout for snapshot get requests.
	GetRequestTimeout time.Duration
	// StartupDelayMin is the minimum randomized delay before first watch
	StartupDelayMin time.Duration
	// StartupDelayMax is the maximum randomized delay before first watch
	StartupDelayMax time.Duration
}

// EventFilter 定义EventFilter类型。
type EventFilter int

const (
	FilterNone EventFilter = iota
	FilterPut
	FilterDelete
	FilterPrevKV
)

// DefaultWatchConfig 返回默认的配置。
func DefaultWatchConfig() WatchConfig {
	return WatchConfig{
		StartRevision:     0,
		PrevKV:            false,
		ProgressNotify:    true,
		Filter:            FilterNone,
		RetryInterval:     15 * time.Second,
		RequestTimeout:    time.Hour,
		GetRequestTimeout: 3 * time.Minute,
		StartupDelayMin:   0,
		StartupDelayMax:   0,
	}
}

// EventHandler 定义EventHandler回调函数类型。
type EventHandler func(event EtcdWatchEvent)

// ErrorHandler 定义ErrorHandler回调函数类型。
type ErrorHandler func(err error)

// BatchConfig 定义BatchConfig配置结构。
type BatchConfig struct {
	// Enabled indicates whether batch processing is active
	Enabled bool
	// MaxBatchSize is the maximum number of events to accumulate before triggering handler
	MaxBatchSize int
	// MaxBatchDelay is the maximum time to wait before flushing accumulated events
	MaxBatchDelay time.Duration
}

// BatchEventHandler 定义BatchEventHandler回调函数类型。
type BatchEventHandler func(events []*EtcdWatchEvent)

// DefaultBatchConfig 返回默认的配置。
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		Enabled:       true,
		MaxBatchSize:  100,
		MaxBatchDelay: 500 * time.Millisecond,
	}
}

// EtcdWatcher 定义EtcdWatcher类型。
type EtcdWatcher struct {
	client            client.EtcdClient
	logger            *log.Logger
	config            WatchConfig
	handler           EventHandler
	errorHandler      ErrorHandler
	snapshotHandler   func(revision int64, nodeCount int)
	connectionHandler func(state ConnectionState, revision int64)
	watchErrorHandler func(kind WatchErrorKind, err error)

	mu        sync.RWMutex
	isRunning bool
	stopChan  chan struct{}
	wg        sync.WaitGroup

	// Current watch state
	watchRevision int64
	isConnected   bool
	everConnected bool
	lastRevision  int64

	// Batch processing
	batchConfig  BatchConfig
	batchHandler BatchEventHandler
	batchBuffer  []*EtcdWatchEvent
	batchTimer   *time.Timer
	batchMutex   sync.Mutex

	startupOnce sync.Once

	snapshotLoadedHandler  func(nodes []*EtcdWatchEvent)
	snapshotLoadingHandler func()

	callbackMu       sync.Mutex
	callbackList     []EventHandler
	callbackOnceList []EventHandler
}

type ConnectionState int

const (
	ConnectionStateConnected ConnectionState = iota
	ConnectionStateDisconnected
	ConnectionStateReconnected
)

type WatchErrorKind int

const (
	WatchErrorUnknown WatchErrorKind = iota
	WatchErrorCompacted
	WatchErrorCanceled
)

func (w *EtcdWatcher) applyStartupDelay(ctx context.Context) {
	w.mu.RLock()
	minDelay := w.config.StartupDelayMin
	maxDelay := w.config.StartupDelayMax
	w.mu.RUnlock()

	if maxDelay <= 0 {
		return
	}
	if minDelay < 0 {
		minDelay = 0
	}
	if maxDelay < minDelay {
		maxDelay = minDelay
	}

	if maxDelay == 0 {
		return
	}

	var delay time.Duration
	if maxDelay == minDelay {
		delay = maxDelay
	} else {
		jitter := time.Duration(randInt63n(int64(maxDelay - minDelay)))
		delay = minDelay + jitter
	}

	if delay <= 0 {
		return
	}

	select {
	case <-time.After(delay):
	case <-ctx.Done():
	case <-w.stopChan:
	}
}

func (w *EtcdWatcher) getStartRevision() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.watchRevision > 0 {
		return w.watchRevision
	}
	return w.config.StartRevision
}

func (w *EtcdWatcher) loadSnapshot(ctx context.Context) error {
	w.mu.RLock()
	currentRevision := w.watchRevision
	startRevision := w.config.StartRevision
	key := w.config.Key
	rangeEnd := w.config.RangeEnd
	getRequestTimeout := w.config.GetRequestTimeout
	w.mu.RUnlock()

	// Allow snapshot load when watchRevision is 0 (initial or after compaction)
	// Skip if we have explicit startRevision (user wants to watch from specific point)
	if currentRevision > 0 && startRevision > 0 {
		return nil
	}

	if w.client == nil {
		return nil
	}

	snapshotCtx := ctx
	var cancel context.CancelFunc
	if getRequestTimeout > 0 {
		snapshotCtx, cancel = context.WithTimeout(ctx, getRequestTimeout)
		defer cancel()
	}

	opts := []clientv3.OpOption{}
	if rangeEnd != "" {
		opts = append(opts, clientv3.WithRange(rangeEnd))
	}

	w.mu.RLock()
	loadingHandler := w.snapshotLoadingHandler
	snapshotLoadedHandler := w.snapshotLoadedHandler
	w.mu.RUnlock()
	if loadingHandler != nil {
		loadingHandler()
	}

	var snapshotEvents []*EtcdWatchEvent
	defer func() {
		if snapshotLoadedHandler != nil {
			snapshotLoadedHandler(snapshotEvents)
		}
	}()

	resp, err := w.client.Get(snapshotCtx, key, opts...)
	if err != nil {
		return err
	}

	snapshotEvents = make([]*EtcdWatchEvent, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		event := &clientv3.Event{
			Type: mvccpb.PUT,
			Kv:   kv,
		}
		watchEvent, parseErr := w.parseEvent(event)
		if parseErr != nil {
			w.logger.Error("failed to parse snapshot event", "error", parseErr)
			continue
		}
		snapshotEvents = append(snapshotEvents, watchEvent)
	}

	w.mu.Lock()
	w.watchRevision = resp.Header.Revision + 1
	w.lastRevision = resp.Header.Revision
	snapshotHandler := w.snapshotHandler
	loadedCount := len(resp.Kvs)
	w.mu.Unlock()

	if snapshotHandler != nil {
		snapshotHandler(resp.Header.Revision, loadedCount)
	}
	return nil
}

func randInt63n(max int64) int64 {
	if max <= 0 {
		return 0
	}
	jitterRandMu.Lock()
	defer jitterRandMu.Unlock()
	return jitterRand.Int63n(max)
}

var (
	jitterRandMu sync.Mutex
	jitterRand   = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// NewEtcdWatcher 创建并返回EtcdWatcher。
func NewEtcdWatcher(etcdClient client.EtcdClient, cfg WatchConfig, logger *log.Logger) *EtcdWatcher {
	if logger == nil {
		logger = log.Default()
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = DefaultWatchConfig().RetryInterval
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultWatchConfig().RequestTimeout
	}
	if cfg.GetRequestTimeout <= 0 {
		cfg.GetRequestTimeout = DefaultWatchConfig().GetRequestTimeout
	}

	return &EtcdWatcher{
		client:           etcdClient,
		logger:           logger,
		config:           cfg,
		stopChan:         make(chan struct{}),
		batchConfig:      BatchConfig{Enabled: false},
		batchBuffer:      make([]*EtcdWatchEvent, 0, DefaultBatchConfig().MaxBatchSize),
		callbackList:     make([]EventHandler, 0, 8),
		callbackOnceList: make([]EventHandler, 0, 8),
	}
}

// SetHandler 设置Handler。
func (w *EtcdWatcher) SetHandler(handler EventHandler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handler = handler
}

// AddHandler 添加Handler。
func (w *EtcdWatcher) AddHandler(handler EventHandler) {
	if handler == nil {
		return
	}
	w.callbackMu.Lock()
	defer w.callbackMu.Unlock()
	w.callbackList = append(w.callbackList, handler)
}

// AddOneShotHandler 添加OneShotHandler。
func (w *EtcdWatcher) AddOneShotHandler(handler EventHandler) {
	if handler == nil {
		return
	}
	w.callbackMu.Lock()
	defer w.callbackMu.Unlock()
	w.callbackOnceList = append(w.callbackOnceList, handler)
}

func (w *EtcdWatcher) dispatchEvent(event EtcdWatchEvent) {
	w.mu.RLock()
	mainHandler := w.handler
	w.mu.RUnlock()
	if mainHandler != nil {
		mainHandler(event)
	}

	regularHandlers, onceHandlers := w.snapshotHandlersForDispatch()
	dispatchHandlers(regularHandlers, event)
	dispatchHandlers(onceHandlers, event)
}

func (w *EtcdWatcher) snapshotHandlersForDispatch() ([]EventHandler, []EventHandler) {
	w.callbackMu.Lock()
	defer w.callbackMu.Unlock()

	regularHandlers := snapshotEventHandlers(w.callbackList)
	onceHandlers := snapshotEventHandlers(w.callbackOnceList)
	w.callbackOnceList = w.callbackOnceList[:0]
	return regularHandlers, onceHandlers
}

func snapshotEventHandlers(source []EventHandler) []EventHandler {
	handlers := make([]EventHandler, 0, len(source))
	for _, handler := range source {
		if handler == nil {
			continue
		}
		handlers = append(handlers, handler)
	}
	return handlers
}

func dispatchHandlers(handlers []EventHandler, event EtcdWatchEvent) {
	for _, handler := range handlers {
		handler(event)
	}
}

// SetErrorHandler 设置ErrorHandler。
func (w *EtcdWatcher) SetErrorHandler(handler ErrorHandler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.errorHandler = handler
}

// SetSnapshotHandler 设置SnapshotHandler。
func (w *EtcdWatcher) SetSnapshotHandler(handler func(revision int64, nodeCount int)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.snapshotHandler = handler
}

// SetSnapshotLoadedHandler 设置SnapshotLoadedHandler。
func (w *EtcdWatcher) SetSnapshotLoadedHandler(handler func(nodes []*EtcdWatchEvent)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.snapshotLoadedHandler = handler
}

// SetSnapshotLoadingHandler 设置SnapshotLoadingHandler。
func (w *EtcdWatcher) SetSnapshotLoadingHandler(handler func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.snapshotLoadingHandler = handler
}

// SetConnectionHandler 设置ConnectionHandler。
func (w *EtcdWatcher) SetConnectionHandler(handler func(state ConnectionState, revision int64)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.connectionHandler = handler
}

// SetWatchErrorHandler 设置WatchErrorHandler。
func (w *EtcdWatcher) SetWatchErrorHandler(handler func(kind WatchErrorKind, err error)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.watchErrorHandler = handler
}

// SetBatchMode 设置BatchMode。
func (w *EtcdWatcher) SetBatchMode(config BatchConfig) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.batchConfig = config
}

// SetBatchHandler 设置BatchHandler。
func (w *EtcdWatcher) SetBatchHandler(handler BatchEventHandler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.batchHandler = handler
}

// Start 启动Start。
func (w *EtcdWatcher) Start(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context is required for watcher start")
	}

	w.mu.Lock()
	if w.isRunning {
		w.mu.Unlock()
		return fmt.Errorf("watcher is already running")
	}
	w.isRunning = true
	w.stopChan = make(chan struct{})
	w.mu.Unlock()

	w.startWatchLoop(ctx)

	w.logger.Info("watcher started",
		"key", w.config.Key,
		"range_end", w.config.RangeEnd,
		"start_revision", w.config.StartRevision)

	return nil
}

func (w *EtcdWatcher) startWatchLoop(ctx context.Context) {
	w.wg.Add(1)
	go w.watchLoop(ctx)
}

// Stop 停止Stop。
func (w *EtcdWatcher) Stop() {
	w.mu.Lock()
	if !w.isRunning {
		w.mu.Unlock()
		return
	}
	w.isRunning = false
	stopChan := w.stopChan
	w.mu.Unlock()

	close(stopChan)
	w.wg.Wait()

	w.logger.Info("watcher stopped", "key", w.config.Key)
}

// IsRunning 判断是否满足Running条件。
func (w *EtcdWatcher) IsRunning() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.isRunning
}

// IsConnected 判断是否满足Connected条件。
func (w *EtcdWatcher) IsConnected() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.isConnected
}

// GetWatchRevision 获取WatchRevision。
func (w *EtcdWatcher) GetWatchRevision() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.watchRevision
}

// GetKey returns watch key for C++ actor-style cluster APIs.
func (w *EtcdWatcher) GetKey() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.config.Key
}

// watchLoop 实现。
func (w *EtcdWatcher) watchLoop(ctx context.Context) {
	defer w.wg.Done()

	w.startupOnce.Do(func() {
		w.applyStartupDelay(ctx)
	})

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("watcher context cancelled")
			return
		case <-w.stopChan:
			w.logger.Debug("watcher stopped")
			return
		default:
			if err := w.watch(ctx); err != nil {
				w.logger.Error("watch failed", "error", err)
				w.handleError(err)

				// Wait before retry
				select {
				case <-time.After(w.config.RetryInterval):
				case <-ctx.Done():
					return
				case <-w.stopChan:
					return
				}
			}
		}
	}
}

// watch 实现。
func (w *EtcdWatcher) watch(ctx context.Context) error {
	w.mu.RLock()
	batchEnabled := w.batchConfig.Enabled
	w.mu.RUnlock()

	if err := w.loadSnapshot(ctx); err != nil {
		return err
	}

	if batchEnabled {
		return w.watchWithBatch(ctx)
	}
	return w.watchDirect(ctx)
}

// watchDirect 实现。
func (w *EtcdWatcher) watchDirect(ctx context.Context) error {
	watchCtx, cancel := w.buildWatchContext(ctx)
	defer cancel()

	opts := w.buildWatchOptions()
	w.resetConnectionState()
	watchChan := w.client.Watch(watchCtx, w.config.Key, opts...)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-watchCtx.Done():
			if errors.Is(watchCtx.Err(), context.DeadlineExceeded) {
				w.logger.Debug("watch request timeout, restart watch", "key", w.config.Key)
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return watchCtx.Err()
		case <-w.stopChan:
			return nil
		case watchResp, ok := <-watchChan:
			if !ok {
				return w.handleClosedWatchChannel(WatchErrorUnknown, fmt.Errorf("watch channel closed"))
			}

			if watchResp.Canceled {
				return w.handleCanceledWatch(ctx, watchResp)
			}

			if err := watchResp.Err(); err != nil {
				return w.handleWatchResponseError(err)
			}

			w.updateConnectionState(watchResp)

			for _, event := range watchResp.Events {
				watchEvent, err := w.parseEvent(event)
				if err != nil {
					w.logger.Error("failed to parse watch event", "error", err)
					continue
				}

				w.dispatchEvent(*watchEvent)
			}
		}
	}
}

// watchWithBatch 实现。
func (w *EtcdWatcher) watchWithBatch(ctx context.Context) error {
	watchCtx, cancel := w.buildWatchContext(ctx)
	defer cancel()

	opts := w.buildWatchOptions()
	w.resetConnectionState()
	watchChan := w.client.Watch(watchCtx, w.config.Key, opts...)

	// Initialize batch buffer and timer
	w.batchMutex.Lock()
	w.batchBuffer = make([]*EtcdWatchEvent, 0, w.batchConfig.MaxBatchSize)
	w.batchTimer = time.NewTimer(w.batchConfig.MaxBatchDelay)
	w.batchTimer.Stop()
	w.batchMutex.Unlock()

	batchTimerChan := make(<-chan time.Time)

	for {
		select {
		case <-ctx.Done():
			w.flushBatch()
			return ctx.Err()

		case <-watchCtx.Done():
			w.flushBatch()
			if errors.Is(watchCtx.Err(), context.DeadlineExceeded) {
				w.logger.Debug("watch request timeout, restart watch", "key", w.config.Key)
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return watchCtx.Err()

		case <-w.stopChan:
			w.flushBatch()
			return nil

		case watchResp, ok := <-watchChan:
			if !ok {
				w.flushBatch()
				return w.handleClosedWatchChannel(WatchErrorUnknown, fmt.Errorf("watch channel closed"))
			}

			if watchResp.Canceled {
				w.flushBatch()
				return w.handleCanceledWatch(ctx, watchResp)
			}

			if err := watchResp.Err(); err != nil {
				w.flushBatch()
				return w.handleWatchResponseError(err)
			}

			w.updateConnectionState(watchResp)

			for _, event := range watchResp.Events {
				watchEvent, err := w.parseEvent(event)
				if err != nil {
					w.logger.Error("failed to parse watch event", "error", err)
					continue
				}

				w.addToBatch(watchEvent)
			}

			w.batchMutex.Lock()
			batchTimerChan = w.batchTimer.C
			w.batchMutex.Unlock()

		case <-batchTimerChan:
			w.flushBatch()
			w.batchMutex.Lock()
			batchTimerChan = make(<-chan time.Time)
			w.batchMutex.Unlock()
		}
	}
}

// addToBatch 添加ToBatch。
func (w *EtcdWatcher) addToBatch(event *EtcdWatchEvent) {
	w.batchMutex.Lock()
	defer w.batchMutex.Unlock()

	w.batchBuffer = append(w.batchBuffer, event)

	// Start timer if this is the first event in batch
	if len(w.batchBuffer) == 1 {
		w.batchTimer.Reset(w.batchConfig.MaxBatchDelay)
	}

	// Flush if batch reaches size limit
	if len(w.batchBuffer) >= w.batchConfig.MaxBatchSize {
		w.flushBatchLocked()
	}
}

// flushBatch 实现。
func (w *EtcdWatcher) flushBatch() {
	w.batchMutex.Lock()
	defer w.batchMutex.Unlock()
	w.flushBatchLocked()
}

// flushBatchLocked 实现。
func (w *EtcdWatcher) flushBatchLocked() {
	if len(w.batchBuffer) == 0 {
		w.batchTimer.Stop()
		return
	}

	// Copy buffer to prevent data races
	events := make([]*EtcdWatchEvent, len(w.batchBuffer))
	copy(events, w.batchBuffer)
	w.batchBuffer = w.batchBuffer[:0]

	w.batchTimer.Stop()

	// Get handler reference
	w.mu.RLock()
	handler := w.batchHandler
	w.mu.RUnlock()

	// Release batch lock before calling handler to prevent deadlocks
	w.batchMutex.Unlock()

	if handler != nil {
		handler(events)
	}

	w.batchMutex.Lock()
}

// parseEvent 解析Event。
func (w *EtcdWatcher) parseEvent(event *clientv3.Event) (*EtcdWatchEvent, error) {
	watchEvent := &EtcdWatchEvent{
		Key:         string(event.Kv.Key),
		Revision:    event.Kv.ModRevision,
		DataVersion: etcdversion.New(event.Kv.CreateRevision, event.Kv.ModRevision, event.Kv.Version),
	}

	// Set event type
	switch event.Type {
	case mvccpb.PUT:
		watchEvent.Type = pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT
	case mvccpb.DELETE:
		watchEvent.Type = pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE
	}

	watchEvent.RawValue = append([]byte(nil), event.Kv.Value...)
	watchEvent.Value = internalcodec.DecodeDiscoveryValue(event.Kv.Value)
	if event.PrevKv != nil {
		watchEvent.RawPrevValue = append([]byte(nil), event.PrevKv.Value...)
		watchEvent.PrevValue = internalcodec.DecodeDiscoveryValue(event.PrevKv.Value)
		if event.PrevKv.CreateRevision > watchEvent.CreateRevision {
			watchEvent.CreateRevision = event.PrevKv.CreateRevision
		}
		if event.PrevKv.ModRevision > watchEvent.ModRevision {
			watchEvent.ModRevision = event.PrevKv.ModRevision
			watchEvent.Revision = watchEvent.ModRevision
		}
		if event.PrevKv.Version > watchEvent.Version {
			watchEvent.Version = event.PrevKv.Version
		}
	}

	return watchEvent, nil
}

func (w *EtcdWatcher) buildWatchOptions() []clientv3.OpOption {
	opts := []clientv3.OpOption{
		clientv3.WithRev(w.getStartRevision()),
	}

	if w.config.PrevKV {
		opts = append(opts, clientv3.WithPrevKV())
	}

	if w.config.ProgressNotify {
		opts = append(opts, clientv3.WithProgressNotify())
	}

	if w.config.RangeEnd != "" {
		opts = append(opts, clientv3.WithRange(w.config.RangeEnd))
	}

	switch w.config.Filter {
	case FilterPut:
		opts = append(opts, clientv3.WithFilterPut())
	case FilterDelete:
		opts = append(opts, clientv3.WithFilterDelete())
	}

	return opts
}

func (w *EtcdWatcher) buildWatchContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}

	w.mu.RLock()
	timeout := w.config.RequestTimeout
	w.mu.RUnlock()

	if timeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, timeout)
}

func (w *EtcdWatcher) resetConnectionState() {
	w.mu.Lock()
	w.isConnected = false
	w.mu.Unlock()
}

func (w *EtcdWatcher) handleClosedWatchChannel(kind WatchErrorKind, err error) error {
	if errHandler := w.watchErrorHandler; errHandler != nil {
		errHandler(kind, err)
	}
	return err
}

func (w *EtcdWatcher) handleWatchResponseError(err error) error {
	if errHandler := w.watchErrorHandler; errHandler != nil {
		errHandler(WatchErrorUnknown, err)
	}
	return err
}

func (w *EtcdWatcher) updateConnectionState(watchResp clientv3.WatchResponse) {
	w.mu.Lock()
	stateChanged := !w.isConnected
	state := ConnectionStateConnected
	if stateChanged {
		if w.everConnected {
			state = ConnectionStateReconnected
		}
		w.isConnected = true
		w.everConnected = true
	}
	if watchResp.Header.Revision > 0 {
		w.lastRevision = watchResp.Header.Revision
		headerNext := watchResp.Header.Revision + 1
		if headerNext > w.watchRevision {
			w.watchRevision = headerNext
		}
	}
	if len(watchResp.Events) > 0 {
		eventNext := watchResp.Events[len(watchResp.Events)-1].Kv.ModRevision + 1
		if eventNext > w.watchRevision {
			w.watchRevision = eventNext
		}
	}
	connHandler := w.connectionHandler
	lastRevision := w.lastRevision
	w.mu.Unlock()
	if connHandler != nil && stateChanged {
		connHandler(state, lastRevision)
	}
}

func (w *EtcdWatcher) handleCanceledWatch(ctx context.Context, watchResp clientv3.WatchResponse) error {
	w.mu.RLock()
	connHandler := w.connectionHandler
	errHandler := w.watchErrorHandler
	lastRevision := w.lastRevision
	w.mu.RUnlock()
	if connHandler != nil {
		connHandler(ConnectionStateDisconnected, lastRevision)
	}

	if watchResp.CompactRevision > 0 {
		w.mu.Lock()
		if w.batchConfig.Enabled {
			w.watchRevision = watchResp.CompactRevision + 1
		} else {
			w.watchRevision = 0
		}
		w.mu.Unlock()
		if loadErr := w.loadSnapshot(ctx); loadErr != nil {
			w.logger.Warn("failed to load snapshot after compaction", "error", loadErr)
		}
		if errHandler != nil {
			errHandler(WatchErrorCompacted, watchResp.Err())
		}
		return fmt.Errorf("watch compacted at %d", watchResp.CompactRevision)
	}
	if errHandler != nil {
		errHandler(WatchErrorCanceled, watchResp.Err())
	}
	return fmt.Errorf("watch cancelled: %v", watchResp.Err())
}

// handleError 处理Error。
func (w *EtcdWatcher) handleError(err error) {
	w.mu.RLock()
	errorHandler := w.errorHandler
	w.mu.RUnlock()

	if errorHandler != nil {
		errorHandler(err)
	}
}

// EtcdWatcherManager 定义EtcdWatcherManager管理器结构。
type EtcdWatcherManager struct {
	logger   *log.Logger
	watchers map[string]*EtcdWatcher
	mu       sync.RWMutex
}

// NewEtcdWatcherManager 创建并返回EtcdWatcherManager。
func NewEtcdWatcherManager(logger *log.Logger) *EtcdWatcherManager {
	if logger == nil {
		logger = log.Default()
	}
	return &EtcdWatcherManager{
		logger:   logger,
		watchers: make(map[string]*EtcdWatcher),
	}
}

// AddWatcher 添加Watcher。
func (m *EtcdWatcherManager) AddWatcher(key string, watcher *EtcdWatcher) {
	_ = m.AddWatcherIfAbsent(key, watcher)
}

// AddWatcherIfAbsent adds a watcher only when key does not exist.
func (m *EtcdWatcherManager) AddWatcherIfAbsent(key string, watcher *EtcdWatcher) bool {
	if key == "" || watcher == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.watchers[key]; ok {
		return false
	}
	m.watchers[key] = watcher
	return true
}

// RemoveWatcher 移除Watcher。
func (m *EtcdWatcherManager) RemoveWatcher(key string) {
	m.mu.Lock()
	watcher, ok := m.watchers[key]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(m.watchers, key)
	m.mu.Unlock()

	// 移除前停止 watcher 的 goroutine，避免泄漏
	watcher.Stop()
}

// GetWatcher 获取Watcher。
func (m *EtcdWatcherManager) GetWatcher(key string) (*EtcdWatcher, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	watcher, ok := m.watchers[key]
	return watcher, ok
}

// StartAll 启动All。
func (m *EtcdWatcherManager) StartAll(ctx context.Context) error {
	m.mu.RLock()
	items := make(map[string]*EtcdWatcher, len(m.watchers))
	for key, w := range m.watchers {
		items[key] = w
	}
	m.mu.RUnlock()

	for key, watcher := range items {
		if err := watcher.Start(ctx); err != nil {
			m.logger.Error("failed to start watcher", "key", key, "error", err)
			return err
		}
	}
	return nil
}

// StopAll 停止All。
func (m *EtcdWatcherManager) StopAll() {
	m.mu.Lock()
	items := make([]*EtcdWatcher, 0, len(m.watchers))
	for key, watcher := range m.watchers {
		items = append(items, watcher)
		delete(m.watchers, key)
	}
	m.mu.Unlock()

	for _, watcher := range items {
		if watcher != nil {
			watcher.Stop()
		}
	}
}

// ActiveAll starts watchers not running and keeps existing running watchers unchanged.
func (m *EtcdWatcherManager) ActiveAll(ctx context.Context) error {
	m.mu.RLock()
	items := make(map[string]*EtcdWatcher, len(m.watchers))
	for key, w := range m.watchers {
		items[key] = w
	}
	m.mu.RUnlock()

	var errs []error
	for key, watcher := range items {
		if watcher == nil || watcher.IsRunning() {
			continue
		}
		if err := watcher.Start(ctx); err != nil {
			m.logger.Error("failed to activate watcher", "key", key, "error", err)
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// CreateServiceWatcher 实现。
func CreateServiceWatcher(etcdClient client.EtcdClient, servicePrefix string, handler EventHandler, logger *log.Logger) *EtcdWatcher {
	cfg := DefaultWatchConfig()
	cfg.Key = servicePrefix
	cfg.RangeEnd = clientv3.GetPrefixRangeEnd(servicePrefix) // Watch range for prefix
	cfg.PrevKV = true

	watcher := NewEtcdWatcher(etcdClient, cfg, logger)
	watcher.SetHandler(handler)
	return watcher
}

// EtcdWatchResponse 定义EtcdWatchResponse类型。
type EtcdWatchResponse struct {
	Events          []EtcdWatchEvent
	Revision        int64
	Canceled        bool
	CompactRevision int64
}

// MarshalJSON 实现。
func (e EtcdWatchEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"type":       e.Type.String(),
		"key":        e.Key,
		"value":      e.Value,
		"revision":   e.Revision,
		"prev_value": e.PrevValue,
	})
}

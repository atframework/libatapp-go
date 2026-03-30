package cluster

import "sync/atomic"

// ClusterStats 定义ClusterStats类型。
type ClusterStats struct {
	watcherStarts     uint64
	watcherFailures   uint64
	watcherCompaction uint64
	watcherCanceled   uint64
	sumErrorRequests  uint64
	contErrorRequests uint64
	sumSuccessReqs    uint64
	contSuccessReqs   uint64
	sumCreateRequests uint64

	keepaliveRegistered uint64
	keepaliveFailed     uint64
	keepaliveRetryQueue uint64
	keepaliveRetryOK    uint64
	keepaliveRetryFail  uint64
	keepaliveDeleteQ    uint64
	keepaliveDeleteOK   uint64
	keepaliveDeleteFail uint64
	retryQueueSize      int64
	deleteQueueSize     int64

	lastWatcherErrorAt   int64
	lastWatcherErrorKind int32
	lastKeepaliveErrorAt int64
}

func (s *ClusterStats) IncWatcherStart() {
	atomic.AddUint64(&s.watcherStarts, 1)
}

func (s *ClusterStats) IncWatcherFailure() {
	atomic.AddUint64(&s.watcherFailures, 1)
}

func (s *ClusterStats) IncWatcherCompaction() {
	atomic.AddUint64(&s.watcherCompaction, 1)
}

func (s *ClusterStats) IncWatcherCanceled() {
	atomic.AddUint64(&s.watcherCanceled, 1)
}

func (s *ClusterStats) IncKeepaliveRegistered() {
	atomic.AddUint64(&s.keepaliveRegistered, 1)
}

func (s *ClusterStats) IncKeepaliveFailed() {
	atomic.AddUint64(&s.keepaliveFailed, 1)
}

func (s *ClusterStats) IncKeepaliveRetryQueued() {
	atomic.AddUint64(&s.keepaliveRetryQueue, 1)
}

func (s *ClusterStats) IncKeepaliveRetrySuccess() {
	atomic.AddUint64(&s.keepaliveRetryOK, 1)
}

func (s *ClusterStats) IncKeepaliveRetryFailure() {
	atomic.AddUint64(&s.keepaliveRetryFail, 1)
}

func (s *ClusterStats) IncKeepaliveDeleteQueued() {
	atomic.AddUint64(&s.keepaliveDeleteQ, 1)
}

func (s *ClusterStats) IncKeepaliveDeleteSuccess() {
	atomic.AddUint64(&s.keepaliveDeleteOK, 1)
}

func (s *ClusterStats) IncKeepaliveDeleteFailure() {
	atomic.AddUint64(&s.keepaliveDeleteFail, 1)
}

func (s *ClusterStats) RecordWatcherError(kind int32, at int64) {
	atomic.StoreInt32(&s.lastWatcherErrorKind, kind)
	atomic.StoreInt64(&s.lastWatcherErrorAt, at)
}

func (s *ClusterStats) RecordKeepaliveError(at int64) {
	atomic.StoreInt64(&s.lastKeepaliveErrorAt, at)
}

func (s *ClusterStats) SetRetryQueueSize(v int64) {
	atomic.StoreInt64(&s.retryQueueSize, v)
}

func (s *ClusterStats) SetDeleteQueueSize(v int64) {
	atomic.StoreInt64(&s.deleteQueueSize, v)
}

// AddStatsCreateRequest mirrors C++ stats_t::sum_create_requests.
func (s *ClusterStats) AddStatsCreateRequest() {
	atomic.AddUint64(&s.sumCreateRequests, 1)
}

// AddStatsErrorRequest mirrors C++ stats_t error counters.
func (s *ClusterStats) AddStatsErrorRequest() {
	atomic.AddUint64(&s.sumErrorRequests, 1)
	atomic.AddUint64(&s.contErrorRequests, 1)
	atomic.StoreUint64(&s.contSuccessReqs, 0)
}

// AddStatsSuccessRequest mirrors C++ stats_t success counters.
func (s *ClusterStats) AddStatsSuccessRequest() {
	atomic.AddUint64(&s.sumSuccessReqs, 1)
	atomic.AddUint64(&s.contSuccessReqs, 1)
	atomic.StoreUint64(&s.contErrorRequests, 0)
}

// Reset clears all counters.
func (s *ClusterStats) Reset() {
	atomic.StoreUint64(&s.watcherStarts, 0)
	atomic.StoreUint64(&s.watcherFailures, 0)
	atomic.StoreUint64(&s.watcherCompaction, 0)
	atomic.StoreUint64(&s.watcherCanceled, 0)
	atomic.StoreUint64(&s.sumErrorRequests, 0)
	atomic.StoreUint64(&s.contErrorRequests, 0)
	atomic.StoreUint64(&s.sumSuccessReqs, 0)
	atomic.StoreUint64(&s.contSuccessReqs, 0)
	atomic.StoreUint64(&s.sumCreateRequests, 0)

	atomic.StoreUint64(&s.keepaliveRegistered, 0)
	atomic.StoreUint64(&s.keepaliveFailed, 0)
	atomic.StoreUint64(&s.keepaliveRetryQueue, 0)
	atomic.StoreUint64(&s.keepaliveRetryOK, 0)
	atomic.StoreUint64(&s.keepaliveRetryFail, 0)
	atomic.StoreUint64(&s.keepaliveDeleteQ, 0)
	atomic.StoreUint64(&s.keepaliveDeleteOK, 0)
	atomic.StoreUint64(&s.keepaliveDeleteFail, 0)
	atomic.StoreInt64(&s.retryQueueSize, 0)
	atomic.StoreInt64(&s.deleteQueueSize, 0)
	atomic.StoreInt64(&s.lastWatcherErrorAt, 0)
	atomic.StoreInt32(&s.lastWatcherErrorKind, 0)
	atomic.StoreInt64(&s.lastKeepaliveErrorAt, 0)
}

type ClusterStatsSnapshot struct {
	WatcherStarts         uint64
	WatcherFailures       uint64
	SumErrorRequests      uint64
	ContinueErrorRequests uint64
	SumSuccessRequests    uint64
	ContinueSuccessReqs   uint64
	SumCreateRequests     uint64
	KeepaliveRegistered   uint64
	KeepaliveFailed       uint64
	KeepaliveRetryFail    uint64
	KeepaliveDeleteFail   uint64
	RetryQueueSize        int64
	DeleteQueueSize       int64
}

func (s *ClusterStats) Snapshot() ClusterStatsSnapshot {
	return ClusterStatsSnapshot{
		WatcherStarts:         atomic.LoadUint64(&s.watcherStarts),
		WatcherFailures:       atomic.LoadUint64(&s.watcherFailures),
		SumErrorRequests:      atomic.LoadUint64(&s.sumErrorRequests),
		ContinueErrorRequests: atomic.LoadUint64(&s.contErrorRequests),
		SumSuccessRequests:    atomic.LoadUint64(&s.sumSuccessReqs),
		ContinueSuccessReqs:   atomic.LoadUint64(&s.contSuccessReqs),
		SumCreateRequests:     atomic.LoadUint64(&s.sumCreateRequests),
		KeepaliveRegistered:   atomic.LoadUint64(&s.keepaliveRegistered),
		KeepaliveFailed:       atomic.LoadUint64(&s.keepaliveFailed),
		KeepaliveRetryFail:    atomic.LoadUint64(&s.keepaliveRetryFail),
		KeepaliveDeleteFail:   atomic.LoadUint64(&s.keepaliveDeleteFail),
		RetryQueueSize:        atomic.LoadInt64(&s.retryQueueSize),
		DeleteQueueSize:       atomic.LoadInt64(&s.deleteQueueSize),
	}
}

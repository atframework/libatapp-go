package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultKeepaliveConfig(t *testing.T) {
	// Arrange
	config := DefaultKeepaliveConfig()

	// Assert
	assert.Equal(t, 16*time.Second, config.Timeout, "Default keepalive timeout should be 16s")
	assert.Equal(t, 5*time.Second, config.Interval, "Default keepalive interval should be 5s")
	assert.Equal(t, 3*time.Second, config.RetryInterval, "Default keepalive retry interval should be 3s")
	assert.Equal(t, 8, config.RetryTimes, "Default keepalive retry times should be 8")
}

func TestDefaultWatcherConfig(t *testing.T) {
	// Arrange
	config := DefaultWatcherConfig()

	// Assert
	assert.Equal(t, 15*time.Second, config.RetryInterval, "Default watcher retry interval should be 15s")
	assert.Equal(t, 1*time.Hour, config.RequestTimeout, "Default watcher request timeout should be 1h")
	assert.Equal(t, 3*time.Minute, config.GetRequestTimeout, "Default watcher get request timeout should be 3m")
	assert.True(t, config.ProgressNotify, "Default watcher progress notify should be true")
	assert.False(t, config.PrevKV, "Default watcher prev_kv should be false")
}

func TestKeepaliveConfigConstants(t *testing.T) {
	// Assert
	assert.Equal(t, 16*time.Second, DefaultKeepaliveTimeout)
	assert.Equal(t, 5*time.Second, DefaultKeepaliveInterval)
	assert.Equal(t, 3*time.Second, DefaultKeepaliveRetryInterval)
	assert.Equal(t, 8, DefaultKeepaliveRetryTimes)
}

func TestWatcherConfigConstants(t *testing.T) {
	// Assert
	assert.Equal(t, 15*time.Second, DefaultWatcherRetryInterval)
	assert.Equal(t, 1*time.Hour, DefaultWatcherRequestTimeout)
	assert.Equal(t, 3*time.Minute, DefaultWatcherGetRequestTimeout)
	assert.True(t, DefaultWatcherProgressNotify)
	assert.False(t, DefaultWatcherPrevKV)
}

func TestKeepaliveConfigMatchesCppDefaults(t *testing.T) {
	// Arrange
	config := DefaultKeepaliveConfig()

	// Assert
	assert.Equal(t, 16*time.Second, config.Timeout, "Should match C++ keepalive_timeout default of 16s")
	assert.Equal(t, 5*time.Second, config.Interval, "Should match C++ keepalive_interval default of 5s")
	assert.Equal(t, 3*time.Second, config.RetryInterval, "Should match C++ keepalive_retry_interval default of 3s")
	assert.Equal(t, 8, config.RetryTimes, "Should match C++ keepalive_retry_times default of 8")
}

func TestWatcherConfigMatchesCppDefaults(t *testing.T) {
	// Arrange
	config := DefaultWatcherConfig()

	// Assert
	assert.Equal(t, 15*time.Second, config.RetryInterval, "Should match C++ watcher_retry_interval default of 15s")
	assert.Equal(t, 1*time.Hour, config.RequestTimeout, "Should match C++ watcher_request_timeout default of 1h")
	assert.Equal(t, 3*time.Minute, config.GetRequestTimeout, "Should match C++ watcher_get_request_timeout default of 3m")
	assert.True(t, config.ProgressNotify, "Should match C++ watcher_progress_notify default of true")
	assert.False(t, config.PrevKV, "Should match C++ watcher_prev_kv default of false")
}

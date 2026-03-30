package client

import "time"

const (
	DefaultKeepaliveTimeout       = 16 * time.Second
	DefaultKeepaliveInterval      = 5 * time.Second
	DefaultKeepaliveRetryInterval = 3 * time.Second
	DefaultKeepaliveRetryTimes    = 8

	DefaultWatcherRetryInterval     = 15 * time.Second
	DefaultWatcherRequestTimeout    = 1 * time.Hour
	DefaultWatcherGetRequestTimeout = 3 * time.Minute
	DefaultWatcherProgressNotify    = true
	DefaultWatcherPrevKV            = false
)

type KeepaliveConfig struct {
	Timeout       time.Duration
	Interval      time.Duration
	RetryInterval time.Duration
	RetryTimes    int
}

func DefaultKeepaliveConfig() *KeepaliveConfig {
	return &KeepaliveConfig{
		Timeout:       DefaultKeepaliveTimeout,
		Interval:      DefaultKeepaliveInterval,
		RetryInterval: DefaultKeepaliveRetryInterval,
		RetryTimes:    DefaultKeepaliveRetryTimes,
	}
}

type WatcherConfig struct {
	RetryInterval     time.Duration
	RequestTimeout    time.Duration
	GetRequestTimeout time.Duration
	ProgressNotify    bool
	PrevKV            bool
}

func DefaultWatcherConfig() *WatcherConfig {
	return &WatcherConfig{
		RetryInterval:     DefaultWatcherRetryInterval,
		RequestTimeout:    DefaultWatcherRequestTimeout,
		GetRequestTimeout: DefaultWatcherGetRequestTimeout,
		ProgressNotify:    DefaultWatcherProgressNotify,
		PrevKV:            DefaultWatcherPrevKV,
	}
}

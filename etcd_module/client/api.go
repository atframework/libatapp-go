//go:generate mockgen -destination=mocks/mock_etcd_client.go -package=mocks . EtcdClient,Watcher
package client

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// This file defines interfaces for the etcd client operations,
// allowing for easy mocking in unit tests.

// Watcher 定义Watcher接口。
type Watcher interface {
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
	Close() error
}

// EtcdClient 定义EtcdClient接口。
type EtcdClient interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
	KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
	Close() error
}

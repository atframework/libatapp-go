// Package orchestrator contains the four business actors and their shared
// infrastructure (EtcdClient interface, event types, payloads).
//
// Dependency rule: orchestrator → runtime, snapshot, clientv3 SDK.
// orchestrator must NOT import any other package under etcd_module/ or
// etcd_module_v2/ that is not in the internal/ tree.
package orchestrator

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdClient is the minimal interface that all actors use to talk to etcd.
// It is intentionally a local copy of etcd_module/client.EtcdClient so that
// the v2 package has zero imports from the sibling etcd_module package.
// Any value satisfying etcd_module/client.EtcdClient will automatically
// satisfy this interface (structural subtyping / duck typing).
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

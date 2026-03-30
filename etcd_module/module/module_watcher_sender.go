package module

import (
	"context"

	"github.com/atframework/libatapp-go/etcd_module/discovery"
	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// WatcherSender 定义WatcherSender类型。
type WatcherSender struct {
	AtappModule *EtcdModule
	EtcdHeader  *pb.EtcdResponseHeader
	EtcdBody    []*watcher.EtcdWatchEvent
	Event       *watcher.EtcdWatchEvent
	Node        *discovery.DiscoveryNode
	Action      DiscoveryAction
}

// WatcherSenderList 定义WatcherSenderList类型。
type WatcherSenderList = WatcherSender
type WatcherSenderOne = WatcherSender

// DiscoveryWatcherSender 定义DiscoveryWatcherSender类型。
type DiscoveryWatcherSender = WatcherSender

// WatcherSenderListCallback 定义WatcherSenderListCallback回调函数类型。
type WatcherSenderListCallback func(sender *WatcherSenderList)

// WatcherSenderOneCallback 定义WatcherSenderOneCallback回调函数类型。
type WatcherSenderOneCallback func(sender *WatcherSenderOne)

// DiscoveryWatcherListCallback 定义DiscoveryWatcherListCallback回调函数类型。
type DiscoveryWatcherListCallback = WatcherSenderListCallback

// DiscoveryWatcherOneCallback 定义DiscoveryWatcherOneCallback回调函数类型。
type DiscoveryWatcherOneCallback = WatcherSenderOneCallback

// AddWatcherByIDWithSender 添加WatcherByIDWithSender。
func (m *EtcdModule) AddWatcherByIDWithSender(ctx context.Context, fn WatcherSenderListCallback) error {
	return m.addWatcherWithSender(ctx, m.GetByIDWatcherPath(), fn)
}

// AddWatcherByNameWithSender 添加WatcherByNameWithSender。
func (m *EtcdModule) AddWatcherByNameWithSender(ctx context.Context, fn WatcherSenderListCallback) error {
	return m.addWatcherWithSender(ctx, m.GetByNameWatcherPath(), fn)
}

// AddWatcherByTypeIDWithSender 添加WatcherByTypeIDWithSender。
func (m *EtcdModule) AddWatcherByTypeIDWithSender(ctx context.Context, typeID uint64, fn WatcherSenderOneCallback) error {
	return m.addWatcherWithSender(ctx, m.GetByTypeIDWatcherPath(typeID), fn)
}

// AddWatcherByTypeNameWithSender 添加WatcherByTypeNameWithSender。
func (m *EtcdModule) AddWatcherByTypeNameWithSender(ctx context.Context, typeName string, fn WatcherSenderOneCallback) error {
	return m.addWatcherWithSender(ctx, m.GetByTypeNameWatcherPath(typeName), fn)
}

// AddWatcherByTagWithSender 添加WatcherByTagWithSender。
func (m *EtcdModule) AddWatcherByTagWithSender(ctx context.Context, tag string, fn WatcherSenderOneCallback) error {
	return m.addWatcherWithSender(ctx, m.GetByTagWatcherPath(tag), fn)
}

// AddWatcherByCustomPathWithSender 添加WatcherByCustomPathWithSender。
func (m *EtcdModule) AddWatcherByCustomPathWithSender(ctx context.Context, path string, fn WatcherSenderOneCallback) (*watcher.EtcdWatcher, error) {
	w, err := m.AddWatcherByCustomPathAndGet(ctx, path)
	if err != nil {
		return nil, err
	}
	if fn != nil {
		w.AddHandler(func(event watcher.EtcdWatchEvent) {
			fn(m.buildDiscoveryWatcherSender(event))
		})
	}
	return w, nil
}

func (m *EtcdModule) addWatcherWithSender(ctx context.Context, path string, fn func(sender *WatcherSender)) error {
	w, err := m.AddWatcherByCustomPathAndGet(ctx, path)
	if err != nil {
		return err
	}
	if fn == nil {
		return nil
	}
	w.AddHandler(func(event watcher.EtcdWatchEvent) {
		fn(m.buildDiscoveryWatcherSender(event))
	})
	return nil
}

func (m *EtcdModule) buildDiscoveryWatcherSender(event watcher.EtcdWatchEvent) *WatcherSender {
	e := event
	header := &pb.EtcdResponseHeader{Revision: e.Revision}
	m.setLastEtcdEventHeader(header)
	sender := &WatcherSender{
		AtappModule: m,
		EtcdHeader:  header,
		EtcdBody:    []*watcher.EtcdWatchEvent{&e},
		Event:       &e,
		Action:      discoveryActionFromWatchEvent(e.Type),
	}

	nodeInfo := e.Value
	if e.Type == pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE && e.PrevValue != nil {
		nodeInfo = e.PrevValue
	}
	if nodeInfo == nil {
		return sender
	}

	node := m.lookupDiscoveryNode(nodeInfo)
	if node == nil {
		node = &discovery.DiscoveryNode{
			Path:        e.Key,
			Info:        nodeInfo,
			DataVersion: etcdversion.New(e.CreateRevision, e.ModRevision, e.Version),
		}
	}
	sender.Node = node
	return sender
}

func (m *EtcdModule) lookupDiscoveryNode(info *pb.AtappDiscovery) *discovery.DiscoveryNode {
	ds, err := m.GetGlobalDiscovery()
	if err != nil || ds == nil || info == nil {
		return nil
	}
	if info.GetId() != 0 {
		if node := ds.GetNodeByID(info.GetId()); node != nil {
			return node
		}
	}
	if info.GetName() != "" {
		if node := ds.GetNodeByName(info.GetName()); node != nil {
			return node
		}
	}
	return nil
}

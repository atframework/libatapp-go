package module

import (
	"github.com/atframework/libatapp-go/etcd_module/events"
	"github.com/atframework/libatapp-go/etcd_module/internal/topology"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func (m *EtcdModule) dispatchOnLoadTopologySnapshot() {
	if m == nil {
		return
	}
	m.mu.RLock()
	callbacks := m.topology.getOnLoadSnapshotCallbacks()
	m.mu.RUnlock()
	if callbacks == nil {
		return
	}
	callbacks.Publish(&events.Event{})
}

func (m *EtcdModule) dispatchOnTopologySnapshotLoaded() {
	if m == nil {
		return
	}
	m.mu.RLock()
	callbacks := m.topology.getOnSnapshotLoadedCallbacks()
	m.mu.RUnlock()
	if callbacks == nil {
		return
	}
	callbacks.Publish(&events.Event{})
}

func (m *EtcdModule) dispatchOnTopologyInfoEvent(sender *TopologyWatcherSender) {
	if m == nil || sender == nil {
		return
	}
	m.mu.RLock()
	callbacks := m.topology.getOnInfoEventCallbacks()
	m.mu.RUnlock()
	if callbacks == nil {
		return
	}
	callbacks.Publish(&events.Event{
		Type:     topologyActionToEventType(sender.Action),
		Revision: sender.EtcdHeader.GetRevision(),
		Metadata: map[string]interface{}{topologySenderMetadataKey: sender},
	})
}

func (m *EtcdModule) buildTopologyWatcherSender(event watcher.EtcdWatchEvent) *TopologyWatcherSender {
	e := event
	header := &pb.EtcdResponseHeader{Revision: e.Revision}
	m.setLastEtcdEventTopologyHeader(header)

	payload := e.RawValue
	if e.Type == pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE && len(e.RawPrevValue) > 0 {
		payload = e.RawPrevValue
	}

	topInfo := topology.ParseInfo(e.Key, e.Revision, e.CreateRevision, e.ModRevision, e.Version, payload)
	return &TopologyWatcherSender{
		AtappModule: m,
		EtcdHeader:  header,
		EtcdBody:    []*watcher.EtcdWatchEvent{&e},
		Event:       &e,
		Topology:    topInfo,
		Action:      topologyActionFromWatchEvent(e.Type),
	}
}

func (m *EtcdModule) applyTopologyEvent(sender *TopologyWatcherSender) {
	if m == nil || sender == nil || sender.Event == nil || sender.Topology == nil {
		return
	}
	m.mu.Lock()
	if sender.Event.Type == pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE {
		if _, ok := m.topology.infoSet[sender.Topology.Path]; !ok {
			m.mu.Unlock()
			return
		}
		m.topology.deleteInfo(sender.Topology.Path)
		m.mu.Unlock()
		m.dispatchOnTopologyInfoEvent(sender)
		return
	}
	if cached, ok := m.topology.infoSet[sender.Topology.Path]; ok {
		if topology.SameRecord(cached, sender.Topology) {
			m.mu.Unlock()
			return
		}
	}
	m.topology.upsertInfo(sender.Topology)
	m.mu.Unlock()
	m.dispatchOnTopologyInfoEvent(sender)
}

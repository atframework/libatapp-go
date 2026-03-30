package module

import (
	"github.com/atframework/libatapp-go/etcd_module/events"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// DiscoveryAction 定义DiscoveryAction类型。
type DiscoveryAction = pb.EtcdWatchEventType

const (
	DiscoveryActionUnknown DiscoveryAction = pb.EtcdWatchEventType_ETCD_WATCH_EVENT_TYPE_UNSPECIFIED
	DiscoveryActionPut     DiscoveryAction = pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT
	DiscoveryActionDelete  DiscoveryAction = pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE
)

// TopologyAction 定义TopologyAction类型。
type TopologyAction = pb.EtcdWatchEventType

const (
	TopologyActionUnknown TopologyAction = pb.EtcdWatchEventType_ETCD_WATCH_EVENT_TYPE_UNSPECIFIED
	TopologyActionPut     TopologyAction = pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT
	TopologyActionDelete  TopologyAction = pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE
)

func discoveryActionFromWatchEvent(evtType pb.EtcdWatchEventType) DiscoveryAction {
	switch evtType {
	case DiscoveryActionDelete:
		return DiscoveryActionDelete
	case DiscoveryActionPut:
		return DiscoveryActionPut
	default:
		return DiscoveryActionUnknown
	}
}

func topologyActionFromWatchEvent(evtType pb.EtcdWatchEventType) TopologyAction {
	switch evtType {
	case TopologyActionDelete:
		return TopologyActionDelete
	case TopologyActionPut:
		return TopologyActionPut
	default:
		return TopologyActionUnknown
	}
}

func discoveryActionFromEventType(evtType events.EventType) DiscoveryAction {
	switch evtType {
	case events.EventTypeNodeUp, events.EventTypeNodeUpdate:
		return DiscoveryActionPut
	case events.EventTypeNodeDown:
		return DiscoveryActionDelete
	default:
		return DiscoveryActionUnknown
	}
}

func topologyActionToEventType(action TopologyAction) events.EventType {
	switch action {
	case TopologyActionPut:
		return events.EventTypeNodeUp
	case TopologyActionDelete:
		return events.EventTypeNodeDown
	default:
		return events.EventTypeNodeUpdate
	}
}

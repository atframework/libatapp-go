package module

import (
	"testing"

	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func TestBuildDiscoveryWatcherSender_UpdatesLastHeaderAndFields(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	e := watcher.EtcdWatchEvent{
		Type:        pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT,
		Key:         "/svc/by_id/app-1",
		Revision:    123,
		DataVersion: etcdversion.DataVersion{CreateRevision: 10, ModRevision: 11, Version: 2},
		Value: &pb.AtappDiscovery{
			Id:   1,
			Name: "app",
		},
	}

	sender := mod.buildDiscoveryWatcherSender(e)
	if sender == nil {
		t.Fatalf("expected sender")
	}
	if sender.Action != DiscoveryActionPut {
		t.Fatalf("unexpected action: %v", sender.Action)
	}
	if sender.EtcdHeader == nil || sender.EtcdHeader.GetRevision() != 123 {
		t.Fatalf("unexpected sender header: %+v", sender.EtcdHeader)
	}
	if sender.Node == nil || sender.Node.Info == nil || sender.Node.Info.GetId() != 1 {
		t.Fatalf("expected sender node from event value")
	}

	got := mod.GetLastEtcdEventHeader()
	if got == nil || got.GetRevision() != 123 {
		t.Fatalf("unexpected last header: %+v", got)
	}
	got.Revision = 999
	got2 := mod.GetLastEtcdEventHeader()
	if got2 == nil || got2.GetRevision() != 123 {
		t.Fatalf("expected cloned header, got %+v", got2)
	}
}

func TestBuildDiscoveryWatcherSender_DeleteUsesPrevValue(t *testing.T) {
	mod, _, ctrl := newTestModule(t)
	t.Cleanup(ctrl.Finish)

	sender := mod.buildDiscoveryWatcherSender(watcher.EtcdWatchEvent{
		Type:     pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE,
		Key:      "/svc/by_name/app-2",
		Revision: 77,
		PrevValue: &pb.AtappDiscovery{
			Id:   2,
			Name: "app-2",
		},
	})

	if sender == nil {
		t.Fatalf("expected sender")
	}
	if sender.Action != DiscoveryActionDelete {
		t.Fatalf("unexpected action: %v", sender.Action)
	}
	if sender.Node == nil || sender.Node.Info == nil || sender.Node.Info.GetId() != 2 {
		t.Fatalf("expected node built from prev value")
	}
}

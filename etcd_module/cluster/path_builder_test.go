package cluster

import (
	"reflect"
	"testing"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func TestBuildReportAlivePaths(t *testing.T) {
	// Arrange
	info := &pb.AtappDiscovery{Id: 42, Name: "svc", TypeId: 7, TypeName: "alpha"}
	keepaliveCfg := &pb.AtappEtcdKeepalive{}
	base := "/base"

	// Act
	paths := buildReportAlivePaths(info, keepaliveCfg, base)
	want := []string{
		"/base/by_id/svc-42",
		"/base/by_type_id/7/svc-42",
		"/base/by_type_name/alpha/svc-42",
		"/base/by_name/svc-42",
	}

	// Assert
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("unexpected reportAlive paths: %#v", paths)
	}
}

func TestBuildWatcherPrefixes(t *testing.T) {
	// Arrange
	watcher := &pb.AtappEtcdWatcher{}
	base := "/base"

	// Act
	prefixes := buildWatcherPrefixes(watcher, base)
	want := []string{
		"/base/by_id",
		"/base/by_name",
		"/base/by_type_id",
		"/base/by_type_name",
	}

	// Assert
	if !reflect.DeepEqual(prefixes, want) {
		t.Fatalf("unexpected watcher prefixes: %#v", prefixes)
	}
}

func TestBuildWatcherPrefixes_NilWatcherConfig(t *testing.T) {
	// Arrange
	base := "/base"

	// Act
	prefixes := buildWatcherPrefixes(nil, base)

	// Assert
	if prefixes != nil {
		t.Fatalf("unexpected watcher prefixes for nil watcher config: %#v", prefixes)
	}
}

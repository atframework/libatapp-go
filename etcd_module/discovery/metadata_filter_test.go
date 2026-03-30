package discovery

import (
	"hash/fnv"
	"maps"
	"sort"
	"testing"

	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
	log "log/slog"
)

func makeMetadataFilterBase() (map[string]string, *DiscoveryNode) {
	metadata := &pb.AtappMetadata{
		NamespaceName: "namespace",
		ApiVersion:    "v1",
		Kind:          "unit test",
		Group:         "atapp_discovery",
		ServiceSubset: "next",
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
	}

	node := &DiscoveryNode{
		Info: &pb.AtappDiscovery{
			Metadata: metadata,
		},
	}

	filter := map[string]string{
		"namespace_name": "namespace",
		"api_version":    "v1",
		"kind":           "unit test",
		"group":          "atapp_discovery",
		"service_subset": "next",
		"label1":         "value1",
		"label2":         "value2",
	}

	return filter, node
}

func cloneMetadata(source *pb.AtappMetadata) *pb.AtappMetadata {
	if source == nil {
		return nil
	}
	return &pb.AtappMetadata{
		NamespaceName: source.NamespaceName,
		ApiVersion:    source.ApiVersion,
		Kind:          source.Kind,
		Group:         source.Group,
		ServiceSubset: source.ServiceSubset,
		Name:          source.Name,
		Uid:           source.Uid,
		Labels:        maps.Clone(source.Labels),
	}
}

func matchesFilterForTest(node *DiscoveryNode, filter map[string]string) bool {
	set := &EtcdDiscoverySet{}
	return set.matchesFilter(node, filter)
}

func metadataEqualForTest(left, right *pb.AtappMetadata) bool {
	if left == nil && right == nil {
		return true
	}
	if left == nil || right == nil {
		return false
	}
	if left.ApiVersion != right.ApiVersion || left.Kind != right.Kind || left.Group != right.Group {
		return false
	}
	if left.Name != right.Name || left.NamespaceName != right.NamespaceName || left.Uid != right.Uid {
		return false
	}
	if left.ServiceSubset != right.ServiceSubset {
		return false
	}
	if len(left.Labels) != len(right.Labels) {
		return false
	}
	if !maps.Equal(left.Labels, right.Labels) {
		return false
	}
	return true
}

func metadataHashForTest(metadata *pb.AtappMetadata) uint64 {
	hasher := fnv.New64a()
	if metadata == nil {
		return 0
	}
	write := func(value string) {
		_, _ = hasher.Write([]byte(value))
		_, _ = hasher.Write([]byte{0})
	}
	write(metadata.NamespaceName)
	write(metadata.ApiVersion)
	write(metadata.Kind)
	write(metadata.Group)
	write(metadata.ServiceSubset)
	write(metadata.Name)
	write(metadata.Uid)

	keys := make([]string, 0, len(metadata.Labels))
	for key := range metadata.Labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		write(key)
		write(metadata.Labels[key])
	}

	return hasher.Sum64()
}

func TestMetadataFilterEmptyRule(t *testing.T) {
	// Arrange
	_, node := makeMetadataFilterBase()

	// Act
	if !matchesFilterForTest(node, nil) {
		t.Fatal("expected empty filter (nil) to match")
	}
	if !matchesFilterForTest(node, map[string]string{}) {
		t.Fatal("expected empty filter (empty map) to match")
	}
}

func TestMetadataFilterFullMatch(t *testing.T) {
	// Arrange
	filter, node := makeMetadataFilterBase()

	// Act
	if !matchesFilterForTest(node, filter) {
		t.Fatal("expected full metadata filter to match")
	}
}

func TestMetadataFilterPartialFields(t *testing.T) {
	// Arrange
	cases := []struct {
		name  string
		key   string
		value string
	}{
		{name: "namespace_name", key: "namespace_name", value: "namespace"},
		{name: "api_version", key: "api_version", value: "v1"},
		{name: "kind", key: "kind", value: "unit test"},
		{name: "group", key: "group", value: "atapp_discovery"},
		{name: "service_subset", key: "service_subset", value: "next"},
	}

	for _, tc := range cases {
		t.Run(tc.name+"_clear", func(t *testing.T) {
			// Arrange
			filter, node := makeMetadataFilterBase()
			delete(filter, tc.key)

			// Act
			if !matchesFilterForTest(node, filter) {
				t.Fatalf("expected filter without %s to match", tc.key)
			}
		})

		t.Run(tc.name+"_mismatch", func(t *testing.T) {
			// Arrange
			filter, node := makeMetadataFilterBase()
			filter[tc.key] = "mismatch value"

			// Act
			if matchesFilterForTest(node, filter) {
				t.Fatalf("expected mismatched %s to fail", tc.key)
			}
		})
	}
}

func TestMetadataFilterNamespaceAlias(t *testing.T) {
	// Arrange
	filter, node := makeMetadataFilterBase()
	delete(filter, "namespace_name")
	filter["namespace"] = "namespace"

	// Act
	if !matchesFilterForTest(node, filter) {
		t.Fatal("expected namespace alias to match")
	}

	// Act
	filter["namespace"] = "mismatch value"
	if matchesFilterForTest(node, filter) {
		t.Fatal("expected namespace alias mismatch to fail")
	}
}

func TestMetadataFilterLabels(t *testing.T) {
	// Arrange
	filter, node := makeMetadataFilterBase()

	// Act
	delete(filter, "label1")
	if !matchesFilterForTest(node, filter) {
		t.Fatal("expected missing label constraint to match")
	}

	// Act
	filter["label1"] = ""
	if !matchesFilterForTest(node, filter) {
		t.Fatal("expected empty label value to match")
	}

	// Act
	filter["label1"] = "mismatch value"
	if matchesFilterForTest(node, filter) {
		t.Fatal("expected mismatched label value to fail")
	}

	// Act
	delete(filter, "label1")
	delete(filter, "label2")
	if !matchesFilterForTest(node, filter) {
		t.Fatal("expected cleared labels to match")
	}
}

func TestMetadataFilterHashEqual(t *testing.T) {
	// Arrange
	_, node := makeMetadataFilterBase()
	metadata := node.Info.Metadata
	copy := cloneMetadata(metadata)

	// Assert
	if !metadataEqualForTest(metadata, copy) {
		t.Fatal("expected metadataEqual to consider identical metadata equal")
	}

	// Assert
	if metadataHashForTest(metadata) != metadataHashForTest(copy) {
		t.Fatal("expected metadataHash to match for identical metadata")
	}
}

func TestMetadataFilterCacheInvalidation(t *testing.T) {
	logger := log.Default()
	discovery, _ := NewEtcdDiscoverySet("/svc/", logger)

	filter := map[string]string{"version": "v1"}
	node := &DiscoveryNode{Path: "/svc/by_name/svc-1", Info: &pb.AtappDiscovery{Name: "svc", Metadata: &pb.AtappMetadata{Labels: map[string]string{"version": "v1"}}}}
	discovery.AddNode(node)
	if _, err := discovery.GetNodeByRandom(filter); err != nil {
		t.Fatalf("expected initial filter selection")
	}

	updated := &DiscoveryNode{Path: node.Path, Info: &pb.AtappDiscovery{Name: "svc", Metadata: &pb.AtappMetadata{Labels: map[string]string{"version": "v2"}}}, DataVersion: etcdversion.DataVersion{ModRevision: 2}}
	discovery.HandleWatcherEvent(watcher.EtcdWatchEvent{Type: pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT, Key: node.Path, Value: updated.Info, Revision: 2})
	if _, err := discovery.GetNodeByRandom(filter); err == nil {
		t.Fatalf("expected filter miss after metadata update")
	}
}

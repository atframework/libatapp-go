// Package pathbuilder provides helpers to build etcd key paths for service
// discovery and topology records.
package pathbuilder

import (
	"fmt"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// Sub-directory constants under the discovery base prefix.
const (
	ByIDDir     = "by_id"
	ByNameDir   = "by_name"
	ByTagDir    = "by_tag"
	TopologyDir = "topology"
)

func formatUint(v uint64) string {
	return fmt.Sprintf("%d", v)
}

// BuildByIDPath returns the by-id indexed etcd key for a discovery record.
// Format: <base>/by_id/<name>-<id>
func BuildByIDPath(base string, info *pb.AtappDiscovery) string {
	if info == nil {
		return ""
	}
	return base + "/" + ByIDDir + "/" + info.GetName() + "-" + formatUint(info.GetId())
}

// BuildByTypeIDPath returns the by-type-id indexed etcd key.
// Format: <base>/by_type_id/<type_id>/<name>-<id>
func BuildByTypeIDPath(base string, info *pb.AtappDiscovery) string {
	if info == nil {
		return ""
	}
	return base + "/by_type_id/" + formatUint(info.GetTypeId()) + "/" + info.GetName() + "-" + formatUint(info.GetId())
}

// BuildByTypeNamePath returns the by-type-name indexed etcd key.
// Format: <base>/by_type_name/<type_name>/<name>-<id>
func BuildByTypeNamePath(base string, info *pb.AtappDiscovery) string {
	if info == nil {
		return ""
	}
	return base + "/by_type_name/" + info.GetTypeName() + "/" + info.GetName() + "-" + formatUint(info.GetId())
}

// BuildByNamePath returns the by-name indexed etcd key.
// Format: <base>/by_name/<name>-<id>
func BuildByNamePath(base string, info *pb.AtappDiscovery) string {
	if info == nil {
		return ""
	}
	return base + "/" + ByNameDir + "/" + info.GetName() + "-" + formatUint(info.GetId())
}

// BuildByTagPath returns the by-tag indexed etcd key for the given tag value.
// Format: <base>/by_tag/<tag>/<name>-<id>
func BuildByTagPath(base string, info *pb.AtappDiscovery, tag string) string {
	if info == nil {
		return ""
	}
	return base + "/" + ByTagDir + "/" + tag + "/" + info.GetName() + "-" + formatUint(info.GetId())
}

// ── Watcher path helpers ──────────────────────────────────────────────────

// BuildByIDWatcherPath returns the prefix to watch all by-id records.
func BuildByIDWatcherPath(base string) string {
	return base + "/" + ByIDDir
}

// BuildByTypeIDWatcherPath returns the prefix to watch all by-type-id records
// for the given typeID.
func BuildByTypeIDWatcherPath(base string, typeID uint64) string {
	return base + "/by_type_id/" + formatUint(typeID)
}

// BuildByTypeNameWatcherPath returns the prefix to watch all by-type-name
// records for the given typeName.
func BuildByTypeNameWatcherPath(base, typeName string) string {
	return base + "/by_type_name/" + typeName
}

// BuildByNameWatcherPath returns the prefix to watch all by-name records.
func BuildByNameWatcherPath(base string) string {
	return base + "/" + ByNameDir
}

// BuildByTagWatcherPath returns the prefix to watch all by-tag records for
// the given tag value.
func BuildByTagWatcherPath(base, tag string) string {
	return base + "/" + ByTagDir + "/" + tag
}

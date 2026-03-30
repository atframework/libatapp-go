package pathbuilder

import (
	"fmt"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

const (
	ByIDDir       = "by_id"
	ByNameDir     = "by_name"
	ByTypeIDDir   = "by_type_id"
	ByTypeNameDir = "by_type_name"
	ByTagDir      = "by_tag"
)

func formatUint(v uint64) string {
	return fmt.Sprintf("%d", v)
}

func BuildByIDPath(base string, info *pb.AtappDiscovery) string {
	if info == nil {
		return ""
	}
	return base + "/" + ByIDDir + "/" + info.GetName() + "-" + formatUint(info.GetId())
}

func BuildByTypeIDPath(base string, info *pb.AtappDiscovery) string {
	if info == nil {
		return ""
	}
	return base + "/" + ByTypeIDDir + "/" + formatUint(info.GetTypeId()) + "/" + info.GetName() + "-" + formatUint(info.GetId())
}

func BuildByTypeNamePath(base string, info *pb.AtappDiscovery) string {
	if info == nil {
		return ""
	}
	return base + "/" + ByTypeNameDir + "/" + info.GetTypeName() + "/" + info.GetName() + "-" + formatUint(info.GetId())
}

func BuildByNamePath(base string, info *pb.AtappDiscovery) string {
	if info == nil {
		return ""
	}
	return base + "/" + ByNameDir + "/" + info.GetName() + "-" + formatUint(info.GetId())
}

func BuildByTagPath(base string, info *pb.AtappDiscovery, tag string) string {
	if info == nil {
		return ""
	}
	return base + "/" + ByTagDir + "/" + tag + "/" + info.GetName() + "-" + formatUint(info.GetId())
}

func BuildByIDWatcherPath(base string) string {
	return base + "/" + ByIDDir
}

func BuildByTypeIDWatcherPath(base string, typeID uint64) string {
	return base + "/" + ByTypeIDDir + "/" + formatUint(typeID)
}

func BuildByTypeNameWatcherPath(base, typeName string) string {
	return base + "/" + ByTypeNameDir + "/" + typeName
}

func BuildByNameWatcherPath(base string) string {
	return base + "/" + ByNameDir
}

func BuildByTagWatcherPath(base, tag string) string {
	return base + "/" + ByTagDir + "/" + tag
}

package cluster

import (
	"fmt"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func buildReportAlivePaths(info *pb.AtappDiscovery, keepaliveCfg *pb.AtappEtcdKeepalive, basePath string) []string {
	if info == nil {
		return nil
	}
	_ = keepaliveCfg

	paths := make([]string, 0)
	paths = append(paths, basePath+"/by_id/"+info.Name+"-"+formatUint(info.Id))
	if info.TypeId != 0 {
		paths = append(paths, basePath+"/by_type_id/"+formatUint(info.TypeId)+"/"+info.Name+"-"+formatUint(info.Id))
	}
	if info.TypeName != "" {
		paths = append(paths, basePath+"/by_type_name/"+info.TypeName+"/"+info.Name+"-"+formatUint(info.Id))
	}
	paths = append(paths, basePath+"/by_name/"+info.Name+"-"+formatUint(info.Id))

	return paths
}

func buildWatcherPrefixes(watcherCfg *pb.AtappEtcdWatcher, basePath string) []string {
	if watcherCfg == nil || basePath == "" {
		return nil
	}

	return []string{
		basePath + "/by_id",
		basePath + "/by_name",
		basePath + "/by_type_id",
		basePath + "/by_type_name",
	}
}

func formatUint(value uint64) string {
	return fmt.Sprintf("%d", value)
}

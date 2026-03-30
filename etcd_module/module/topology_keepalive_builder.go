package module

import (
	"os"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
	"google.golang.org/protobuf/proto"
)

// TopologyKeepaliveSeed 定义TopologyKeepaliveSeed类型。
type TopologyKeepaliveSeed struct {
	ID           uint64
	Name         string
	Identity     string
	HashCode     string
	AppVersion   string
	BuildVersion string
	TopologyData *pb.AtbusTopologyData
}

// BuildTopologyKeepaliveInfoFromFields 构建TopologyKeepaliveInfoFromFields。
func BuildTopologyKeepaliveInfoFromFields(
	id uint64,
	name string,
	identity string,
	hashCode string,
	appVersion string,
	buildVersion string,
	cfg *pb.AtappConfigure,
) *pb.AtappTopologyInfo {
	var topologyData *pb.AtbusTopologyData
	if cfg != nil {
		if bus := cfg.GetBus(); bus != nil {
			if top := bus.GetTopology(); top != nil && top.GetData() != nil {
				topologyData = top.GetData()
			}
		}
	}

	return BuildTopologyKeepaliveInfo(TopologyKeepaliveSeed{
		ID:           id,
		Name:         name,
		Identity:     identity,
		HashCode:     hashCode,
		AppVersion:   appVersion,
		BuildVersion: buildVersion,
		TopologyData: topologyData,
	})
}

// BuildTopologyKeepaliveInfo 构建TopologyKeepaliveInfo。
func BuildTopologyKeepaliveInfo(seed TopologyKeepaliveSeed) *pb.AtappTopologyInfo {
	if seed.ID == 0 && seed.Name == "" && seed.Identity == "" && seed.HashCode == "" && seed.AppVersion == "" && seed.BuildVersion == "" && seed.TopologyData == nil {
		return nil
	}

	hostname, _ := os.Hostname()
	identity := seed.Identity
	if identity == "" {
		identity = seed.HashCode
	}
	version := seed.AppVersion
	if version == "" {
		version = seed.BuildVersion
	}

	var topologyData *pb.AtbusTopologyData
	if seed.TopologyData != nil {
		topologyData = proto.Clone(seed.TopologyData).(*pb.AtbusTopologyData)
	}

	return &pb.AtappTopologyInfo{
		Id:       seed.ID,
		Name:     seed.Name,
		Hostname: hostname,
		Pid:      int32(os.Getpid()),
		Identity: identity,
		Version:  version,
		Data:     topologyData,
	}
}

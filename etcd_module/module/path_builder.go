package module

import (
	"github.com/atframework/libatapp-go/etcd_module/internal/pathbuilder"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// GetByIDPath 获取ByIDPath。
func (m *EtcdModule) GetByIDPath(info *pb.AtappDiscovery) string {
	return pathbuilder.BuildByIDPath(m.GetConfigurePath(), info)
}

// GetDiscoveryByIDPath 获取DiscoveryByIDPath。
func (m *EtcdModule) GetDiscoveryByIDPath(info *pb.AtappDiscovery) string {
	return m.GetByIDPath(info)
}

// GetByTypeIDPath 获取ByTypeIDPath。
func (m *EtcdModule) GetByTypeIDPath(info *pb.AtappDiscovery) string {
	return pathbuilder.BuildByTypeIDPath(m.GetConfigurePath(), info)
}

// GetByTypeNamePath 获取ByTypeNamePath。
func (m *EtcdModule) GetByTypeNamePath(info *pb.AtappDiscovery) string {
	return pathbuilder.BuildByTypeNamePath(m.GetConfigurePath(), info)
}

// GetByNamePath 获取ByNamePath。
func (m *EtcdModule) GetByNamePath(info *pb.AtappDiscovery) string {
	return pathbuilder.BuildByNamePath(m.GetConfigurePath(), info)
}

// GetDiscoveryByNamePath 获取DiscoveryByNamePath。
func (m *EtcdModule) GetDiscoveryByNamePath(info *pb.AtappDiscovery) string {
	return m.GetByNamePath(info)
}

// GetByTagPath 获取ByTagPath。
func (m *EtcdModule) GetByTagPath(info *pb.AtappDiscovery, tag string) string {
	return pathbuilder.BuildByTagPath(m.GetConfigurePath(), info, tag)
}

// GetByIDWatcherPath 获取ByIDWatcherPath。
func (m *EtcdModule) GetByIDWatcherPath() string {
	return pathbuilder.BuildByIDWatcherPath(m.GetConfigurePath())
}

// GetDiscoveryByIDWatcherPath 获取DiscoveryByIDWatcherPath。
func (m *EtcdModule) GetDiscoveryByIDWatcherPath() string {
	return m.GetByIDWatcherPath()
}

// GetByTypeIDWatcherPath 获取ByTypeIDWatcherPath。
func (m *EtcdModule) GetByTypeIDWatcherPath(typeID uint64) string {
	return pathbuilder.BuildByTypeIDWatcherPath(m.GetConfigurePath(), typeID)
}

// GetByTypeNameWatcherPath 获取ByTypeNameWatcherPath。
func (m *EtcdModule) GetByTypeNameWatcherPath(typeName string) string {
	return pathbuilder.BuildByTypeNameWatcherPath(m.GetConfigurePath(), typeName)
}

// GetByNameWatcherPath 获取ByNameWatcherPath。
func (m *EtcdModule) GetByNameWatcherPath() string {
	return pathbuilder.BuildByNameWatcherPath(m.GetConfigurePath())
}

// GetDiscoveryByNameWatcherPath 获取DiscoveryByNameWatcherPath。
func (m *EtcdModule) GetDiscoveryByNameWatcherPath() string {
	return m.GetByNameWatcherPath()
}

// GetByTagWatcherPath 获取ByTagWatcherPath。
func (m *EtcdModule) GetByTagWatcherPath(tag string) string {
	return pathbuilder.BuildByTagWatcherPath(m.GetConfigurePath(), tag)
}

package topology

import (
	"encoding/json"
	"reflect"

	"github.com/atframework/libatapp-go/etcd_module/internal/codec"
	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// CompatStorage 定义CompatStorage类型。
type CompatStorage struct {
	Info *pb.AtappTopologyInfo
	etcdversion.DataVersion
}

// ValueToProto 将 ValueToProto 转换为 Proto 数据。
func ValueToProto(raw map[string]any) *pb.AtappTopologyInfo {
	if len(raw) == 0 {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	out := &pb.AtappTopologyInfo{}
	if err := codec.UnmarshalProtoFromJSON(b, out); err != nil {
		return nil
	}
	return out
}

// BuildCompatStorage 构建CompatStorage。
func BuildCompatStorage(info *Info) *CompatStorage {
	if info == nil {
		return nil
	}
	pbInfo := ValueToProto(info.Value)
	if pbInfo == nil {
		return nil
	}
	return &CompatStorage{
		Info:        pbInfo,
		DataVersion: info.DataVersion,
	}
}

// SameRecord 实现。
func SameRecord(left *Info, right *Info) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.CreateRevision != right.CreateRevision ||
		left.ModRevision != right.ModRevision ||
		left.Version != right.Version {
		return false
	}
	return reflect.DeepEqual(left.Value, right.Value)
}

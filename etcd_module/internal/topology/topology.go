package topology

import (
	"encoding/json"
	"sort"

	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
)

// DirectoryName is the etcd sub-directory name for topology records.
const DirectoryName = "topology"

// Info 定义Info类型。
type Info struct {
	Path     string
	Revision int64
	etcdversion.DataVersion
	Value map[string]any
}

// ParseInfo 解析Info。
func ParseInfo(path string, revision int64, createRevision int64, modRevision int64, version int64, raw []byte) *Info {
	if len(raw) == 0 {
		return nil
	}
	obj := map[string]any{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	if createRevision <= 0 {
		createRevision = revision
	}
	if modRevision <= 0 {
		modRevision = revision
	}
	if version <= 0 {
		version = etcdversion.DefaultVersion
	}
	return &Info{Path: path, Revision: revision, DataVersion: etcdversion.New(createRevision, modRevision, version), Value: obj}
}

// CloneMap 实现。
func CloneMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	buf, err := json.Marshal(src)
	if err != nil {
		return nil
	}
	dst := map[string]any{}
	if err := json.Unmarshal(buf, &dst); err != nil {
		return nil
	}
	return dst
}

// SortedInfos 实现。
func SortedInfos(set map[string]*Info) []*Info {
	if len(set) == 0 {
		return nil
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ret := make([]*Info, 0, len(keys))
	for _, k := range keys {
		item := set[k]
		if item == nil {
			continue
		}
		ret = append(ret, &Info{
			Path:        item.Path,
			Revision:    item.Revision,
			DataVersion: item.DataVersion,
			Value:       CloneMap(item.Value),
		})
	}
	return ret
}

// Package etcdversion models the etcd key version tuple.
package etcdversion

// Defaults aligned with C++ etcd_data_version in include/atframe/etcdcli/etcd_def.h.
const (
	DefaultCreateRevision int64 = 0
	DefaultModRevision    int64 = 0
	DefaultVersion        int64 = 0
)

// DataVersion models etcd key version tuple.
type DataVersion struct {
	CreateRevision int64
	ModRevision    int64
	Version        int64
}

// New builds a version tuple from explicit values.
func New(createRevision, modRevision, version int64) DataVersion {
	return DataVersion{
		CreateRevision: createRevision,
		ModRevision:    modRevision,
		Version:        version,
	}
}

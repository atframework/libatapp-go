// Package consistenthash 提供使用 SHA256 的一致散列实现
// 使用 SHA256 代替 Murmur3 以获得更好的线程安全性
//
// The ring supports multiple virtual nodes per member for better distribution
// and provides various search modes for node selection.
package consistenthash

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

type hashPoint struct {
	hashLo uint64 // Lower 64 bits of the 128-bit hash
	hashHi uint64 // Higher 64 bits of the 128-bit hash
	member string // The member string (e.g., node path)
}

type hashPoints []hashPoint

func (p hashPoints) Len() int      { return len(p) }
func (p hashPoints) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p hashPoints) Less(i, j int) bool {
	if p[i].hashHi < p[j].hashHi {
		return true
	}
	if p[i].hashHi > p[j].hashHi {
		return false
	}
	return p[i].hashLo < p[j].hashLo
}

// Ring 定义Ring类型。
type Ring struct {
	virtualNodes int
	points       hashPoints
	members      map[string]bool
}

// NewRing 创建并返回Ring。
func NewRing(virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = 80 // 默认值
	}
	return &Ring{
		virtualNodes: virtualNodes,
		points:       make(hashPoints, 0),
		members:      make(map[string]bool),
	}
}

// hashKey 判断是否存在hKey。
func (r *Ring) hashKey(key []byte, seed uint32) (uint64, uint64) {
	// Create a new hasher for each call (thread-safe)
	h := sha256.New()
	// 种子被写入散列状态以模拟种子参数
	var seedBytes [4]byte
	binary.LittleEndian.PutUint32(seedBytes[:], seed)
	h.Write(seedBytes[:]) // Seed the hash
	h.Write(key)          // Now hash the key
	sum := h.Sum(nil)
	// Extract first 16 bytes as 128-bit hash value
	lo := binary.LittleEndian.Uint64(sum[:8])
	hi := binary.LittleEndian.Uint64(sum[8:16])
	return lo, hi
}

// Add 添加Add。
func (r *Ring) Add(member string) {
	if _, ok := r.members[member]; ok {
		// Member already exists
		return
	}
	r.members[member] = true
	r.rebuild()
}

// Set 设置Set。
func (r *Ring) Set(members []string) {
	r.members = make(map[string]bool, len(members))
	for _, member := range members {
		r.members[member] = true
	}
	r.rebuild()
}

// rebuild 实现。
func (r *Ring) rebuild() {
	r.points = make(hashPoints, 0, len(r.members)*r.virtualNodes)
	for member := range r.members {
		for i := 0; i < r.virtualNodes; i++ {
			lo, hi := r.hashKey([]byte(member), uint32(i))
			r.points = append(r.points, hashPoint{
				hashLo: lo,
				hashHi: hi,
				member: member,
			})
		}
	}
	sort.Sort(r.points)
}

// Get 获取Get。
func (r *Ring) Get(key string) string {
	if len(r.points) == 0 {
		return ""
	}

	// 终赓9 种子（与旧版本类似）
	lo, hi := r.hashKey([]byte(key), 0x2a) // 0x2a is an arbitrary seed, similar to LIBATAPP_MACRO_HASH_MAGIC_NUMBER

	// Binary search to find the first point >= key's hash
	idx := sort.Search(len(r.points), func(i int) bool {
		p := r.points[i]
		if p.hashHi < hi {
			return false
		}
		if p.hashHi > hi {
			return true
		}
		return p.hashLo >= lo
	})

	// Wrap around if the key's hash is larger than all points
	if idx == len(r.points) {
		idx = 0
	}

	return r.points[idx].member
}

// GetN 获取N。
func (r *Ring) GetN(key string, n int, mode pb.EtcdSearchMode) ([]string, error) {
	if len(r.points) == 0 {
		return nil, nil
	}

	if n > len(r.members) {
		n = len(r.members)
	}

	lo, hi := r.hashKey([]byte(key), 0x2a)

	idx := sort.Search(len(r.points), func(i int) bool {
		p := r.points[i]
		if p.hashHi < hi {
			return false
		}
		if p.hashHi > hi {
			return true
		}
		return p.hashLo >= lo
	})

	if idx == len(r.points) {
		idx = 0
	}

	startIdx := idx
	if mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE || mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT || mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE || mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT_UNIQUE_NODE {
		if len(r.points) > 1 {
			startIdx = (idx + 1) % len(r.points)
		}
	}

	var points []hashPoint
	switch mode {
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL:
		points = r.points[idx:]
		if idx != 0 {
			points = append(points, r.points[:idx]...)
		}
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT:
		points = r.getCompactPoints(idx, false)
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE:
		points = r.getUniquePoints(idx, false)
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE:
		points = r.getUniquePoints(idx, true)
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE:
		points = r.points[startIdx:]
		if startIdx != 0 {
			points = append(points, r.points[:startIdx]...)
		}
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT:
		points = r.getCompactPoints(startIdx, false)
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE:
		points = r.getUniquePoints(startIdx, false)
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT_UNIQUE_NODE:
		points = r.getUniquePoints(startIdx, true)
	}

	members := make([]string, 0, n)
	seen := make(map[string]bool, n)

	for _, p := range points {
		if len(members) >= n {
			break
		}
		if !seen[p.member] {
			members = append(members, p.member)
			seen[p.member] = true
		}
	}

	return members, nil
}

func (r *Ring) getCompactPoints(startIdx int, skipConsecutive bool) []hashPoint {
	result := make([]hashPoint, 0, len(r.points))
	for i := 0; i < len(r.points); i++ {
		currentIdx := (startIdx + i) % len(r.points)
		p := r.points[currentIdx]
		if len(result) == 0 {
			result = append(result, p)
			continue
		}
		lastMember := result[len(result)-1].member
		if !skipConsecutive || p.member != lastMember {
			result = append(result, p)
		}
	}
	return result
}

func (r *Ring) getUniquePoints(startIdx int, compact bool) []hashPoint {
	if compact {
		return r.getCompactPoints(startIdx, true)
	}

	result := make([]hashPoint, 0, len(r.members))
	seen := make(map[string]bool)

	for i := 0; i < len(r.points); i++ {
		currentIdx := (startIdx + i) % len(r.points)
		p := r.points[currentIdx]
		if !seen[p.member] {
			result = append(result, p)
			seen[p.member] = true
		}
	}
	return result
}

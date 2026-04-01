// Package consistenthash provides a consistent hash ring implementation using
// SHA256.  The ring supports multiple virtual nodes per member for better
// distribution and provides various search modes for node selection.
package consistenthash

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"

	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

type hashPoint struct {
	hashLo uint64
	hashHi uint64
	member string
}

type hashPoints []hashPoint

func (p hashPoints) Len() int      { return len(p) }
func (p hashPoints) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p hashPoints) Less(i, j int) bool {
	if p[i].hashHi != p[j].hashHi {
		return p[i].hashHi < p[j].hashHi
	}
	return p[i].hashLo < p[j].hashLo
}

// Ring is a consistent hash ring.
type Ring struct {
	virtualNodes int
	points       hashPoints
	members      map[string]bool
}

// NewRing creates a Ring with virtualNodes virtual nodes per member.
// If virtualNodes ≤ 0 the default of 80 is used.
func NewRing(virtualNodes int) *Ring {
	if virtualNodes <= 0 {
		virtualNodes = 80
	}
	return &Ring{
		virtualNodes: virtualNodes,
		points:       make(hashPoints, 0),
		members:      make(map[string]bool),
	}
}

func (r *Ring) hashKey(key []byte, seed uint32) (uint64, uint64) {
	h := sha256.New()
	var seedBytes [4]byte
	binary.LittleEndian.PutUint32(seedBytes[:], seed)
	h.Write(seedBytes[:])
	h.Write(key)
	sum := h.Sum(nil)
	lo := binary.LittleEndian.Uint64(sum[:8])
	hi := binary.LittleEndian.Uint64(sum[8:16])
	return lo, hi
}

// Add adds a single member to the ring.  If the member already exists this is
// a no-op.
func (r *Ring) Add(member string) {
	if r.members[member] {
		return
	}
	r.members[member] = true
	r.rebuild()
}

// Set replaces the member set with the provided slice and rebuilds the ring.
func (r *Ring) Set(members []string) {
	r.members = make(map[string]bool, len(members))
	for _, m := range members {
		r.members[m] = true
	}
	r.rebuild()
}

func (r *Ring) rebuild() {
	r.points = make(hashPoints, 0, len(r.members)*r.virtualNodes)
	for member := range r.members {
		for i := 0; i < r.virtualNodes; i++ {
			lo, hi := r.hashKey([]byte(member), uint32(i))
			r.points = append(r.points, hashPoint{hashLo: lo, hashHi: hi, member: member})
		}
	}
	sort.Sort(r.points)
}

// Get returns the member responsible for the given key. Returns "" when the
// ring is empty.
func (r *Ring) Get(key string) string {
	if len(r.points) == 0 {
		return ""
	}
	lo, hi := r.hashKey([]byte(key), 0x2a)
	idx := sort.Search(len(r.points), func(i int) bool {
		p := r.points[i]
		if p.hashHi != hi {
			return p.hashHi > hi
		}
		return p.hashLo >= lo
	})
	if idx == len(r.points) {
		idx = 0
	}
	return r.points[idx].member
}

// GetN returns up to n members for the given key using the supplied search
// mode.  When n exceeds the number of members, all members are returned.
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
		if p.hashHi != hi {
			return p.hashHi > hi
		}
		return p.hashLo >= lo
	})
	if idx == len(r.points) {
		idx = 0
	}

	startIdx := idx
	if mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE ||
		mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT ||
		mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE ||
		mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT_UNIQUE_NODE {
		if len(r.points) > 1 {
			startIdx = (idx + 1) % len(r.points)
		}
	}

	var points []hashPoint
	switch mode {
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL:
		points = r.rotateFrom(idx)
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT:
		points = r.getCompactPoints(idx, false)
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE:
		points = r.getUniquePoints(idx, false)
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE:
		points = r.getUniquePoints(idx, true)
	case pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE:
		points = r.rotateFrom(startIdx)
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

func (r *Ring) rotateFrom(startIdx int) []hashPoint {
	result := make([]hashPoint, len(r.points))
	for i := range r.points {
		result[i] = r.points[(startIdx+i)%len(r.points)]
	}
	return result
}

func (r *Ring) getCompactPoints(startIdx int, skipConsecutive bool) []hashPoint {
	result := make([]hashPoint, 0, len(r.points))
	for i := 0; i < len(r.points); i++ {
		p := r.points[(startIdx+i)%len(r.points)]
		if len(result) == 0 {
			result = append(result, p)
			continue
		}
		if !skipConsecutive || p.member != result[len(result)-1].member {
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
		p := r.points[(startIdx+i)%len(r.points)]
		if !seen[p.member] {
			result = append(result, p)
			seen[p.member] = true
		}
	}
	return result
}

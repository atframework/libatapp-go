// Package discovery provides node tracking and routing state for service discovery.
package discovery

import (
	"encoding/binary"
	"fmt"
	log "log/slog"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spaolacci/murmur3"
	"google.golang.org/protobuf/proto"

	"github.com/atframework/libatapp-go/etcd_module/internal/consistenthash"
	"github.com/atframework/libatapp-go/etcd_module/internal/etcdversion"
	"github.com/atframework/libatapp-go/etcd_module/watcher"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// cacheEntry 定义cacheEntry类型。
type cacheEntry struct {
	sorted      []*DiscoveryNode
	consistent  *consistenthash.Ring
	normalRing  []nodeHashType
	compactRing []nodeHashType
	filter      map[string]string
	normalOnce  sync.Once
	compactOnce sync.Once
	roundRobin  uint64
}

type nodeHashType struct {
	node   *DiscoveryNode
	hashLo uint64
	hashHi uint64
}

const (
	consistentHashMagicSeed uint32 = 0x2a
	consistentHashVNodes           = 80
)

func hashLess(leftLo, leftHi, rightLo, rightHi uint64) bool {
	if leftHi != rightHi {
		return leftHi < rightHi
	}
	return leftLo < rightLo
}

func hashEqual(leftLo, leftHi, rightLo, rightHi uint64) bool {
	return leftLo == rightLo && leftHi == rightHi
}

func consistentHashKey(data []byte, seed uint32) (uint64, uint64) {
	h := murmur3.New128WithSeed(seed)
	_, _ = h.Write(data)
	return h.Sum128()
}

func compareRoundRobinIndex(left, right *DiscoveryNode) bool {
	if left == nil || right == nil {
		if left == right {
			return false
		}
		return left == nil
	}

	var leftInfo, rightInfo *pb.AtappDiscovery
	left.WithDiscoveryInfo(func(info *pb.AtappDiscovery) {
		leftInfo = info
	})
	right.WithDiscoveryInfo(func(info *pb.AtappDiscovery) {
		rightInfo = info
	})
	if leftInfo != nil && rightInfo != nil {
		leftPod := int32(0)
		rightPod := int32(0)
		if leftInfo.Runtime != nil {
			leftPod = leftInfo.Runtime.StatefulPodIndex
		}
		if rightInfo.Runtime != nil {
			rightPod = rightInfo.Runtime.StatefulPodIndex
		}
		if leftPod != rightPod {
			return leftPod < rightPod
		}
		if leftInfo.Id != rightInfo.Id {
			return leftInfo.Id < rightInfo.Id
		}
		leftLo, leftHi := left.GetNameHash()
		rightLo, rightHi := right.GetNameHash()
		if !hashEqual(leftLo, leftHi, rightLo, rightHi) {
			return hashLess(leftLo, leftHi, rightLo, rightHi)
		}
		if leftInfo.Name != rightInfo.Name {
			return leftInfo.Name < rightInfo.Name
		}
	}

	return left.Path < right.Path
}

func compareNodeHash(left, right nodeHashType) bool {
	if !hashEqual(left.hashLo, left.hashHi, right.hashLo, right.hashHi) {
		return hashLess(left.hashLo, left.hashHi, right.hashLo, right.hashHi)
	}
	return compareRoundRobinIndex(left.node, right.node)
}

// EtcdDiscoverySet 定义EtcdDiscoverySet类型。
type EtcdDiscoverySet struct {
	logger      *log.Logger
	watchPrefix string

	nodesByPath             map[string]*DiscoveryNode
	nodesByID               map[uint64]*DiscoveryNode
	nodesByName             map[string]*DiscoveryNode
	mu                      sync.RWMutex
	cache                   map[string]*cacheEntry
	eventPublisher          func(eventType pb.EtcdWatchEventType, node *DiscoveryNode, existed bool)
	cacheDirty              bool
	cacheDirtySince         time.Time
	cacheInvalidateInterval time.Duration
}

// NewEtcdDiscoverySet 创建并返回EtcdDiscoverySet。
func NewEtcdDiscoverySet(watchPrefix string, logger *log.Logger) (*EtcdDiscoverySet, error) {
	if logger == nil {
		logger = log.Default()
	}

	return &EtcdDiscoverySet{
		logger:      logger,
		watchPrefix: watchPrefix,
		nodesByPath: make(map[string]*DiscoveryNode),
		nodesByID:   make(map[uint64]*DiscoveryNode),
		nodesByName: make(map[string]*DiscoveryNode),
		cache:       make(map[string]*cacheEntry),
		cacheDirty:  true,
	}, nil
}

// SetCacheInvalidationInterval 设置CacheInvalidationInterval。
func (d *EtcdDiscoverySet) SetCacheInvalidationInterval(interval time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if interval < 0 {
		interval = 0
	}
	d.cacheInvalidateInterval = interval
}

// SetEventPublisher 设置EventPublisher。
func (d *EtcdDiscoverySet) SetEventPublisher(publisher func(eventType pb.EtcdWatchEventType, node *DiscoveryNode, existed bool)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.eventPublisher = publisher
}

// HandleWatcherEvent 处理WatcherEvent。
func (d *EtcdDiscoverySet) HandleWatcherEvent(event watcher.EtcdWatchEvent) {
	d.mu.Lock()
	affected, publishes := d.applyWatchEventLocked(event)
	d.invalidateCacheByNodesLocked(affected)
	d.mu.Unlock()

	for _, fn := range publishes {
		fn()
	}
}

// HandleBatch 处理Batch。
func (d *EtcdDiscoverySet) HandleBatch(events []*watcher.EtcdWatchEvent) {
	d.mu.Lock()
	affected := make([]*DiscoveryNode, 0, len(events))
	publishes := make([]func(), 0, len(events))
	for _, event := range events {
		if event == nil {
			continue
		}
		a, p := d.applyWatchEventLocked(*event)
		affected = append(affected, a...)
		publishes = append(publishes, p...)
	}
	d.invalidateCacheByNodesLocked(affected)
	d.mu.Unlock()

	for _, fn := range publishes {
		fn()
	}
}

// ApplySnapshot 应用Snapshot。
func (d *EtcdDiscoverySet) ApplySnapshot(events []*watcher.EtcdWatchEvent) {
	d.mu.Lock()

	if len(events) == 0 {
		d.resetNodesLocked()
		d.invalidateCacheLocked()
		d.mu.Unlock()
		return
	}

	publishes := make([]func(), 0, len(events))
	affected := make([]*DiscoveryNode, 0, len(events))
	oldIDs, oldNames := d.snapshotExistingNodes()

	for _, event := range events {
		if event == nil || event.Value == nil {
			continue
		}
		node := d.nodeFromEvent(event)
		a, p := d.applyNodeUpdate(node)
		affected = append(affected, a...)
		if p != nil {
			publishes = append(publishes, p)
		}
		d.dropFromOldSets(node, oldIDs, oldNames)
	}

	a, p := d.removeMissingNodes(oldIDs, oldNames)
	affected = append(affected, a...)
	publishes = append(publishes, p...)

	d.invalidateCacheByNodesLocked(affected)
	d.mu.Unlock()

	for _, fn := range publishes {
		fn()
	}
}

// AddNode 添加Node。
func (d *EtcdDiscoverySet) AddNode(node *DiscoveryNode) {
	d.mu.Lock()
	affected, publish := d.applyNodeUpdate(node)
	d.invalidateCacheByNodesLocked(affected)
	d.mu.Unlock()

	if publish != nil {
		publish()
	}
}

// RemoveNode 移除Node。
func (d *EtcdDiscoverySet) RemoveNode(path string) {
	d.mu.Lock()
	affected, publish := d.applyNodeDelete(path, nil)
	d.invalidateCacheByNodesLocked(affected)
	d.mu.Unlock()

	if publish != nil {
		publish()
	}
}

func (d *EtcdDiscoverySet) applyWatchEventLocked(event watcher.EtcdWatchEvent) ([]*DiscoveryNode, []func()) {
	var node *DiscoveryNode
	if event.Value != nil {
		node = d.nodeFromWatchEvent(event)
	}

	switch event.Type {
	case pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT:
		affected, publish := d.applyNodeUpdate(node)
		if publish == nil {
			return affected, nil
		}
		return affected, []func(){publish}
	case pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE:
		affected, publish := d.applyNodeDelete(event.Key, event.Value)
		if publish == nil {
			return affected, nil
		}
		return affected, []func(){publish}
	}

	return nil, nil
}

func (d *EtcdDiscoverySet) nodeFromWatchEvent(event watcher.EtcdWatchEvent) *DiscoveryNode {
	createRevision := event.CreateRevision
	if createRevision <= 0 {
		createRevision = event.Revision
	}
	modRevision := event.ModRevision
	if modRevision <= 0 {
		modRevision = event.Revision
	}
	version := event.Version
	if version <= 0 {
		version = event.Revision
	}

	return &DiscoveryNode{
		Info:        event.Value,
		Path:        event.Key,
		DataVersion: etcdversion.New(createRevision, modRevision, version),
	}
}

func (d *EtcdDiscoverySet) nodeFromEvent(event *watcher.EtcdWatchEvent) *DiscoveryNode {
	if event == nil {
		return nil
	}
	createRevision := event.CreateRevision
	if createRevision <= 0 {
		createRevision = event.Revision
	}
	modRevision := event.ModRevision
	if modRevision <= 0 {
		modRevision = event.Revision
	}
	version := event.Version
	if version <= 0 {
		version = event.Revision
	}
	return &DiscoveryNode{
		Info:        event.Value,
		Path:        event.Key,
		DataVersion: etcdversion.New(createRevision, modRevision, version),
	}
}

func (d *EtcdDiscoverySet) snapshotExistingNodes() (map[uint64]*DiscoveryNode, map[string]*DiscoveryNode) {
	oldNames := make(map[string]*DiscoveryNode)
	oldIDs := make(map[uint64]*DiscoveryNode)
	for _, node := range d.nodesByPath {
		if node.Info == nil {
			continue
		}
		if node.Info.Id != 0 {
			oldIDs[node.Info.Id] = node
		} else if node.Info.Name != "" {
			oldNames[node.Info.Name] = node
		}
	}
	return oldIDs, oldNames
}

func (d *EtcdDiscoverySet) dropFromOldSets(node *DiscoveryNode, oldIDs map[uint64]*DiscoveryNode, oldNames map[string]*DiscoveryNode) {
	if node == nil || node.Info == nil {
		return
	}
	if node.Info.Id != 0 {
		delete(oldIDs, node.Info.Id)
		return
	}
	if node.Info.Name != "" {
		delete(oldNames, node.Info.Name)
	}
}

func (d *EtcdDiscoverySet) removeMissingNodes(oldIDs map[uint64]*DiscoveryNode, oldNames map[string]*DiscoveryNode) ([]*DiscoveryNode, []func()) {
	affected := make([]*DiscoveryNode, 0, len(oldIDs)+len(oldNames))
	publishes := make([]func(), 0, len(oldIDs)+len(oldNames))
	for _, node := range oldIDs {
		a, p := d.applyNodeDelete(node.Path, node.Info)
		affected = append(affected, a...)
		if p != nil {
			publishes = append(publishes, p)
		}
	}
	for _, node := range oldNames {
		a, p := d.applyNodeDelete(node.Path, node.Info)
		affected = append(affected, a...)
		if p != nil {
			publishes = append(publishes, p)
		}
	}
	return affected, publishes
}

func (d *EtcdDiscoverySet) resetNodesLocked() {
	d.nodesByPath = make(map[string]*DiscoveryNode)
	d.nodesByID = make(map[uint64]*DiscoveryNode)
	d.nodesByName = make(map[string]*DiscoveryNode)
}

func (d *EtcdDiscoverySet) getOrCreateCache(filter map[string]string) *cacheEntry {
	filterSnapshot := cloneFilter(filter)
	cacheKey := d.generateCacheKey(filterSnapshot)
	d.mu.RLock()
	entry, ok := d.cache[cacheKey]
	d.mu.RUnlock()
	if ok {
		return entry
	}

	return d.buildCacheEntry(cacheKey, filterSnapshot)
}

func cloneFilter(filter map[string]string) map[string]string {
	if len(filter) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(filter))
	for k, v := range filter {
		cloned[k] = v
	}
	return cloned
}

func (d *EtcdDiscoverySet) buildCacheEntry(cacheKey string, filter map[string]string) *cacheEntry {
	d.mu.Lock()
	defer d.mu.Unlock()

	entry, ok := d.cache[cacheKey]
	if ok {
		if d.cacheDirty && d.cacheInvalidateInterval > 0 {
			if time.Since(d.cacheDirtySince) < d.cacheInvalidateInterval {
				return entry
			}
		}
		return entry
	}

	if d.cacheDirty {
		if d.cacheInvalidateInterval == 0 || time.Since(d.cacheDirtySince) >= d.cacheInvalidateInterval {
			d.cache = make(map[string]*cacheEntry)
			d.cacheDirty = false
			d.cacheDirtySince = time.Time{}
		}
		entry, ok = d.cache[cacheKey]
		if ok {
			return entry
		}
	}

	newEntry := &cacheEntry{
		sorted:      make([]*DiscoveryNode, 0),
		consistent:  consistenthash.NewRing(80),
		normalRing:  make([]nodeHashType, 0),
		compactRing: make([]nodeHashType, 0),
		filter:      cloneFilter(filter),
	}

	filteredNodes := d.filterNodes(filter)
	newEntry.sorted = d.sortNodes(filteredNodes)
	newEntry.consistent.Set(d.buildMembers(filteredNodes))

	d.cache[cacheKey] = newEntry
	d.logger.Debug("Lazily created new cache for filter", "key", cacheKey, "node_count", len(filteredNodes))
	return newEntry
}

func (d *EtcdDiscoverySet) rebuildCache(entry *cacheEntry) {
	if entry == nil {
		return
	}
	entry.normalOnce.Do(func() {
		entry.normalRing = d.buildNodeHashRing(entry.sorted)
	})
}

func (d *EtcdDiscoverySet) rebuildCompactCache(entry *cacheEntry) {
	if entry == nil {
		return
	}
	entry.compactOnce.Do(func() {
		d.rebuildCache(entry)
		entry.compactRing = d.buildCompactNodeHashRing(entry.normalRing)
	})
}

func (d *EtcdDiscoverySet) filterNodes(filter map[string]string) []*DiscoveryNode {
	filteredNodes := make([]*DiscoveryNode, 0)
	for _, node := range d.nodesByPath {
		if d.matchesFilter(node, filter) {
			filteredNodes = append(filteredNodes, node)
		}
	}
	return filteredNodes
}

func (d *EtcdDiscoverySet) sortNodes(nodes []*DiscoveryNode) []*DiscoveryNode {
	sorted := make([]*DiscoveryNode, len(nodes))
	copy(sorted, nodes)
	sort.Slice(sorted, func(i, j int) bool {
		left := sorted[i]
		right := sorted[j]
		if left.Info != nil && right.Info != nil {
			leftPod := int32(0)
			rightPod := int32(0)
			if left.Info.Runtime != nil {
				leftPod = left.Info.Runtime.StatefulPodIndex
			}
			if right.Info.Runtime != nil {
				rightPod = right.Info.Runtime.StatefulPodIndex
			}
			if leftPod != rightPod {
				return leftPod < rightPod
			}
			if left.Info.Id != right.Info.Id {
				return left.Info.Id < right.Info.Id
			}
			leftLo, leftHi := left.GetNameHash()
			rightLo, rightHi := right.GetNameHash()
			if !hashEqual(leftLo, leftHi, rightLo, rightHi) {
				return hashLess(leftLo, leftHi, rightLo, rightHi)
			}
			if left.Info.Name != right.Info.Name {
				return left.Info.Name < right.Info.Name
			}
		}
		return left.Path < right.Path
	})
	return sorted
}

func (d *EtcdDiscoverySet) buildMembers(nodes []*DiscoveryNode) []string {
	members := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node.Info == nil {
			members = append(members, node.Path)
			continue
		}
		members = append(members, d.memberKey(node.Info))
	}
	return members
}

func (d *EtcdDiscoverySet) buildNodeHashRing(nodes []*DiscoveryNode) []nodeHashType {
	ring := make([]nodeHashType, 0, len(nodes)*consistentHashVNodes)
	for _, node := range nodes {
		if node == nil {
			continue
		}
		if node.Info == nil {
			continue
		}
		name := node.Info.GetName()
		id := node.Info.GetId()
		for i := 0; i < consistentHashVNodes; i++ {
			var lo, hi uint64
			if name != "" {
				lo, hi = consistentHashKey([]byte(name), uint32(i))
			} else {
				var idBuf [8]byte
				binary.LittleEndian.PutUint64(idBuf[:], id)
				lo, hi = consistentHashKey(idBuf[:], uint32(i))
			}
			ring = append(ring, nodeHashType{node: node, hashLo: lo, hashHi: hi})
		}
	}
	sort.Slice(ring, func(i, j int) bool {
		return compareNodeHash(ring[i], ring[j])
	})
	return ring
}

func (d *EtcdDiscoverySet) buildCompactNodeHashRing(normal []nodeHashType) []nodeHashType {
	if len(normal) == 0 {
		return nil
	}
	compact := make([]nodeHashType, 0, len(normal))
	previous := normal[0]
	for i := 1; i < len(normal); i++ {
		if !normal[i].node.Equal(previous.node) {
			compact = append(compact, previous)
		}
		previous = normal[i]
	}

	if len(compact) == 0 {
		compact = append(compact, previous)
	} else if !previous.node.Equal(compact[0].node) {
		compact = append(compact, previous)
	}

	return compact
}

func isCompactMode(mode pb.EtcdSearchMode) bool {
	return mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT ||
		mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE ||
		mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT ||
		mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT_UNIQUE_NODE
}

func isUniqueMode(mode pb.EtcdSearchMode) bool {
	return mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_UNIQUE_NODE ||
		mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_COMPACT_UNIQUE_NODE ||
		mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE ||
		mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT_UNIQUE_NODE
}

func isNextMode(mode pb.EtcdSearchMode) bool {
	return mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_NODE ||
		mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT ||
		mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_UNIQUE_NODE ||
		mode == pb.EtcdSearchMode_ETCD_SEARCH_MODE_NEXT_COMPACT_UNIQUE_NODE
}

func (d *EtcdDiscoverySet) lowerBoundNodeHashByConsistentHash(output []nodeHashType, key nodeHashType, filter map[string]string, mode pb.EtcdSearchMode) int {
	if len(output) == 0 {
		return 0
	}

	cache := d.getOrCreateCache(filter)
	compactMode := isCompactMode(mode)
	uniqueMode := isUniqueMode(mode)
	excludeSelf := isNextMode(mode)

	selectRing := cache.normalRing
	if compactMode {
		d.rebuildCompactCache(cache)
		selectRing = cache.compactRing
	} else {
		d.rebuildCache(cache)
		selectRing = cache.normalRing
	}

	if len(selectRing) == 0 {
		return 0
	}

	maxOutputSize := len(selectRing)
	if uniqueMode {
		maxOutputSize = len(cache.sorted)
	}

	idx := sort.Search(len(selectRing), func(i int) bool {
		return !hashLess(selectRing[i].hashLo, selectRing[i].hashHi, key.hashLo, key.hashHi)
	})
	if idx == len(selectRing) {
		idx = 0
	}

	uniqueCache := make(map[*DiscoveryNode]struct{}, maxOutputSize)
	ret := 0
	checkCount := len(selectRing)
	for ret < len(output) && ret < maxOutputSize && checkCount > 0 {
		entry := selectRing[idx]

		if excludeSelf {
			if (uniqueMode || hashEqual(key.hashLo, key.hashHi, entry.hashLo, entry.hashHi)) && key.node != nil && entry.node != nil && entry.node.Equal(key.node) {
				idx++
				if idx == len(selectRing) {
					idx = 0
				}
				checkCount--
				continue
			}
		}

		if uniqueMode {
			if _, ok := uniqueCache[entry.node]; ok {
				idx++
				if idx == len(selectRing) {
					idx = 0
				}
				checkCount--
				continue
			}
			uniqueCache[entry.node] = struct{}{}
		}

		output[ret] = entry
		ret++

		idx++
		if idx == len(selectRing) {
			idx = 0
		}
		checkCount--
	}

	return ret
}

func (d *EtcdDiscoverySet) generateCacheKey(filter map[string]string) string {
	if filter == nil || len(filter) == 0 {
		return "default"
	}
	keys := make([]string, 0, len(filter))
	for k := range filter {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteString(":")
		sb.WriteString(filter[k])
		sb.WriteString(";")
	}
	return sb.String()
}

func (d *EtcdDiscoverySet) matchesFilter(node *DiscoveryNode, filter map[string]string) bool {
	if filter == nil {
		return true
	}
	if node.Info == nil {
		return false
	}
	metadata := node.Info.Metadata
	for k, v := range filter {
		if !d.matchesFilterKey(metadata, k, v) {
			return false
		}
	}
	return true
}

func (d *EtcdDiscoverySet) matchesFilterKey(metadata *pb.AtappMetadata, key string, value string) bool {
	if value == "" {
		return true
	}
	if metadata == nil {
		return false
	}
	switch key {
	case "api_version":
		return metadata.ApiVersion == value
	case "kind":
		return metadata.Kind == value
	case "group":
		return metadata.Group == value
	case "name":
		return metadata.Name == value
	case "namespace", "namespace_name":
		return metadata.NamespaceName == value
	case "uid":
		return metadata.Uid == value
	case "service_subset":
		return metadata.ServiceSubset == value
	default:
		if metadata.Labels == nil {
			return false
		}
		nodeVal, ok := metadata.Labels[key]
		return ok && nodeVal == value
	}
}

func (d *EtcdDiscoverySet) GetNodeByConsistentHash(key string, filter map[string]string) (*DiscoveryNode, error) {
	return d.GetNodeByConsistentHashMode(key, filter, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
}

func (d *EtcdDiscoverySet) GetNodeByConsistentHashMode(key string, filter map[string]string, mode pb.EtcdSearchMode) (*DiscoveryNode, error) {
	cache := d.getOrCreateCache(filter)
	if len(cache.sorted) == 0 {
		return nil, fmt.Errorf("no service nodes available for the given filter")
	}
	keyLo, keyHi := consistentHashKey([]byte(key), consistentHashMagicSeed)
	ret := make([]nodeHashType, 1)
	count := d.lowerBoundNodeHashByConsistentHash(ret, nodeHashType{hashLo: keyLo, hashHi: keyHi}, filter, mode)
	if count <= 0 || ret[0].node == nil {
		return nil, fmt.Errorf("consistent hashing returned no members")
	}
	return ret[0].node, nil
}

func (d *EtcdDiscoverySet) GetNodesByConsistentHash(key string, n int, filter map[string]string, mode pb.EtcdSearchMode) ([]*DiscoveryNode, error) {
	cache := d.getOrCreateCache(filter)
	if len(cache.sorted) == 0 {
		return nil, fmt.Errorf("no service nodes available for the given filter")
	}
	if n <= 0 {
		return []*DiscoveryNode{}, nil
	}
	keyLo, keyHi := consistentHashKey([]byte(key), consistentHashMagicSeed)
	hashes := make([]nodeHashType, n)
	count := d.lowerBoundNodeHashByConsistentHash(hashes, nodeHashType{hashLo: keyLo, hashHi: keyHi}, filter, mode)
	if count <= 0 {
		return nil, fmt.Errorf("consistent hashing returned no members")
	}
	nodes := make([]*DiscoveryNode, 0, count)
	seen := make(map[*DiscoveryNode]struct{}, count)
	for i := 0; i < count; i++ {
		if hashes[i].node != nil {
			if _, ok := seen[hashes[i].node]; ok {
				continue
			}
			seen[hashes[i].node] = struct{}{}
			nodes = append(nodes, hashes[i].node)
		}
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("consistent hashing returned no members")
	}
	return nodes, nil
}

func (d *EtcdDiscoverySet) GetNodeByRoundRobin(filter map[string]string) (*DiscoveryNode, error) {
	cache := d.getOrCreateCache(filter)
	if len(cache.sorted) == 0 {
		return nil, fmt.Errorf("no service nodes available for the given filter")
	}
	idx := (atomic.AddUint64(&cache.roundRobin, 1) - 1) % uint64(len(cache.sorted))
	return cache.sorted[idx], nil
}

func (d *EtcdDiscoverySet) GetNodeByRandom(filter map[string]string) (*DiscoveryNode, error) {
	cache := d.getOrCreateCache(filter)
	if len(cache.sorted) == 0 {
		return nil, fmt.Errorf("no service nodes available for the given filter")
	}
	idx := rand.Intn(len(cache.sorted))
	return cache.sorted[idx], nil
}

func (d *EtcdDiscoverySet) GetAllNodes() []*DiscoveryNode {
	d.mu.RLock()
	defer d.mu.RUnlock()
	nodes := make([]*DiscoveryNode, 0, len(d.nodesByPath))
	for _, node := range d.nodesByPath {
		nodes = append(nodes, node)
	}
	return nodes
}

// Empty 实现。
func (d *EtcdDiscoverySet) Empty() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.nodesByID) == 0 && len(d.nodesByName) == 0
}

// GetNodeByID 获取NodeByID。
func (d *EtcdDiscoverySet) GetNodeByID(id uint64) *DiscoveryNode {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.nodesByID[id]
}

// GetNodeByName 获取NodeByName。
func (d *EtcdDiscoverySet) GetNodeByName(name string) *DiscoveryNode {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.nodesByName[name]
}

// GetSortedNodes 获取SortedNodes。
func (d *EtcdDiscoverySet) GetSortedNodes(filter map[string]string) []*DiscoveryNode {
	cache := d.getOrCreateCache(filter)
	result := make([]*DiscoveryNode, len(cache.sorted))
	copy(result, cache.sorted)
	return result
}

func lowerBoundSortedCompare(node *DiscoveryNode, id uint64, name string, nameLo, nameHi uint64) bool {
	if node == nil || node.Info == nil {
		return true
	}

	if node.Info.GetId() != id {
		return node.Info.GetId() < id
	}

	if name == "" {
		return false
	}

	nodeLo, nodeHi := node.GetNameHash()
	if !hashEqual(nodeLo, nodeHi, nameLo, nameHi) {
		return hashLess(nodeLo, nodeHi, nameLo, nameHi)
	}

	return strings.Compare(node.Info.GetName(), name) < 0
}

func lessNodeByBoundOrder(left, right *DiscoveryNode) bool {
	if left == nil || left.Info == nil {
		return right != nil && right.Info != nil
	}
	if right == nil || right.Info == nil {
		return false
	}

	if left.Info.GetId() != right.Info.GetId() {
		return left.Info.GetId() < right.Info.GetId()
	}

	leftLo, leftHi := left.GetNameHash()
	rightLo, rightHi := right.GetNameHash()
	if !hashEqual(leftLo, leftHi, rightLo, rightHi) {
		return hashLess(leftLo, leftHi, rightLo, rightHi)
	}

	return strings.Compare(left.Info.GetName(), right.Info.GetName()) < 0
}

func compareNodeWithBoundKey(node *DiscoveryNode, id uint64, name string, nameLo, nameHi uint64) int {
	if node == nil || node.Info == nil {
		return -1
	}

	if node.Info.GetId() != id {
		if node.Info.GetId() < id {
			return -1
		}
		return 1
	}

	if name == "" {
		return 0
	}

	nodeLo, nodeHi := node.GetNameHash()
	if !hashEqual(nodeLo, nodeHi, nameLo, nameHi) {
		if hashLess(nodeLo, nodeHi, nameLo, nameHi) {
			return -1
		}
		return 1
	}

	cmp := strings.Compare(node.Info.GetName(), name)
	if cmp < 0 {
		return -1
	}
	if cmp > 0 {
		return 1
	}
	return 0
}

func upperBoundSortedCompare(id uint64, name string, nameLo, nameHi uint64, node *DiscoveryNode) bool {
	if node == nil || node.Info == nil {
		return false
	}

	if id != node.Info.GetId() {
		return id < node.Info.GetId()
	}

	if name == "" {
		return true
	}

	nodeLo, nodeHi := node.GetNameHash()
	if !hashEqual(nameLo, nameHi, nodeLo, nodeHi) {
		return hashLess(nameLo, nameHi, nodeLo, nodeHi)
	}

	return strings.Compare(name, node.Info.GetName()) < 0
}

// LowerBoundSortedNodes 返回按 (id, name_hash, name) 排序集合中的 lower_bound 索引。
func (d *EtcdDiscoverySet) LowerBoundSortedNodes(id uint64, name string, filter map[string]string) int {
	sorted := d.GetSortedNodes(filter)
	sort.Slice(sorted, func(i, j int) bool {
		return lessNodeByBoundOrder(sorted[i], sorted[j])
	})
	nameLo, nameHi := uint64(0), uint64(0)
	if name != "" {
		nameLo, nameHi = consistentHashKey([]byte(name), consistentHashMagicSeed)
	}

	return sort.Search(len(sorted), func(i int) bool {
		return compareNodeWithBoundKey(sorted[i], id, name, nameLo, nameHi) >= 0
	})
}

// UpperBoundSortedNodes 返回按 (id, name_hash, name) 排序集合中的 upper_bound 索引。
func (d *EtcdDiscoverySet) UpperBoundSortedNodes(id uint64, name string, filter map[string]string) int {
	sorted := d.GetSortedNodes(filter)
	sort.Slice(sorted, func(i, j int) bool {
		return lessNodeByBoundOrder(sorted[i], sorted[j])
	})
	nameLo, nameHi := uint64(0), uint64(0)
	if name != "" {
		nameLo, nameHi = consistentHashKey([]byte(name), consistentHashMagicSeed)
	}

	return sort.Search(len(sorted), func(i int) bool {
		return compareNodeWithBoundKey(sorted[i], id, name, nameLo, nameHi) > 0
	})
}

// GetNodeByConsistentHashBytes 获取NodeByConsistentHashBytes。
func (d *EtcdDiscoverySet) GetNodeByConsistentHashBytes(key []byte, filter map[string]string) (*DiscoveryNode, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}
	// Convert bytes to string for consistent hashing (same as C++ libatapp behavior)
	return d.GetNodeByConsistentHash(string(key), filter)
}

// GetNodeByConsistentHashUint64 获取NodeByConsistentHashUint64。
func (d *EtcdDiscoverySet) GetNodeByConsistentHashUint64(hash uint64, filter map[string]string) (*DiscoveryNode, error) {
	var key [8]byte
	binary.LittleEndian.PutUint64(key[:], hash)
	return d.GetNodeByConsistentHashBytes(key[:], filter)
}

func (d *EtcdDiscoverySet) GetNodeByConsistentHashInt64(hash int64, filter map[string]string) (*DiscoveryNode, error) {
	var key [8]byte
	binary.LittleEndian.PutUint64(key[:], uint64(hash))
	return d.GetNodeByConsistentHashBytes(key[:], filter)
}

func appendAffectedNode(nodes []*DiscoveryNode, node *DiscoveryNode) []*DiscoveryNode {
	if node == nil {
		return nodes
	}
	for _, exist := range nodes {
		if exist == node {
			return nodes
		}
	}
	return append(nodes, node)
}

func (d *EtcdDiscoverySet) applyNodeUpdate(node *DiscoveryNode) ([]*DiscoveryNode, func()) {
	if node == nil || node.Info == nil {
		return nil, nil
	}

	affected := make([]*DiscoveryNode, 0, 4)
	affected = appendAffectedNode(affected, node)

	path := node.Path
	prev := d.nodesByPath[path]
	if prev != nil {
		if !d.shouldReplaceNode(prev, node) {
			return nil, nil
		}
		affected = appendAffectedNode(affected, prev)
	}
	if prev != nil {
		d.removeNodeIndexes(prev)
	}

	if node.Info.Id != 0 {
		if existing, ok := d.nodesByID[node.Info.Id]; ok && existing != prev && existing != node {
			affected = appendAffectedNode(affected, existing)
			d.removeNodeIndexes(existing)
			delete(d.nodesByPath, existing.Path)
		}
	}
	if node.Info.Name != "" {
		if existing, ok := d.nodesByName[node.Info.Name]; ok && existing != prev && existing != node {
			affected = appendAffectedNode(affected, existing)
			d.removeNodeIndexes(existing)
			delete(d.nodesByPath, existing.Path)
		}
	}

	d.nodesByPath[path] = node
	if node.Info.Id != 0 {
		d.nodesByID[node.Info.Id] = node
	}
	if node.Info.Name != "" {
		d.nodesByName[node.Info.Name] = node
	}

	if node != nil && d.eventPublisher != nil {
		pub, n, e := d.eventPublisher, node, prev != nil
		return affected, func() { pub(pb.EtcdWatchEventType_ETCD_WATCH_EVENT_PUT, n, e) }
	}

	return affected, nil
}

func (d *EtcdDiscoverySet) shouldReplaceNode(prev, next *DiscoveryNode) bool {
	if prev.CreateRevision > next.CreateRevision {
		return false
	}
	if prev.ModRevision > next.ModRevision {
		return false
	}
	if prev.Version > next.Version {
		return false
	}
	if prev.Equal(next) && proto.Equal(prev.Info, next.Info) {
		return false
	}
	return true
}

func (d *EtcdDiscoverySet) applyNodeDelete(path string, info *pb.AtappDiscovery) ([]*DiscoveryNode, func()) {
	if path != "" {
		if existing, ok := d.nodesByPath[path]; ok {
			deleted, publish := d.removeNodeWithEvent(existing, true)
			return []*DiscoveryNode{deleted}, publish
		}
	}

	if info != nil {
		if info.Id != 0 {
			if existing, ok := d.nodesByID[info.Id]; ok {
				deleted, publish := d.removeNodeWithEvent(existing, true)
				return []*DiscoveryNode{deleted}, publish
			}
		}
		if info.Name != "" {
			if existing, ok := d.nodesByName[info.Name]; ok {
				deleted, publish := d.removeNodeWithEvent(existing, true)
				return []*DiscoveryNode{deleted}, publish
			}
		}
	}

	return nil, nil
}

func (d *EtcdDiscoverySet) removeNodeWithEvent(node *DiscoveryNode, existed bool) (*DiscoveryNode, func()) {
	if node == nil {
		return nil, nil
	}
	d.removeNodeIndexes(node)
	delete(d.nodesByPath, node.Path)
	if fn := node.GetOnDestroy(); fn != nil {
		fn(node)
	}
	var publish func()
	if d.eventPublisher != nil {
		pub, n, e := d.eventPublisher, node, existed
		publish = func() { pub(pb.EtcdWatchEventType_ETCD_WATCH_EVENT_DELETE, n, e) }
	}

	return node, publish
}

func (d *EtcdDiscoverySet) invalidateCacheLocked() {
	d.cacheDirty = true
	d.cache = make(map[string]*cacheEntry)
	if d.cacheInvalidateInterval > 0 {
		d.cacheDirtySince = time.Now()
	}
}

func cacheEntryReferencesNode(entry *cacheEntry, node *DiscoveryNode) bool {
	if entry == nil || node == nil {
		return false
	}
	for _, n := range entry.sorted {
		if n == node {
			return true
		}
	}
	return false
}

func (d *EtcdDiscoverySet) invalidateCacheByNodesLocked(nodes []*DiscoveryNode) {
	if len(d.cache) == 0 {
		return
	}
	if len(nodes) == 0 {
		d.invalidateCacheLocked()
		return
	}

	delete(d.cache, "default")
	for key, entry := range d.cache {
		if key == "default" {
			continue
		}

		invalidate := false
		for _, node := range nodes {
			if cacheEntryReferencesNode(entry, node) {
				invalidate = true
				break
			}
			if node != nil && d.matchesFilter(node, entry.filter) {
				invalidate = true
				break
			}
		}

		if invalidate {
			delete(d.cache, key)
		}
	}
}

func (d *EtcdDiscoverySet) removeNodeIndexes(node *DiscoveryNode) {
	if node == nil || node.Info == nil {
		return
	}

	if node.Info.Id != 0 {
		if existing, ok := d.nodesByID[node.Info.Id]; ok && existing == node {
			delete(d.nodesByID, node.Info.Id)
		}
	}
	if node.Info.Name != "" {
		if existing, ok := d.nodesByName[node.Info.Name]; ok && existing == node {
			delete(d.nodesByName, node.Info.Name)
		}
	}
}

func (d *EtcdDiscoverySet) memberKey(info *pb.AtappDiscovery) string {
	if info == nil {
		return ""
	}
	if info.Id != 0 {
		return fmt.Sprintf("id:%d", info.Id)
	}
	if info.Name != "" {
		return fmt.Sprintf("name:%s", info.Name)
	}
	return ""
}

func (d *EtcdDiscoverySet) lookupMember(member string) *DiscoveryNode {
	if strings.HasPrefix(member, "id:") {
		idStr := strings.TrimPrefix(member, "id:")
		if idStr == "" {
			return nil
		}
		id, err := parseUint64(idStr)
		if err != nil {
			return nil
		}
		return d.nodesByID[id]
	}
	if strings.HasPrefix(member, "name:") {
		name := strings.TrimPrefix(member, "name:")
		if name == "" {
			return nil
		}
		return d.nodesByName[name]
	}
	return d.nodesByPath[member]
}

func parseUint64(value string) (uint64, error) {
	result, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid uint64: %s", value)
	}
	return result, nil
}

// MetadataIndexSize 实现。
func (d *EtcdDiscoverySet) MetadataIndexSize() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if len(d.cache) == 0 {
		return 0
	}
	if _, ok := d.cache["default"]; ok {
		return len(d.cache) - 1
	}
	return len(d.cache)
}

// GetNodeHashByConsistentHash 获取NodeHashByConsistentHash。
func (d *EtcdDiscoverySet) GetNodeHashByConsistentHash(key string, filter map[string]string) (string, *DiscoveryNode, error) {
	cache := d.getOrCreateCache(filter)
	if len(cache.sorted) == 0 {
		return "", nil, fmt.Errorf("no service nodes available for the given filter")
	}
	keyLo, keyHi := consistentHashKey([]byte(key), consistentHashMagicSeed)
	ret := make([]nodeHashType, 1)
	count := d.lowerBoundNodeHashByConsistentHash(ret, nodeHashType{hashLo: keyLo, hashHi: keyHi}, filter, pb.EtcdSearchMode_ETCD_SEARCH_MODE_ALL)
	if count <= 0 || ret[0].node == nil {
		return "", nil, fmt.Errorf("consistent hashing returned no members")
	}
	node := ret[0].node
	if node.Info != nil {
		return d.memberKey(node.Info), node, nil
	}
	return node.Path, node, nil
}

// RemoveNodeByID 移除NodeByID。
func (d *EtcdDiscoverySet) RemoveNodeByID(id uint64) bool {
	d.mu.RLock()
	node := d.nodesByID[id]
	d.mu.RUnlock()
	if node == nil {
		return false
	}
	d.RemoveNode(node.Path)
	return true
}

// RemoveNodeByName 移除NodeByName。
func (d *EtcdDiscoverySet) RemoveNodeByName(name string) bool {
	d.mu.RLock()
	node := d.nodesByName[name]
	d.mu.RUnlock()
	if node == nil {
		return false
	}
	d.RemoveNode(node.Path)
	return true
}

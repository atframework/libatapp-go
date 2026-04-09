package integration_test

// TestModule_Embed_Watch 系列测试覆盖增量事件（NodeUp/Down/Update、
// TopologyUp/Down/Update）及初始快照预填充的完整流水线。
//
// 这些测试需要真实的嵌入式 etcd，因为 mockserver（用于
// etcd_module_v2/module_watch_integration_test.go）无法：
//   - 在初始快照之后再下发增量 Watch 事件
//   - 在 Put 和 Get 调用之间持久化 KV 数据
//   - 为 EventWatchNodeDown / NodeUpdate /
//     EventWatchTopologyUp / Down / Update 提供真实 Watch 流
//
// 与现有单元测试 / mockserver 测试的对应关系：
//
//   现有（mockserver）                              │ 此处嵌入式 etcd 测试
//   ───────────────────────────────────────────────┼──────────────────────────────────────────────────
//   TestModule_Watch_ByID_SnapshotFlow             │ TestModule_Embed_Watch_InitialSnapshot_WithNodes
//   TestModule_Watch_Topology_SnapshotFlow         │ TestModule_Embed_Watch_InitialTopologySnapshot_WithNodes
//   TestModule_Embed_ExternalPut_TriggersWatchNodeUp│ （已在 module_embed_integration_test.go 中）
//   （无对应）                                     │ TestModule_Embed_Watch_NodeDown
//   （无对应）                                     │ TestModule_Embed_Watch_NodeUpdate
//   （无对应）                                     │ TestModule_Embed_Watch_TopologyUp
//   （无对应）                                     │ TestModule_Embed_Watch_TopologyDown
//   （无对应）                                     │ TestModule_Embed_Watch_TopologyUpdate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

// marshalTopologyJSON 将 info 编码为 proto-JSON（使用 proto 字段名），
// 格式与 RegistrationActor（codec.MarshalTopologyToJSON）写入的格式一致，
// 可被 WatchActor 中的 decodeTopologyInfo 正确解码。
func marshalTopologyJSON(t *testing.T, info *pb.AtappTopologyInfo) string {
	t.Helper()
	opts := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false}
	b, err := opts.Marshal(info)
	require.NoError(t, err)
	return string(b)
}

// ── Discovery watch 测试 ─────────────────────────────────────────────────

// TestModule_Embed_Watch_NodeDown 验证完整的删除流水线：
//
//  1. 外部 PUT 触发 EventWatchNodeUp，节点出现在快照中。
//  2. 外部 DELETE 触发 EventWatchNodeDown，Payload.Value 为 nil。
//  3. ProjectionActor 将该节点从快照中移除。
func TestModule_Embed_Watch_NodeDown(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedByIDPrefix})
	ch := subscribeEmbedEvents(t, m)

	// 等待初始空快照，确认 Watch 流已从已知 revision 建立；
	// 后续写入操作可以确保落到该流中。
	waitForEmbedEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 15*time.Second)

	extCli := newEmbedClient(t, etcdAddr)
	nodeKey := embedByIDPrefix + "/node-down-400"
	disc := &pb.AtappDiscovery{Id: 400, Name: "node-down-400"}

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer putCancel()
	_, err := extCli.Put(putCtx, nodeKey, marshalDiscoveryJSON(t, disc))
	require.NoError(t, err, "PUT 写入测试节点")

	// 等待 NodeUp 事件，再等待 ProjectionActor mailbox goroutine 完成快照更新
	// 并发布 EventProjectionSnapshotUpdated，然后直接断言。
	waitForEmbedEvent(t, ch, modulev2.EventWatchNodeUp, 5*time.Second)
	waitForEmbedEvent(t, ch, modulev2.EventProjectionSnapshotUpdated, 3*time.Second)
	snap := m.GetSnapshot()
	require.NotNil(t, snap)
	assert.NotNil(t, snap.Discovery.NodesByPath[nodeKey], "PUT 后节点必须出现在快照中")

	// 删除该节点。
	delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer delCancel()
	_, err = extCli.Delete(delCtx, nodeKey)
	require.NoError(t, err, "DELETE 测试节点")

	// Watch 流必须下发 EventWatchNodeDown。
	env := waitForEmbedEvent(t, ch, modulev2.EventWatchNodeDown, 5*time.Second)
	pl, ok := env.Payload.(modulev2.WatchNodePayload)
	require.True(t, ok, "Payload 必须是 WatchNodePayload")
	assert.Equal(t, nodeKey, pl.Key)
	assert.Nil(t, pl.Value, "Down 事件的 WatchNodePayload.Value 必须为 nil")

	// ProjectionActor 必须将该节点从快照中移除。
	waitForEmbedEvent(t, ch, modulev2.EventProjectionSnapshotUpdated, 3*time.Second)
	snap = m.GetSnapshot()
	require.NotNil(t, snap)
	assert.Nil(t, snap.Discovery.NodesByPath[nodeKey], "DELETE 后节点必须从快照中消失")
}

// TestModule_Embed_Watch_NodeUpdate 验证完整的更新流水线：
//
//  1. 外部 PUT 触发 EventWatchNodeUp，节点出现在快照中。
//  2. 对同一 key 的第二次 PUT 触发 EventWatchNodeUpdate，携带更新后的 Value。
//  3. ProjectionActor 将快照中的数据更新为新值。
func TestModule_Embed_Watch_NodeUpdate(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedByIDPrefix})
	ch := subscribeEmbedEvents(t, m)

	waitForEmbedEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 15*time.Second)

	extCli := newEmbedClient(t, etcdAddr)
	nodeKey := embedByIDPrefix + "/node-update-500"

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer putCancel()

	// 初始 PUT。
	_, err := extCli.Put(putCtx, nodeKey, marshalDiscoveryJSON(t, &pb.AtappDiscovery{Id: 500, Name: "original-name"}))
	require.NoError(t, err, "初始 PUT")

	waitForEmbedEvent(t, ch, modulev2.EventWatchNodeUp, 5*time.Second)
	waitForEmbedEvent(t, ch, modulev2.EventProjectionSnapshotUpdated, 3*time.Second)
	snap := m.GetSnapshot()
	require.NotNil(t, snap)
	n := snap.Discovery.NodesByPath[nodeKey]
	require.NotNil(t, n, "节点必须以原始名称出现在快照中")
	assert.Equal(t, "original-name", n.Info.GetName())

	// 更新：相同 key，新 value。
	updCtx, updCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer updCancel()
	_, err = extCli.Put(updCtx, nodeKey, marshalDiscoveryJSON(t, &pb.AtappDiscovery{Id: 500, Name: "updated-name"}))
	require.NoError(t, err, "更新 PUT")

	// Watch 流必须下发携带新解码值的 EventWatchNodeUpdate。
	env := waitForEmbedEvent(t, ch, modulev2.EventWatchNodeUpdate, 5*time.Second)
	pl, ok := env.Payload.(modulev2.WatchNodePayload)
	require.True(t, ok, "Payload 必须是 WatchNodePayload")
	assert.Equal(t, nodeKey, pl.Key)
	require.NotNil(t, pl.Value, "Update 事件的 WatchNodePayload.Value 不能为 nil")
	assert.Equal(t, uint64(500), pl.Value.GetId())
	assert.Equal(t, "updated-name", pl.Value.GetName())

	// 快照必须反映更新后的名称。
	waitForEmbedEvent(t, ch, modulev2.EventProjectionSnapshotUpdated, 3*time.Second)
	snap = m.GetSnapshot()
	require.NotNil(t, snap)
	n = snap.Discovery.NodesByPath[nodeKey]
	require.NotNil(t, n, "快照必须反映节点的更新名称")
	assert.Equal(t, "updated-name", n.Info.GetName())
}

// TestModule_Embed_Watch_InitialSnapshot_WithNodes 验证：调用 AddWatchPrefix
// 时 etcd 中已存在的节点会出现在 WatchSnapshotLoadedPayload.Nodes（初始 Get 快照）
// 中，而不是作为增量 NodeUp 事件下发。
func TestModule_Embed_Watch_InitialSnapshot_WithNodes(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	extCli := newEmbedClient(t, etcdAddr)

	// 在模块开始监听该前缀之前，先向 etcd 写入节点。
	preCtx, preCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer preCancel()

	keys := []string{
		embedByIDPrefix + "/pre-node-601",
		embedByIDPrefix + "/pre-node-602",
	}
	_, err := extCli.Put(preCtx, keys[0], marshalDiscoveryJSON(t, &pb.AtappDiscovery{Id: 601, Name: "pre-601"}))
	require.NoError(t, err)
	_, err = extCli.Put(preCtx, keys[1], marshalDiscoveryJSON(t, &pb.AtappDiscovery{Id: 602, Name: "pre-602"}))
	require.NoError(t, err)

	// 不带 watch 前缀启动模块，确保订阅先于初始 Get 建立。
	m := startEmbedModule(t, etcdAddr, nil)
	ch := subscribeEmbedEvents(t, m)

	// AddWatchPrefix 触发初始 Get——etcd 中已存在的节点将出现在
	// Loaded payload 中，而非作为增量 NodeUp 事件下发。
	require.NoError(t, m.AddWatchPrefix(embedByIDPrefix))

	loadedEnv := waitForEmbedEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 15*time.Second)
	lpl, ok := loadedEnv.Payload.(modulev2.WatchSnapshotLoadedPayload)
	require.True(t, ok, "Payload 必须是 WatchSnapshotLoadedPayload")
	assert.Equal(t, embedByIDPrefix, lpl.Prefix)

	// 两个预写节点必须出现在快照 payload 中。
	require.Len(t, lpl.Nodes, 2, "初始 Get 必须捕获两个预写节点")
	require.Contains(t, lpl.Nodes, keys[0])
	require.Contains(t, lpl.Nodes, keys[1])
	assert.Equal(t, "pre-601", lpl.Nodes[keys[0]].Info.GetName())
	assert.Equal(t, "pre-602", lpl.Nodes[keys[1]].Info.GetName())

	// ProjectionActor 应用事件后，GetSnapshot 也必须包含两个节点。
	waitForEmbedEvent(t, ch, modulev2.EventProjectionSnapshotUpdated, 3*time.Second)
	snap := m.GetSnapshot()
	require.NotNil(t, snap)
	assert.NotNil(t, snap.Discovery.NodesByPath[keys[0]], "快照必须包含节点 keys[0]")
	assert.NotNil(t, snap.Discovery.NodesByPath[keys[1]], "快照必须包含节点 keys[1]")
}

// ── Topology watch 测试 ───────────────────────────────────────────────────

// TestModule_Embed_Watch_TopologyUp 验证：外部向 topology 前缀下 PUT 数据后，
// 触发 EventWatchTopologyUp，并且节点出现在 GetSnapshot 的 topology 子视图中。
func TestModule_Embed_Watch_TopologyUp(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedTopoPrefix})
	ch := subscribeEmbedEvents(t, m)

	waitForEmbedEvent(t, ch, modulev2.EventWatchTopologySnapshotLoaded, 15*time.Second)

	extCli := newEmbedClient(t, etcdAddr)
	topoKey := embedTopoPrefix + "/topo-700"
	topoInfo := &pb.AtappTopologyInfo{Id: 700, Name: "topo-700"}

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer putCancel()
	_, err := extCli.Put(putCtx, topoKey, marshalTopologyJSON(t, topoInfo))
	require.NoError(t, err, "PUT topology 节点")

	// Watch 流必须下发 EventWatchTopologyUp。
	env := waitForEmbedEvent(t, ch, modulev2.EventWatchTopologyUp, 5*time.Second)
	pl, ok := env.Payload.(modulev2.WatchTopologyPayload)
	require.True(t, ok, "Payload 必须是 WatchTopologyPayload")
	assert.Equal(t, topoKey, pl.Key)
	require.NotNil(t, pl.Value, "Up 事件的 WatchTopologyPayload.Value 不能为 nil")
	assert.Equal(t, uint64(700), pl.Value.GetId())
	assert.Equal(t, "topo-700", pl.Value.GetName())

	// topology 快照必须包含新节点。
	waitForEmbedEvent(t, ch, modulev2.EventProjectionSnapshotUpdated, 3*time.Second)
	snap := m.GetSnapshot()
	require.NotNil(t, snap)
	topoNode := snap.Topology.NodesByID[700]
	require.NotNil(t, topoNode, "topology 快照必须包含节点 700")
	assert.Equal(t, "topo-700", topoNode.Info.GetName())
}

// TestModule_Embed_Watch_TopologyDown 验证 topology 的完整删除流水线：
//
//  1. 外部 PUT 触发 EventWatchTopologyUp，节点出现在快照中。
//  2. 外部 DELETE 触发 EventWatchTopologyDown。
//  3. ProjectionActor 将该节点从 topology 快照中移除。
func TestModule_Embed_Watch_TopologyDown(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedTopoPrefix})
	ch := subscribeEmbedEvents(t, m)

	waitForEmbedEvent(t, ch, modulev2.EventWatchTopologySnapshotLoaded, 15*time.Second)

	extCli := newEmbedClient(t, etcdAddr)
	topoKey := embedTopoPrefix + "/topo-down-800"
	topoInfo := &pb.AtappTopologyInfo{Id: 800, Name: "topo-down-800"}

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer putCancel()
	_, err := extCli.Put(putCtx, topoKey, marshalTopologyJSON(t, topoInfo))
	require.NoError(t, err, "PUT topology 节点")

	waitForEmbedEvent(t, ch, modulev2.EventWatchTopologyUp, 5*time.Second)
	waitForEmbedEvent(t, ch, modulev2.EventProjectionSnapshotUpdated, 3*time.Second)
	snap := m.GetSnapshot()
	require.NotNil(t, snap)
	assert.NotNil(t, snap.Topology.NodesByID[800], "topology 快照必须包含节点 800")

	// 删除该 topology 节点。
	delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer delCancel()
	_, err = extCli.Delete(delCtx, topoKey)
	require.NoError(t, err, "DELETE topology 节点")

	// Watch 流必须下发 EventWatchTopologyDown。
	// WatchActor 设置了 WithPrevKV()，Value 携带解码后的旧值。
	env := waitForEmbedEvent(t, ch, modulev2.EventWatchTopologyDown, 5*time.Second)
	pl, ok := env.Payload.(modulev2.WatchTopologyPayload)
	require.True(t, ok, "Payload 必须是 WatchTopologyPayload")
	assert.Equal(t, topoKey, pl.Key)

	// ProjectionActor 必须将该节点从 topology 快照中移除。
	waitForEmbedEvent(t, ch, modulev2.EventProjectionSnapshotUpdated, 3*time.Second)
	snap = m.GetSnapshot()
	require.NotNil(t, snap)
	assert.Nil(t, snap.Topology.NodesByID[800], "DELETE 后 topology 快照不应包含节点 800")
}

// TestModule_Embed_Watch_TopologyUpdate 验证：对已存在 topology key 的第二次 PUT
// 触发 EventWatchTopologyUpdate，并且快照反映更新后的数据。
func TestModule_Embed_Watch_TopologyUpdate(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedTopoPrefix})
	ch := subscribeEmbedEvents(t, m)

	waitForEmbedEvent(t, ch, modulev2.EventWatchTopologySnapshotLoaded, 15*time.Second)

	extCli := newEmbedClient(t, etcdAddr)
	topoKey := embedTopoPrefix + "/topo-update-900"

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer putCancel()

	// 初始 PUT（Version 变为 1）。
	_, err := extCli.Put(putCtx, topoKey, marshalTopologyJSON(t, &pb.AtappTopologyInfo{Id: 900, Name: "original-topo"}))
	require.NoError(t, err, "初始 PUT")

	waitForEmbedEvent(t, ch, modulev2.EventWatchTopologyUp, 5*time.Second)
	waitForEmbedEvent(t, ch, modulev2.EventProjectionSnapshotUpdated, 3*time.Second)
	snap := m.GetSnapshot()
	require.NotNil(t, snap)
	topo := snap.Topology.NodesByID[900]
	require.NotNil(t, topo, "topology 快照必须反映原始名称")
	assert.Equal(t, "original-topo", topo.Info.GetName())

	// 更新：相同 key，新名称（Version 变为 2 → EventWatchTopologyUpdate）。
	updCtx, updCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer updCancel()
	_, err = extCli.Put(updCtx, topoKey, marshalTopologyJSON(t, &pb.AtappTopologyInfo{Id: 900, Name: "updated-topo"}))
	require.NoError(t, err, "更新 PUT")

	// Watch 流必须下发 EventWatchTopologyUpdate（Version > 1）。
	env := waitForEmbedEvent(t, ch, modulev2.EventWatchTopologyUpdate, 5*time.Second)
	pl, ok := env.Payload.(modulev2.WatchTopologyPayload)
	require.True(t, ok, "Payload 必须是 WatchTopologyPayload")
	assert.Equal(t, topoKey, pl.Key)
	require.NotNil(t, pl.Value, "Update 事件的 WatchTopologyPayload.Value 不能为 nil")
	assert.Equal(t, uint64(900), pl.Value.GetId())
	assert.Equal(t, "updated-topo", pl.Value.GetName())

	// 快照必须反映更新后的名称。
	waitForEmbedEvent(t, ch, modulev2.EventProjectionSnapshotUpdated, 3*time.Second)
	snap = m.GetSnapshot()
	require.NotNil(t, snap)
	topo = snap.Topology.NodesByID[900]
	require.NotNil(t, topo, "topology 快照必须反映更新后的名称")
	assert.Equal(t, "updated-topo", topo.Info.GetName())
}

// TestModule_Embed_Watch_InitialTopologySnapshot_WithNodes 验证：添加 topology
// 前缀时 etcd 中已存在的 topology 节点会出现在
// WatchTopologySnapshotLoadedPayload.Nodes map（初始 Get 快照）中。
func TestModule_Embed_Watch_InitialTopologySnapshot_WithNodes(t *testing.T) {
	etcdAddr := embedEtcdEndpoint(t)
	extCli := newEmbedClient(t, etcdAddr)

	// 在模块开始监听之前，先向 etcd 写入 topology 节点。
	preCtx, preCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer preCancel()

	_, err := extCli.Put(preCtx, embedTopoPrefix+"/pre-topo-1001",
		marshalTopologyJSON(t, &pb.AtappTopologyInfo{Id: 1001, Name: "pre-topo-1001"}))
	require.NoError(t, err)
	_, err = extCli.Put(preCtx, embedTopoPrefix+"/pre-topo-1002",
		marshalTopologyJSON(t, &pb.AtappTopologyInfo{Id: 1002, Name: "pre-topo-1002"}))
	require.NoError(t, err)

	// 不带前缀启动模块，确保订阅先于初始 Get 建立。
	m := startEmbedModule(t, etcdAddr, nil)
	ch := subscribeEmbedEvents(t, m)

	require.NoError(t, m.AddWatchPrefix(embedTopoPrefix))

	loadedEnv := waitForEmbedEvent(t, ch, modulev2.EventWatchTopologySnapshotLoaded, 15*time.Second)
	lpl, ok := loadedEnv.Payload.(modulev2.WatchTopologySnapshotLoadedPayload)
	require.True(t, ok, "Payload 必须是 WatchTopologySnapshotLoadedPayload")

	// 两个预写 topology 节点必须出现在快照 payload 中。
	require.Len(t, lpl.Nodes, 2, "初始 Get 必须捕获两个预写 topology 节点")
	require.Contains(t, lpl.Nodes, uint64(1001))
	require.Contains(t, lpl.Nodes, uint64(1002))
	assert.Equal(t, "pre-topo-1001", lpl.Nodes[1001].Info.GetName())
	assert.Equal(t, "pre-topo-1002", lpl.Nodes[1002].Info.GetName())

	// GetSnapshot topology 也必须包含两个节点。
	waitForEmbedEvent(t, ch, modulev2.EventProjectionSnapshotUpdated, 3*time.Second)
	snap := m.GetSnapshot()
	require.NotNil(t, snap)
	assert.NotNil(t, snap.Topology.NodesByID[1001], "topology 快照必须包含节点 1001")
	assert.NotNil(t, snap.Topology.NodesByID[1002], "topology 快照必须包含节点 1002")
}
 // TestModule_Watch_RevisionContinuity corresponds to C++ I.3.6
// (watcher_revision_continuity).
//
// It verifies that ModRevision values carried in consecutive WatchNodePayload
// events are monotonically non-decreasing.  In C++ the test writes N keys via
// direct HTTP PUT and records the mod_revision from each watcher callback.
// In Go, keys are written via a separate external etcd client; the Watch stream
// delivers WatchNodePayload.ModRevision which mirrors the etcd KV's ModRevision.
//
// Sequential PUTs to different keys each consume a new etcd revision; the
// resulting ModRevision sequence is strictly increasing.  The assertion uses
// >= (non-decreasing) to stay consistent with the C++ CASE_EXPECT_GE and to
// remain resilient to future batching optimisations.
func TestModule_Watch_RevisionContinuity(t *testing.T) {
	// Arrange
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, []string{embedByIDPrefix})
	ch := subscribeEmbedEvents(t, m)
	extCli := newEmbedClient(t, etcdAddr)

	// Wait for the initial (empty) snapshot so the Watch stream is established
	// at a known revision before we start writing.
	waitForEmbedEvent(t, ch, modulev2.EventWatchSnapshotLoaded, 15*time.Second)

	const numKeys = 5
	disc := [numKeys]*pb.AtappDiscovery{
		{Id: 3601, Name: "rev-svc-3601"},
		{Id: 3602, Name: "rev-svc-3602"},
		{Id: 3603, Name: "rev-svc-3603"},
		{Id: 3604, Name: "rev-svc-3604"},
		{Id: 3605, Name: "rev-svc-3605"},
	}

	// Act: write all keys sequentially via the external raw client.
	// Each Put gets its own etcd revision, so ModRevision values will be
	// strictly increasing.
	putCtx, putCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer putCancel()
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("%s/%s-%d", embedByIDPrefix, disc[i].Name, disc[i].Id)
		_, err := extCli.Put(putCtx, key, marshalDiscoveryJSON(t, disc[i]))
		require.NoError(t, err, "failed to PUT key %d", i)
	}

	// Collect numKeys EventWatchNodeUp events and extract their ModRevision.
	var revisions []int64
	deadline := time.After(15 * time.Second)
	for len(revisions) < numKeys {
		select {
		case env := <-ch:
			if env.Type != modulev2.EventWatchNodeUp {
				continue
			}
			pl, ok := env.Payload.(modulev2.WatchNodePayload)
			require.True(t, ok, "NodeUp payload must be WatchNodePayload")
			revisions = append(revisions, pl.ModRevision)
		case <-deadline:
			t.Fatalf("timed out collecting EventWatchNodeUp events: got %d of %d",
				len(revisions), numKeys)
		}
	}

	// Assert: revisions must be monotonically non-decreasing.
	require.Len(t, revisions, numKeys)
	for i := 1; i < len(revisions); i++ {
		assert.GreaterOrEqual(t, revisions[i], revisions[i-1],
			"ModRevision[%d]=%d must be >= ModRevision[%d]=%d",
			i, revisions[i], i-1, revisions[i-1])
	}
}
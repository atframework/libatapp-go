package integration_test

// TestModule_Embed_TwoModules_MutualDiscovery 对应 C++ I.4.4 + I.5.3：
// 两个独立的 EtcdModule 实例共享同一嵌入式 etcd，验证相互注册后能通过
// Discovery 快照（NodesByID、NodesByName）和 Topology 快照（NodesByID）
// 发现对方。
//
// 对应关系：
//
//	C++ I.4.4  watcher_with_register:
//	    module2 注册服务后，module1 能通过 GetNodeByID / GetNodeByName 查到对方。
//	C++ I.5.3  topology_watcher_with_register:
//	    module2 注册服务后，module1 能通过 topology 快照查到对方的 Topology 信息。

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	modulev2 "github.com/atframework/libatapp-go/etcd_module_v2"
	pb "github.com/atframework/libatapp-go/protocol/atframe"
)

func TestModule_Embed_TwoModules_MutualDiscovery(t *testing.T) {
	// ── Arrange ───────────────────────────────────────────────────────────
	etcdAddr := embedEtcdEndpoint(t)

	const (
		svc1ID   = uint64(701)
		svc1Name = "mutual-disc-701"
		svc2ID   = uint64(702)
		svc2Name = "mutual-disc-702"
	)
	svc1Path := fmt.Sprintf("%s/%s-%d", embedByIDPrefix, svc1Name, svc1ID)
	svc2Path := fmt.Sprintf("%s/%s-%d", embedByIDPrefix, svc2Name, svc2ID)

	// 两个模块都同时监视 Discovery 前缀和 Topology 前缀，
	// 以便能各自发现对方写入的 discovery key 和 topology key。
	watchPrefixes := []string{embedByIDPrefix, embedTopoPrefix}

	m1 := startEmbedModule(t, etcdAddr, watchPrefixes)
	m2 := startEmbedModule(t, etcdAddr, watchPrefixes)

	ch1 := subscribeEmbedEvents(t, m1)
	ch2 := subscribeEmbedEvents(t, m2)

	// 等待两个模块均完成初始快照加载，
	// 确保后续写入不会与初始 Watch Reset 产生竞争。
	waitForEmbedEvent(t, ch1, modulev2.EventWatchSnapshotLoaded, 15*time.Second)
	waitForEmbedEvent(t, ch2, modulev2.EventWatchSnapshotLoaded, 15*time.Second)

	// ── Act 1：m1 注册 svc1 ───────────────────────────────────────────────
	ctx1, cancel1 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel1()

	h1, err := m1.RegisterService(ctx1, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: svc1ID, Name: svc1Name},
		Path:      svc1Path,
		TTL:       10,
	})
	require.NoError(t, err)
	require.NoError(t, h1.Wait(ctx1))

	// ── Assert 1a：m1 完成自身注册（EventRegistrationChanged 确认写入） ────
	waitForRegChangedWith(t, ch1, svc1Path, true, 20*time.Second)

	// ── Assert 1b：m2 通过 Watch 流发现 svc1 的 Discovery 信息 ─────────────
	// m2 监视 embedByIDPrefix，RegistrationActor 写入 discovery key 后
	// WatchActor 下发 EventWatchNodeUp，ProjectionActor 更新快照。
	require.Eventually(t, func() bool {
		snap := m2.GetSnapshot()
		return snap != nil && snap.Discovery.NodesByPath[svc1Path] != nil
	}, 10*time.Second, 20*time.Millisecond,
		"m2 必须通过 Watch 流在 Discovery 快照中发现 svc1")

	snap2 := m2.GetSnapshot()
	require.NotNil(t, snap2)
	assert.NotNil(t, snap2.Discovery.NodesByID[svc1ID],
		"m2 必须能通过 NodesByID[svc1ID] 查到 svc1")
	assert.NotNil(t, snap2.Discovery.NodesByName[svc1Name],
		"m2 必须能通过 NodesByName[svc1Name] 查到 svc1")

	// ── Assert 1c：m2 通过 Watch 流发现 svc1 的 Topology 信息 ─────────────
	// RegistrationActor 在 discovery 写入后还会写入 topology key
	// （auto-derive：key = embedTopoPrefix/<svc1Name>-<svc1ID>，
	// TopologyInfo.Id = svc1ID）。m2 监视 embedTopoPrefix，
	// WatchActor 下发 EventWatchTopologyUp，ProjectionActor 更新 topology 快照。
	require.Eventually(t, func() bool {
		snap := m2.GetSnapshot()
		return snap != nil && snap.Topology.NodesByID[svc1ID] != nil
	}, 10*time.Second, 20*time.Millisecond,
		"m2 必须通过 Watch 流在 Topology 快照中发现 svc1 的拓扑信息")

	topoNode1 := m2.GetSnapshot().Topology.NodesByID[svc1ID]
	require.NotNil(t, topoNode1)
	assert.Equal(t, svc1ID, topoNode1.Info.GetId())

	// ── Act 2：m2 注册 svc2 ───────────────────────────────────────────────
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()

	h2, err := m2.RegisterService(ctx2, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: svc2ID, Name: svc2Name},
		Path:      svc2Path,
		TTL:       10,
	})
	require.NoError(t, err)
	require.NoError(t, h2.Wait(ctx2))

	// ── Assert 2a：m2 完成自身注册 ────────────────────────────────────────
	waitForRegChangedWith(t, ch2, svc2Path, true, 20*time.Second)

	// ── Assert 2b：m1 通过 Watch 流发现 svc2 的 Discovery 信息 ─────────────
	require.Eventually(t, func() bool {
		snap := m1.GetSnapshot()
		return snap != nil && snap.Discovery.NodesByPath[svc2Path] != nil
	}, 10*time.Second, 20*time.Millisecond,
		"m1 必须通过 Watch 流在 Discovery 快照中发现 svc2")

	snap1 := m1.GetSnapshot()
	require.NotNil(t, snap1)
	assert.NotNil(t, snap1.Discovery.NodesByID[svc2ID],
		"m1 必须能通过 NodesByID[svc2ID] 查到 svc2")
	assert.NotNil(t, snap1.Discovery.NodesByName[svc2Name],
		"m1 必须能通过 NodesByName[svc2Name] 查到 svc2")

	// ── Assert 2c：m1 通过 Watch 流发现 svc2 的 Topology 信息 ─────────────
	require.Eventually(t, func() bool {
		snap := m1.GetSnapshot()
		return snap != nil && snap.Topology.NodesByID[svc2ID] != nil
	}, 10*time.Second, 20*time.Millisecond,
		"m1 必须通过 Watch 流在 Topology 快照中发现 svc2 的拓扑信息")

	topoNode2 := m1.GetSnapshot().Topology.NodesByID[svc2ID]
	require.NotNil(t, topoNode2)
	assert.Equal(t, svc2ID, topoNode2.Info.GetId())
}

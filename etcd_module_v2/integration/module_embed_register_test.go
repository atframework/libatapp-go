package integration_test

// TestModule_Embed_Register 针对真实单节点嵌入式 etcd 验证 RegistrationActor 完整写入流水线：
//
//  1. RegisterService  → 通过真实 lease 按 Topology 优先、Discovery 其次写入 etcd
//  2. 触发 EventRegistrationChanged，携带 ByPath / ByName / ByID / TopologyServices
//  3. 通过独立外部客户端直接读取 etcd 验证所有 key
//  4. 注册第二个服务 → 快照累积两条记录
//  5. UnregisterService → etcd key 被删除，事件快照不再包含 svc1

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

// waitForRegChangedWith 阻塞直到收到满足条件的 EventRegistrationChanged 事件：
// ByPath[path] 存在（present=true）或不存在（present=false）。
//
// 一次 RegisterService 调用可能触发多个事件（topology 写入一个，discovery 写入一个）；
// 本 helper 跳过不满足谓词的事件，返回第一个满足条件的快照。
func waitForRegChangedWith(
	t *testing.T,
	ch <-chan modulev2.EventEnvelope,
	path string,
	present bool,
	timeout time.Duration,
) modulev2.RegistrationChangedPayload {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case env := <-ch:
			if env.Type != modulev2.EventRegistrationChanged {
				continue
			}
			pl, ok := env.Payload.(modulev2.RegistrationChangedPayload)
			if !ok {
				continue
			}
			_, has := pl.ByPath[path]
			if has == present {
				return pl
			}
		case <-deadline:
			t.Fatalf("timed out waiting for EventRegistrationChanged with ByPath[%q] present=%v",
				path, present)
			return modulev2.RegistrationChangedPayload{}
		}
	}
}

func TestModule_Embed_Register(t *testing.T) {
	// ── Arrange ───────────────────────────────────────────────────────────
	etcdAddr := embedEtcdEndpoint(t)
	m := startEmbedModule(t, etcdAddr, nil)
	extCli := newEmbedClient(t, etcdAddr) // 独立外部客户端，用于直接读取 etcd key
	ch := subscribeEmbedEvents(t, m)

	// key 路径格式（discoveryBase = "/svc"，由 embedByNamePrefix 派生）：
	//   by-path  : <embedByIDPrefix>/<name>-<id>   （主 key，同时也是 by-id）
	//   by-name  : <embedByNamePrefix>/<name>-<id>
	//   topology : <embedTopoPrefix>/<name>-<id>
	const (
		svc1ID   = uint64(401)
		svc1Name = "reg-svc-401"
		svc2ID   = uint64(402)
		svc2Name = "reg-svc-402"
		// svc3 显式提供了与 Discovery 不同的 TopologyInfo（Name/Id 均不同），
		// 验证显式 TopologyInfo 不会被 auto-derive 逻辑覆盖。
		svc3ID       = uint64(403)
		svc3Name     = "reg-svc-403"
		svc3TopoID   = uint64(9403)
		svc3TopoName = "reg-topo-9403"
	)
	svc1Path := fmt.Sprintf("%s/%s-%d", embedByIDPrefix, svc1Name, svc1ID)
	svc1ByName := fmt.Sprintf("%s/%s-%d", embedByNamePrefix, svc1Name, svc1ID)
	svc1TopoKey := fmt.Sprintf("%s/%s-%d", embedTopoPrefix, svc1Name, svc1ID)
	svc2Path := fmt.Sprintf("%s/%s-%d", embedByIDPrefix, svc2Name, svc2ID)
	svc3Path := fmt.Sprintf("%s/%s-%d", embedByIDPrefix, svc3Name, svc3ID)
	// 显式 topology key 使用 svc3TopoName/svc3TopoID，而非 svc3Name/svc3ID。
	svc3ExplicitTopoKey := fmt.Sprintf("%s/%s-%d", embedTopoPrefix, svc3TopoName, svc3TopoID)
	// auto-derive 路径（基于 Discovery Name/Id 推导），必须**不**出现在 etcd 中。
	svc3AutoTopoKey := fmt.Sprintf("%s/%s-%d", embedTopoPrefix, svc3Name, svc3ID)

	// ── Act 1：在 lease 建立之前调用 RegisterService ─────────────────────
	// RegistrationActor 立即将条目入队；LeaseGranted 触发后执行 replay
	//（putDiscoveryWithLease + putTopologyWithLease）并发布 EventRegistrationChanged。
	// 调用方无需显式等待 lease——这正是被测试的协作式架构。
	ctx1, cancel1 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel1()

	handle1, err := m.RegisterService(ctx1, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: svc1ID, Name: svc1Name},
		Path:      svc1Path,
		TTL:       10,
	})
	require.NoError(t, err)
	// handle1.Wait 反映 actor 的 reply 时机：
	//  • 尚无 lease → 立即返回 nil（写入推迟到 LeaseGranted replay 后执行）
	//  • lease 已存在 → etcd 写入成功后返回 nil
	// 写入成功的权威证明是 EventRegistrationChanged，而非此 reply。
	require.NoError(t, handle1.Wait(ctx1))

	// ── Assert 1a：EventRegistrationChanged 证明 lease 已建立且写入已完成 ──
	// 此事件仅在 LeaseGranted 且 putDiscoveryWithLease 成功后才会到达，
	// 覆盖了直接写入和 replay 两条路径。超时时间包含完整的 lease 获取延迟。
	pl1 := waitForRegChangedWith(t, ch, svc1Path, true, 20*time.Second)

	require.NotNil(t, pl1.ByPath[svc1Path], "ByPath 必须包含 svc1 路径")
	require.NotNil(t, pl1.ByID[svc1ID], "ByID 必须包含 svc1 id")
	assert.Equal(t, svc1ID, pl1.ByID[svc1ID].GetId())
	require.NotNil(t, pl1.ByName[svc1Name], "ByName 必须包含 svc1 name")
	assert.Equal(t, svc1Name, pl1.ByName[svc1Name].GetName())
	// 未设置 TopologyInfo 时，topology 由 Discovery 自动派生。
	assert.NotNil(t, pl1.TopologyServices[svc1TopoKey], "自动派生的 topology key 必须出现在快照中")
	assert.NotZero(t, pl1.LeaseID, "真实 lease 下 LeaseID 不能为零")

	// ── Assert 1b：直接读取 etcd 验证所有 key 已写入 ────────────────────
	kCtx, kCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer kCancel()

	for _, wantKey := range []string{svc1Path, svc1ByName, svc1TopoKey} {
		resp, getErr := extCli.Get(kCtx, wantKey)
		require.NoError(t, getErr)
		assert.Equal(t, int64(1), resp.Count, "RegisterService 后 etcd key %q 必须存在", wantKey)
	}

	// ── Act 2：注册第二个服务 ──────────────────────────────────────────────
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	handle2, err := m.RegisterService(ctx2, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: svc2ID, Name: svc2Name},
		Path:      svc2Path,
		TTL:       10,
	})
	require.NoError(t, err)
	// handle2.Wait 阻塞到 actor reply；此时 lease 已存在，onAddDiscovery
	// 在 reply 前同步完成 etcd PUT。Wait 返回即代表 key 已落盘。
	require.NoError(t, handle2.Wait(ctx2))

	// ── Assert 2a：直接读取 etcd（无需异步等待）────────────────────────────
	k2Ctx, k2Cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer k2Cancel()
	svc2Resp, svc2Err := extCli.Get(k2Ctx, svc2Path)
	require.NoError(t, svc2Err)
	assert.Equal(t, int64(1), svc2Resp.Count, "handle.Wait 返回后 svc2 by-path key 必须存在于 etcd")

	// ── Assert 2b：快照累积两条记录 ────────────────────────────────────────
	// publishRegistrationChanged 在 reply 前触发，事件已在队列中；
	// waitForRegChangedWith 在此处的作用是验证内存聚合模型（ByPath/ByID/ByName），
	// 而非充当同步屏障。
	pl2 := waitForRegChangedWith(t, ch, svc2Path, true, 5*time.Second)
	assert.Len(t, pl2.ByPath, 2, "快照 ByPath 必须包含且仅包含两条记录")
	assert.NotNil(t, pl2.ByPath[svc1Path], "svc2 注册后 svc1 必须仍在快照中")
	assert.NotNil(t, pl2.ByPath[svc2Path], "svc2 必须出现在快照 ByPath 中")

	// ── Act 2b：注册 svc3（显式 TopologyInfo）────────────────────────────
	// TopologyInfo.Name/Id 与 Discovery.Name/Id 不同，端到端验证显式值
	// 在整个 actor 流水线中不被 auto-derive 逻辑覆盖。
	ctx2b, cancel2b := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2b()

	handle3, err := m.RegisterService(ctx2b, modulev2.ServiceInfo{
		Discovery: &pb.AtappDiscovery{Id: svc3ID, Name: svc3Name},
		TopologyInfo: &pb.AtappTopologyInfo{
			Id:   svc3TopoID,
			Name: svc3TopoName,
		},
		Path: svc3Path,
		TTL:  10,
	})
	require.NoError(t, err)
	// 同 svc2：调用 RegisterService 时 lease 已存在，actor 在 reply 前
	// 已将 topology 和 discovery 均写入 etcd。
	require.NoError(t, handle3.Wait(ctx2b))

	// ── Assert 2c：直接读取 etcd 验证 key 正确（无需异步等待）──────────────
	t3Ctx, t3Cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer t3Cancel()
	explicitResp, tErr := extCli.Get(t3Ctx, svc3ExplicitTopoKey)
	require.NoError(t, tErr)
	assert.Equal(t, int64(1), explicitResp.Count, "显式 topology key 必须存在于 etcd")
	autoResp, tErr := extCli.Get(t3Ctx, svc3AutoTopoKey)
	require.NoError(t, tErr)
	assert.Equal(t, int64(0), autoResp.Count, "auto-derive 的 topology key 不能出现在 etcd 中")

	// ── Assert 2d：快照使用显式 topology key，不含 auto-derive key ────────
	pl2b := waitForRegChangedWith(t, ch, svc3Path, true, 5*time.Second)
	assert.NotNil(t, pl2b.TopologyServices[svc3ExplicitTopoKey],
		"显式 topology key 必须出现在快照中")
	assert.Nil(t, pl2b.TopologyServices[svc3AutoTopoKey],
		"显式 TopologyInfo 时 auto-derive key 不能被写入")

	// ── Act 3：注销第一个服务 ─────────────────────────────────────────────
	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()

	// UnregisterService 阻塞到 onRemoveService reply；actor 在 reply 前
	// 先删除 etcd key，再调用 publishRegistrationChanged。
	// UnregisterService 返回时 key 已从 etcd 删除。
	require.NoError(t, m.UnregisterService(ctx3, svc1Path))

	// ── Assert 3a：直接读取 etcd 确认 svc1 所有 key 已删除 ─────────────────
	dCtx, dCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dCancel()
	for _, deletedKey := range []string{svc1Path, svc1ByName, svc1TopoKey} {
		resp, getErr := extCli.Get(dCtx, deletedKey)
		require.NoError(t, getErr)
		assert.Equal(t, int64(0), resp.Count, "UnregisterService 后 etcd key %q 必须被删除", deletedKey)
	}

	// ── Assert 3b：快照不再包含 svc1 ───────────────────────────────────────
	pl3 := waitForRegChangedWith(t, ch, svc1Path, false, 5*time.Second)
	assert.Nil(t, pl3.ByPath[svc1Path], "注销后 svc1 必须从快照 ByPath 中移除")
	assert.NotNil(t, pl3.ByPath[svc2Path], "svc2 必须仍在快照 ByPath 中")
	assert.NotNil(t, pl3.ByPath[svc3Path], "svc3 必须仍在快照 ByPath 中")
	assert.Len(t, pl3.ByPath, 2, "svc1 注销后快照 ByPath 中应剩余 svc2 和 svc3")
}

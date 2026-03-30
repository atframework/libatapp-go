# discovery 与 C++ 对齐说明

本文档记录 Go `discovery` 对 C++ `etcd_discovery_set` 的关键语义对齐点，避免后续回归。

## 已对齐语义

- 一致性哈希算法：使用 `murmur3 x64 128 + seed`，与 C++ `consistent_hash_calc` 对齐。
- `DiscoveryNode` 的 `name_hash` 计算：与一致性哈希算法一致。
- `lower_bound_node_hash_by_consistent_hash`：
  - 支持 `ALL/COMPACT/UNIQUE/NEXT` 及组合模式。
  - `compact`、`unique`、`exclude_self` 逻辑与 C++ 同构。
  - normal/compact ring 在查询路径按需懒重建。
- `GetNodeByConsistentHashUint64/Int64`：按原始 8 字节输入哈希，不做十进制字符串化。
- `GetNodeByRoundRobin`：首个返回 index 0（先取后增）。
- 同名/同 id 节点更新：新节点会替换旧索引映射，避免索引漂移。
- cache 失效策略：按受影响节点与 metadata 规则做定向失效（默认索引必失效）。
- `LowerBoundSortedNodes/UpperBoundSortedNodes`：新增并按 C++ `(id, name_hash, name)` 比较语义实现。

## 仍可能存在实现差异

- C++ 使用 shared_ptr 及 iterator 语义；Go 使用指针与切片索引语义。
- C++ metadata 索引结构为独立容器；Go 使用 filter key 到 cache entry 的映射。

这些差异在当前实现中属于结构层差异，不影响已对齐的行为语义。

## 禁止回退项

以下规则属于强一致约束，后续改动禁止回退：

- 一致性哈希算法必须保持 `murmur3 x64 128 + seed`，不可替换为 SHA 系列。
- `GetNodeByConsistentHashUint64/Int64` 必须按原始 8 字节输入哈希，不可转十进制字符串。
- round-robin 必须保持“先取后增”语义（首个返回 index 0）。
- `lower_bound` 路径必须保持 normal/compact ring 的懒重建语义。

若未来必须改动上述规则，需先同步更新 C++ 对照实现并补充跨语言一致性验证结果。

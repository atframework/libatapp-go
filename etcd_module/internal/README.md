# internal

内部实现目录，承接 etcd_module 内部复用能力，避免被外部 API 直接依赖。

当前子包与职责：
- `internal/codec`: discovery 数据的 JSON / base64 protobuf 编解码
- `internal/consistenthash`: discovery 集合的哈希环实现
- `internal/etcdversion`: etcd 版本三元组（create/mod/version）
- `internal/pathbuilder`: key/path 拼装辅助
- `internal/topology`: module 兼容层使用的拓扑结构与转换逻辑

维护约定：
- 新能力优先放在明确业务域包；仅跨域复用且不应暴露给外部时再放入 internal。
- 若 internal 包只被单一上层包使用，优先考虑下沉回该上层包，减少“伪通用”抽象。

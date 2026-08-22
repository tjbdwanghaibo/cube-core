# Remote Entity 生产协议

Remote Entity 只保留一条写链路：Nest 声明目标 → 全局顺序获取 write gate/ownership read lock/分布式 lock → Entity mutex 内冻结 commit → WAL 或同步 Mongo transaction → snapshot/outbox 发布 → ACK dirty → 释放 fence。

## 一致性向量

每次提交同时校验四个互不混用的维度：`StateVersion`、`MarkerEpoch`、`LockFence`、`RouteEpoch`。StateVersion 必须严格 `base+1`；ownership/route 代际只能前进；shared write 必须带非零 LockFence。TransactionID 是幂等键。

## 存储事务

kit 的 `MongoCommitter` 在同一 Mongo transaction 中完成：

1. entity meta 的 StateVersion/fence CAS；
2. 全部 DAO full document mutation 或 delete target；
3. 权威 snapshot；
4. transaction receipt 与待发布 outbox。

多实体写要求 `IRemoteAtomicBatchCommitter`，不支持时 fail-fast，绝不退化成逐实体提交。删除 commit 必须带完整 `RemoteDataDelete` 集合，codegen 自动生成。

框架只接受完整的 `IRemoteEntityBackend`，并通过 `IRemoteEntityManager.SetBackend` 一次注入，缺少 load、原子 batch commit、snapshot loader 或 outbox 任一能力都会在编译期暴露。kit 的 `NewMongoBackend` 用来组合业务只需实现的 `LoadRemoteEntity` 与通用 `MongoCommitter`。

## WAL 与 outbox

Nest WAL codec v4 持久化完整 RemoteCommit（mutation、delete、snapshot、invalidation）。Async handler 在 WAL admission 后返回，write gate 由 finalizer 持有，直到 backend status 确认。坏事务采用单次检查后退避重排队，不独占 worker。

Mongo transaction 首先标记 `Applied`；snapshot/replica 发布成功后标记 `Committed`。运行期 finalizer 会直接重放发布失败的 `Applied` 事务，模块启动时也会先恢复全部 `Applied` outbox，再绑定业务入口。未发布记录不设置 TTL，只有完成发布的记录才进入过期回收。发布和 ACK 都是幂等的。

## 读取

业务读取 `RemoteSnapshotEnvelope`，不读取远端 live Entity。L1/L2 以 tenant/entity/kind/scope/policy 为完整 key；payload 是 `FrozenRemoteSnapshotPayload`，L1 命中不复制 backing bytes，解码时显式取 copy。相同 epoch/version 的不同 checksum 被拒绝。

- Cached：允许当前缓存版本。
- Monotonic：要求不低于调用方 minVersion，短暂等待后回源。
- Linearizable：直接权威存储读取。

Interest 使用 TTL 软状态，本机在半 TTL 前不重复发布 renewal，避免每次 L1 read 产生网络消息。

## 启动约束

RemoteEntityMod 缺少以下任一能力必须启动失败：Redis marker/versioned lock、跨服 SyncBus、`IRemoteAtomicBatchCommitter`、`IRemoteSnapshotLoader`、`IRemoteCommitOutbox`。依赖在 Start 后封存，禁止运行期替换造成 data race。

Broadcast 和匿名 callback 不允许写 remote-managed entity，因为它们不具备一个明确的原子 transaction/rollback 边界。跨实体写使用声明目标的 Multi handler。

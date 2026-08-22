# Roost 核心基建生产实施说明

本文描述当前唯一保留的生产路径。旧 `AsyncSync`、Nest `Send/Sync`、匿名回调以及 Remote Entity 旧同步链路均不再提供兼容入口。

## 1. 运行模型

- 每个应用实例持有独立的 `EntityManager`、`LockManager`、`ManagerAccess` 和 `NestMgr`。Entity release/remove hook、Remote Entity manager、frame ticker 与 group lock 都属于实例；进程级注册表只允许在启动前登记不可变的 handler/component 定义。
- Entity 由 `EntityManager.Create/CreateInScope` 创建。框架先构建 Entity、获取 Entity mutex，再发布到全局索引；业务 handler 被调用前，Nest 已完成 `Touch`、确定性排序和加锁。
- 冷实体通过 `ManagerAccess` 的 `AggregateLoader` 加载。加载器必须一次读取完整聚合、完成 schema migration 和版本向量恢复，再通过 `EntityManager` 原子发布。
- 删除期间，同一 Entity ID 处于 tombstone 状态；生命周期回调完成前禁止同 ID 重建，避免旧对象清理与新对象复用同一锁或存储身份。

## 2. Nest 与事务一致性

- 业务只使用实例化 `nest.Client`，生成的 sender 负责调用 `Dispatch/Request` 系列接口；handler 只操作已加锁的 Entity/component。
- codegen 生成的业务 handler 默认 `rollback=state durability=async`；只有显式声明 `durability=memory` 才能绕过 WAL，强一致请求使用 `durability=strict`。
- 一个 handler 的普通 Entity DAO mutation 与 Remote Entity mutation 组成同一个事务描述，由一个 Mongo transaction 原子提交。
- WAL 先持久化事务意图，再提交 Mongo，最后确认 WAL。Mongo 返回不确定提交结果时进程立即 fence，拒绝新请求并进入受控退出，恢复阶段按 transaction ID 幂等重放。
- 同步请求入队后不再读取池化 `Msg`；返回通道、请求 context 和复制到栈上的 timeout 是唯一等待状态。

生产环境要求 MongoDB 使用支持事务的 replica set/sharded cluster。所有需要原子提交的 collection 必须处于同一 Mongo database scope；跨 database 原子性不被宣称。

## 3. Save/Load 与主动 Flush

- `checkpoint.Mod` 是注册到应用 Registry 的唯一 checkpoint 能力。应用不得绕过 Mod 直接持有内部 checkpoint。
- admission 失败的 save/delete 会进入有界 pending 集合，由后台 worker 重试；`Flush(ctx)` 同时排空 pending 与 checkpoint 队列。
- `checkpoint.admission_pending_capacity` 是硬上限；达到上限会触发 RuntimeFailure 并 fence 进程，禁止用无限内存换可用性。
- `Stop(ctx)` 在关闭 worker 前执行最终 flush；超时或失败必须向上返回，不允许静默丢弃。
- Mongo 保存使用版本、marker epoch、lock fence 和 route epoch 做条件更新；加载按完整聚合 snapshot 恢复，不发布半初始化 Entity。

## 4. Remote Entity

- Read 模式只暴露不可变 snapshot，L1 使用进程内原子快照，L2 使用共享 snapshot store；业务读取不获取分布式锁。
- Write 模式先通过 Redis ownership marker CAS 获取 owner/marker epoch，再获取带 fence 的分布式锁，之后才加载和修改权威 Entity。
- 提交条件包含 `StateVersion + MarkerEpoch + LockFence + RouteEpoch`。任一维度落后都会被存储层拒绝。
- ownership 切换、shared mode 和 owner transfer 均为显式状态机；状态迁移错误必须返回，不能忽略。
- Remote wrapper 使用 `remote_entity.wrapper_capacity` 和 `remote_entity.wrapper_idle_ttl` 做有界空闲淘汰；分布式锁释放失败会进入健康检查并触发 RuntimeFailure。
- marker epoch 与 lock fence 是两个独立概念，API 中分别命名，禁止混用。

## 5. 帧同步与重同步

- replication 数据面支持 snapshot、delta、LOD/interest、分片、压缩和可靠重传；服务器可按房间以 20 Hz 驱动。
- UDP 控制面使用固定长度带校验的 ACK/Resync 报文，包含 room、epoch、tick 和单调 sequence。过期 epoch、回退 sequence、非法 checksum 均被拒绝。
- `cube-kit/replication.ControlPlane` 可直接接管 UDP 控制报文；业务层不解析协议。QUIC/KCP transport 只承担传输，不改变 replication 一致性语义。

## 6. 发布顺序与门禁

发布顺序：

1. 发布 `cube-core v1.2.0`。
2. 使用已发布 core 构建并发布 `cube-kit v1.2.0`。
3. 发布 `roost-codegen v1.2.0`，新项目默认引用上述两个版本。

每次发布至少执行：

```text
go test ./...
go vet ./...
go test -race ./entity ./nest ./checkpoint ./entitysync ./replication
go test -race ./checkpoint ./remote_entity ./nestwal ./nest ./replication
git diff --check
```

生产压测必须覆盖 20 Hz、单房间 100 Entity、目标房间并发量下的 P95/P99、UDP 丢包/乱序、Redis 重启、Mongo primary 切换和 etcd compaction。CI 负责 race/vet/单元回归；依赖真实基础设施的故障演练必须在 staging release gate 执行，不能用 fake 测试替代。

第二条 race 命令在 `cube-kit` 仓库执行，并使用包含本次 core/kit 的本地 `go.work`；正式发布验证应再关闭 `go.work`，只使用已发布 module 运行一次全量测试。

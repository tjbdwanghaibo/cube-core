# cube-core

[English](#english) | [中文](#cube-core-中文说明)

`cube-core` 是 Cube 游戏服务端的通用运行时。它定义应用生命周期、能力注册、Entity / Component、Nest Actor 调度、同步、缓存、可观测性和基础安全能力；它不包含具体玩法、玩家协议或某个中间件的连接实现。

## cube-core 中文说明

### 仓库关系

```text
cube (业务服务、玩法、协议和部署)
  ├── cube-core (通用运行时与抽象)
  └── cube-kit  (Redis、Mongo、NATS、etcd 等 Mod 实现)
             └── cube-core
```

业务代码优先依赖本仓库暴露的接口和稳定类型；具体 Redis、Mongo、NATS、etcd 客户端由 `cube-kit` 提供。这样业务服务可以替换基础设施实现，而不把连接细节扩散到玩法代码中。

### 能力边界

| 领域 | 包 | 责任 |
| --- | --- | --- |
| 应用装配 | `app` | `Mod` 和 `Service` 生命周期、CLI、配置、`Registry` |
| 实体运行时 | `entity`、`nest`、`replica`、`ownerroute` | Entity / Component、串行 Actor、锁、跨实体投递、副本和路由抽象 |
| 数据一致性 | `entitysync`、`checkpoint`、`sync`、`migration` | dirty 同步、WAL/checkpoint、事件与数据版本迁移机制 |
| 通用连接抽象 | `cache`、`mongo`、`redis`、`nats`、`etcd`、`httpclient` | 面向业务的接口、类型和通用辅助逻辑；不负责生产连接装配 |
| 平台能力 | `health`、`obs`、`log`、`admin`、`lifecycle`、`security` | 健康检查、指标、日志、管理命令、生命周期 hook 与安全辅助 |
| 调度与工具 | `worker`、`timer`、`clock`、`lock`、`map`、`query` | worker pool、时间任务、逻辑时间、锁和通用容器 |

包可以依赖标准库、稳定第三方库和其他 core 抽象；不得引入 `player`、`alliance`、`activity` 等玩法语义，也不得直接依赖 `cube` 的业务包。

### 安装

发布版本可用后，业务模块按需引用：

```bash
go get github.com/tjbdwanghaibo/cube-core@latest
```

开发 Cube 三仓库时，在共同父目录创建临时工作区，不要将此 `go.work` 提交到任一仓库：

```bash
cd /path/to/workspace
go work init ./cube ./cube-core ./cube-kit
```

### 应用生命周期

`app.App` 统一管理配置与启动顺序。`Mod` 是基础能力提供者，`Service` 是业务入口。

```text
Mod.Init -> Mod.Provide -> Mod.Start -> Service.Init -> Service.Serve
                                                     -> Service.Shutdown
Mod.Stop (按注册逆序)
```

- `Init` 只读取配置、创建对象，不启动后台任务。
- `Provide` 将能力注册到 `app.Registry`；同名 capability 会失败，避免静默覆盖。
- `Start` 建立外部连接、注册健康检查并启动后台任务。
- `Stop` 必须释放资源；服务退出时按逆序执行。
- `Service.Serve` 阻塞直到上层 context 被取消，`Shutdown` 处理服务自身的优雅收敛。

最小装配形态如下。中间件 Mod 位于 `cube-kit`，业务 `Service` 由应用自行实现。

```go
application := app.New("game", "v1.0.0").
    Mods(sharedMods...).
    RegisterServer("game", gameService, gameOnlyMods...)

if err := application.Execute(); err != nil {
    panic(err)
}
```

服务从 `Registry` 使用类型安全的 capability，而不是引用全局单例：

```go
client, ok := app.Lookup[MyClient](registry, "my_client")
if !ok {
    return errors.New("my_client capability is unavailable")
}
_ = client
```

Entity 生命周期同样使用服务拥有的实例。启动阶段为 manager 配置由 Redis/etcd
分配 block 的 `entity.IDGen.Generate`，随后把 `ManagerAccess` 同时注入 Nest、
Remote Entity 和业务 Service；创建、销毁、分组索引都不会经过全局 manager：

```go
manager := entity.NewEntityManager()
idGen, err := entity.NewIDGen(initialBlock, acquireNextBlock)
if err != nil { return err }
if err := manager.ConfigureIDGenerator(idGen.Generate); err != nil { return err }
entities := entity.NewManagerAccess(manager)
```

### 并发与一致性约束

- Entity 状态修改在 Nest handler 或明确的 entity guard 内完成，避免绕过 Actor 串行模型直接写指针。
- 不能在 Nest handler 内进行同步跨实体调用；跨实体写使用 cast，必须等待结果时把同步调用放到 handler 外层。
- 数据库、RPC、消息发布等外部副作用使用 `nest.AfterCommit`、outbox 或独立 service 流程，不能在锁内执行。
- entity sync 只保留 observer-free 链路：`EntityBase.Sync()` 保存内容状态，`SubscriptionCoordinator` 保存订阅关系，业务 packer 在 Entity mutex 内生成 `FrozenSyncPayload`。
- `ReliableEnvelopeSink` 必须原子接收完整 envelope batch。只有 admission 成功后 prepared state 才提交 version、清除 dirty；失败保留 dirty，可安全重试。
- 定时同步和主动 flush 都调用 `SubscriptionCoordinator.FlushSubject`，业务层不接触 Entity mutex，也不持有 observer/session/history 状态。完整契约见 [Entity Sync](ENTITY_SYNC.md)。
- 长期状态必须有可恢复的 source；内存 map 只能作为缓存或索引。

### 日志与可观测性

`log` 基于标准库 `slog`，支持 JSON、文件轮转、服务标识、逻辑帧和调用点输出。应用配置中启用调用点：

```yaml
log:
  level: info
  json: false
  caller: true
```

`health`、`obs` 和 `admin` 在 `app.Registry` 创建时即注册。具体 Mod 应在 `Start` 阶段把外部依赖的健康检查和指标接入这些能力。

### etcd Watch 回调契约

`etcd.IWatcher` 保留 channel 形式，供需要自行控制背压的底层代码使用；`etcd.WatchCallback` 将一个 watcher 交给单个有序回调消费，并通过 `IWatchSubscription` 暴露 `Done/Err/CloseWithContext`。回调返回错误或 panic 时只终止该订阅，错误可由 `Err()` 观测。

类型化本地镜像通过 `ILocalMirrorSubscriber.Subscribe` 提供更适合业务的回调：默认先发送完整 Snapshot，随后发送 Put/Delete，断线重建时再次发送 Snapshot。实现必须保证订阅注册与初始快照原子化、回调在镜像锁外运行，并用有界队列隔离慢订阅者。

### 实时复制 LOD

`replication.LODProjector` 在不可变 Snapshot 上执行 per-session Object LOD，不读取或锁定业务 Entity。它支持：

- 八个细节等级和 `LODCulled` 对象裁剪。
- 按组件 LOD mask、优先级和最大更新频率裁剪数据。
- 非采样帧沿用该 Session 上次已发送的组件值，避免产生错误的 ComponentRemove。
- 网络质量档位、当前投影、上一投影和 Full Resync 上下文。
- 对象裁剪、组件省略、组件降频保持等原子统计。

可见性 Projector 应作为 `LODProjectorConfig.Upstream`，先消除客户端无权得知的数据，再执行距离和质量降级。业务只负责返回 LOD 决策，基线、Delta、恢复和并发安全由 replication 底层处理。

发送状态采用 prepare/commit 事务。`SendLatest` 会在完整 datagram batch 被 transport 接收后自动提交；自定义发送链路必须使用 `PrepareLatest`，成功后调用 `PreparedFrame.Commit()`，失败调用 `Abort()`。`BuildLatest` 仅用于无副作用预览，预览帧不能被 ACK。传输切换会等待在途发送结束，并强制下一帧 full refresh。

### checkpoint WAL 删除语义

`SnapshotWALRequired` / `SnapshotWALModeDurable` 下，WAL 实现必须同时实现 `DeleteSnapshotWAL`；durable 模式还必须实现 `DurableDeleteSnapshotWAL`。删除先写 tombstone、后进入 journal，后端 `BulkRemove` 成功后才 ACK tombstone，因此进程崩溃后不会由旧 save WAL 复活已删除实体。Checkpoint 启动时先完成 WAL replay，再接受实时 flush。

### 开发与验证

业务请求的新执行模型、锁边界和兼容迁移见
[Roost 业务执行模型](RUNTIME_EXECUTION_MODEL.md)。新代码使用实例化
`nest.NewEngine`/`nest.Client` 与协议无关的 `gateway` 契约，不再新增业务 Runtime
门面或直接依赖 `nest.Nest`。

Remote Entity 的唯一生产协议、Mongo fenced commit、Nest WAL/outbox、L1/L2 snapshot 与恢复边界见 [Remote Entity](REMOTE_ENTITY.md)。

```bash
go test ./app ./entity ./nest ./entitysync ./log
go test ./...
```

修改公开接口时，请同时检查：生命周期是否可停止、是否需要 health/metrics、是否泄漏业务语义、以及是否能在没有具体中间件的测试环境中被替换。

### 许可证

本仓库随 Cube 项目以 [MIT License](LICENSE) 发布。

## English

`cube-core` is the generic runtime for Cube game services. It provides application lifecycle management, typed capability registration, entity/Nest scheduling, synchronization, observability, and infrastructure abstractions. Gameplay and concrete middleware clients belong in [`cube`](https://github.com/tjbdwanghaibo/cube) and [`cube-kit`](https://github.com/tjbdwanghaibo/cube-kit).

```bash
go get github.com/tjbdwanghaibo/cube-core@latest
```

For local development of all three repositories, create a temporary workspace with `go work init ./cube ./cube-core ./cube-kit` from their common parent directory.

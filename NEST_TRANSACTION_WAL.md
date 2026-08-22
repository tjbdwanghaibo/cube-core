# Nest Transaction、WAL 与 Outbox 生产方案

## 1. 目标

Nest handler 在进入业务代码前已经持有所有目标 entity 的 mutex。本方案在不把锁暴露给业务层的前提下，提供：

- handler 失败或 panic 时的内存状态回滚；
- 多 entity 修改组成一个原子提交记录；
- 返回成功前可选择 WAL 严格刷盘；
- 进程崩溃后的顺序恢复；
- 外部消息的 transactional outbox；
- 主动 `Flush(ctx)`；
- 不在 entity 锁内执行数据库和消息系统 I/O；
- 生成代码的低开销 undo 路径，并拒绝不完整的旧事务标记。

## 2. 职责边界

### cube-core

`core/nest` 只定义框架语义：事务状态机、rollback/durability 策略、commit record、participant、committer 和 entity 解锁通知。core 不依赖文件系统、Mongo、Redis 或消息中间件。

### cube-kit

`kit/nestwal` 提供生产基础设施：分段文件 WAL、CRC、group commit、fsync、ack checkpoint、replay、重试、outbox、健康状态和主动 flush。`CheckpointApplier` 将 replay 接到现有 `checkpoint.StorageBackend`。

### roost-codegen

生成的 DAO 提供：

- 无副作用的 `CaptureRollbackState` / `RestoreRollbackState`；
- setter 级 inverse undo；
- map 按 key 去重的 undo；
- 不消费 dirty/patch 的最终持久化 after-image；
- `CommitParticipant.PrepareCommit`。

业务层仍然只调用生成的 component/entity API，不处理锁、WAL 和 rollback。

## 3. handler 策略

```go
//nest:rollback undo
//nest:durability strict
func Transfer(...) error
```

Rollback：

- `none`：无事务开销，只适合不修改状态的 handler；
- `state`：handler 前抓取完整、无副作用 snapshot；
- `undo`：生成 setter 第一次修改字段时记录 inverse operation，推荐热路径。

Durability：

- `memory`：不写 commit WAL，沿用 entity release/checkpoint；
- `async`：等待 WAL write，不等待本批 fsync；后台按间隔刷盘；
- `strict`：等待所在 group commit 完成 fsync 后才返回成功。

durability 非 `memory` 时必须配置 rollback。调用 `nest.Emit` 会自动把 memory 事务提升为 strict，防止出现名义上的 outbox 实际可能丢失。

## 4. 提交时序

1. Nest 按既有顺序持有涉及的 entity mutex。
2. 建立 `RollbackTx`，抓取 dirty snapshot 或注册 undo。
3. 执行 handler。setter 修改状态并标 dirty。
4. handler 失败：逆序执行 undo/snapshot restore，恢复 dirty/version。
5. handler 成功：DAO participant 生成最终持久化 after-image；该过程不清 dirty，不消费 patch。
6. committer 将整个多 entity record 追加到 WAL。
7. strict 在 group fsync 成功后形成 commit point。
8. Nest 提交内存事务，随后 entity release hook 运行既有 checkpoint/sync 逻辑并释放锁。
9. 解锁后通知 committer，replay 才允许落库和发布 outbox。
10. mutations 与 effects 全部成功后，持久化 ack checkpoint。

同一进程内的“暂缓到解锁后 replay”避免 WAL applier 与 entity release checkpoint 并发写同一实体。若进程在第 7 步后、第 9 步前崩溃，新进程没有暂缓表，会直接从 WAL 恢复，符合 commit point 语义。

## 5. WAL 格式与恢复

- 单 writer goroutine，调用方可并发 append；
- segment 默认 256 MiB；
- record 默认上限 16 MiB；
- frame 包含 magic、格式版本、payload 长度、header CRC、payload CRC；
- payload 为确定性二进制编码，header map 按 key 排序；
- strict 请求在 batch 中合并为一次 fsync；
- async 默认每 10 ms 刷盘；
- 启动时只截断最后 segment 的不完整尾 frame；
- 完整 frame CRC 错误、segment 缺口、ack 越界均拒绝启动，不静默跳过；
- writer 目录使用 OS 文件锁，禁止双进程同时写；
- ack 使用双槽 checkpoint，更新一个槽时另一个槽保持可恢复；
- replay 严格按 append 顺序；ack 后清理过老且已确认的 segment。

## 6. 一致性和幂等

WAL 是 at-least-once replay。以下情况会重复执行：mutation 已落库但 ack 尚未刷盘；effect 已发布但 ack 尚未刷盘；ack 文件更新失败。

因此：

- mutation applier 必须按 `(entity, version)` 做 CAS；“已存在相同或更高 version”视为成功；
- `MutationApplier.ApplyMutations` 一次接收整个多 entity transaction；需要对外可见的跨实体原子性时，实现必须使用数据库原生 transaction；
- effect consumer/publisher 必须以 `Effect.ID` 去重；
- 不允许把不可幂等的外部调用放入 `AfterCommit`；应改为 `nest.Emit`；
- `CheckpointApplier` 已将 `VersionConflict` 视为幂等成功。

`CheckpointApplier` 会把同一 transaction 的 mutations 合并为一次 `BulkSave`，但 `StorageBackend.BulkSave` 接口本身不承诺跨文档 ACID；它提供 WAL 原子提交和最终完整恢复。转账、交易等要求读侧永远看不到中间态的场景，应提供使用 Mongo transaction、SQL transaction 等实现的自定义 `MutationApplier`。

生成 DAO 的 WAL version 使用 `DirtyTracker.Version()+1`，与随后 entity release 的正常 checkpoint 版本一致。

## 7. fsync 不确定结果

设备在 write/fsync 报错时可能无法证明数据究竟是否持久化。WAL 返回 `nest.ErrCommitIndeterminate` 并进入 terminal 状态：

- core 不执行内存 rollback，避免 WAL 实际已提交时产生第二条相反历史；
- 不运行 outbox release 通知；
- WAL 拒绝后续 append；
- `OnFatal` 必须让实例停止接流并退出，由新进程 replay 判定最终历史。

这不是可在线重试的普通错误。生产环境必须将 `OnFatal` 接到进程 fencing/shutdown。

## 8. 接入示例

```go
walOpts := nestwal.DefaultOptions("./data/nest-wal")
walOpts.OnFatal = func(err error) {
    // 标记实例不可用、停止接流并触发进程退出。
}

commitOpts := nestwal.DefaultCommitterOptions()
runtime, err := nestwal.OpenRuntime(
    walOpts,
    nestwal.CheckpointApplier{Backend: storageBackend},
    messagePublisher,
    commitOpts,
)
if err != nil {
    return err
}

nestMod := nestkit.NewMod(entityGetter, runtime.NestOption())
```

主动排空：

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
if err := runtime.Flush(ctx); err != nil {
    return err
}
```

停服顺序：停止网关接流 -> 等待 Nest 请求和 entity guard 排空 -> `runtime.Shutdown(ctx)` -> 关闭 storage/message client。

## 9. 监控建议

至少采集：WAL queue、segment/offset、append bytes、fsync 次数和耗时、unacked 年龄、replay failures、mutation/effect 重试次数、最后错误、segment 磁盘占用。以下情况告警：

- terminal/indeterminate error：立即最高级别告警并 fencing；
- unacked oldest age 持续超过业务目标；
- WAL 目录空间低于可容纳最大恢复窗口；
- queue 长时间高水位；
- replay 连续失败或 outbox 重试持续增长。

## 10. 发布与升级

这次新增了 core Nest 契约，发布顺序必须是：

1. 发布包含 transaction API 的 core v1.2.0；
2. roost-codegen 依赖该 core 版本并重新生成 DAO/Nest handler；
3. kit 依赖同一 core 版本并发布；
4. 应用升级生成代码和 kit；
5. 先以 memory handler 灰度，再逐个把关键命令切换为 async/strict。

不存在 V1/V2 双写或旧 `rollback=dirty` 兼容链路。升级时必须重新生成 DAO/Nest 代码；生产 durable handler 必须使用无副作用的生成快照，或显式实现 `RollbackSnapshotter`。

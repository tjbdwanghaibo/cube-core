# Entity Sync 生产契约

Entity Sync 只保留一套实现：Entity 持有 observer-free 内容状态，`entitysync.SubscriptionCoordinator` 持有 subscriber membership，kit 的 room replication 负责帧调度和 transport admission。

## 写入与锁

- 业务只调用 `EntityBase.MarkSyncDirty` 或 `MarkSyncFullDirty`。
- packer 总是在 Entity mutex 内执行，返回 `FrozenSyncPayload`；返回后业务不得再持有可变 payload 引用。
- Entity 不保存 player/session/observer/history，避免生命周期和网络背压反向污染业务实体。

## Flush

- 定时任务和主动 flush 都调用 `SubscriptionCoordinator.FlushSubject(ctx, base.Sync())`。
- coordinator 对一个 subject 串行 prepare/distribute，对不同 subject 并行。
- `ReliableEnvelopeSink.AdmitEnvelopes` 必须整批成功或返回错误，禁止部分 admission。
- admission 成功才 `Commit` prepared state；失败执行 abort，version 不前进、dirty 不清除。

## 订阅

Subscribe 先安装 pending membership，再生成对应 profile 的完整 snapshot；snapshot admission 成功后才转 active。Unsubscribe 的 leave envelope 同样使用可靠 admission。LOD、权限和阵营视图通过有限的 `SyncProfile` 表达，不能把 subscriber ID 放进 Entity packer。

## 关闭与容量

房间销毁必须逐个 unsubscribe 或释放 coordinator；sink 必须提供有界队列、背压、session sequence、ACK/history 和 transport retry。Entity 清理会关闭唯一的 sync state，不存在 AsyncSync、observer sync 或兼容双写。

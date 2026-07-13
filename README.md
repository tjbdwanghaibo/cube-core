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

### 并发与一致性约束

- Entity 状态修改在 Nest handler 或明确的 entity guard 内完成，避免绕过 Actor 串行模型直接写指针。
- 不能在 Nest handler 内进行同步跨实体调用；跨实体写使用 cast，必须等待结果时把同步调用放到 handler 外层。
- 数据库、RPC、消息发布等外部副作用使用 `nest.AfterCommit`、outbox 或独立 service 流程，不能在锁内执行。
- entity sync 的 payload 由业务提供 snapshot/patch；底层同步模块不理解玩法 protobuf。
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

### 开发与验证

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

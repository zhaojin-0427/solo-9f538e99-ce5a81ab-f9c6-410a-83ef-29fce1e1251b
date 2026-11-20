# 单浮标潮位校时服务

浮标每次重启都会把毫秒计数归零，潮位分析却需要一条可信的 UTC 时间线。
本服务（FastAPI + SQLite）接收「开机段 + 段内计数」的观测与校时点，
把计数线性换算成 UTC 毫秒，并维护观测的可用状态供分析查询。

## 快速开始

```bash
docker compose up --build
# 服务监听 http://localhost:8000 ，交互式文档在 http://localhost:8000/docs
```

本地开发（Python 3.11+）：

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --reload
pytest                 # 运行测试
```

数据默认存放在命名卷 `buoy-data` 的 `/data/buoy.db`（可用环境变量
`BUOY_DB` 覆盖）。SQLite 为单写者模型，容器以单进程运行。

## 概念与规则

- **开机段（boot_segment）**：一次开机周期。计数只在段内单调，重启归零。
- **校时点（calibration point）**：`(开机段, 计数) -> UTC 毫秒`。
  同段同计数重复提交时新值替换旧值。
- **观测（observation）**：`(观测 ID, 开机段, 计数, 潮位)`。
  同一观测 ID 重复提交会覆盖原文并重算。

### 校时换算

- 计数落在相邻两个校时点之间时线性换算并**向下取整**：
  `utc = t_lo + (c - c_lo) * (t_hi - t_lo) // (c_hi - c_lo)`。
- 恰好命中校时点时直接取该点 UTC；只有一个校时点时仅该计数可换算。
- 计数落在本段校时区间之外：保留原文，状态置 `pending_calibration`（待校时）。

### 观测状态机

| 状态 | 含义 | 是否供分析 |
|---|---|---|
| `ok` | 已校时且无冲突 | ✅ |
| `pending_calibration` | 计数在校时区间外，待校时 | ❌ |
| `conflict` | 不同开机段落在同一 UTC 毫秒 | ❌ |

- **补传/更正校时点**会重算受影响观测（区间扩大可让待校时观测转为可用）。
- **冲突**：同一 UTC 毫秒被 ≥2 个不同开机段的观测占用时，这些观测全部
  置为 `conflict` 不供分析；更正校时点或覆盖观测解除冲突后自动恢复。
  同一开机段内多条观测共用同一毫秒不算冲突。
- **整批拒绝**：校时点合并后若使任一段内 UTC 随计数倒退，整个校时批次
  以 `409` 拒绝并回滚，旧数据照常供数。

### 原子性

每批提交（原文 + 校时点 + 重算 + 冲突刷新）都在**单个 SQLite 事务**中
完成，并由进程级锁串行化读写：原文、校时依据、分析供数同次生效，
查询和（崩溃后的）重启都不会看到半次重算的中间态。

## 潮位无损分块压缩

密集采样使潮位占用大量数据卷空间。可对指定开机段启用**无损分块存储**：

- 压缩段的潮位原值不再存于 `observations`，而是按段归集到压缩块：
  每块**至多 256 条**，条目为 `[obs_id, tide_level]`，先以 JSON 编码
  （浮点逐位可逆）再经 **zlib 无损压缩**落库；`observations` 只保留
  `block_id` 块引用且 `tide_level` 为 NULL。
- 启用后该段**后续收到的潮位也按块保存**，自动追加进未满块，写满新建。
- 同 ID 改值或跨段覆盖后，块内原值同步更正，详情 `/observations/{id}`
  与供数 `/tide-levels` 都返回新值；跨段时原值从旧块移除、按新段形态落位。
- **按需解压**：查单条、查 UTC 短窗、更正单条时只解压涉及的块
  （同一次请求内同一块最多解压一次）；**补登校时点完全不解压潮位**，
  仍只更新观测的 UTC、状态与跨段冲突。
- 普通段与压缩段可在同一库内长期**混存**，重启后依旧混查；旧 SQLite 卷
  首次打开时自动迁移（新增块引用列、数据逐列保留），接口字段与回执规则不变。
- 启用压缩或提交批次失败时，事务整体回滚：原值、块引用与供数均保持
  提交前状态。

### `POST /segments/{boot_segment}/compression` — 启用段压缩

无请求体。响应给出当次转入压缩块的观测数 `compressed_count`：

```json
{
  "boot_segment": "boot-1",
  "compression_enabled": true,
  "compressed_count": 300,
  "block_count": 2,
  "max_entries_per_block": 256
}
```

接口幂等：对已启用的段再次调用时 `compressed_count` 为 0。

## API

均可在 `/docs` 中直接调用。

### `POST /observations` — 分批提交观测

```json
{
  "observations": [
    {"obs_id": "obs-1", "boot_segment": "boot-1", "counter": 1500, "tide_level": 2.34}
  ]
}
```

### `POST /calibrations` — 分批提交校时点

```json
{
  "calibrations": [
    {"boot_segment": "boot-1", "counter": 1000, "utc_ms": 1700000000000},
    {"boot_segment": "boot-1", "counter": 2000, "utc_ms": 1700000001000}
  ]
}
```

两个提交接口的响应都会列出**本次提交中时刻或状态发生变化的观测 ID**
（含新观测，以及被卷入/解除冲突的其他观测）：

```json
{
  "changed_count": 1,
  "changed": [{"obs_id": "obs-1", "utc_ms": 1700000000500, "status": "ok"}]
}
```

### `GET /tide-levels?start_ms=...&end_ms=...` — 按 UTC 范围查可用潮位

只返回 `status=ok` 的观测，区间含端点，按 UTC 毫秒升序。

### `GET /observations/{obs_id}` — 按观测 ID 查状态和原因

返回原文、换算结果、状态、原因与校时依据（用于换算的上下界校时点）：

```json
{
  "obs_id": "obs-1", "boot_segment": "boot-1", "counter": 1500,
  "tide_level": 2.34, "utc_ms": 1700000000500, "status": "ok",
  "reason": null,
  "calibration": {
    "lo": {"counter": 1000, "utc_ms": 1700000000000},
    "hi": {"counter": 2000, "utc_ms": 1700000001000}
  }
}
```

### 其他

- `GET /health` — 健康检查
- `GET /docs` — Swagger UI；`GET /openapi.json` — OpenAPI 模式

## 目录结构

```
app/
  main.py   # FastAPI 路由与请求/响应模型
  core.py   # 校时换算、批量写入、重算、冲突检测、压缩开关与变更快照
  blocks.py # 潮位块的 zlib 无损编解码、按需解压与块内增删改
  db.py     # SQLite 连接、模式、旧卷迁移、事务与锁
tests/
  test_api.py          # 原有接口行为
  test_compression.py  # 分块压缩、按需解压、回滚、迁移与重启混查
Dockerfile
docker-compose.yml
```

# 逐行数据分支与三方合并系统

分析人员在独立分支上修订一批数据，主线同期继续接收新增/删除，下游按主线变更
订阅。本项目从零实现真实行数据、分支、三方合并、冲突裁决、合并撤销和订阅切换，
**不调用 Git 代替数据合并，也不只画版本树**——所有合并语义都是自己实现的行级
状态机。

## 1. 核心问题与设计

### 1.1 共同基线（common baseline）

创建分支时冻结主线当时的完整行快照（`branch_baseline`）和主线事件位点
（`base_seq`）。之后对每个 key 比较三方状态：

| 符号 | 含义 |
|---|---|
| `base` | 分支创建时的共同基线状态 |
| `branch` | 分支当前状态（value / tombstone / absent）|
| `main` | 主线当前状态 |

### 1.2 三方判定表（`app/merge.py`，纯函数、无 I/O）

| 分支是否改过 | 主线是否改过 | 判定 | 结果 |
|---|---|---|---|
| 否 | 任意 | 干净 | **NOOP，主线决定一切**——即使分支里留着旧副本也绝不覆盖主线新值（stale guard）|
| 是 | 否 | 干净 | 快进分支变更（upsert/delete）|
| 是 | 是，且两边结果相同 | 干净（收敛）| NOOP |
| 是 | 是，结果不同 | **冲突** | 进入逐项裁决 |

冲突覆盖两类：edit/edit（两边都改值）与 edit/delete（一边改、一边删）。
裁决选项：`branch`（重放分支变更）、`main`（丢弃分支变更）、`custom`
（人工合并快照）、`delete`（逻辑删除）。

### 1.3 两种合并输出方式

- **replay（重放分支变更）**：按分支操作流水的原始 seq 顺序回放干净变更，
  裁决项随后应用。消费者看到的事件形状与普通主线流量一致。
- **snapshot（合并快照）**：把合并后每个受影响 key 的最终状态作为一次性
  物化波次发出，消费者可整体替换而非排序回放。

两种模式最后都写一条 **`merge_switch`** 事件作为原子切换点标记。

### 1.4 合并工作进程状态机

```
prepared ──begin──> resolving ──commit──> committing ──> committed
     \                 \                     ▲
      \──abort──────────┴──abort─────────────┘（崩溃后 parked，recover 续做）
                     abort 后分支回 open，主线零改动
```

提交时在单事务内**重新基于当前主线校验计划**：裁决期间主线又变化会产生新冲突，
拒绝提交要求重新裁决；主线已与分支收敛则人工裁决自动失效、走干净判定。

### 1.5 订阅与切换点跨越（at-least-once）

- 每个主线事件在**同一事务**里写入 `main_events` 和每个订阅的 `outbox`
  （事务性发件箱模式），数据先持久化再通知。
- SSE 客户端重连带 `?last_seq=N`：先补发 durable backlog，再接实时尾流，
  outbox 行在帧成功产出后才标记 delivered。
- 消费者按 event id（`seq`）幂等去重；即使断线恰好发生在切换瞬间，
  `merge_switch` 也会随 backlog 补齐，消费者在该标记处原子跨越到合并后视图。
- 新订阅自动回填全部历史事件。

### 1.6 两类可演示故障

| 故障开关 | 行为 | 恢复手段 |
|---|---|---|
| `crash_in_committing` | 工作进程切到 committing 后死亡，事务回滚，合并 parked、主线未动 | `/api/recover`（或进程重启）从持久化裁决计划续做，switch 标记恰好一次 |
| `drop_notification_after_commit` | 提交成功但实时 SSE 推送被静默吞掉 | outbox 保持未送达；重连 backlog 或 `/api/subscriptions/{id}/replay` 手动重放 |

## 2. 目录结构

```
app/
  merge.py      # 纯三方合并逻辑（状态、判定表、裁决），单测覆盖
  storage.py    # SQLite schema + 全部数据操作 + 合并状态机 + outbox
  hub.py        # asyncio SSE 中枢，sync 请求线程 → 事件循环桥接
  api.py        # FastAPI 路由
scripts/
  seed.py       # 演示场景播种（edit/edit、edit/delete、stale guard 全包含）
  run.sh        # 启动脚本
web/
  index.html / styles.css / app.js   # 零构建控制台
tests/
  test_merge_logic.py     # 16 个纯规则测试
  test_integration.py     # 10 个端到端 API/订阅/故障测试
```

## 3. 运行

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
bash scripts/run.sh                 # 首次自动播种，监听 :8000
# 浏览器打开 http://localhost:8000
```

测试：

```bash
.venv/bin/python -m pytest tests/ -q
```

## 4. 网页控制台功能

- **主线/分支页**：主线行写入/删除、事件日志；创建分支（记录共同基线）、
  分支行写入/删除、分支操作流水。
- **合并与冲突裁决页**：选择 replay/snapshot；表格逐 key 并列展示
  基线/分支/主线三值与判定徽章；冲突项可逐项选择采用分支、保留主线、
  删除或填写自定义快照 JSON；提交、切换输出模式、撤销未完成合并。
- **订阅与切换点页**：创建订阅（回填历史）、SSE 连接/断开模拟重连、
  实时事件流（merge_switch 高亮）、客户端幂等去重开关、手动重放未送达事件。
- **故障注入页**：两个故障开关 + 一键恢复，附语义说明。

## 5. 播种场景说明

| key | 基线 | 分支动作 | 主线同期动作 | 合并预期 |
|---|---|---|---|---|
| k1 | v1 | 修改 | 无 | 干净，快进分支值 |
| k2 | v1 | 修改 | 修改 | edit/edit 冲突，需裁决 |
| k3 | v1 | 修改 | 删除 | edit/delete 冲突，需裁决 |
| k4 | v1 | **不动** | 修改 | 干净 NOOP，主线新值保留（stale guard）|
| k5 | v1 | 删除 | 无 | 干净删除 |
| k6 | 无 | 无 | 新增 | 干净 NOOP |
| k7 | 无 | 新增 | 无 | 干净插入 |

## 6. 主要 API

```
POST   /api/main/rows                 DELETE /api/main/rows/{key}
POST   /api/branches                  GET    /api/branches/{id}
POST   /api/branches/{id}/rows        DELETE /api/branches/{id}/rows/{key}
POST   /api/branches/{id}/merge       {mode: replay|snapshot}
POST   /api/merges/{id}/begin|mode|resolve/{key}|commit|abort
POST   /api/subscriptions             GET    /api/subscriptions/{id}
GET    /api/subscriptions/{id}/stream?last_seq=N     (SSE)
POST   /api/subscriptions/{id}/replay
POST   /api/faults | /api/recover | /api/reset
GET    /api/state | /api/main/events
```

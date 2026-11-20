# AIOps-Sentry 生产级开发与系统实现规范蓝图 (Implementation Blueprint)

本文档旨在在系统进入编码开发前，将所有涉及的数据结构、系统拓扑、锁拓扑分析算法、多副本高可用状态机、Kubernetes 交互细节与安全防线彻底确定，避免后续开发出现反复。

---

## 1. 核心架构与端到端时序全景

```
[故障发生 (如死锁/OOM)]
        │
        ▼ (微服务全局异常拦截器 / Envoy Access Log)
[阶段 1: 毫秒级内联快照直推]
  - 格式: HTTP POST /v1/sentry/webhook/alertmanager
  - 携带: annotations["sentry.aiops/stacktrace"] (<=2KB) + 指标快照
        │
        ▼ (无网络 I/O，直接内存解析，耗时 < 3ms)
[阶段 2: Jev 单次前向 Prefill 仲裁]
  - Endpoint: POST /v1/systemone (AnyJev / vLLM-jev)
  - 输入: 标准化 SRE Prompt
  - 约束 Tokens: FORCE_KILL_LONG_TRANSACTION, DRAIN_TRAFFIC, RESTART_POD, SCALE_OUT, ESCALATE_HUMAN
  - 输出: Logits 归一化概率分布 (耗时 ~20ms)
        │
        ▼ (同步拦截器管道，耗时 < 2ms)
[阶段 3: Safety Envelopes 静态红线与多副本租约校验]
  - 校验项 1: 置信度 >= 0.75 且 Margin >= 0.20
  - 校验项 2: K8s Lease / 内存分布式防震荡锁 (180s Cooldown)
  - 校验项 3: 物理指标约束 (死锁/连接池爆满 100% 严禁 Pod 重启)
  - 校验项 4: PDB 爆炸半径配额 (单次最多驱逐 1 个 Pod 或 20% 流量)
        │
        ├─ [未通过 / 需人工介入] ──> 发送飞书/钉钉 Interactive Card (带一键授权回调)
        │
        ▼ [安全校验通过]
[阶段 4: 两级执行器快速处置]
  - 若为 K8s 动作 (DRAIN/RESTART/SCALE): 调用 K8s API (耗时 15~35ms)
  - 若为 DB 动作 (FORCE_KILL): 路由至 DB Remediation Proxy，解析锁拓扑树定位 Root Blocker 并查杀
        │
        ▼
[阶段 5: 异步后验探针与审计]
  - 记录完整 TraceID、决策分布与操作结果
  - 5 秒后检测服务健康状态 (HTTP 200 比率)，未恢复触发二级降级
```

---

## 2. 数据契约与接口设计规范 (Data Contracts)

### 2.1 告警接入统一契约 (`sentry.schemas.FailureContext`)
无论告警来源于 Prometheus Alertmanager 还是网关直推，网关适配器均必须提取并归一化为如下结构：

```json
{
  "service_name": "order-service",
  "namespace": "production",
  "pod_name": "order-service-67b8d9-x1y2",
  "cluster_id": "k8s-prod-us-west",
  "telemetry": {
    "cpu_usage_pct": 98.2,
    "memory_usage_pct": 46.5,
    "active_connections": 100,
    "max_connections": 100,
    "p99_latency_ms": 3200.0
  },
  "error_logs": "HikariPool-1 - Connection is not available, request timed out after 30000ms. Lock wait timeout exceeded",
  "trigger_rule": "HikariPoolConnectionStarvation",
  "raw_labels": {
    "severity": "P0",
    "team": "core-transaction"
  }
}
```

### 2.2 Alertmanager 注解约定标准
在 Prometheus 的 `alerting_rules.yml` 中，强制统一注解字段命名：
* `sentry.aiops/stacktrace`: 存储崩溃时捕获的最近 2KB 关键错误堆栈；
* `sentry.aiops/active-connections`: 存储当前的连接池水位或数据库活跃连接数；
* `sentry.aiops/db-target`: 关联的目标数据库实例名称（如 `order-mysql-primary`）。

---

## 3. 两级执行器设计细节 (Two-Tier SOP Execution)

### 3.1 核心难题突破：DB 锁拓扑发现与 Root Blocker 精准查杀
在传统的 SRE 体系中，直接执行 `KILL` 极易误杀无辜的等待者（Waiters）。本设计在 `sentry/executor.py` 中引入专有的 `LockGraphResolver`：

#### 算法流程：
1. **连接受限管理端口**：DB 代理使用具备 `PROCESS` 与 `CONNECTION_ADMIN` 权限（绝不给予数据表 SELECT 权限）的轻量运维账号；
2. **查询锁等待视图**（以 MySQL 8.0 `performance_schema.data_lock_waits` 与 `information_schema.innodb_trx` 为例）：
   ```sql
   SELECT 
       r.trx_mysql_thread_id AS requesting_thread_id,
       b.trx_mysql_thread_id AS blocking_thread_id,
       TIMESTAMPDIFF(SECOND, b.trx_started, NOW()) AS duration_sec,
       b.trx_query AS blocking_query
   FROM performance_schema.data_lock_waits w
   JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_engine_transaction_id
   JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_engine_transaction_id;
   ```
3. **构建有向等待图 (DAG)**：
   * 节点为 `thread_id`；
   * 有向边 `A -> B` 表示线程 A 正在等待线程 B 释放锁。
4. **定位 Root Blocker**：
   * 拓扑排序中**入度为 0 且出度 > 0**（被多个线程等待、但自己不等待任何人）且运行时间最长的节点即为**持锁元凶**；
5. **安全查杀**：
   * 执行 `KILL CONNECTION <root_blocker_thread_id>`；
   * 记录审计日志，释放上百个级联等待者的连接池。

### 3.2 Kubernetes 资源动作实现标准
* **`DRAIN_TRAFFIC`（流量毫秒级摘除）**：
  * **实现方案**：通过 Kubernetes API 向目标 Pod 添加 Label `sentry.traffic/status: isolated`，同时从 Service 的 Selector 中临时移除，Pod 不会被销毁，保持现场以便后续排障；
  * **回滚支持**：自愈验证通过后，重新贴回正常标签即可瞬间恢复流量。
* **`RESTART_POD`（优雅驱逐与重建）**：
  * **实现方案**：调用 `delete_namespaced_pod`，设置 `grace_period_seconds: 5`，触发关联的 Deployment/ReplicaSet 重新拉起新 Pod。
* **`SCALE_OUT`（安全受限弹性扩容）**：
  * **实现方案**：调用 `patch_namespaced_deployment_scale`；
  * **配额防线**：`new_replicas = min(current_replicas + 1, max_allowed_replicas)`，单次步进仅为 1，严禁指数级膨胀打垮 Kubernetes 节点。

---

## 4. 多副本高可用与状态防震荡设计 (Multi-Replica HA & Anti-Flapping)

如果 Sentry 在生产集群部署了多个副本，内存型的状态机无法跨 Pod 共享。为此设计双模式支持：

1. **单机/开发测试模式 (`STANDALONE`)**：
   * 使用内存字典 `_action_history` 与时间戳计算；
2. **生产集群多副本模式 (`K8S_LEASE`)**：
   * 采用 Kubernetes 原生的 `coordination.k8s.io/v1` `Lease` 资源作为分布式锁，**无需引入 Redis 等外部状态中间件**；
   * 键名格式：`sentry-cooldown-<service_name>`，有效时间（LeaseDurationSeconds）设为 180 秒；
   * 任何副本试图执行自愈前，先发起创建/更新 Lease，若租约尚未过期则直接判定为处于冷却期，自动拦截并压制二次震荡。

---

## 5. 人机协同二次确认交互闭环 (Human-in-the-Loop Protocol)

当自愈决策触发以下任一条件时，强制切断自动化，转为生成飞书/钉钉交互卡片：
1. Jev 置信度 $< 0.75$ 或 Margin $< 0.20$；
2. 目标服务处于 180s 冷却窗口内；
3. 触碰安全信封红线（如死锁场景判定为重启）；
4. 动作被人工标记为高危类别。

### 交互卡片协议结构
* **Card Payload**：
  * 故障概要：服务名、命名空间、受损指标快照；
  * Jev 推荐动作与概率解释；
  * 阻断/升级原因；
  * 交互按钮：【批准执行推荐动作】（回调端点 `POST /v1/sentry/approval`，携带 5 分钟有效期的 JWT 签名令牌）、【忽略告警】。

---

## 6. 四阶段代码开发规划 (Development Roadmap)

| 阶段 | 交付核心组件 | 关键交付产物与验证标准 |
| :--- | :--- | :--- |
| **Phase 1** *(已完成)* | 骨架与仲裁管道 | `sentry/server.py`, `MockJevEngine`, `SafetyEnvelope`, 零依赖轻量单测全部跑通。 |
| **Phase 2** | 两级执行器与 DB 锁拓扑 | 实现 `LockGraphResolver`，编写模拟锁等待 DAG 拓扑解析与 Root Blocker 提取单测。 |
| **Phase 3** | K8s Lease 分布式防震荡与双模网关 | 实现基于 Kubernetes Lease 的防震荡租约；支持 Prometheus 内联快照与 Direct Push 接收。 |
| **Phase 4** | 人机协同审批端点与端到端集成 | 实现 `/v1/sentry/approval` 授权审批闭环；输出完整的压测与端到端故障演练报告。 |

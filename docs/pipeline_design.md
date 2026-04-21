# 流水线设计文档
## 基于 CCTV 静态图像的分布式交通监控流水线

---

## 1. 系统架构总览

```
511ny.org（数据源）
        ↓
  ┌─────────────────────────────────────────┐
  │            采集层（node1）               │
  │  ingest_web / ingest_web_priority        │
  └───────────────┬─────────────────────────┘
                  ↓
        ┌─────────┴──────────┐
        ▼                    ▼
  MinIO（原始图像）     MongoDB（元数据）
        │                    │
        └─────────┬──────────┘
                  ▼
  ┌─────────────────────────────────────────┐
  │          处理层（node2）                 │
  │  quality_check → curate → feature_extract│
  └───────────────┬─────────────────────────┘
                  ↓
  ┌─────────────────────────────────────────┐
  │          服务层（node2）                 │
  │        FastAPI + Web Dashboard           │
  └─────────────────────────────────────────┘
```

---

## 2. DAG 设计

系统共有 4 个 Airflow DAG，覆盖摄像头目录同步、标准采集、优先采集和特征提取四条流水线。

---

### 2.1 DAG：`traffic_camera_catalog_sync_web`

**文件：** `dags/traffic_cameras_sync_web_pipeline.py`  
**调度：** `@daily`（每天一次）  
**触发属性：** `catchup=False`，`max_active_runs=1`  
**执行节点：** node2（`processing` queue）

#### 任务结构

```
sync_web_cameras
```

#### 流程说明

每天从 511ny.org 分页拉取所有摄像头元数据，解析后写入 MongoDB `cameras` 集合。对源数据缺少 state/county 的摄像头从内置 FIPS 编码表补全；对坐标有效的摄像头调用 Nominatim API 反向地理编码。曾被标记为 `no_feed` 的摄像头状态不被覆盖，防止状态错误重置。

---

### 2.2 DAG：`traffic_snapshot_web_pipeline`（标准全量采集）

**文件：** `dags/traffic_snapshot_web_pipeline.py`  
**调度：** `*/10 * * * *`（每 10 分钟）  
**触发属性：** `catchup=False`，`max_active_runs=1`  
**执行节点：** ingest_batch_* → node1；quality_check / curate → node2

#### 任务结构

```
ingest_batch_0 ──┐
ingest_batch_1 ──┼──→ quality_check ──→ curate_hourly_summary
ingest_batch_2 ──┘
```

#### 流程说明

读取所有 `status=active` 摄像头，分 3 个批次并行下载（每批最多 100 台），时间槽对齐到 10 分钟边界。每张图像：
1. HTTP GET 下载（带时间戳参数防缓存）
2. 检查 HTTP 状态码与 Content-Type
3. 计算 MD5 检测"无信号"占位图
4. 计算 SHA-256 校验和
5. 上传至 MinIO（路径：`camera_id={id}/date={YYYY-MM-DD}/hour={HH}/{id}_{ts}.jpg`）
6. 元数据写入 MongoDB `raw_captures`

采集完成后依次执行质量检查（`quality_check`）和小时汇总（`curate`）。

---

### 2.3 DAG：`traffic_snapshot_web_priority_pipeline`（优先路段高频采集）

**文件：** `dags/traffic_snapshot_web_priority_pipeline.py`  
**调度：** `*/2 * * * *`（每 2 分钟）  
**触发属性：** `catchup=False`，`max_active_runs=1`  
**执行节点：** ingest_priority_images → node1；quality_check / curate → node2

#### 任务结构

```
ingest_priority_images ──→ quality_check ──→ curate_hourly_summary
```

#### 流程说明

只处理 `priority=True` 的摄像头（I-87、I-95、I-495 等主干道），时间槽对齐到 2 分钟边界。处理逻辑与标准流水线相同，但质量检查以 `cycle_minutes=2` 调用，只审计优先摄像头。

---

### 2.4 DAG：`traffic_feature_extract_pipeline`

**文件：** `dags/traffic_feature_extract_pipeline.py`  
**调度：** `*/2 * * * *`（每 2 分钟）  
**触发属性：** `catchup=False`，`max_active_runs=1`  
**执行节点：** node2（`processing` queue）

#### 任务结构

```
extract_mock_features
```

#### 流程说明

独立运行，扫描过去 2 小时内尚未被处理（`derived_features` 中无对应记录）的成功采集记录。优先处理 `priority` 摄像头，结果写入 `derived_features` 集合。每条结果通过 `object_key` 直接关联 MinIO 中的原始图像。当前为 mock 实现，数据模型已为真实 CV 模型预留接口。

---

## 3. 触发属性汇总

| DAG | 调度 | catchup | max_active_runs | Queue |
|-----|------|---------|-----------------|-------|
| `traffic_camera_catalog_sync_web` | `@daily` | False | 1 | processing |
| `traffic_snapshot_web_pipeline` | `*/10 * * * *` | False | 1 | ingest / processing |
| `traffic_snapshot_web_priority_pipeline` | `*/2 * * * *` | False | 1 | ingest / processing |
| `traffic_feature_extract_pipeline` | `*/2 * * * *` | False | 1 | processing |

**`catchup=False` 的理由：** 图像 API 无历史回放能力，补跑历史时间槽只会请求当前图像，产生错误的时间戳对齐。

**`max_active_runs=1` 的理由：** 防止并发运行同一 DAG 造成同一时间槽被重复采集或重复审计。

---

## 4. 分布式部署

系统部署在三台物理机上，通过 Airflow CeleryExecutor + Celery Queue 路由机制控制任务分发。

### 节点分工

| 节点 | IP | 服务 | 监听 Queue | 职责定位 |
|------|-----|------|-----------|---------|
| node0 | 10.10.8.10 | Postgres、Redis、MongoDB、MinIO、Airflow Webserver、Scheduler | — | 基础设施 + 调度控制面 |
| node1 | 10.10.8.11 | Airflow Worker | `ingest` | I/O 密集型：HTTP 下载、MinIO 上传 |
| node2 | 10.10.8.12 | Airflow Worker、FastAPI | `processing` | 计算密集型：图像解码、特征计算、聚合查询、API 服务 |

**node0 不跑业务任务的理由：** 调度器对延迟敏感，与大量 I/O 或计算任务共置会导致调度抖动，影响全局时间精度。

### Queue 路由机制

```
Scheduler 发布任务（带 queue 标签）
        ↓
  Redis Broker
  ┌────────────┬─────────────┐
  │  ingest    │  processing  │
  └─────┬──────┴──────┬───── ┘
        ↓              ↓
      node1           node2
```

---

## 5. 数据流时序

```
T+0:00   Priority ingest    → 采集优先摄像头（node1）
T+0:01   Quality check 2min → 审计优先摄像头（node2）
T+0:01   Curate             → 更新小时汇总（node2）
T+0:02   Feature extract    → 处理新图像，priority 优先（node2）
         …（每 2 分钟循环）

T+0:10   Normal ingest      → 3 批并行采集所有摄像头（node1）
T+0:11   Quality check 10min→ 审计所有摄像头（node2）
T+0:11   Curate             → 更新小时汇总（node2）
         …（每 10 分钟循环）

T+24:00  Catalog sync       → 从 511ny.org 更新摄像头目录（node2）
```

---

## 6. 技术选型与理由

### 6.1 Apache Airflow（工作流调度）

**选择理由：**
- 基于 DAG 的任务编排，任务依赖关系（ingest → quality_check → curate）以代码形式表达，可版本控制
- CeleryExecutor 原生支持多节点分布式执行，Queue 机制实现任务到节点的精确路由
- 内置重试、超时、告警机制，减少运维开发量
- Web UI 提供实时任务状态监控和历史运行记录

**对比课程中其他方案：**
- *Cron + shell scripts*：无依赖管理、无分布式支持、无可视化，适合单机简单任务
- *Spark Structured Streaming*：面向持续流处理，本项目为周期性批量采集，引入流处理框架增加不必要复杂度

### 6.2 MongoDB（元数据存储）

**选择理由：**
- 文档模型适合摄像头元数据的异构字段（不同摄像头缺失不同地理字段），无需 schema migration 即可扩展
- 聚合管道（Aggregate Pipeline）原生支持 `camera_hourly_summary` 所需的 group-by 统计
- `cameras`、`raw_captures`、`quality_audits`、`derived_features` 四个集合之间通过 `camera_id + capture_ts` 关联，适合 document store 的引用模式
- 写入吞吐量满足每 2 分钟数百条记录的写入需求

**对比课程中其他方案：**
- *PostgreSQL*：强 schema 约束，摄像头地理字段的可选性处理需要大量 nullable 列或 EAV 设计，不如文档模型直观
- *Cassandra*：适合极高写入吞吐的时序数据，本项目写入量适中，且需要灵活的聚合查询，Cassandra 的查询限制（不支持任意 group-by）不适合

### 6.3 MinIO（对象存储）

**选择理由：**
- 兼容 S3 API，路径分区方案（`camera_id/date/hour/`）直接对应 S3 prefix 查询模式，未来可无缝迁移至云存储
- 图像文件（JPEG, ~50–200KB）天然适合对象存储，不适合存入关系型或文档型数据库
- 自托管，数据不离开私有集群，符合合规要求

**对比课程中其他方案：**
- *HDFS*：适合大文件批量处理（MapReduce/Spark），对小文件（每张图像几十 KB）存储效率低，且运维复杂度高
- *本地文件系统*：无法跨节点共享访问，不支持分布式部署

### 6.4 Redis（消息代理）

**选择理由：**
- Airflow CeleryExecutor 的官方推荐 broker，配置简单，延迟低（亚毫秒级任务分发）
- 内存数据库，任务队列操作（push/pop）性能远超基于磁盘的 broker

**对比课程中其他方案：**
- *RabbitMQ*：功能更丰富（路由规则、死信队列），但本项目队列逻辑简单（两个 queue），引入 RabbitMQ 是过度设计
- *Kafka*：适合高吞吐持久化消息流，作为 Celery broker 配置复杂，且本项目不需要消息持久化回放

### 6.5 FastAPI（API 服务）

**选择理由：**
- 异步支持（async/await），适合同时处理 MinIO 图像代理和 MongoDB 查询的 I/O 密集型请求
- 自动生成 OpenAPI 文档，便于下游系统接入
- 轻量，与 Airflow Worker 共置于 node2 不产生资源冲突

**对比课程中其他方案：**
- *Flask*：同步模型，并发图像代理请求会产生线程阻塞；需额外配置 Gunicorn 才能生产可用
- *Django*：全栈框架，本项目只需纯 API 层，Django 引入过多不需要的组件

---

## 7. MongoDB 集合总览

| 集合 | 写入时机 | 主要用途 |
|------|---------|---------|
| `cameras` | 每日同步 | 摄像头目录、状态管理 |
| `raw_captures` | 每次采集 | 原始图像元数据、采集结果 |
| `quality_audits` | 每次质量检查 | 逐条质量标记 |
| `camera_hourly_summary` | 每次汇总 | 小时级完整率报表 |
| `derived_features` | 每次特征提取 | CV 分析结果 |

---

## 8. 对外访问地址

| 服务 | 地址 |
|------|------|
| Airflow UI | `http://10.10.8.10:8080` |
| MinIO Console | `http://10.10.8.10:9001` |
| FastAPI | `http://10.10.8.12:8000` |
| Web Dashboard | 浏览器打开 `web/index.html`，API 地址填 `http://10.10.8.12:8000` |

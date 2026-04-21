# 流水线需求规格说明书
## 基于 CCTV 静态图像的分布式交通监控流水线

---

## 1. 概述

本文档定义交通监控流水线所需支持的下游数据需求。流水线从纽约州 511 系统的 1000+ 个交通摄像头采集静态图像，可靠地存储原始资产，并为下游分析准备经过整理的数据集。流水线本身不执行下游分析——它提供使此类分析成为可能的数据基础设施。

---

## 2. 数据来源

| 来源 | 类型 | 说明 |
|------|------|------|
| 511ny.org 图像 API | REST/HTTP | 根据摄像头 ID 和时间戳返回 JPEG 静态图像 |
| 511ny.org 目录 API | REST/HTTP | 返回所有已注册交通摄像头的元数据（分页） |

源 API 不提供历史回放能力。图像必须在采集时捕获，错过的采集无法事后补回。

---

## 3. 下游使用场景与流水线支持需求

### 3.1 交通状况分析

**下游需求：** 分析人员需要评估每个摄像头随时间变化的道路状况，识别拥堵模式、车道占用情况或可见事故。

**流水线必须提供：**
- 每个摄像头在每个时间槽采集图像的可靠记录，存储于对象存储中，路径格式一致且可查询
- 将每张图像与其摄像头、时间戳及地理上下文（路段、方向、区域）关联的元数据
- 包含每张图像预计算指标的派生特征表（`derived_features`）：估计车辆数、交通密度（0.0–1.0）、拥堵等级（`free` / `moderate` / `heavy` / `standstill`）和场景变化分数
- 稳定的连接键（`camera_id + capture_ts + object_key`），使下游任务能够为任意派生特征记录检索源图像

### 3.2 基于时间的监控

**下游需求：** 支持同一摄像头在不同时段或不同日期之间的对比；支持检索指定时间窗口内的图像序列。

**流水线必须提供：**
- 图像存储时使用时间对齐的采集时间戳（对齐到调度间隔边界，而非原始下载时间），确保跨日对比的一致性
- 每条记录同时存储 `capture_ts`（对齐时间槽）和 `ingest_ts`（实际下载时间），使下游能区分计划时间与实际可用时间
- 优先摄像头的高频采集（每 2 分钟）与标准摄像头（每 10 分钟）分开运行，使时间序列分辨率能够按摄像头级别匹配下游需求
- `camera_hourly_summary` 表，按（摄像头、日期、小时）汇总完整率、重复和延迟指标，使下游在对给定时间窗口运行分析之前能评估数据可靠性

### 3.3 以摄像头为中心的查询

**下游需求：** 高效访问给定摄像头的所有图像和元数据；按路段、地区、方向或时间间隔过滤。

**流水线必须提供：**
- `cameras` 集合，包含每个摄像头的结构化、可查询元数据：`camera_id`、`roadway`、`direction`、`location`、`latitude`、`longitude`、`state`、`county`、`region`、`status`、`priority`
- 超出源 API 提供范围的地理信息增强：当源数据缺少 county/state 时从 FIPS 编码表补全；对坐标有效的摄像头通过 Nominatim 进行反向地理编码
- 对象存储路径按 `camera_id / date / hour` 分区，无需全表扫描即可高效检索某摄像头或某时间范围内的所有图像
- API 端点支持按 `roadway`、`county`、`state`、`priority`、`status` 和时间范围过滤（见第 5 节）

### 3.4 派生特征提取

**下游需求：** 使图像处理任务能够计算车辆数、交通密度和场景变化等特征；保留从特征到源图像的可追溯性。

**流水线必须提供：**
- `derived_features` 集合，包含字段：`camera_id`、`capture_ts`、`object_key`、`model_id`、`model_version`、`vehicle_count`、`traffic_density`、`congestion_level`、`scene_change_score`、`confidence`、`processed_at`
- `derived_features` 中的 `object_key` 字段直接指向 MinIO 中的原始图像，确保特征始终可追溯到来源图像
- `model_id` 和 `model_version` 字段支持多个模型版本迭代，不覆盖历史结果
- 幂等处理设计：对已处理记录重新运行特征提取不产生重复（通过 `camera_id + capture_ts + model_id` 唯一索引强制执行）
- 优先摄像头图像在采集后的同一个 2 分钟周期内处理；标准摄像头在下一个可用的 2 分钟特征提取窗口内处理

> **说明：** 当前流水线使用模拟特征提取器（`feature_extract_mock.py`）。数据模型和处理接口已完整定义；将模拟替换为真实 CV 模型只需重新实现特征计算函数，无需更改 schema 或流水线结构。

### 3.5 运营报表

**下游需求：** 按走廊、区域或时段显示交通状况的仪表盘或报表；能够同时查看汇总指标和源图像。

**流水线必须提供：**
- `camera_hourly_summary` 集合，提供每摄像头每小时指标：`images_expected`、`images_received`、`completeness_rate`、`duplicate_count`、`corrupted_count`、`delayed_count`、`avg_delay_sec`
- 区分 `no_feed` 摄像头（源端硬件/信号问题）与采集失败，使报表能准确反映运营健康状况与流水线健康状况
- REST API（`/cameras`、`/cameras/{id}/summary`、`/cameras/{id}/features`、`/cameras/{id}/captures`、`/images/{object_key}`），使仪表盘无需直接访问数据库即可查询元数据、指标、特征并检索原始图像

---

## 4. 数据质量需求

流水线必须产生足够高质量的数据，使下游用户在消费之前能评估可靠性。

| 需求 | 实现方式 |
|------|---------|
| **完整性追踪** | 每个预期采集时间槽均被审计；缺失或失败的采集在 `quality_audits` 中以 `is_missing_expected=True` 显式记录 |
| **重复检测** | 对每个摄像头的连续采集比较 SHA-256 哈希；通过 MD5 检测静态"无信号"占位图并排除 |
| **损坏检测** | 从 MinIO 下载图像后用 PIL 解码；失败记录标记为 `is_corrupted=True` |
| **延迟追踪** | 每张图像记录 `ingest_ts − capture_ts`；超过 120 秒的采集标记为 `is_delayed=True` |
| **校验和完整性** | 每张图像在 `raw_captures` 中存储 SHA-256 校验和；支持下游随时验证文件完整性 |

---

## 5. 下游访问 API 接口

| 端点 | 用途 |
|------|------|
| `GET /cameras` | 摄像头列表，支持 `roadway`、`county`、`state`、`priority`、`status` 过滤 |
| `GET /cameras/{id}` | 单个摄像头完整元数据 |
| `GET /cameras/{id}/captures` | 历史采集记录，支持时间范围过滤 |
| `GET /cameras/{id}/summary` | 小时级完整率与质量指标 |
| `GET /cameras/{id}/features` | 摄像头的派生 CV 特征 |
| `GET /images/{object_key}` | 从 MinIO 代理返回原始图像 |

---

## 6. 可扩展性需求

流水线必须支持业务增长而无需大规模重新设计：

- **更多摄像头：** 采集按批次分区；增加摄像头只增加批次负载，无需结构变更
- **更高频率：** 更改调度间隔只需重新配置 DAG；存储和数据模型与间隔无关
- **附加元数据：** `cameras` 集合基于文档模型（MongoDB）；新字段无需迁移即可添加
- **新 CV 模型：** `derived_features` 中的 `model_id` / `model_version` 字段支持多个模型版本在同一集合中共存
- **新下游系统：** REST API 将下游消费者与直接数据库访问解耦

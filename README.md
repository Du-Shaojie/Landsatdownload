# Landsat 8/9 影像批量下载系统

基于 USGS STAC API 发现场景、AWS S3 下载 Landsat 8/9 L1 + L2 双产品影像。支持按 GeoJSON 边界自动发现 WRS-2 Path/Row、智能场景筛选、断点续传、校验重试等功能。

## 系统架构与工作流程

```
main.py 启动
  │
  ├── 加载 .env 配置 → 初始化日志 → 校验 AWS 密钥
  ├── 初始化 SQLite 数据库 → 重置中断的 "downloading" 状态（崩溃恢复）
  │
  ├── [正常模式] 自动发现 Path/Row 或使用 --path/--row 参数
  │     └── wrs2_path_rows.discover_path_rows()
  │         ├── 优先读取本地缓存 config/wrs2_path_rows.json
  │         └── 无缓存时查询 3 个采样月 STAC API，提取所有 (path, row) 组合
  │
  └── DownloadOrchestrator.run()
        │
        └── 按月顺序遍历，每月内所有 Path/Row 并发处理（最多 8 个场景同时）
              │
              └── _do_process_scene(path, row, year_month)
                    │
                    ├── 检查是否已全部下载 → 跳过
                    ├── 查询缓存的最优场景 → 无缓存时执行场景筛选
                    │     └── stac_query.select_best_scene()
                    │         ├── STAC API 查询当月 + 边界范围内所有 L2 场景
                    │         ├── 客户端过滤 path/row/cloud_cover
                    │         ├── 按 (云量, |日期-15日|) 排序，取最优
                    │         └── 查找同日期同 Path/Row 的 L1 场景
                    │
                    ├── 构建文件列表（L2 全波段 + MTL，L1 指定波段 + MTL）
                    ├── 过滤已下载文件（文件级断点续传）
                    │
                    └── 逐文件下载
                          ├── 标记 "downloading" → download_file()
                          │     ├── 已存在且大小匹配 → 跳过
                          │     ├── boto3 分块下载（≥16MB 走 multipart）
                          │     ├── 失败重试 3 次，指数退避
                          │     └── 下载后校验 checksum（SHA-256/MD5）
                          │           └── 校验失败 → 删除重下 1 次
                          └── 标记 "downloaded" 或 "failed" → 同步 CSV
```

## 关键算法

### 1. 场景筛选算法

每个 (Path, Row, 月) 仅选 1 景最优场景：

1. 查询当月（1日~月末）边界内所有 L2 场景（STAC API 不支持 query 参数，在 Python 客户端过滤）
2. 过滤条件：匹配 Path/Row，`cloud_cover_land ≤ MAX_CLOUD_COVER`
3. 排序：主键 `cloud_cover_land` 升序，次键 `|采集日期 - 当月15日|` 升序
4. 取排序后前 2 条，再从中选离 15 日最近的 1 条作为最终选择
5. 根据 L2 场景的采集日期、卫星编号、Path/Row 查找对应的 L1 场景

### 2. 断点续传机制

- **文件级粒度**：SQLite 记录每个文件的下载状态（pending → downloading → downloaded/failed）
- **崩溃恢复**：程序启动时自动将所有 `downloading` 状态重置为 `pending`
- **智能跳过**：已下载文件检查本地文件大小与 S3 远程大小，匹配则跳过
- **场景选择缓存**：已选场景存入 DB，重新运行时跳过 STAC 查询直接复用

### 3. 下载与校验

- **分块下载**：文件 ≥ 16MB 走 boto3 multipart，每块 16MB，单文件 12 并发线程
- **重试策略**：网络错误最多重试 3 次，指数退避（2^n 秒）
- **Checksum 校验**：下载完成后验证 SHA-256 或 MD5，失败则删除重下 1 次
- **Requester-Pays**：USGS S3 存储桶为请求者付费模式，下载时自动设置

### 4. WRS-2 Path/Row 自动发现

- 查询 3 个采样月（1月、4月、7月）的 STAC API
- 从返回结果中提取所有唯一的 (Path, Row) 组合
- 结果缓存到 `config/wrs2_path_rows.json`，避免重复查询

## 数据目录结构

下载完成后，数据按月份归档，每月包含若干场景文件夹：

```
{LANDSAT_DATA_ROOT}/                                  # 数据根目录
├── logs/                                             # 日志与状态文件
│   ├── download.db                                  # SQLite 下载状态数据库
│   ├── download.csv                                 # 下载记录 CSV（可用 Excel 打开）
│   └── download.log                                 # 运行日志
├── 202506/                                            # 2025年6月数据
│   ├── LC08_L2SP_119038_20250615_20250624_02_T1_SR/ # L2 地表反射率产品（全波段）
│   │   ├── LC08_L2SP_119038_20250615_20250624_02_T1_SR_B1.TIF
│   │   ├── ...
│   │   ├── LC08_L2SP_119038_20250615_20250624_02_T1_SR_B7.TIF
│   │   ├── LC08_L2SP_119038_20250615_20250624_02_T1_QA_PIXEL.TIF
│   │   ├── LC08_L2SP_119038_20250615_20250624_02_T1_QA_RADSAT.TIF
│   │   ├── LC08_L2SP_119038_20250615_20250624_02_T1_SR_QA_AEROSOL.TIF
│   │   ├── LC08_L2SP_119038_20250615_20250624_02_T1_ANG.txt
│   │   ├── LC08_L2SP_119038_20250615_20250624_02_T1_MTL.txt
│   │   ├── LC08_L2SP_119038_20250615_20250624_02_T1_MTL.xml
│   │   └── LC08_L2SP_119038_20250615_20250624_02_T1_MTL.json
│   ├── LC08_L1TP_119038_20250615_20250624_02_T1/    # L1 精纠正产品（仅指定波段）
│   │   ├── LC08_L1TP_119038_20250615_20250624_02_T1_B8.TIF   # 全色 15m
│   │   ├── LC08_L1TP_119038_20250615_20250624_02_T1_BQA.TIF  # 质量评估
│   │   ├── LC08_L1TP_119038_20250615_20250624_02_T1_B10.TIF  # 热红外 100m
│   │   ├── LC08_L1TP_119038_20250615_20250624_02_T1_B11.TIF  # 热红外 100m
│   │   ├── LC08_L1TP_119038_20250615_20250624_02_T1_MTL.txt
│   │   ├── LC08_L1TP_119038_20250615_20250624_02_T1_MTL.xml
│   │   └── LC08_L1TP_119038_20250615_20250624_02_T1_MTL.json
│   └── ...
└── ...
```

## Scene ID 命名规则

```
LC08_L2SP_119038_20250615_20250624_02_T1_SR
│     │     │       │         │       │  │  │
│     │     │       │         │       │  │  └── 产品后缀（仅 L2 带 _SR）
│     │     │       │         │       │  └───── 质量等级（T1/T2）
│     │     │       │         │       └──────── 集合版本号
│     │     │       │         └──────────────── 处理日期
│     │     │       └────────────────────────── 采集日期
│     │     └────────────────────────────────── WRS-2 Path(3位)Row(3位)
│     └──────────────────────────────────────── 处理级别
└────────────────────────────────────────────── 卫星编号（LC08/LC09）
```

## 代码文件结构

```
Landsatdownload/
├── main.py                    # 入口脚本，CLI 参数解析与启动
├── config.py                  # 中央配置（路径、STAC、S3、波段、并发等）
├── stac_query.py             # STAC API 查询 + 最优场景筛选算法
├── downloader.py              # S3 分块下载、重试、checksum 校验
├── download_orchestrator.py   # asyncio 异步任务编排（月顺序 + 场景并发）
├── db_logger.py              # SQLite 文件级状态管理 + CSV 同步
├── s3_path_builder.py        # STAC Asset → S3 Key 映射与波段过滤
├── wrs2_path_rows.py          # WRS-2 Path/Row 自动发现与缓存
├── requirements.txt           # Python 依赖
├── .env                       # AWS 密钥（不入库）
├── config/
│   ├── china_boundary.geojson       # 中国大陆边界矢量
│   ├── zhejiang_boundary.geojson    # 浙江省边界矢量（备选）
│   └── wrs2_path_rows.json          # Path/Row 发现缓存
└── README.md
```

## 配置说明

所有配置集中在 `config.py`，关键参数：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `LANDSAT_DATA_ROOT` | `/mnt/ht2-nas2/EO_Pretrain_Data/Landsat_china` | 下载数据根目录 |
| `BOUNDARY_GEOJSON` | `china_boundary.geojson` | 目标区域边界文件 |
| `YEAR_MONTHS` | `["202506"]` | 待下载月份列表（YYYYMM） |
| `MAX_CLOUD_COVER` | `100` | 最大云量百分比（0-100） |
| `L1_BANDS` | `["B8", "BQA", "B10", "B11"]` | L1 下载波段（全色+热红外+质量） |
| `L2_BANDS` | `None` | L2 下载波段（None = 全部） |
| `MAX_CONCURRENT_SCENES` | `8` | 同时下载场景数 |
| `MAX_CONCURRENCY` | `12` | 单文件分块并发线程数 |
| `MAX_DOWNLOAD_RETRIES` | `3` | 网络错误重试次数 |
| `CHECKSUM_RETRY_COUNT` | `1` | 校验失败重下次数 |

## 使用方法

```bash
# 完整下载
python main.py

# 仅预览（不实际下载）
python main.py --dry-run

# 指定时间范围
python main.py --start 202506 --end 202509

# 测试单个场景
python main.py --path 118 --row 039 --month 202506

# 重试所有失败记录
python main.py --retry
```

## CLI 参数

| 参数 | 说明 |
|------|------|
| `--dry-run` | 仅查询和打印，不实际下载 |
| `--start YYYYMM` | 起始月份（含） |
| `--end YYYYMM` | 结束月份（含） |
| `--month YYYYMM` | 指定单月 |
| `--path INT` | 指定 WRS Path |
| `--row INT` | 指定 WRS Row |
| `--retry` | 重试数据库中所有失败记录 |

## 依赖

```
pystac-client>=0.9.0    # STAC API 客户端
boto3>=1.35.0           # AWS S3 下载
aiohttp>=3.9.0          # 异步 HTTP（pystac-client 依赖）
python-dotenv            # .env 文件加载
```

## 数据来源

- **发现 API**：USGS Landsat STAC Server（https://landsatlook.usgs.gov/stac-server）
- **下载源**：AWS S3 `s3://usgs-landsat/collection02/landsat/`（Requester-Pays）
- **L1 集合**：`landsat-c2l1`（Collection 2 Level-1）
- **L2 集合**：`landsat-c2l2-sr`（Collection 2 Level-2 Surface Reflectance）

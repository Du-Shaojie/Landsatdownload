"""
Landsat 8/9 影像下载系统 - 配置中心
"""

import os

# ========== 路径配置 ==========
# 项目根目录（代码所在目录）
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
# 下载数据根目录
LANDSAT_DATA_ROOT = os.path.join(PROJECT_ROOT, "Landsat_china")
# 日志目录
LOGS_DIR = os.path.join(LANDSAT_DATA_ROOT, "logs")
# SQLite 数据库路径
DB_PATH = os.path.join(LOGS_DIR, "download.db")
# CSV 日志路径
CSV_PATH = os.path.join(LOGS_DIR, "download.csv")

# ========== 边界配置 ==========
# 浙江省边界 GeoJSON
ZHEJIANG_GEOJSON = os.path.join(PROJECT_ROOT, "config", "zhejiang_boundary.geojson")
# 中国边界 GeoJSON（大陆 + 台湾，不含南海诸岛）
CHINA_GEOJSON = os.path.join(PROJECT_ROOT, "config", "china_boundary.geojson")
# 当前使用的边界文件（切换下载范围时修改此项）
BOUNDARY_GEOJSON = CHINA_GEOJSON

# ========== STAC API 配置 ==========
STAC_API_URL = "https://landsatlook.usgs.gov/stac-server"
STAC_COLLECTION_L1 = "landsat-c2l1"
STAC_COLLECTION_L2 = "landsat-c2l2-sr"

# ========== AWS S3 配置 ==========
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = "us-east-1"
S3_BUCKET = "usgs-landsat"
S3_PREFIX = "collection02/landsat"

# ========== 时间范围 ==========
# 2025年4月 至 2025年9月
YEAR_MONTHS = [
    "202504", 
]

# ========== 云量与筛选 ==========
MAX_CLOUD_COVER = 100  # 最大允许云量（百分比）

# ========== 并发配置 ==========
MAX_CONCURRENT_SCENES = 8   # 同时下载的场景数
MAX_CONCURRENCY = 10        # 单文件分块下载并发数
MULTIPART_THRESHOLD = 16 * 1024 * 1024   # 16MB，超过此大小走分块
MULTIPART_CHUNKSIZE = 16 * 1024 * 1024   # 每块 16MB

# ========== 下载重试 ==========
MAX_DOWNLOAD_RETRIES = 3        # 网络错误最大重试次数
CHECKSUM_RETRY_COUNT = 1        # 校验失败重试次数
RETRY_BACKOFF_BASE = 2          # 重试退避基数（秒）

# ========== 波段配置 ==========
# L1 仅下载这些波段 + MTL
L1_BANDS = ["B8", "BQA", "B10", "B11"]
# L2 下载所有波段 + MTL（None 表示下载 assets 中所有波段）
L2_BANDS = None  # None = 全部下载

# ========== 日志配置 ==========
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

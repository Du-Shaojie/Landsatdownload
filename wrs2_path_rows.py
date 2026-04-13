"""
WRS-2 Path/Row 发现 - 通过 STAC API 查询获取覆盖目标区域的所有 Path/Row 组合
"""

import json
import logging
import os

import config
from pystac_client import Client

logger = logging.getLogger(__name__)

# 缓存文件路径
_CACHE_FILE = os.path.join(config.PROJECT_ROOT, "config", "wrs2_path_rows.json")


def load_cached_path_rows() -> list[tuple[int, int]] | None:
    """从本地缓存加载 Path/Row 列表"""
    if os.path.exists(_CACHE_FILE):
        with open(_CACHE_FILE, "r") as f:
            data = json.load(f)
            logger.info(f"从缓存加载 WRS-2 Path/Row: {len(data)} 组")
            return [tuple(item) for item in data]
    return None


def save_path_rows_cache(path_rows: list[tuple[int, int]]):
    """缓存 Path/Row 列表到本地"""
    os.makedirs(os.path.dirname(_CACHE_FILE), exist_ok=True)
    with open(_CACHE_FILE, "w") as f:
        json.dump(path_rows, f)
    logger.info(f"已缓存 {len(path_rows)} 组 Path/Row 到 {_CACHE_FILE}")


def discover_path_rows() -> list[tuple[int, int]]:
    """
    通过 STAC API 查询目标区域范围内的所有 WRS-2 Path/Row 组合。

    策略：查询 landsat-c2l2-sr 集合，使用目标区域边界 intersects 查询，
    采样 2025-01 的数据，收集所有唯一的 (path, row) 组合。
    """
    # 先检查缓存
    cached = load_cached_path_rows()
    if cached is not None:
        return cached

    logger.info("正在通过 STAC API 发现覆盖目标区域的 WRS-2 Path/Row...")

    # 加载目标区域边界
    with open(config.BOUNDARY_GEOJSON, "r") as f:
        boundary_geojson = json.load(f)

    # 连接 STAC API
    client = Client.open(config.STAC_API_URL)

    # 采样 2025-01 的数据来发现 Path/Row
    # 使用较大的时间窗口确保覆盖所有重访
    path_rows_set = set()

    # 查询多个月份以确保覆盖所有可能的 Path/Row
    sample_months = ["202501", "202504", "202507"]
    for ym in sample_months:
        start_date, end_date = _year_month_to_range(ym)
        logger.info(f"  采样 {ym} ({start_date} ~ {end_date})...")

        try:
            results = client.search(
                collections=[config.STAC_COLLECTION_L2],
                intersects=boundary_geojson["geometry"],
                datetime=[start_date, end_date],
                max_items=500,
            )
            items = list(results.items())
            logger.info(f"    找到 {len(items)} 个场景")

            for item in items:
                props = item.properties
                path_val = props.get("landsat:wrs_path")
                row_val = props.get("landsat:wrs_row")
                if path_val is not None and row_val is not None:
                    path_rows_set.add((int(path_val), int(row_val)))
        except Exception as e:
            logger.warning(f"    查询 {ym} 失败: {e}")

    # 转换为排序列表
    path_rows = sorted(path_rows_set)

    if not path_rows:
        logger.error("未发现任何 Path/Row！请检查边界文件或网络连接。")
        raise RuntimeError("无法发现任何 WRS-2 Path/Row")

    logger.info(f"发现 {len(path_rows)} 组 Path/Row:")
    for p, r in path_rows:
        logger.info(f"  Path={p:03d}, Row={r:03d}")

    # 缓存结果
    save_path_rows_cache(path_rows)

    return path_rows


def _year_month_to_range(year_month: str) -> tuple[str, str]:
    """将 'YYYYMM' 转换为 ('YYYY-MM-01', 'YYYY-MM-DD') 日期范围"""
    year = int(year_month[:4])
    month = int(year_month[4:6])
    start = f"{year:04d}-{month:02d}-01"
    # 计算月末
    if month == 12:
        end = f"{year + 1:04d}-01-01"
    else:
        end = f"{year:04d}-{month + 1:02d}-01"
    return start, end

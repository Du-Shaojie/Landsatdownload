"""
STAC 查询与场景选择 - 按月/Path/Row 查询 Landsat 8/9 L2 场景，
按云量排序选出最优场景，并查找对应的 L1 场景。

注意：USGS STAC API 不支持 query 参数，因此使用 intersects + datetime 查询，
然后在 Python 中按 path/row/cloud_cover 过滤。
"""

import json
import logging
from datetime import date, timedelta

import config
from pystac_client import Client

logger = logging.getLogger(__name__)

# 加载目标区域边界
_boundary_geometry = None


def _get_boundary_geometry():
    global _boundary_geometry
    if _boundary_geometry is None:
        with open(config.BOUNDARY_GEOJSON, "r") as f:
            _boundary_geometry = json.load(f)["geometry"]
    return _boundary_geometry


def year_month_to_range(year_month: str) -> tuple[str, str]:
    """将 'YYYYMM' 转换为 ('YYYY-MM-01', 'YYYY-MM-DD') 日期范围（当月最后一天）"""
    year = int(year_month[:4])
    month = int(year_month[4:6])
    start = f"{year:04d}-{month:02d}-01"
    # 下月1日减1天 = 当月最后一天
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    end = last_day.isoformat()
    return start, end


def _day_15(year_month: str) -> date:
    """返回当月15号的 date 对象"""
    year = int(year_month[:4])
    month = int(year_month[4:6])
    return date(year, month, 15)


def select_best_scene(
    client: Client,
    path: int,
    row: int,
    year_month: str,
) -> dict | None:
    """
    为指定的 (path, row, year_month) 选择最优场景。

    逻辑：
    1. 查询 L2 集合当月目标区域内所有 Landsat 8/9 场景
    2. 在 Python 中按 path/row 过滤
    3. 按 landsat:cloud_cover_land 升序、|date-15日| 升序排序
    4. 取前2条，再选离15日最近的一条
    5. 查找对应的 L1 场景

    返回：
        {
            'l2_item': pystac.Item,
            'l1_item': pystac.Item | None,
            'scene_id_l2': str,
            'scene_id_l1': str | None,
            'cloud_cover': float,
            'acquisition_date': str,
        }
        或 None
    """
    start_date, end_date = year_month_to_range(year_month)
    day15 = _day_15(year_month)

    logger.info(f"查询 Path={path:03d} Row={row:03d} {year_month} ...")

    # 查询 L2 集合（使用 intersects + datetime，不做 query 过滤）
    try:
        results = client.search(
            collections=[config.STAC_COLLECTION_L2],
            intersects=_get_boundary_geometry(),
            datetime=[start_date, end_date],
            max_items=500,
        )
        all_items = list(results.items())
    except Exception as e:
        logger.error(f"STAC 查询失败: {e}")
        return None

    # 在 Python 中按 path/row/cloud_cover 过滤
    items = []
    for item in all_items:
        props = item.properties
        item_path = int(props.get("landsat:wrs_path", 0))
        item_row = int(props.get("landsat:wrs_row", 0))
        cloud = props.get("landsat:cloud_cover_land",
                          props.get("eo:cloud_cover", 999))

        if item_path != path or item_row != row:
            continue
        if cloud > config.MAX_CLOUD_COVER:
            continue
        items.append(item)

    if not items:
        logger.warning(f"  未找到满足条件的场景")
        return None

    # 按 cloud_cover_land 升序，再按 |date-15| 升序排序
    def sort_key(item):
        props = item.properties
        cloud = props.get("landsat:cloud_cover_land",
                          props.get("eo:cloud_cover", 999))
        item_date = item.datetime.date() if item.datetime else day15
        return (cloud, abs((item_date - day15).days))

    items_sorted = sorted(items, key=sort_key)

    # 取前2条，再选离15日最近的1条
    top2 = items_sorted[:2]
    best = min(top2, key=lambda x: abs(
        (x.datetime.date() if x.datetime else day15) - day15
    ))

    cloud_cover = best.properties.get(
        "landsat:cloud_cover_land",
        best.properties.get("eo:cloud_cover", -1)
    )
    acquisition_date = best.properties.get("datetime", "")[:10]

    logger.info(
        f"  选定场景: {best.id}, 云量={cloud_cover:.1f}%, 日期={acquisition_date}"
    )

    # 查找对应的 L1 场景
    l1_item = _find_corresponding_l1(client, best, path, row)
    if l1_item:
        logger.info(f"  对应 L1 场景: {l1_item.id}")
    else:
        logger.warning(f"  未找到对应的 L1 场景，将仅下载 L2")

    return {
        "l2_item": best,
        "l1_item": l1_item,
        "scene_id_l2": best.id,
        "scene_id_l1": l1_item.id if l1_item else None,
        "cloud_cover": cloud_cover,
        "acquisition_date": acquisition_date,
    }


def _find_corresponding_l1(
    client: Client,
    l2_item,
    path: int,
    row: int,
) -> object | None:
    """
    根据 L2 场景查找对应的 L1 场景。

    匹配条件：相同 path, row, acquisition_date, satellite。
    """
    l2_id = l2_item.id
    parts = l2_id.split("_")
    if len(parts) < 4:
        return None

    satellite = parts[0]  # LC08 or LC09
    acq_date = parts[3]   # YYYYMMDD

    try:
        results = client.search(
            collections=[config.STAC_COLLECTION_L1],
            intersects=_get_boundary_geometry(),
            datetime=[f"{acq_date[:4]}-{acq_date[4:6]}-{acq_date[6:8]}",
                       f"{acq_date[:4]}-{acq_date[4:6]}-{acq_date[6:8]}"],
            max_items=200,
        )
        all_items = list(results.items())

        # 在 Python 中过滤匹配的 L1 场景
        for item in all_items:
            item_path = int(item.properties.get("landsat:wrs_path", 0))
            item_row = int(item.properties.get("landsat:wrs_row", 0))
            if item_path != path or item_row != row:
                continue

            item_parts = item.id.split("_")
            if len(item_parts) >= 4:
                if item_parts[0] == satellite and item_parts[3] == acq_date:
                    return item

        return None
    except Exception as e:
        logger.warning(f"查找 L1 场景失败: {e}")
        return None

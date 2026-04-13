"""
S3 路径构建 - 从 STAC item 的 assets 中提取需要下载的文件信息
"""

import logging
import re

import config

logger = logging.getLogger(__name__)

# 需要跳过的 asset key（不下载）
SKIP_ASSETS = {"thumbnail", "reduced_resolution_browse", "full_resolution_browse",
               "mtl_xml", "ANG", "ST_CDIST"}

# MTL 相关的 asset key 模式
MTL_PATTERNS = ["mtl_txt", "mtl_xml", "MTL"]


def extract_download_files(stac_item, product: str) -> list[dict]:
    """
    从 STAC item 的 assets 中提取需要下载的文件列表。

    参数：
        stac_item: pystac.Item
        product: 'L1' 或 'L2'

    返回：
        list[dict]: 每个元素包含:
            - s3_key: str  (S3 对象键，如 'collection02/landsat/.../B1.TIF')
            - local_filename: str (本地文件名，如 'LC08_L2SP_..._B1.TIF')
            - checksum: str | None (STAC 中的 checksum)
            - asset_key: str (STAC asset key)
    """
    assets = stac_item.assets
    files = []

    if product == "L1":
        # L1 仅下载 B8, BQA + MTL
        target_bands = config.L1_BANDS  # ["B8", "BQA"]
    else:
        # L2 下载所有波段
        target_bands = config.L2_BANDS  # None = 全部

    for asset_key, asset in assets.items():
        # 跳过不需要的 assets
        if asset_key.lower() in SKIP_ASSETS:
            continue

        href = asset.href

        # 跳过非数据文件（缩略图、浏览图等）
        if not _is_data_file(href, asset_key):
            continue

        if product == "L1":
            # L1: 仅下载指定波段 + MTL
            if not _should_download_l1(href, asset_key, target_bands):
                continue
        else:
            # L2: 下载所有波段 + MTL
            if target_bands is not None:
                if not _band_matches(href, asset_key, target_bands):
                    continue

        # 提取 S3 key
        s3_key = _href_to_s3_key(href)
        if not s3_key:
            logger.warning(f"无法从 href 提取 S3 key: {href}")
            continue

        # 提取本地文件名
        local_filename = _extract_filename(s3_key)

        # 提取 checksum
        checksum = _extract_checksum(asset)

        files.append({
            "s3_key": s3_key,
            "local_filename": local_filename,
            "checksum": checksum,
            "asset_key": asset_key,
        })

    logger.info(f"  {product}: 提取 {len(files)} 个文件待下载")
    return files


def _is_data_file(href: str, asset_key: str) -> bool:
    """判断是否为数据文件（TIFF, MTL 等）"""
    href_lower = href.lower()
    # 跳过缩略图和浏览图
    if any(skip in href_lower for skip in ["browse", "thumbnail", "preview"]):
        return False
    # 数据文件扩展名
    if href_lower.endswith((".tif", ".tiff", ".txt", ".xml")):
        return True
    # STAC asset key 中包含数据标识
    data_keys = ["b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9", "b10",
                 "b11", "bqa", "qa_pixel", "qa_radsat", "sr_b", "st_b",
                 "mtl", "ang"]
    if any(dk in asset_key.lower() for dk in data_keys):
        return True
    return False


def _should_download_l1(href: str, asset_key: str, target_bands: list[str]) -> bool:
    """判断 L1 文件是否需要下载"""
    # MTL 文件始终下载
    if _is_mtl_file(href, asset_key):
        return True
    # 检查是否在目标波段中
    return _band_matches(href, asset_key, target_bands)


def _band_matches(href: str, asset_key: str, target_bands: list[str]) -> bool:
    """检查文件是否匹配目标波段"""
    href_upper = href.upper()
    for band in target_bands:
        band_upper = band.upper()
        # 匹配模式：_B1.TIF, _SR_B1.TIF, _ST_B10.TIF, _QA_PIXEL.TIF 等
        # 在文件名中查找 _BAND_UPPER.
        if f"_{band_upper}." in href_upper:
            return True
        if asset_key.upper() == band_upper:
            return True
        # QA 波段特殊处理
        if band_upper in href_upper and band_upper.startswith("QA"):
            return True
    return False


def _is_mtl_file(href: str, asset_key: str) -> bool:
    """判断是否为 MTL 文件"""
    href_lower = href.lower()
    asset_lower = asset_key.lower()
    if "mtl" in href_lower:
        return True
    if any(p in asset_lower for p in MTL_PATTERNS):
        return True
    return False


def _href_to_s3_key(href: str) -> str | None:
    """
    从 STAC asset href 提取 S3 key。

    实际 href 格式：
    - https://landsatlook.usgs.gov/data/collection02/level-2/standard/oli-tirs/2025/120/039/LC08_L2SP_.../LC08_L2SP_..._SR_B1.TIF
    - s3://usgs-landsat/collection02/level-2/standard/oli-tirs/2025/120/039/LC08_L2SP_.../LC08_L2SP_..._SR_B1.TIF

    S3 key = collection02/ 之后的所有路径部分
    """
    # s3:// 协议
    if href.startswith("s3://"):
        parts = href[5:].split("/", 1)
        if len(parts) == 2:
            return parts[1]
        return None

    # https:// 协议 - 提取 /data/collection02/ 之后的部分
    match = re.search(r"/data/(collection02/.+)$", href)
    if match:
        return match.group(1)

    # 也尝试直接匹配 collection02/ 开头（兼容其他 URL 格式）
    match = re.search(r"(collection02/.+)$", href)
    if match:
        return match.group(1)

    return None


def _extract_filename(s3_key: str) -> str:
    """从 S3 key 中提取文件名（最后一段）"""
    return s3_key.split("/")[-1]


def _extract_checksum(asset) -> str | None:
    """
    从 STAC asset 中提取 checksum。

    USGS STAC 使用 file:checksum 扩展字段，
    格式为多字节自定义格式（非标准 multihash），暂不支持校验。
    仅返回标准格式（含 ':' 前缀）的 checksum。
    """
    # 检查常见 checksum 字段名
    for key in ["file:checksum", "checksum", "sha256", "md5"]:
        if hasattr(asset, "extra_fields") and key in asset.extra_fields:
            val = asset.extra_fields[key]
            if isinstance(val, str) and ":" in val:
                # 标准格式如 "sha2-256:abc..." 或 "md5:abc..."
                return val
            # USGS 自定义格式（纯十六进制，无算法前缀），跳过校验
            return None

    if hasattr(asset, "checksum") and asset.checksum:
        return asset.checksum

    return None

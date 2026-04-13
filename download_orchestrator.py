"""
异步下载编排器 - 文件级追踪，逐波段判断是否已下载
"""

import asyncio
import logging
import os
import time

import config
import db_logger
from downloader import download_file
from s3_path_builder import extract_download_files
from stac_query import select_best_scene
from db_logger import should_skip_downloaded

logger = logging.getLogger(__name__)


class DownloadOrchestrator:
    """异步下载编排器"""

    def __init__(self, db: db_logger.DownloadDB, dry_run: bool = False):
        self.db = db
        self.dry_run = dry_run
        self._semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_SCENES)
        self._stats = {
            "total_scenes": 0,
            "scenes_completed": 0,
            "files_total": 0,
            "files_downloaded": 0,
            "files_skipped": 0,
            "files_failed": 0,
            "no_scene": 0,
        }

    async def run(
        self,
        path_rows: list[tuple[int, int]],
        year_months: list[str],
        stac_client,
    ):
        """执行完整的下载流程"""
        total = len(path_rows) * len(year_months)
        self._stats["total_scenes"] = total
        logger.info(f"共 {total} 个场景需要检查")

        if not total:
            logger.info("没有需要处理的场景")
            return

        start_time = time.time()
        coros = [
            self._process_scene(stac_client, path, row, ym)
            for ym in year_months
            for path, row in path_rows
        ]
        await asyncio.gather(*coros, return_exceptions=True)

        elapsed = time.time() - start_time
        s = self._stats
        logger.info("=" * 60)
        logger.info(f"下载完成! 耗时: {elapsed:.0f}s")
        logger.info(
            f"  场景: {s['total_scenes']} | 完成: {s['scenes_completed']} | "
            f"无可用场景: {s['no_scene']}"
        )
        logger.info(
            f"  文件: 总计 {s['files_total']} | "
            f"下载: {s['files_downloaded']} | "
            f"跳过(已下载): {s['files_skipped']} | "
            f"失败: {s['files_failed']}"
        )
        logger.info("=" * 60)

    async def _process_scene(
        self,
        stac_client,
        path: int,
        row: int,
        year_month: str,
    ):
        """处理单个场景（带并发控制）"""
        async with self._semaphore:
            try:
                await self._do_process_scene(stac_client, path, row, year_month)
            except Exception as e:
                logger.error(
                    f"[Path={path:03d} Row={row:03d} {year_month}] 处理异常: {e}",
                    exc_info=True,
                )

    async def _do_process_scene(
        self,
        stac_client,
        path: int,
        row: int,
        year_month: str,
    ):
        """实际处理单个场景的下载逻辑"""
        tag = f"[Path={path:03d} Row={row:03d} {year_month}]"

        # 跳过已下载完成的场景（如之前浙江省下载已覆盖的场景）
        if should_skip_downloaded(self.db, path, row, year_month):
            logger.info(f"{tag} 已下载完成，跳过")
            self._stats["scenes_completed"] += 1
            return

        # Step 1: 检查是否已有场景选择（避免重复 STAC 查询）
        cached = self.db.get_scene_selection(path, row, year_month)

        if cached:
            # 复用已选场景，不需要重新查询 STAC
            scene_id_l2 = cached["scene_id_l2"]
            scene_id_l1 = cached.get("scene_id_l1")
            cloud_cover = cached.get("cloud_cover", -1)
            logger.info(f"{tag} 使用已选场景 (云量={cloud_cover:.1f}%)")

            # 从 STAC 获取 item 对象（用于构建文件列表）
            l2_item = await asyncio.to_thread(
                _fetch_stac_item, stac_client, scene_id_l2, config.STAC_COLLECTION_L2
            )
            l1_item = None
            if scene_id_l1:
                l1_item = await asyncio.to_thread(
                    _fetch_stac_item, stac_client, scene_id_l1, config.STAC_COLLECTION_L1
                )

            if not l2_item:
                logger.warning(f"{tag} 无法获取 L2 场景信息，将重新查询")
                cached = None

        if not cached:
            # Step 1b: STAC 查询选择最优场景
            logger.info(f"{tag} 查询最优场景...")
            scene_info = await asyncio.to_thread(
                select_best_scene, stac_client, path, row, year_month
            )

            if scene_info is None:
                logger.warning(f"{tag} 无可用场景")
                self._stats["no_scene"] += 1
                return

            l2_item = scene_info["l2_item"]
            l1_item = scene_info["l1_item"]
            scene_id_l2 = scene_info["scene_id_l2"]
            scene_id_l1 = scene_info["scene_id_l1"]
            cloud_cover = scene_info["cloud_cover"]

            # 保存场景选择
            self.db.save_scene_selection(
                path, row, year_month,
                scene_id_l2, scene_id_l1, cloud_cover
            )

        # Step 2: 构建下载文件列表
        l2_files = await asyncio.to_thread(
            extract_download_files, l2_item, "L2"
        )
        l1_files = []
        if l1_item:
            l1_files = await asyncio.to_thread(
                extract_download_files, l1_item, "L1"
            )

        all_files = [
            (f, scene_id_l2, "L2") for f in l2_files
        ] + [
            (f, scene_id_l1, "L1") for f in l1_files
        ]

        if not all_files:
            logger.warning(f"{tag} 无文件需要下载")
            return

        # Step 3: 逐文件检查已下载状态，过滤出需要下载的文件
        month_dir = os.path.join(config.LANDSAT_DATA_ROOT, year_month)
        to_download = []

        for file_info, scene_id, product in all_files:
            filename = file_info["local_filename"]
            if self.db.is_file_downloaded(path, row, year_month, scene_id, product, filename):
                self._stats["files_skipped"] += 1
            else:
                scene_dir = os.path.join(month_dir, scene_id)
                to_download.append((file_info, scene_id, product, scene_dir))

        skipped_count = len(all_files) - len(to_download)
        if skipped_count > 0:
            logger.info(f"{tag} 跳过 {skipped_count} 个已下载文件")

        if not to_download:
            logger.info(f"{tag} 所有文件已下载，跳过")
            self._stats["scenes_completed"] += 1
            return

        self._stats["files_total"] += len(to_download)

        # Step 4: 下载
        if self.dry_run:
            logger.info(f"{tag} [DRY RUN] 需下载 {len(to_download)} 个文件")
            logger.info(f"  L2: {scene_id_l2}")
            if scene_id_l1:
                logger.info(f"  L1: {scene_id_l1}")
            for f, _, _, _ in to_download:
                logger.info(f"    - {f['local_filename']}")
            self._stats["files_downloaded"] += len(to_download)
            self._stats["scenes_completed"] += 1
            return

        # 逐文件下载并标记状态
        scene_success = True
        for file_info, scene_id, product, scene_dir in to_download:
            filename = file_info["local_filename"]
            s3_key = file_info["s3_key"]
            local_path = os.path.join(scene_dir, filename)

            # 标记为 downloading
            self.db.mark_file_downloading(
                path, row, year_month, scene_id, product, filename, scene_dir
            )

            # 下载
            result = await asyncio.to_thread(
                download_file, s3_key, local_path, file_info.get("checksum")
            )

            if result["success"]:
                self.db.mark_file_downloaded(
                    path, row, year_month, scene_id, product, filename, scene_dir
                )
                self._stats["files_downloaded"] += 1
            else:
                self.db.mark_file_failed(
                    path, row, year_month, scene_id, product, filename
                )
                self._stats["files_failed"] += 1
                scene_success = False
                logger.error(
                    f"{tag} 文件下载失败: {filename} - {result.get('error', 'unknown')}"
                )

        if scene_success:
            self._stats["scenes_completed"] += 1
            logger.info(f"{tag} 完成 (下载 {len(to_download)}, 跳过 {skipped_count})")
        else:
            logger.error(f"{tag} 部分文件下载失败")


def _fetch_stac_item(client, scene_id: str, collection: str):
    """通过 scene_id 从 STAC API 获取 item"""
    try:
        results = client.search(
            collections=[collection],
            ids=[scene_id],
        )
        items = list(results.items())
        return items[0] if items else None
    except Exception:
        return None

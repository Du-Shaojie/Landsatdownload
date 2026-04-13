"""
Landsat 8/9 影像下载系统 - 入口脚本

用法：
    python main.py                    # 正常下载
    python main.py --dry-run          # 仅查询，不下载
    python main.py --start 202506     # 从202506开始下载
    python main.py --end 202503       # 下载到202503为止
    python main.py --retry            # 重试失败的记录
"""

import argparse
import asyncio
import logging
import os
import sys

import config
import db_logger
import download_orchestrator
import wrs2_path_rows
from pystac_client import Client


def setup_logging():
    """配置日志"""
    os.makedirs(config.LOGS_DIR, exist_ok=True)

    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(config.LOG_FORMAT))

    # 文件日志
    file_handler = logging.FileHandler(
        os.path.join(config.LOGS_DIR, "download.log"),
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(config.LOG_FORMAT))

    # 根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.LOG_LEVEL, logging.INFO))
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    return logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Landsat 8/9 影像下载系统"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅查询和打印，不实际下载",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="起始月份 (格式: YYYYMM，如 202506)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="结束月份 (格式: YYYYMM，如 202503)",
    )
    parser.add_argument(
        "--retry",
        action="store_true",
        help="仅重试失败的记录",
    )
    parser.add_argument(
        "--path",
        type=int,
        default=None,
        help="指定 WRS Path（用于测试单个 Path）",
    )
    parser.add_argument(
        "--row",
        type=int,
        default=None,
        help="指定 WRS Row（用于测试单个 Row）",
    )
    parser.add_argument(
        "--month",
        type=str,
        default=None,
        help="指定月份 (格式: YYYYMM，用于测试单月)",
    )
    return parser.parse_args()


def get_year_months(args) -> list[str]:
    """根据参数获取需要处理的月份列表"""
    months = config.YEAR_MONTHS[:]

    if args.start:
        months = [m for m in months if m >= args.start]
    if args.end:
        months = [m for m in months if m <= args.end]
    if args.month:
        months = [args.month]

    return months


async def main():
    args = parse_args()
    logger = setup_logging()

    logger.info("=" * 60)
    logger.info("Landsat 8/9 影像下载系统")
    logger.info("=" * 60)

    # 检查 AWS 密钥
    if not config.AWS_ACCESS_KEY_ID or not config.AWS_SECRET_ACCESS_KEY:
        logger.error("未设置 AWS 密钥，请先设置环境变量：")
        logger.error("  Linux/Mac:  export AWS_ACCESS_KEY_ID=xxx && export AWS_SECRET_ACCESS_KEY=xxx")
        logger.error("  Windows:    set AWS_ACCESS_KEY_ID=xxx && set AWS_SECRET_ACCESS_KEY=xxx")
        return

    # 初始化目录
    os.makedirs(config.LANDSAT_DATA_ROOT, exist_ok=True)
    os.makedirs(config.LOGS_DIR, exist_ok=True)

    # 初始化数据库
    db = db_logger.DownloadDB()

    # 重置中断的 "downloading" 状态为 "pending"（支持断点续传）
    db.reset_downloading_to_pending()

    # 获取月份列表
    year_months = get_year_months(args)
    logger.info(f"时间范围: {year_months[0]} ~ {year_months[-1]} ({len(year_months)} 个月)")

    # 重试模式
    if args.retry:
        failed = db.get_failed_records()
        if not failed:
            logger.info("没有失败的记录需要重试")
            return
        logger.info(f"重试 {len(failed)} 条失败记录")
        db.reset_failed_to_pending()
        # 提取涉及的 path_rows 和 months
        path_rows = list(set((r["path"], r["row"]) for r in failed))
        retry_months = list(set(r["year_month"] for r in failed))
        # 过滤 year_months
        year_months = [m for m in year_months if m in retry_months]
    else:
        # 发现 Path/Row
        if args.path and args.row:
            path_rows = [(args.path, args.row)]
            logger.info(f"指定 Path/Row: {path_rows}")
        else:
            path_rows = wrs2_path_rows.discover_path_rows()

    logger.info(f"Path/Row 数量: {len(path_rows)}")

    # 连接 STAC API
    logger.info(f"连接 STAC API: {config.STAC_API_URL}")
    try:
        stac_client = Client.open(config.STAC_API_URL)
    except Exception as e:
        logger.error(f"无法连接 STAC API: {e}")
        return

    # 创建编排器并运行
    orchestrator = download_orchestrator.DownloadOrchestrator(
        db=db,
        dry_run=args.dry_run,
    )

    await orchestrator.run(path_rows, year_months, stac_client)


if __name__ == "__main__":
    asyncio.run(main())

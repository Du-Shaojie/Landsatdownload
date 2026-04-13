"""
文件下载器 - 使用 boto3 + TransferConfig 实现多线程分块下载与校验
"""

import hashlib
import logging
import os
import re
import time

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

import config

logger = logging.getLogger(__name__)

# 全局 S3 客户端（线程安全，boto3 会自动管理连接池）
_s3_client = None
_s3_resource = None


def get_s3_client():
    """获取或创建 S3 客户端（单例模式）"""
    global _s3_client
    if _s3_client is None:
        boto_config = BotoConfig(
            region_name=config.AWS_REGION,
            retries={
                "max_attempts": 5,
                "mode": "adaptive",
            },
        )
        _s3_client = boto3.client(
            "s3",
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            config=boto_config,
        )
    return _s3_client


def download_file(
    s3_key: str,
    local_path: str,
    checksum: str | None = None,
) -> dict:
    """
    下载单个 S3 文件到本地。

    参数：
        s3_key: S3 对象键 (如 'collection02/landsat/.../B1.TIF')
        local_path: 本地保存路径
        checksum: STAC 中的 checksum (可选，格式如 'sha2-256:abc...')

    返回：
        {
            'success': bool,
            's3_key': str,
            'local_path': str,
            'size': int,
            'checksum_ok': bool | None,
            'error': str | None,
        }
    """
    result = {
        "success": False,
        "s3_key": s3_key,
        "local_path": local_path,
        "size": 0,
        "checksum_ok": None,
        "error": None,
    }

    # 确保本地目录存在
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # 检查文件是否已存在且大小合理
    if os.path.exists(local_path):
        local_size = os.path.getsize(local_path)
        try:
            remote_size = _get_remote_size(s3_key)
            if remote_size and local_size == remote_size:
                logger.debug(f"文件已存在且大小匹配，跳过: {os.path.basename(local_path)}")
                result["success"] = True
                result["size"] = local_size
                # 仍然需要校验
                if checksum:
                    result["checksum_ok"] = _verify_checksum(local_path, checksum)
                else:
                    result["checksum_ok"] = True
                return result
            elif remote_size and local_size > 0:
                # 大小不匹配但文件存在，删除后重新下载
                logger.info(f"文件大小不匹配 (本地={local_size}, 远程={remote_size})，重新下载")
                os.remove(local_path)
        except Exception as e:
            logger.warning(f"检查远程文件大小失败: {e}")

    # 多次重试下载
    last_error = None
    for attempt in range(1, config.MAX_DOWNLOAD_RETRIES + 1):
        try:
            logger.info(
                f"  下载 [{attempt}/{config.MAX_DOWNLOAD_RETRIES}]: "
                f"{os.path.basename(local_path)}"
            )

            # 使用 TransferConfig 实现多线程分块下载
            from boto3.s3.transfer import TransferConfig

            transfer_config = TransferConfig(
                multipart_threshold=config.MULTIPART_THRESHOLD,
                max_concurrency=config.MAX_CONCURRENCY,
                multipart_chunksize=config.MULTIPART_CHUNKSIZE,
            )

            s3 = get_s3_client()
            s3.download_file(
                Bucket=config.S3_BUCKET,
                Key=s3_key,
                Filename=local_path,
                Config=transfer_config,
                ExtraArgs={"RequestPayer": "requester"},
            )

            # 验证文件大小
            if os.path.exists(local_path):
                result["size"] = os.path.getsize(local_path)
                if result["size"] == 0:
                    raise RuntimeError("下载的文件大小为 0")

            result["success"] = True
            break

        except Exception as e:
            last_error = str(e)
            logger.warning(f"  下载失败 (尝试 {attempt}): {e}")
            # 删除可能不完整的文件
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    pass
            if attempt < config.MAX_DOWNLOAD_RETRIES:
                wait_time = config.RETRY_BACKOFF_BASE ** attempt
                logger.info(f"  等待 {wait_time}s 后重试...")
                time.sleep(wait_time)

    if not result["success"]:
        result["error"] = last_error
        logger.error(f"  下载最终失败: {s3_key} - {last_error}")
        return result

    # 校验 checksum
    if checksum:
        result["checksum_ok"] = _verify_checksum(local_path, checksum)
        if not result["checksum_ok"]:
            logger.warning(f"  校验失败: {os.path.basename(local_path)}")
            # 重试一次
            if config.CHECKSUM_RETRY_COUNT > 0:
                logger.info(f"  校验失败，重新下载...")
                os.remove(local_path)
                retry_result = download_file(s3_key, local_path, checksum=None)
                if retry_result["success"]:
                    result["size"] = retry_result["size"]
                    result["checksum_ok"] = _verify_checksum(local_path, checksum)
                    if not result["checksum_ok"]:
                        result["error"] = "checksum_mismatch_after_retry"
    else:
        result["checksum_ok"] = True

    return result


def _get_remote_size(s3_key: str) -> int | None:
    """获取 S3 对象的大小"""
    try:
        s3 = get_s3_client()
        resp = s3.head_object(
            Bucket=config.S3_BUCKET,
            Key=s3_key,
            RequestPayer="requester",
        )
        return resp.get("ContentLength")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return None
        raise


def _verify_checksum(local_path: str, checksum: str) -> bool:
    """
    校验本地文件的 checksum。

    checksum 格式：'sha2-256:abc123...' 或 'md5:abc123...'
    """
    try:
        # 解析 checksum 格式
        if ":" in checksum:
            algo_str, expected_hash = checksum.split(":", 1)
            algo_str = algo_str.lower().replace("sha2-", "sha").replace("sha-", "sha")
        else:
            # 默认假设为 MD5
            algo_str = "md5"
            expected_hash = checksum

        # 计算本地文件哈希
        if algo_str in ("sha256", "sha"):
            actual_hash = _compute_file_hash(local_path, hashlib.sha256())
        elif algo_str == "sha512":
            actual_hash = _compute_file_hash(local_path, hashlib.sha512())
        elif algo_str == "md5":
            actual_hash = _compute_file_hash(local_path, hashlib.md5())
        else:
            logger.warning(f"不支持的校验算法: {algo_str}，跳过校验")
            return True

        # 比较（忽略大小写）
        return actual_hash.lower() == expected_hash.lower()

    except Exception as e:
        logger.warning(f"校验出错: {e}")
        return False


def _compute_file_hash(filepath: str, hasher) -> str:
    """计算文件的哈希值（支持大文件，逐块读取）"""
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

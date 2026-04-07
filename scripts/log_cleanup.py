#!/usr/bin/env python3
"""
日志轮转清理脚本

功能：
- 轮转超过阈值的日志文件
- 压缩旧日志
- 清理过期日志
"""
import os
import sys
import gzip
import shutil
from pathlib import Path
from datetime import datetime, timedelta

# 配置
LOG_DIR = Path.home() / "logs"
MAX_SIZE_MB = 10  # 超过此大小触发轮转（从5MB提高到10MB）
KEEP_DAYS = 7    # 保留最近 N 天的日志
KEEP_ROTATED = 5 # 保留最近 N 个轮转文件（从3提高到5）


def get_log_files() -> list[Path]:
    """获取所有日志文件"""
    return sorted(LOG_DIR.glob("*.log"), key=lambda p: p.stat().st_size, reverse=True)


def rotate_log(log_path: Path) -> bool:
    """轮转单个日志文件"""
    size_mb = log_path.stat().st_size / (1024 * 1024)

    if size_mb < MAX_SIZE_MB:
        return False

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    rotated_name = f"{log_path.stem}.{timestamp}.log"
    rotated_path = LOG_DIR / rotated_name

    # 移动文件
    shutil.move(str(log_path), str(rotated_path))

    # 压缩
    with open(rotated_path, 'rb') as f_in:
        with gzip.open(rotated_path.with_suffix('.log.gz'), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # 删除未压缩的轮转文件
    rotated_path.unlink()

    # 创建新的空日志文件
    log_path.touch()

    print(f"[Rotate] {log_path.name}: {size_mb:.1f}MB -> {rotated_name}.gz")
    return True


def cleanup_old_logs():
    """清理过期日志"""
    cutoff = datetime.now() - timedelta(days=KEEP_DAYS)

    for gz_file in LOG_DIR.glob("*.log.gz"):
        # 从文件名提取日期
        parts = gz_file.stem.split('.')
        if len(parts) >= 2:
            try:
                date_str = parts[-2][:8]  # YYYYMMDD
                file_date = datetime.strptime(date_str, "%Y%m%d")
                if file_date < cutoff:
                    gz_file.unlink()
                    print(f"[Delete] {gz_file.name}: 过期删除")
            except ValueError:
                pass

    # 清理空日志文件
    for log_file in LOG_DIR.glob("*.log"):
        if log_file.stat().st_size == 1:  # 只有一个字节（空文件）
            content = log_file.read_text()
            if content.strip() == "":
                log_file.unlink()
                print(f"[Delete] {log_file.name}: 空文件删除")


def limit_rotated_files():
    """限制轮转文件数量"""
    log_bases = {}

    for gz_file in LOG_DIR.glob("*.log.gz"):
        # 提取基础名（去掉日期时间戳）
        parts = gz_file.stem.split('.')
        if len(parts) >= 2:
            base = parts[0]
            if base not in log_bases:
                log_bases[base] = []
            log_bases[base].append(gz_file)

    # 对每个基础名，只保留最近的 N 个
    for base, files in log_bases.items():
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for old_file in files[KEEP_ROTATED:]:
            old_file.unlink()
            print(f"[Delete] {old_file.name}: 超出保留数量")


def main():
    """主函数"""
    print(f"[Log Cleanup] 开始清理: {LOG_DIR}")

    # 1. 轮转大日志
    rotated_count = 0
    for log_file in get_log_files():
        if rotate_log(log_file):
            rotated_count += 1

    # 2. 清理过期日志
    cleanup_old_logs()

    # 3. 限制轮转数量
    limit_rotated_files()

    # 4. 统计
    total_size = sum(f.stat().st_size for f in LOG_DIR.glob("*.log*"))
    print(f"[Log Cleanup] 完成: 轮转 {rotated_count} 个, 总大小 {total_size / (1024*1024):.1f}MB")


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
视频笔记整理脚本
- 扫描 Inbox 中的 VIDEO_ 前缀文件
- 移动到 72_视频笔记_Video-Distill/ 目录
- 更新数据库中的 output_path

定时任务: 每日 04:00 执行
"""

import os
import re
import shutil
import sqlite3
from pathlib import Path
from datetime import datetime

# 路径配置
VAULT_PATH = Path.home() / "Documents" / "ZhiweiVault"
INBOX_PATH = VAULT_PATH / "Inbox"
VIDEO_DISTILL_PATH = VAULT_PATH / "70-79_个人笔记_Personal" / "72_视频笔记_Video-Distill"
VIDEO_DB_PATH = Path.home() / "zhiwei-bot" / "video_history.db"


def ensure_target_dir():
    """确保目标目录存在"""
    VIDEO_DISTILL_PATH.mkdir(parents=True, exist_ok=True)
    return VIDEO_DISTILL_PATH


def scan_inbox_videos():
    """扫描 Inbox 中的 VIDEO_ 前缀文件"""
    video_files = []
    for f in INBOX_PATH.glob("VIDEO_*.md"):
        video_files.append(f)
    return video_files


def parse_video_info(filename: str) -> dict:
    """
    解析视频笔记文件名

    格式: VIDEO_YYYY-MM-DD_标题.md
    或: YYYY-MM-DD_标题.md (新格式)
    """
    stem = Path(filename).stem

    # VIDEO_ 前缀格式
    if stem.startswith("VIDEO_"):
        parts = stem[6:].split("_", 1)
        if len(parts) == 2:
            date_str, title = parts
            return {
                "prefix": "VIDEO_",
                "date": date_str,
                "title": title,
                "original_name": filename
            }

    # 日期前缀格式（可能是视频笔记）
    date_pattern = r"^(\d{4}-\d{2}-\d{2})_(.+)$"
    match = re.match(date_pattern, stem)
    if match:
        return {
            "prefix": "",
            "date": match.group(1),
            "title": match.group(2),
            "original_name": filename
        }

    return None


def is_video_note(filepath: Path) -> bool:
    """
    判断文件是否是视频笔记

    通过检查文件内容中的视频来源标识（逐行读取，命中即退出）
    """
    video_markers = [
        "来源平台",
        "douyin",
        "bilibili",
        "b23.tv",
        "抖音",
        "B站",
        "v.douyin.com",
    ]
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                lower_line = line.lower()
                if "arxiv.org" in lower_line:
                    return False
                for marker in video_markers:
                    if marker in line:
                        return True
        return False
    except Exception:
        return False


def move_video_note(filepath: Path, target_dir: Path) -> Path:
    """
    移动视频笔记到目标目录

    统一命名格式: VIDEO_YYYY-MM-DD_标题.md
    """
    info = parse_video_info(filepath.name)

    if info:
        # 使用统一格式命名
        new_name = f"VIDEO_{info['date']}_{info['title']}.md"
    else:
        # 保持原名
        new_name = filepath.name
        if not new_name.startswith("VIDEO_"):
            new_name = f"VIDEO_{new_name}"

    target_path = target_dir / new_name

    # 处理重名
    if target_path.exists() and target_path != filepath:
        # 添加时间戳后缀
        timestamp = datetime.now().strftime("%H%M%S")
        if info:
            new_name = f"VIDEO_{info['date']}_{info['title']}_{timestamp}.md"
        else:
            base = filepath.stem
            new_name = f"VIDEO_{base}_{timestamp}.md"
        target_path = target_dir / new_name

    # 移动文件
    if filepath != target_path:
        shutil.move(str(filepath), str(target_path))
        return target_path

    return filepath


def update_db_output_path(old_path: str, new_path: str):
    """更新数据库中的 output_path"""
    try:
        conn = sqlite3.connect(VIDEO_DB_PATH)
        cursor = conn.cursor()

        # 更新路径
        cursor.execute(
            "UPDATE video_history SET output_path = ? WHERE output_path = ?",
            (str(new_path), str(old_path))
        )

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"  ⚠️ 更新数据库失败: {e}")
        return False


def organize_video_notes(dry_run: bool = False):
    """
    整理视频笔记主函数

    Args:
        dry_run: 如果为 True，只打印将要执行的操作，不实际执行
    """
    print("=" * 60)
    print("🎬 视频笔记整理")
    print("=" * 60)
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Inbox: {INBOX_PATH}")
    print(f"目标: {VIDEO_DISTILL_PATH}")
    print()

    # 确保目标目录存在
    target_dir = ensure_target_dir()

    # 扫描 Inbox 中的视频文件
    video_files = scan_inbox_videos()

    # 同时检查日期前缀的可能视频笔记
    potential_videos = []
    for f in INBOX_PATH.glob("[0-9][0-9][0-9][0-9]-*.md"):
        if is_video_note(f):
            potential_videos.append(f)

    all_videos = video_files + potential_videos

    if not all_videos:
        print("✅ Inbox 中没有待整理的视频笔记")
        return {"moved": 0, "skipped": 0, "errors": 0}

    print(f"📋 发现 {len(all_videos)} 个视频笔记待整理:")
    for f in all_videos:
        print(f"   - {f.name}")
    print()

    if dry_run:
        print("🔍 [DRY RUN] 将执行以下操作:")
        for f in all_videos:
            info = parse_video_info(f.name)
            if info:
                new_name = f"VIDEO_{info['date']}_{info['title']}.md"
            else:
                new_name = f.name
            print(f"   {f.name} → {new_name}")
        return {"moved": len(all_videos), "skipped": 0, "errors": 0}

    # 执行移动
    stats = {"moved": 0, "skipped": 0, "errors": 0}

    for filepath in all_videos:
        try:
            old_path = str(filepath)
            new_path = move_video_note(filepath, target_dir)

            if new_path != filepath:
                print(f"  ✅ {filepath.name} → {new_path.name}")

                # 更新数据库
                update_db_output_path(old_path, str(new_path))
                stats["moved"] += 1
            else:
                stats["skipped"] += 1

        except Exception as e:
            print(f"  ❌ 移动失败 {filepath.name}: {e}")
            stats["errors"] += 1

    print()
    print("=" * 60)
    print(f"✅ 整理完成!")
    print(f"   - 移动: {stats['moved']}")
    print(f"   - 跳过: {stats['skipped']}")
    print(f"   - 错误: {stats['errors']}")

    return stats


def main():
    import argparse

    parser = argparse.ArgumentParser(description="视频笔记整理脚本")
    parser.add_argument("--dry-run", action="store_true", help="只打印将要执行的操作")
    args = parser.parse_args()

    organize_video_notes(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
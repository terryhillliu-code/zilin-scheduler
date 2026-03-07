#!/usr/bin/env python3
"""
视频下载模块 (Phase 4b - T-401)
封装 yt-dlp 以支持 B站、抖音、YouTube 等平台的视频/音频抓取。
"""

import os
import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

# 配置下载路径
VIDEO_INBOX = Path.home() / "knowledge-inbox" / "videos"
VIDEO_INBOX.mkdir(parents=True, exist_ok=True)

def download_video(url: str, output_name: str = None) -> Path:
    """
    下载视频并返回本地路径。
    默认下载最高画质视频。
    """
    logger.info(f"正在尝试从 URL 下载视频: {url}")
    
    # 构造输出模板
    if output_name:
        out_tmpl = str(VIDEO_INBOX / f"{output_name}.%(ext)s")
    else:
        out_tmpl = str(VIDEO_INBOX / "%(title)s.%(ext)s")
        
    # --no-mtime 防止文件日期被改为上传日期，方便后续扫描
    # 增加 User-Agent 伪装以防止 412 错误
    cmd = [
        "yt-dlp",
        "--user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "--referer", "https://www.bilibili.com/",
        "-f", "bestvideo+bestaudio/best",
        "--no-mtime",
        "-o", out_tmpl,
        url
    ]
    
    # 针对某些平台可能需要 cookie (在此留出接口)
    # cmd.extend(["--cookies", "/path/to/cookies.txt"])

    try:
        # 使用 --get-filename 预览最终文件名
        name_check_cmd = ["yt-dlp", "--get-filename", "-o", out_tmpl, url]
        filename = subprocess.check_output(name_check_cmd, text=True).strip()
        final_path = Path(filename)
        
        if final_path.exists():
            logger.info(f"视频已存在，跳过下载: {final_path}")
            return final_path

        subprocess.run(cmd, check=True)
        logger.info(f"视频下载成功: {final_path}")
        return final_path
        
    except Exception as e:
        logger.error(f"视频下载失败: {e}")
        raise

def extract_audio_only(url: str) -> Path:
    """
    仅下载音频（对于转录任务最节省流量和时间）。
    """
    out_tmpl = str(VIDEO_INBOX / "%(title)s.%(ext)s")
    
    cmd = [
        "yt-dlp",
        "--user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "--referer", "https://www.bilibili.com/",
        "-x", # 提取音频
        "--audio-format", "mp3",
        "--audio-quality", "0", # 最高质量
        "--no-mtime",
        "-o", out_tmpl,
        url
    ]
    
    try:
        # 获取预估路径 (注意 -x 之后通常是 .mp3)
        name_check_cmd = [
            "yt-dlp", 
            "--user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "--referer", "https://www.bilibili.com/",
            "--get-filename", "-x", "--audio-format", "mp3", "-o", out_tmpl, url
        ]
        filename = subprocess.check_output(name_check_cmd, text=True).strip()
        final_path = Path(filename)
        
        if final_path.exists():
            logger.info(f"音频已存在: {final_path}")
            return final_path

        subprocess.run(cmd, check=True)
        logger.info(f"音频提取成功: {final_path}")
        return final_path
    except Exception as e:
        logger.error(f"音频抓取失败: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import sys
    if len(sys.argv) > 1:
        test_url = sys.argv[1]
        # 默认执行音频提取（为了 Phase 4b 转录测试）
        extract_audio_only(test_url)

#!/usr/bin/env python3
"""
播客 RSS 监控器
- 监控订阅的播客更新
- 复用视频处理流程（下载、转录、总结、存档）
- 推送通知

使用方式:
    python podcast_monitor.py              # 检查更新并处理
    python podcast_monitor.py --list       # 列出所有订阅
"""

import json
import hashlib
import urllib.request
import ssl
import xml.etree.ElementTree as ET
import logging
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# 默认订阅列表
DEFAULT_PODCASTS = [
    {
        "name": "Latent Space",
        "rss_url": "https://latent.space/feed",
        "category": "AI/工程",
        "priority": "high"
    },
    {
        "name": "No Priors",
        "rss_url": "https://a16z.com/podcasts/no-priors/feed/",
        "category": "AI/投资",
        "priority": "high"
    },
    {
        "name": "Practical AI",
        "rss_url": "https://feeds.transistor.fm/practicalai",
        "category": "AI/工程",
        "priority": "medium"
    },
    {
        "name": "TWIML AI",
        "rss_url": "https://feeds.buzzsprout.com/1661485.rss",
        "category": "AI/研究",
        "priority": "medium"
    },
]


@dataclass
class Episode:
    """播客剧集"""
    podcast_name: str
    title: str
    description: str
    pub_date: str
    audio_url: str
    duration: str = ""
    guid: str = ""


class PodcastMonitor:
    """播客监控器 - 复用视频处理管线"""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.history_file = self.data_dir / "podcast_history.json"
        self.ssl_context = ssl.create_default_context()

    def _load_history(self) -> dict:
        """加载处理历史"""
        if self.history_file.exists():
            with open(self.history_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_history(self, history: dict):
        """保存处理历史"""
        with open(self.history_file, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

    def _get_episode_id(self, episode: Episode) -> str:
        """生成剧集唯一 ID"""
        content = f"{episode.podcast_name}:{episode.title}:{episode.pub_date}"
        return hashlib.md5(content.encode()).hexdigest()[:12]

    def fetch_feed(self, rss_url: str) -> Optional[ET.Element]:
        """获取 RSS Feed"""
        try:
            req = urllib.request.Request(
                rss_url,
                headers={"User-Agent": "Zhiwei-Podcast-Monitor/1.0"}
            )
            with urllib.request.urlopen(req, timeout=30, context=self.ssl_context) as resp:
                content = resp.read().decode("utf-8")
                return ET.fromstring(content)
        except Exception as e:
            logger.error(f"获取 RSS 失败 {rss_url}: {e}")
            return None

    def parse_episodes(self, feed: ET.Element, podcast_name: str, limit: int = 3) -> List[Episode]:
        """解析 RSS Feed 获取剧集列表"""
        episodes = []
        items = feed.findall(".//item")

        for item in items[:limit]:
            title_elem = item.find("title")
            desc_elem = item.find("description")
            pub_date_elem = item.find("pubDate")
            audio_elem = item.find("enclosure")
            duration_elem = item.find("{http://www.itunes.com/dtds/podcast-1.0.dtd}duration")
            guid_elem = item.find("guid")

            title = title_elem.text if title_elem is not None else "未知标题"
            description = desc_elem.text[:200] if desc_elem is not None and desc_elem.text else ""
            pub_date = pub_date_elem.text if pub_date_elem is not None else ""
            audio_url = audio_elem.get("url", "") if audio_elem is not None else ""
            duration = duration_elem.text if duration_elem is not None else ""
            guid = guid_elem.text if guid_elem is not None else ""

            episodes.append(Episode(
                podcast_name=podcast_name,
                title=title,
                description=description,
                pub_date=pub_date,
                audio_url=audio_url,
                duration=duration,
                guid=guid
            ))

        return episodes

    def check_new_episodes(self, podcasts: List[dict] = None) -> List[Episode]:
        """检查新剧集"""
        podcasts = podcasts or DEFAULT_PODCASTS
        history = self._load_history()
        new_episodes = []

        for podcast in podcasts:
            logger.info(f"检查: {podcast['name']}...")
            feed = self.fetch_feed(podcast["rss_url"])
            if not feed:
                continue

            episodes = self.parse_episodes(feed, podcast["name"])
            for episode in episodes:
                episode_id = self._get_episode_id(episode)
                if episode_id not in history:
                    new_episodes.append(episode)

        return new_episodes

    def process_episode(self, episode: Episode) -> bool:
        """
        处理单个剧集 - 复用视频处理管线

        直接调用 video_pipeline.process_single_video()
        """
        try:
            from video_pipeline import process_single_video
            logger.info(f"处理剧集: {episode.title[:50]}...")
            process_single_video(episode.audio_url)
            return True
        except Exception as e:
            logger.error(f"处理失败: {e}")
            return False

    def format_push_message(self, processed: List[Episode]) -> str:
        """格式化推送消息"""
        if not processed:
            return ""

        lines = ["# 🎙️ 播客更新\n"]
        lines.append(f"已处理 {len(processed)} 个新剧集:\n")

        for ep in processed:
            lines.append(f"**{ep.podcast_name}**")
            lines.append(f"📝 {ep.title}")
            if ep.duration:
                lines.append(f"⏱️ 时长: {ep.duration}")
            lines.append("")

        lines.append("📁 摘要已存入 Obsidian (Video_Summaries)")

        return "\n".join(lines)

    def push_to_feishu(self, message: str) -> bool:
        """推送到飞书"""
        if not message:
            return False

        try:
            from pusher import FeishuPusher
            pusher = FeishuPusher()
            return pusher.send_markdown(message)
        except Exception as e:
            logger.error(f"推送失败: {e}")
            return False

    def run(self, max_episodes: int = 3) -> Tuple[int, List[Episode]]:
        """
        执行监控和处理

        Args:
            max_episodes: 最多处理的剧集数量

        Returns:
            (处理数量, 处理结果列表)
        """
        new_episodes = self.check_new_episodes()
        history = self._load_history()
        processed = []

        for episode in new_episodes[:max_episodes]:
            logger.info(f"\n{'='*50}")

            if self.process_episode(episode):
                processed.append(episode)
                episode_id = self._get_episode_id(episode)
                history[episode_id] = {
                    "title": episode.title,
                    "pub_date": episode.pub_date,
                    "podcast": episode.podcast_name,
                    "processed_at": datetime.now().isoformat()
                }

        self._save_history(history)

        if processed:
            message = self.format_push_message(processed)
            print(message)
            self.push_to_feishu(message)

        return len(processed), processed


def main():
    import sys

    monitor = PodcastMonitor()

    if len(sys.argv) > 1 and sys.argv[1] == "--list":
        print("📋 当前订阅的播客:\n")
        for p in DEFAULT_PODCASTS:
            print(f"- {p['name']} ({p['category']}) [{p['priority']}]")
        return

    # 执行监控
    count, processed = monitor.run()
    print(f"\n📊 处理了 {count} 个新剧集")


if __name__ == "__main__":
    main()
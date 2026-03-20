#!/usr/bin/env python3
"""
播客 RSS 监控器
- 监控订阅的播客更新
- 新剧集推送到飞书
- 记录已推送剧集（避免重复）

使用方式:
    python podcast_monitor.py          # 检查更新并推送
    python podcast_monitor.py --list   # 列出所有订阅
"""

import json
import hashlib
import urllib.request
import ssl
import xml.etree.ElementTree as ET
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

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
    """播客监控器"""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.history_file = self.data_dir / "podcast_history.json"
        self.ssl_context = ssl.create_default_context()

    def _load_history(self) -> dict:
        """加载推送历史"""
        if self.history_file.exists():
            with open(self.history_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_history(self, history: dict):
        """保存推送历史"""
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
            print(f"⚠️ 获取 RSS 失败 {rss_url}: {e}")
            return None

    def parse_episodes(self, feed: ET.Element, podcast_name: str, limit: int = 5) -> List[Episode]:
        """解析 RSS Feed 获取剧集列表"""
        episodes = []

        # 查找 item 元素
        items = feed.findall(".//item") or feed.findall(".//{http://www.itunes.com/dtds/podcast-1.0.dtd}item")

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
            print(f"检查: {podcast['name']}...")
            feed = self.fetch_feed(podcast["rss_url"])
            if not feed:
                continue

            episodes = self.parse_episodes(feed, podcast["name"])
            for episode in episodes:
                episode_id = self._get_episode_id(episode)
                if episode_id not in history:
                    new_episodes.append(episode)
                    history[episode_id] = {
                        "title": episode.title,
                        "pub_date": episode.pub_date,
                        "pushed_at": datetime.now().isoformat()
                    }

        self._save_history(history)
        return new_episodes

    def format_push_message(self, episodes: List[Episode]) -> str:
        """格式化推送消息"""
        if not episodes:
            return ""

        lines = ["# 🎙️ 播客更新\n"]
        lines.append(f"发现 {len(episodes)} 个新剧集:\n")

        for ep in episodes:
            lines.append(f"**{ep.podcast_name}**")
            lines.append(f"📝 {ep.title}")
            if ep.duration:
                lines.append(f"⏱️ 时长: {ep.duration}")
            lines.append("")

        return "\n".join(lines)

    def push_to_feishu(self, message: str) -> bool:
        """推送到飞书"""
        if not message:
            return False

        # 导入推送模块
        import sys
        sys.path.insert(0, str(Path(__file__).parent))
        try:
            from pusher import FeishuPusher
            pusher = FeishuPusher()
            return pusher.send_markdown(message)
        except Exception as e:
            print(f"⚠️ 推送失败: {e}")
            return False

    def run(self) -> Tuple[int, List[Episode]]:
        """
        执行监控

        Returns:
            (新剧集数量, 新剧集列表)
        """
        new_episodes = self.check_new_episodes()

        if new_episodes:
            message = self.format_push_message(new_episodes)
            print(message)
            self.push_to_feishu(message)
        else:
            print("✅ 没有新剧集")

        return len(new_episodes), new_episodes


def main():
    import sys

    monitor = PodcastMonitor()

    if len(sys.argv) > 1 and sys.argv[1] == "--list":
        # 列出订阅
        print("📋 当前订阅的播客:\n")
        for p in DEFAULT_PODCASTS:
            print(f"- {p['name']} ({p['category']}) [{p['priority']}]")
        return

    # 执行监控
    count, episodes = monitor.run()
    print(f"\n📊 发现 {count} 个新剧集")


if __name__ == "__main__":
    main()
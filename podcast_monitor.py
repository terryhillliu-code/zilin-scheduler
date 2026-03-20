#!/usr/bin/env python3
"""
播客 RSS 监控器
- 监控订阅的播客更新
- 下载音频、转录、总结
- 存入 Obsidian 并推送通知

使用方式:
    python podcast_monitor.py              # 检查更新并处理
    python podcast_monitor.py --list       # 列出所有订阅
    python podcast_monitor.py --process <audio_url>  # 手动处理单个剧集
"""

import json
import hashlib
import urllib.request
import ssl
import xml.etree.ElementTree as ET
import logging
import subprocess
import re
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

# 路径配置
OBSIDIAN_VAULT = Path.home() / "Documents" / "ZhiweiVault"
PODCAST_NOTES_DIR = OBSIDIAN_VAULT / "10-19_AI-Systems" / "19_其他应用_Other-Applications" / "podcasts"
PODCAST_NOTES_DIR.mkdir(parents=True, exist_ok=True)

AUDIO_CACHE_DIR = Path.home() / "knowledge-inbox" / "podcasts"
AUDIO_CACHE_DIR.mkdir(parents=True, exist_ok=True)

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


class PodcastProcessor:
    """播客处理器：下载、转录、总结、存档"""

    def __init__(self):
        self.audio_cache = AUDIO_CACHE_DIR

    def download_audio(self, audio_url: str, title: str) -> Optional[Path]:
        """下载播客音频"""
        # 清理文件名
        safe_title = re.sub(r'[^\w\s-]', '', title)[:80]
        safe_title = re.sub(r'[-\s]+', '_', safe_title)
        output_path = self.audio_cache / f"{safe_title}.mp3"

        if output_path.exists():
            logger.info(f"音频已存在: {output_path.name}")
            return output_path

        logger.info(f"下载音频: {title[:50]}...")

        try:
            cmd = [
                "yt-dlp",
                "-x",  # 提取音频
                "--audio-format", "mp3",
                "--audio-quality", "0",
                "-o", str(self.audio_cache / "%(title).80s.%(ext)s"),
                audio_url
            ]
            subprocess.run(cmd, check=True, capture_output=True)

            # 查找下载的文件
            for f in self.audio_cache.glob("*.mp3"):
                if safe_title[:30].lower() in f.name.lower():
                    return f

            # 如果找不到精确匹配，返回最新的文件
            mp3_files = sorted(self.audio_cache.glob("*.mp3"), key=lambda x: x.stat().st_mtime, reverse=True)
            if mp3_files:
                return mp3_files[0]

        except Exception as e:
            logger.error(f"下载失败: {e}")
            return None

        return output_path if output_path.exists() else None

    def transcribe_audio(self, audio_path: Path) -> Optional[str]:
        """使用 faster-whisper 转录音频"""
        try:
            from faster_whisper import WhisperModel
        except ImportError:
            logger.error("未安装 faster-whisper，请运行: pip install faster-whisper")
            return None

        logger.info(f"转录音频: {audio_path.name}")

        try:
            model = WhisperModel("base", device="cpu", compute_type="int8")
            segments, info = model.transcribe(str(audio_path), beam_size=5)

            logger.info(f"检测语言: {info.language} (概率: {info.language_probability:.2f})")

            transcript = []
            for segment in segments:
                transcript.append(segment.text)

            return "".join(transcript)
        except Exception as e:
            logger.error(f"转录失败: {e}")
            return None

    def summarize_with_llm(self, transcript: str, episode: Episode) -> str:
        """使用 LLM 总结播客内容"""
        # 尝试导入 LLM 客户端
        try:
            import sys
            sys.path.insert(0, str(Path(__file__).parent.parent / "zhiwei-bot"))
            from core.llm_client import llm_client
        except ImportError:
            # 尝试从 scheduler 导入
            try:
                from scheduler import call_llm_direct
                llm_client = None
            except ImportError:
                logger.error("无法导入 LLM 客户端")
                return self._fallback_summary(transcript, episode)

        prompt = f"""你是一个专业的播客内容总结专家。请基于以下播客转录文本，生成一份结构化的摘要笔记。

播客信息:
- 名称: {episode.podcast_name}
- 标题: {episode.title}

要求:
1. 提取核心观点和关键信息
2. 列出 3-5 个最重要的知识点或洞察
3. 如果有嘉宾，标注嘉宾身份
4. 保持简洁，突出价值

转录文本（截取前 15000 字符）:
{transcript[:15000]}

请输出 Markdown 格式的摘要笔记。"""

        try:
            if llm_client:
                success, content = llm_client.call("research", prompt)
                if success:
                    return content
            else:
                return call_llm_direct(prompt)
        except Exception as e:
            logger.error(f"LLM 调用失败: {e}")

        return self._fallback_summary(transcript, episode)

    def _fallback_summary(self, transcript: str, episode: Episode) -> str:
        """备用摘要（LLM 不可用时）"""
        # 提取前 500 字作为摘要
        preview = transcript[:500] if transcript else "转录失败"
        return f"""# {episode.title}

> 播客: {episode.podcast_name}
> 日期: {episode.pub_date}

## 内容预览

{preview}...

*注: LLM 服务不可用，仅显示转录预览*"""

    def save_to_obsidian(self, summary: str, episode: Episode) -> Path:
        """保存到 Obsidian"""
        # 生成文件名
        safe_title = re.sub(r'[^\w\s-]', '', episode.title)[:80]
        safe_title = re.sub(r'[-\s]+', '_', safe_title)
        date_prefix = datetime.now().strftime("%Y-%m-%d")
        filename = f"PODCAST_{date_prefix}_{safe_title}.md"
        note_path = PODCAST_NOTES_DIR / filename

        # 构建完整笔记
        content = f"""---
tags: [podcast, {episode.podcast_name.replace(' ', '_')}]
source: {episode.audio_url}
podcast: {episode.podcast_name}
date: {datetime.now().strftime('%Y-%m-%d')}
duration: {episode.duration}
---

{summary}
"""

        with open(note_path, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info(f"✅ 笔记已保存: {note_path.name}")
        return note_path

    def process_episode(self, episode: Episode) -> Optional[Path]:
        """处理单个剧集：下载 -> 转录 -> 总结 -> 存档"""
        logger.info(f"处理剧集: {episode.title[:50]}...")

        # 1. 下载音频
        audio_path = self.download_audio(episode.audio_url, episode.title)
        if not audio_path:
            logger.error("音频下载失败")
            return None

        # 2. 转录
        transcript = self.transcribe_audio(audio_path)
        if not transcript:
            logger.error("转录失败")
            transcript = ""

        # 3. 总结
        summary = self.summarize_with_llm(transcript, episode)

        # 4. 保存到 Obsidian
        note_path = self.save_to_obsidian(summary, episode)

        return note_path


class PodcastMonitor:
    """播客监控器"""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.history_file = self.data_dir / "podcast_history.json"
        self.ssl_context = ssl.create_default_context()
        self.processor = PodcastProcessor()

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

    def format_push_message(self, processed: List[Tuple[Episode, Path]]) -> str:
        """格式化推送消息"""
        if not processed:
            return ""

        lines = ["# 🎙️ 播客更新\n"]
        lines.append(f"已处理 {len(processed)} 个新剧集:\n")

        for episode, note_path in processed:
            lines.append(f"**{episode.podcast_name}**")
            lines.append(f"📝 {episode.title}")
            lines.append(f"📁 已存入 Obsidian")
            lines.append("")

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

    def run(self, max_episodes: int = 3) -> Tuple[int, List[Tuple[Episode, Path]]]:
        """
        执行监控和处理

        Args:
            max_episodes: 最多处理的剧集数量（避免一次性处理太多）

        Returns:
            (处理数量, 处理结果列表)
        """
        new_episodes = self.check_new_episodes()
        history = self._load_history()
        processed = []

        # 只处理最新的 N 个剧集
        for episode in new_episodes[:max_episodes]:
            logger.info(f"\n{'='*50}")
            logger.info(f"处理: {episode.title}")

            note_path = self.processor.process_episode(episode)

            if note_path:
                processed.append((episode, note_path))
                episode_id = self._get_episode_id(episode)
                history[episode_id] = {
                    "title": episode.title,
                    "pub_date": episode.pub_date,
                    "processed_at": datetime.now().isoformat(),
                    "note_path": str(note_path)
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

    if len(sys.argv) > 1:
        if sys.argv[1] == "--list":
            print("📋 当前订阅的播客:\n")
            for p in DEFAULT_PODCASTS:
                print(f"- {p['name']} ({p['category']}) [{p['priority']}]")
            return

        if sys.argv[1] == "--process" and len(sys.argv) > 2:
            # 手动处理单个剧集
            audio_url = sys.argv[2]
            episode = Episode(
                podcast_name="手动添加",
                title=f"Manual_{datetime.now().strftime('%Y%m%d_%H%M')}",
                description="",
                pub_date=datetime.now().isoformat(),
                audio_url=audio_url
            )
            processor = PodcastProcessor()
            note_path = processor.process_episode(episode)
            if note_path:
                print(f"✅ 处理完成: {note_path}")
            return

    # 执行监控
    count, processed = monitor.run()
    print(f"\n📊 处理了 {count} 个新剧集")


if __name__ == "__main__":
    main()
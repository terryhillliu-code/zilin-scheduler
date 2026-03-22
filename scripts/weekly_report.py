#!/usr/bin/env python3
"""
知微系统 - 自动化研究周报生成器

每周生成一份基于 RAG 与 TaskHistory 的深度简报：
- 本周阅读的高分论文
- 视频金句摘录
- 技术关键词云图
- 系统活动统计

用法:
    python weekly_report.py              # 生成本周报告
    python weekly_report.py --last-week  # 生成上周报告
    python weekly_report.py --send       # 生成并推送飞书
"""

import json
import os
import sqlite3
import sys
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

# 数据源路径
PAPERS_DB = Path.home() / "arxiv-paper-analyzer" / "backend" / "data" / "papers.db"
TASKS_DB = Path.home() / "zhiwei-dev" / "tasks.db"
VAULT_PATH = Path.home() / "Documents" / "ZhiweiVault"
SCHEDULER_LOG = Path.home() / "logs" / "scheduler.jsonl"
OUTPUT_DIR = Path.home() / "zhiwei-docs" / "reports"


@dataclass
class WeeklyStats:
    """周报统计数据"""
    week_start: str
    week_end: str

    # 论文统计
    papers_total: int = 0
    papers_new: int = 0
    papers_tier_a: int = 0
    papers_tier_b: int = 0
    top_tags: List[tuple] = None

    # 视频统计
    videos_new: int = 0
    video_highlights: List[Dict] = None

    # 任务统计
    tasks_completed: int = 0
    tasks_failed: int = 0
    task_keywords: List[tuple] = None

    # 系统统计
    scheduler_runs: int = 0
    scheduler_success_rate: float = 0.0


class WeeklyReportGenerator:
    """周报生成器"""

    def __init__(self):
        self.stats = None
        self.highlights = []

    def generate(self, week_offset: int = 0) -> WeeklyStats:
        """
        生成周报

        Args:
            week_offset: 0=本周, -1=上周, -2=上上周...
        """
        # 计算日期范围
        today = datetime.now()
        week_start = today - timedelta(days=today.weekday() + 7 * (-week_offset))
        week_end = week_start + timedelta(days=6)

        stats = WeeklyStats(
            week_start=week_start.strftime("%Y-%m-%d"),
            week_end=week_end.strftime("%Y-%m-%d")
        )

        # 采集数据
        self._collect_paper_stats(stats, week_start, week_end)
        self._collect_video_stats(stats, week_start, week_end)
        self._collect_task_stats(stats, week_start, week_end)
        self._collect_system_stats(stats, week_start, week_end)

        self.stats = stats
        return stats

    def _collect_paper_stats(self, stats: WeeklyStats, week_start: datetime, week_end: datetime):
        """采集论文统计"""
        if not PAPERS_DB.exists():
            return

        try:
            conn = sqlite3.connect(str(PAPERS_DB))
            conn.row_factory = sqlite3.Row

            # 总论文数
            stats.papers_total = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]

            # 本周新增
            stats.papers_new = conn.execute("""
                SELECT COUNT(*) FROM papers
                WHERE created_at >= ? AND created_at <= ?
            """, (week_start.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d") + " 23:59:59")).fetchone()[0]

            # Tier 分布（从 analysis_json 提取）
            cursor = conn.execute("""
                SELECT analysis_json FROM papers
                WHERE analysis_json IS NOT NULL
            """)
            tier_counter = Counter()
            tag_counter = Counter()

            for row in cursor:
                try:
                    analysis = json.loads(row["analysis_json"])
                    tier = analysis.get("tier", "C")
                    tier_counter[tier] += 1

                    tags = analysis.get("tags", [])
                    if isinstance(tags, list):
                        for tag in tags:
                            if tag:
                                tag_counter[tag] += 1
                except:
                    pass

            stats.papers_tier_a = tier_counter.get("A", 0)
            stats.papers_tier_b = tier_counter.get("B", 0)
            stats.top_tags = tag_counter.most_common(10)

            conn.close()
        except Exception as e:
            print(f"⚠️ 论文数据采集失败: {e}")

    def _collect_video_stats(self, stats: WeeklyStats, week_start: datetime, week_end: datetime):
        """采集视频笔记统计"""
        video_dir = VAULT_PATH / "70-79_个人笔记_Personal" / "72_视频笔记_Video-Distill"
        if not video_dir.exists():
            return

        video_files = list(video_dir.glob("VIDEO_*.md"))
        stats.videos_new = len(video_files)

        # 提取视频金句（简单实现：提取核心观点）
        stats.video_highlights = []
        for vf in video_files[:5]:  # 取前5个
            try:
                content = vf.read_text(encoding="utf-8")
                # 提取核心观点
                for line in content.split("\n"):
                    if "核心观点" in line or "💡" in line:
                        # 获取下一行
                        idx = content.split("\n").index(line)
                        if idx + 1 < len(content.split("\n")):
                            highlight = content.split("\n")[idx + 1].strip()
                            if highlight and len(highlight) > 20:
                                stats.video_highlights.append({
                                    "title": vf.stem.replace("VIDEO_", ""),
                                    "highlight": highlight[:100]
                                })
                        break
            except:
                pass

    def _collect_task_stats(self, stats: WeeklyStats, week_start: datetime, week_end: datetime):
        """采集任务统计"""
        if not TASKS_DB.exists():
            return

        try:
            conn = sqlite3.connect(str(TASKS_DB))
            conn.row_factory = sqlite3.Row

            # 任务状态统计
            cursor = conn.execute("""
                SELECT status, COUNT(*) as cnt
                FROM tasks
                WHERE created_at >= ? AND created_at <= ?
                GROUP BY status
            """, (week_start.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d") + " 23:59:59"))

            keyword_counter = Counter()
            for row in cursor:
                if row["status"] == "done":
                    stats.tasks_completed = row["cnt"]
                elif row["status"] == "failed":
                    stats.tasks_failed = row["cnt"]

            # 任务关键词提取
            cursor = conn.execute("""
                SELECT input FROM tasks
                WHERE created_at >= ? AND status = 'done'
            """, (week_start.strftime("%Y-%m-%d"),))

            keywords = ["修复", "优化", "新增", "重构", "集成", "测试", "更新", "清理"]
            for row in cursor:
                input_text = row["input"] or ""
                for kw in keywords:
                    if kw in input_text:
                        keyword_counter[kw] += 1

            stats.task_keywords = keyword_counter.most_common(5)
            conn.close()
        except Exception as e:
            print(f"⚠️ 任务数据采集失败: {e}")

    def _collect_system_stats(self, stats: WeeklyStats, week_start: datetime, week_end: datetime):
        """采集系统运行统计"""
        if not SCHEDULER_LOG.exists():
            return

        try:
            runs = 0
            success = 0

            with open(SCHEDULER_LOG, "r") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        start_time = datetime.fromisoformat(record["start_time"])
                        if week_start <= start_time <= week_end + timedelta(days=1):
                            runs += 1
                            if record.get("success"):
                                success += 1
                    except:
                        pass

            stats.scheduler_runs = runs
            stats.scheduler_success_rate = success / runs if runs > 0 else 0.0
        except Exception as e:
            print(f"⚠️ 系统数据采集失败: {e}")

    def render_report(self) -> str:
        """渲染 Markdown 报告"""
        if not self.stats:
            return ""

        s = self.stats
        lines = [
            f"# 知微研究周报",
            f"",
            f"**统计周期**: {s.week_start} ~ {s.week_end}",
            f"",
            f"---",
            f"",
            f"## 📊 数据概览",
            f"",
            f"| 指标 | 数值 |",
            f"|------|------|",
            f"| 论文总数 | {s.papers_total} |",
            f"| 本周新增论文 | {s.papers_new} |",
            f"| Tier A 论文 | {s.papers_tier_a} |",
            f"| Tier B 论文 | {s.papers_tier_b} |",
            f"| 视频笔记 | {s.videos_new} |",
            f"| 完成任务 | {s.tasks_completed} |",
            f"| 定时任务成功率 | {s.scheduler_success_rate:.1%} |",
            f"",
            f"---",
            f"",
            f"## 🏷️ 热门标签",
            f"",
        ]

        if s.top_tags:
            for tag, count in s.top_tags[:10]:
                lines.append(f"- `{tag}` ({count} 篇)")
        else:
            lines.append("_暂无标签数据_")

        lines.extend([
            f"",
            f"---",
            f"",
            f"## 🎬 视频金句",
            f"",
        ])

        if s.video_highlights:
            for vh in s.video_highlights[:3]:
                lines.append(f"**{vh['title']}**")
                lines.append(f"> {vh['highlight']}")
                lines.append(f"")
        else:
            lines.append("_暂无视频金句_")

        lines.extend([
            f"",
            f"---",
            f"",
            f"## 🔧 任务关键词",
            f"",
        ])

        if s.task_keywords:
            for kw, count in s.task_keywords:
                lines.append(f"- {kw}: {count} 次")
        else:
            lines.append("_暂无任务数据_")

        lines.extend([
            f"",
            f"---",
            f"",
            f"*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}*",
        ])

        return "\n".join(lines)

    def save_report(self, content: str) -> Path:
        """保存报告到文件"""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        filename = f"weekly_report_{self.stats.week_start}.md"
        output_path = OUTPUT_DIR / filename

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

        return output_path


def send_to_feishu(self, content: str) -> bool:
        """推送周报到飞书"""
        try:
            import sys
            sys.path.insert(0, str(Path.home() / "zhiwei-bot"))
            from feishu_api import reply_message

            # 使用飞书群 chat_id
            chat_id = os.environ.get("FEISHU_CHAT_ID", "")

            if not chat_id:
                print("⚠️ 未配置 FEISHU_CHAT_ID")
                return False

            # 发送消息（Markdown 格式）
            reply_message(chat_id, content, msg_type="post")

            print(f"✅ 周报已推送到飞书")
            return True
        except Exception as e:
            print(f"❌ 飞书推送失败: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="知微研究周报生成器")
    parser.add_argument("--last-week", action="store_true", help="生成上周报告")
    parser.add_argument("--send", action="store_true", help="推送飞书（待实现）")
    args = parser.parse_args()

    generator = WeeklyReportGenerator()

    # 计算周偏移
    week_offset = -1 if args.last_week else 0

    print(f"📅 生成周报 (week_offset={week_offset})...")

    stats = generator.generate(week_offset)
    report = generator.render_report()

    # 保存报告
    output_path = generator.save_report(report)

    print(f"\n{report}")
    print(f"\n✅ 报告已保存: {output_path}")

    if args.send:
        print("\n📤 推送到飞书...")
        generator.send_to_feishu(report)


if __name__ == "__main__":
    main()
"""AI 产出自动归档到 Obsidian

将 scheduler 生成的 AI 内容（每日简报、ArXiv 论文、市场分析等）
自动写入 Obsidian Vault 形成结构化笔记。

使用方式:
    from obsidian_archive import ObsidianArchiver
    archiver = ObsidianArchiver()
    path = archiver.archive(content, note_type="brief", task_name="morning_brief")
"""

from pathlib import Path
from datetime import datetime
import re
import logging

logger = logging.getLogger(__name__)

VAULT_PATH = Path.home() / "Documents" / "ZhiweiVault"


class ObsidianArchiver:
    """将 AI 产出写入 Obsidian 结构化笔记"""

    DIRS = {
        "brief": "40_AI_Briefs/Daily",
        "arxiv": "40_AI_Briefs/ArXiv",
        "market": "40_AI_Briefs/Market",
    }

    TYPE_NAMES = {
        "brief": "每日简报",
        "arxiv": "ArXiv精选",
        "market": "市场分析",
    }

    def __init__(self, vault_path: Path = None):
        """
        初始化归档器

        Args:
            vault_path: Obsidian Vault 路径，默认为 ~/Documents/ZhiweiVault
        """
        self.vault = vault_path or VAULT_PATH
        self._ensure_dirs()

    def _ensure_dirs(self):
        """确保目标目录存在"""
        for subdir in self.DIRS.values():
            target_dir = self.vault / subdir
            target_dir.mkdir(parents=True, exist_ok=True)

    def archive(
        self,
        content: str,
        note_type: str,
        title: str = None,
        tags: list = None,
        task_name: str = None,
    ) -> Path:
        """
        归档内容到 Obsidian

        Args:
            content: 推送的原始内容（Markdown）
            note_type: brief / arxiv / market
            title: 笔记标题，不传则自动生成
            tags: 额外标签列表
            task_name: 任务名称（如 morning_brief, us_market_open 等）

        Returns:
            写入的文件路径
        """
        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H:%M")

        # 确定子目录
        subdir = self.DIRS.get(note_type, "40_AI_Briefs/Other")
        target_dir = self.vault / subdir
        target_dir.mkdir(parents=True, exist_ok=True)

        # 生成标题
        if not title:
            type_name = self.TYPE_NAMES.get(note_type, note_type)
            title = f"{type_name} {date_str}"

        # 构建标签
        tag_list = tags or []
        tag_list.extend([f"ai-{note_type}", "auto-generated", "zhiwei"])

        # 构建 YAML frontmatter
        tags_yaml = "\n".join(f"  - {t}" for t in tag_list)
        frontmatter = f"""---
title: "{title}"
date: {date_str}
time: {time_str}
type: {note_type}
source: zhiwei-scheduler
task: {task_name or note_type}
tags:
{tags_yaml}
---

"""

        # 清理内容
        clean_content = self._clean_content(content)

        # 文件名: 日期-类型-时间.md
        filename = f"{date_str}-{note_type}-{time_str.replace(':', '')}.md"
        filepath = target_dir / filename

        # 写入文件
        filepath.write_text(frontmatter + clean_content, encoding="utf-8")

        return filepath

    def _clean_content(self, content: str) -> str:
        """
        清理飞书格式标记

        Args:
            content: 原始内容

        Returns:
            清理后的内容
        """
        # 移除飞书 @ 提及
        content = re.sub(r"<at user_id=\"[^\"]*\"[^>]*>.*?</at>", "", content)
        # 移除飞书链接格式，保留文本
        content = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", content)
        # 清理多余空行
        content = re.sub(r"\n{3,}", "\n\n", content)
        return content.strip()


def archive_to_obsidian(
    content: str,
    note_type: str,
    title: str = None,
    tags: list = None,
    task_name: str = None,
) -> Path:
    """
    便捷函数：归档内容到 Obsidian

    Args:
        content: 推送的原始内容（Markdown）
        note_type: brief / arxiv / market
        title: 笔记标题
        tags: 额外标签列表
        task_name: 任务名称

    Returns:
        写入的文件路径
    """
    archiver = ObsidianArchiver()
    return archiver.archive(
        content=content,
        note_type=note_type,
        title=title,
        tags=tags,
        task_name=task_name,
    )
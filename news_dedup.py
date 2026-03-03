"""
新闻去重管理模块
- 记录已推送的新闻
- 每日自动清理
"""

import os
import json
import re
from datetime import datetime, date
from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
SENT_FILE = DATA_DIR / "news_sent.json"


def _ensure_dir():
    """确保数据目录存在"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_sent_today() -> list[dict]:
    """加载今天已推送的新闻"""
    _ensure_dir()
    today = date.today().isoformat()

    if not SENT_FILE.exists():
        return []

    try:
        with open(SENT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get(today, [])
    except (json.JSONDecodeError, Exception):
        return []


def get_sent_titles() -> str:
    """获取已推送的新闻标题，用于注入 prompt"""
    sent = load_sent_today()
    if not sent:
        return "（今天还没有推送过新闻）"

    lines = []
    for item in sent:
        title = item.get('title', '未知新闻')
        time_str = item.get('time', '')
        lines.append(f"- {title} (已在 {time_str} 推送)")
    return "\n".join(lines)


def record_sent(titles: list[str]):
    """记录已推送的新闻标题"""
    _ensure_dir()
    today = date.today().isoformat()
    now = datetime.now().strftime("%H:%M")

    data = {}
    if SENT_FILE.exists():
        try:
            with open(SENT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, Exception):
            data = {}

    if today not in data:
        data[today] = []

    for title in titles:
        # 避免重复记录
        existing_titles = [item['title'] for item in data[today]]
        if title not in existing_titles:
            data[today].append({"title": title, "time": now})

    # 只保留最近3天
    keys = sorted(data.keys())
    if len(keys) > 3:
        for old_key in keys[:-3]:
            del data[old_key]

    with open(SENT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def extract_titles_from_content(content: str) -> list[str]:
    """从推送内容中提取新闻标题"""
    titles = []
    for line in content.split("\n"):
        # 匹配 **标题** 格式
        match = re.search(r'\*\*(.+?)\*\*', line)
        if match:
            title = match.group(1).strip()
            # 过滤短标题和非新闻标题
            if len(title) > 5 and len(title) < 100 and title not in ["新闻标题", "重要资讯"]:
                titles.append(title)
    return titles


def count_new_items(content: str, min_new: int = 2) -> tuple[bool, int, list[str]]:
    """
    检查内容中是否有足够新的新闻
    返回 (是否有足够新内容, 新新闻数量, 提取的标题列表)
    """
    sent_titles = [item['title'] for item in load_sent_today()]
    all_titles = extract_titles_from_content(content)

    # 计算新标题（不在已推送列表中）
    new_titles = [t for t in all_titles if t not in sent_titles]

    # 如果提取的标题太少，认为没有足够内容
    if len(all_titles) < min_new:
        return False, len(all_titles), all_titles

    if len(new_titles) < min_new:
        return False, len(new_titles), all_titles

    return True, len(new_titles), all_titles


def should_push(content: str) -> bool:
    """判断是否应该推送：有新内容则推，无新内容不推"""
    has_new, count, _ = count_new_items(content, min_new=2)
    return has_new

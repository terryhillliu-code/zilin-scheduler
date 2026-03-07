#!/usr/bin/env python3
"""
开发经验记录与检索系统 (错题本)
轻量实现，JSONL 格式，无额外依赖
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional


# 数据存储文件路径
DATA_FILE = Path.home() / "zhiwei-scheduler" / "data" / "dev_memory.jsonl"


def ensure_data_dir():
    """确保数据目录存在"""
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)


def record(task: str, tags: List[str], problem: str, solution: str, files: List[str], status: str = "success"):
    """
    追加一条开发经验记录

    Args:
        task: 任务描述
        tags: 标签列表
        problem: 问题描述
        solution: 解决方案
        files: 涉及的文件列表
        status: 状态 (success/failed/pending)
    """
    ensure_data_dir()

    entry = {
        "timestamp": datetime.now().isoformat(),
        "task": task,
        "tags": tags,
        "problem": problem,
        "solution": solution,
        "files": files,
        "status": status
    }

    with open(DATA_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def search(query: str, top_k: int = 3) -> List[Dict[str, Any]]:
    """
    搜索相关开发经验

    Args:
        query: 查询关键词
        top_k: 返回结果数量

    Returns:
        相关经验列表
    """
    if not DATA_FILE.exists():
        return []

    # 拆分查询词
    query_words = query.lower().split()

    matches = []
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                entry = json.loads(line)

                # 计算匹配度
                score = 0
                combined_text = f"{entry['task']} {entry['problem']} {entry['solution']} {' '.join(entry['tags'])}".lower()

                for word in query_words:
                    if word in combined_text:
                        score += 1

                if score > 0:
                    matches.append({
                        "entry": entry,
                        "score": score
                    })

            except json.JSONDecodeError:
                continue

    # 按匹配度排序
    matches.sort(key=lambda x: x["score"], reverse=True)

    # 返回 top_k 结果
    return [match["entry"] for match in matches[:top_k]]


def get_recent(n: int = 5) -> List[Dict[str, Any]]:
    """
    获取最近的 n 条记录

    Args:
        n: 返回记录数量

    Returns:
        最近的 n 条记录
    """
    if not DATA_FILE.exists():
        return []

    entries = []
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                entry = json.loads(line)
                entries.append(entry)
            except json.JSONDecodeError:
                continue

    # 按时间戳倒序排列，返回最近的 n 条
    entries.sort(key=lambda x: x["timestamp"], reverse=True)
    return entries[:n]


def format_for_prompt(memories: List[Dict[str, Any]]) -> str:
    """
    格式化为 Prompt 注入文本

    Args:
        memories: 经验列表

    Returns:
        格式化的文本
    """
    if not memories:
        return "# 开发经验\n无相关经验记录。\n"

    result = ["# 相关开发经验", ""]

    for i, memory in enumerate(memories, 1):
        result.extend([
            f"## 经验 {i}",
            f"- **任务**: {memory['task']}",
            f"- **问题**: {memory['problem']}",
            f"- **解决方案**: {memory['solution']}",
            f"- **涉及文件**: {', '.join(memory['files'])}",
            f"- **时间**: {memory['timestamp'][:19]}",
            f"- **标签**: {', '.join(memory['tags'])}",
            ""
        ])

    return "\n".join(result)


def preload_sample_data():
    """预填几条今天的踩坑经验"""
    # 检查是否已经有数据，避免重复填充
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            first_line = f.readline()
        if first_line.strip():  # 如果文件非空，则不重复填充
            return

    # 预填今天踩过的坑
    record(
        task="修复 info_brief Prompt 渲染失败",
        tags=["info_brief", "prompt", "template", "weekday"],
        problem="load_prompt 渲染失败时返回原始模板，含未替换占位符",
        solution="失败时返回错误信息而非原始模板",
        files=["scheduler.py"],
        status="success"
    )

    record(
        task="修复 info_brief 超时",
        tags=["info_brief", "timeout", "agent", "scheduler"],
        problem="call_agent timeout=240太短，5个exec串行执行超时",
        solution="timeout 240→360，增加异常捕获记录error_msg",
        files=["scheduler.py"],
        status="success"
    )

    record(
        task="/写稿 同步超时",
        tags=["写稿", "timeout", "async", "article"],
        problem="同步执行60秒超时，用户等待无响应",
        solution="后台线程异步执行，180秒超时，完成后send_direct_message推送",
        files=["command_handler.py", "article_writer.py"],
        status="success"
    )


if __name__ == "__main__":
    # 作为模块使用，预填数据
    preload_sample_data()
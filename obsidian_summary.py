#!/usr/bin/env python3
"""
Obsidian AI 深度摘要生成器

为笔记中的占位符生成 AI 摘要：
- 扫描所有带 "等待向量化及摘要提取后自动写入" 的笔记
- 调用 LLM 生成深度摘要
- 更新笔记文件

使用方式:
    python obsidian_summary.py              # 生成所有摘要
    python obsidian_summary.py --limit 10   # 只处理 10 篇
    python obsidian_summary.py --status     # 查看状态
"""

import os
import re
import sys
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Optional

# 配置
VAULT_PATH = Path.home() / "Documents" / "ZhiweiVault"
PLACEHOLDER = "*(等待向量化及摘要提取后自动写入)*"
PROGRESS_FILE = Path.home() / ".obsidian_summary_progress.json"

# API 配置
DASHSCOPE_API_KEY = os.environ.get("DASHSCOPE_API_KEY", "")
DASHSCOPE_API_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"

# 速率限制
BATCH_SIZE = 10
DELAY_BETWEEN_BATCHES = 2.0  # 秒
DELAY_BETWEEN_REQUESTS = 0.5  # 秒


def call_llm(prompt: str, content: str, max_tokens: int = 500, api_key: str = None) -> Optional[str]:
    """
    调用 LLM 生成摘要

    Args:
        prompt: 系统提示词
        content: 文档内容
        max_tokens: 最大输出 token
        api_key: API Key（可选，默认使用全局变量）

    Returns:
        生成的摘要文本
    """
    try:
        import requests

        key = api_key or DASHSCOPE_API_KEY
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {key}"
        }

        data = {
            "model": "qwen3.5-plus",
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"请为以下文档生成深度摘要：\n\n{content[:4000]}"}
            ],
            "max_tokens": max_tokens,
            "temperature": 0.3
        }

        resp = requests.post(
            DASHSCOPE_API_URL,
            headers=headers,
            json=data,
            timeout=120  # 增加超时时间
        )

        if resp.status_code == 200:
            result = resp.json()
            return result["choices"][0]["message"]["content"]
        else:
            print(f"   ⚠️ API 错误: {resp.status_code}", file=sys.stderr)
            return None

    except Exception as e:
        print(f"   ⚠️ 调用失败: {e}", file=sys.stderr)
        return None


def generate_summary(content: str, title: str, api_key: str = None) -> Optional[str]:
    """
    生成深度摘要

    Args:
        content: 笔记内容（去除 frontmatter 后）
        title: 笔记标题
        api_key: API Key

    Returns:
        摘要文本
    """
    # 提取正文（去除 frontmatter 和元信息）
    body = content

    # 去除 frontmatter
    if body.startswith("---"):
        end = body.find("---", 3)
        if end > 0:
            body = body[end + 3:].strip()

    # 提取主要内容（到 AI 深度摘要之前）
    summary_idx = body.find("## AI 深度摘要")
    if summary_idx > 0:
        body = body[:summary_idx].strip()

    # 跳过太短的内容
    if len(body) < 200:
        return None

    # 系统提示词
    prompt = """你是一个专业的技术文档摘要助手。请为用户提供的技术文档生成深度摘要。

要求：
1. 摘要长度 150-300 字
2. 提炼核心观点和技术要点
3. 突出文档的创新点或关键信息
4. 使用简洁、专业的中文表达
5. 不要使用 "本文"、"文档" 等开头

输出格式：直接输出摘要内容，不需要标题或其他格式。"""

    return call_llm(prompt, body, api_key=api_key)


def find_notes_with_placeholder(vault_path: Path = VAULT_PATH) -> list[Path]:
    """找到所有带占位符的笔记"""
    notes = []
    for md_file in vault_path.rglob("*.md"):
        if ".obsidian" in str(md_file):
            continue
        try:
            content = md_file.read_text(encoding="utf-8", errors="ignore")
            if PLACEHOLDER in content:
                notes.append(md_file)
        except Exception:
            pass
    return notes


def load_progress() -> dict:
    """加载进度"""
    if not PROGRESS_FILE.exists():
        return {"completed": [], "failed": [], "last_run": None}

    try:
        return json.loads(PROGRESS_FILE.read_text())
    except Exception:
        return {"completed": [], "failed": [], "last_run": None}


def save_progress(progress: dict):
    """保存进度"""
    progress["last_run"] = datetime.now().isoformat()
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2))


def update_note_summary(note_path: Path, summary: str) -> bool:
    """
    更新笔记的摘要

    Args:
        note_path: 笔记路径
        summary: 生成的摘要

    Returns:
        是否成功
    """
    try:
        content = note_path.read_text(encoding="utf-8")

        # 替换占位符
        new_content = content.replace(
            f"## AI 深度摘要\n{PLACEHOLDER}",
            f"## AI 深度摘要\n\n{summary}"
        )

        if new_content == content:
            return False

        note_path.write_text(new_content, encoding="utf-8")
        return True

    except Exception as e:
        print(f"   ⚠️ 更新失败: {e}", file=sys.stderr)
        return False


def main():
    """主入口"""
    import argparse

    parser = argparse.ArgumentParser(description="Obsidian AI 深度摘要生成器")
    parser.add_argument("--limit", "-n", type=int, default=0, help="限制处理数量")
    parser.add_argument("--status", "-s", action="store_true", help="查看状态")
    parser.add_argument("--retry", "-r", action="store_true", help="重试失败的")

    args = parser.parse_args()

    if args.status:
        progress = load_progress()
        pending = find_notes_with_placeholder()
        print("📊 摘要生成状态:")
        print(f"   - 待处理: {len(pending)}")
        print(f"   - 已完成: {len(progress.get('completed', []))}")
        print(f"   - 失败: {len(progress.get('failed', []))}")
        print(f"   - 最后运行: {progress.get('last_run', '从未')}")
        return

    print("=" * 60)
    print("📝 Obsidian AI 深度摘要生成器")
    print("=" * 60)

    # 检查 API Key - 优先使用 .env 文件中的
    api_key = None
    env_file = Path.home() / "zhiwei-scheduler" / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("DASHSCOPE_API_KEY="):
                api_key = line.split("=", 1)[1].strip()
                break

    # 如果 .env 没有才用环境变量
    if not api_key:
        api_key = os.environ.get("DASHSCOPE_API_KEY", "")

    if not api_key:
        print("❌ 未找到 DASHSCOPE_API_KEY")
        return

    # 加载进度
    progress = load_progress()

    # 找到待处理笔记
    pending_notes = find_notes_with_placeholder()

    # 排除已完成的
    completed_set = set(progress.get("completed", []))
    pending_notes = [n for n in pending_notes if str(n) not in completed_set]

    if args.retry:
        # 重试失败的
        failed_set = set(progress.get("failed", []))
        pending_notes = [Path(p) for p in failed_set if Path(p).exists()]
        progress["failed"] = []

    # 限制数量
    if args.limit > 0:
        pending_notes = pending_notes[:args.limit]

    print(f"\n📋 待处理: {len(pending_notes)} 篇笔记")
    print(f"   API: qwen3.5-plus")
    print(f"   速率: 每批 {BATCH_SIZE} 篇，间隔 {DELAY_BETWEEN_BATCHES}s")

    if not pending_notes:
        print("\n✅ 没有待处理的笔记")
        return

    # 处理笔记
    stats = {"success": 0, "failed": 0, "skipped": 0}
    start_time = time.time()

    for i, note_path in enumerate(pending_notes):
        rel_path = note_path.relative_to(VAULT_PATH)
        print(f"\n[{i+1}/{len(pending_notes)}] {rel_path.name[:40]}")

        try:
            # 读取内容
            content = note_path.read_text(encoding="utf-8", errors="ignore")

            # 提取标题
            title = note_path.stem

            # 生成摘要
            print("   🤖 生成摘要...", end=" ", flush=True)
            summary = generate_summary(content, title, api_key=api_key)

            if not summary:
                print("❌ 生成失败")
                stats["failed"] += 1
                progress.setdefault("failed", []).append(str(note_path))
                continue

            # 更新笔记
            if update_note_summary(note_path, summary):
                print(f"✅ ({len(summary)} 字)")
                stats["success"] += 1
                progress.setdefault("completed", []).append(str(note_path))

                # 从失败列表移除（如果之前失败过）
                if str(note_path) in progress.get("failed", []):
                    progress["failed"].remove(str(note_path))
            else:
                print("⚠️ 更新失败")
                stats["failed"] += 1

            # 保存进度
            if (i + 1) % BATCH_SIZE == 0:
                save_progress(progress)
                print(f"\n   💾 进度已保存")
                time.sleep(DELAY_BETWEEN_BATCHES)
            else:
                time.sleep(DELAY_BETWEEN_REQUESTS)

        except KeyboardInterrupt:
            print("\n\n⚠️ 用户中断")
            break
        except Exception as e:
            print(f"❌ 异常: {e}")
            stats["failed"] += 1

    # 最终保存
    save_progress(progress)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"✅ 处理完成!")
    print(f"   - 成功: {stats['success']}")
    print(f"   - 失败: {stats['failed']}")
    print(f"   - 耗时: {elapsed:.1f}s")
    print(f"   - 平均: {elapsed/max(1, len(pending_notes)):.1f}s/篇")


if __name__ == "__main__":
    main()
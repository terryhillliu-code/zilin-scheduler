#!/usr/bin/env python3
"""
播客知识蒸馏器 v1.0
复用视频处理流程：ASR 转录 → LLM 蒸馏 → Markdown 输出

用法:
    python podcast_distiller.py --audio ~/path/to/podcast.mp3 --title "播客标题"
"""

import os
import sys
import json
import argparse
import re
from pathlib import Path
from datetime import datetime

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path.home()))

from zhiwei_common.llm import llm_client
from zhiwei_common.secrets import get_asr_key


# ============================================================================
# ASR 转录
# ============================================================================

def transcribe_with_dashscope(audio_path: Path) -> str:
    """使用 DashScope Recognition API 转录（支持本地文件，带回调）"""
    api_key = get_asr_key()
    if not api_key:
        print("⚠️ 未配置 DASHSCOPE_API_KEY")
        return ""

    import dashscope
    from dashscope.audio.asr import Recognition, RecognitionCallback

    dashscope.api_key = api_key

    # 检测音频格式
    suffix = audio_path.suffix.lower().lstrip('.')
    audio_format = suffix if suffix in ['mp3', 'wav', 'pcm', 'opus', 'm4a', 'aac'] else 'mp3'

    print(f"🎤 DashScope Recognition 转录中: {audio_path.name}")

    try:
        # 定义回调类收集结果
        class TranscribeCallback(RecognitionCallback):
            def __init__(self):
                self.result = None
                self.error = None

            def on_result(self, result):
                self.result = result

            def on_error(self, error):
                self.error = error

        # 创建 Recognition 实例（需要 callback）
        callback = TranscribeCallback()
        recognition = Recognition(
            model='paraformer-realtime-v2',
            format=audio_format,
            sample_rate=16000,
            callback=callback
        )

        result = recognition.call(file=str(audio_path.absolute()))

        if result.status_code == 200 and result.output:
            # 解析结果
            full_text = ""
            if 'sentence' in result.output:
                for sentence in result.output['sentence']:
                    full_text += sentence.get('text', '')
            elif 'text' in result.output:
                full_text = result.output['text']

            if full_text:
                print(f"✅ 转录完成: {len(full_text)} 字符")
                return full_text
            else:
                print(f"❌ 转录结果为空")
                return ""
        else:
            print(f"❌ ASR 失败: {result.message if hasattr(result, 'message') else 'unknown'}")
            return ""

    except ImportError:
        print("⚠️ dashscope 未安装，请运行: pip install dashscope")
        return ""
    except Exception as e:
        print(f"❌ DashScope Recognition 异常: {e}")
        return ""


def transcribe_with_local_whisper(audio_path: Path) -> str:
    """使用本地 MLX Whisper 转录（降级方案）"""
    try:
        import whisper
    except ImportError:
        print("⚠️ 本地 Whisper 未安装")
        return ""

    print(f"🎤 本地 Whisper 转录中: {audio_path.name}")

    try:
        model = whisper.load_model("base")
        result = model.transcribe(str(audio_path))
        transcript = result["text"]
        print(f"✅ 转录完成: {len(transcript)} 字符")
        return transcript
    except Exception as e:
        print(f"❌ 本地 Whisper 异常: {e}")
        return ""


def transcribe(audio_path: Path) -> str:
    """转录音频（云端优先，本地降级）"""
    # 优先 DashScope
    transcript = transcribe_with_dashscope(audio_path)

    if not transcript:
        # 降级到本地 Whisper
        transcript = transcribe_with_local_whisper(audio_path)

    return transcript


# ============================================================================
# LLM 知识蒸馏
# ============================================================================

DISTILL_PROMPT = """你是一个顶级的技术研究员与情报分析师，擅长从播客内容中提取高密度知识。

**任务**：从播客转录文本中蒸馏出核心知识，生成结构化的 Markdown 笔记。

**输出格式**：

## 🎧 播客核心观点

> 一句话总结本期核心价值

## 📌 关键时间点

- [00:00] 关键点1
- [00:00] 关键点2
...

## 🔍 深度解读

### 技术要点
- 要点1：详细解释
- 要点2：详细解释

### 工具/资源
- 工具名：用途说明
- 资源链接：说明

### 实践建议
- 建议1
- 建议2

## 💡 启发与思考

- 启发1
- 启发2

## 📚 延伸阅读

- 相关主题1
- 相关主题2

---

**播客信息**
- 标题：{title}
- 时长：约 {duration} 分钟
- 转录字数：{word_count} 字

**转录文本**：
{transcript}
"""


def distill(title: str, transcript: str) -> str:
    """LLM 知识蒸馏"""
    print(f"🧠 LLM 蒸馏中...")

    # 估算时长（假设每分钟约 150 字）
    word_count = len(transcript)
    duration = max(1, word_count // 150)

    prompt = DISTILL_PROMPT.format(
        title=title,
        duration=duration,
        word_count=word_count,
        transcript=transcript[:12000]  # 限制长度
    )

    try:
        success, response = llm_client.call("researcher", prompt, timeout=120)
        if success and response:
            print(f"✅ 蒸馏完成: {len(response)} 字符")
            return response
        else:
            print(f"❌ LLM 蒸馏失败")
            return f"# {title}\n\n转录文本：\n\n{transcript[:2000]}"
    except Exception as e:
        print(f"❌ LLM 蒸馏失败: {e}")
        return f"# {title}\n\n转录文本：\n\n{transcript[:2000]}"


# ============================================================================
# 保存笔记
# ============================================================================

def save_note(title: str, content: str, output_dir: Path) -> Path:
    """保存笔记到 Obsidian Vault"""
    output_dir.mkdir(parents=True, exist_ok=True)

    # 清理文件名
    safe_title = re.sub(r'[^\w\s-]', '', title)[:50]
    safe_title = re.sub(r'[-\s]+', '_', safe_title)

    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"PODCAST_{date_str}_{safe_title}.md"

    output_path = output_dir / filename

    # 添加 YAML frontmatter
    full_content = f"""---
title: {title}
date: {datetime.now().strftime("%Y-%m-%d %H:%M")}
type: podcast
tags: [播客, 技术]
---

{content}
"""

    output_path.write_text(full_content, encoding="utf-8")
    print(f"📝 笔记已保存: {output_path}")

    return output_path


# ============================================================================
# 主流程
# ============================================================================

def process_podcast(audio_path: Path, title: str = None, output_dir: Path = None) -> dict:
    """处理单个播客文件

    Args:
        audio_path: 音频文件路径
        title: 播客标题（可选，从文件名推断）
        output_dir: 输出目录（默认 Obsidian Vault）

    Returns:
        dict: 处理结果
    """
    if not audio_path.exists():
        return {"success": False, "error": f"文件不存在: {audio_path}"}

    # 推断标题
    if not title:
        title = audio_path.stem

    # 默认输出到 Obsidian Vault
    if not output_dir:
        output_dir = Path.home() / "Documents" / "ZhiweiVault" / "70-79_个人笔记" / "播客笔记"

    print(f"\n{'='*50}")
    print(f"🎧 处理播客: {title}")
    print(f"{'='*50}")

    # 1. 转录
    transcript = transcribe(audio_path)
    if not transcript:
        return {"success": False, "error": "转录失败"}

    # 2. 蒸馏
    distilled = distill(title, transcript)

    # 3. 保存
    note_path = save_note(title, distilled, output_dir)

    return {
        "success": True,
        "title": title,
        "transcript_length": len(transcript),
        "note_path": str(note_path)
    }


def main():
    parser = argparse.ArgumentParser(description="播客知识蒸馏器")
    parser.add_argument("--audio", required=True, help="音频文件路径")
    parser.add_argument("--title", help="播客标题")
    parser.add_argument("--output", help="输出目录")

    args = parser.parse_args()

    audio_path = Path(args.audio).expanduser()
    title = args.title
    output_dir = Path(args.output).expanduser() if args.output else None

    result = process_podcast(audio_path, title, output_dir)

    if result["success"]:
        print(f"\n✅ 处理成功!")
        print(f"   笔记: {result['note_path']}")
    else:
        print(f"\n❌ 处理失败: {result['error']}")


if __name__ == "__main__":
    main()
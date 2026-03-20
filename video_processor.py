#!/usr/bin/env python3
"""
视频内容处理器 (Phase 4b - T-402, T-403) + 深度分析增强

1. 使用 faster-whisper 提取音频文本 (Transcript)
2. RAG 检索相关背景知识
3. 调用大模型生成深度分析报告

v45.0 新增深度分析能力：
- RAG 背景知识检索增强
- 专家级洞察要求
- 行业影响分析
"""

import os
import json
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

# 尝试引入调度器的大模型调用能力
try:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from llm_proxy import call_llm_direct
except ImportError:
    call_llm_direct = None

# 尝试引入 RAG 桥接能力
try:
    from rag_bridge import get_context, is_available as rag_available
except ImportError:
    get_context = None
    rag_available = lambda: False

logger = logging.getLogger(__name__)

# 配置视频工作路径
VIDEO_INBOX = Path.home() / "knowledge-inbox" / "videos"
PROMPTS_DIR = Path(__file__).parent / "prompts"


def load_prompt_template(template_name: str) -> str:
    """加载 prompt 模板"""
    template_path = PROMPTS_DIR / f"{template_name}.txt"
    if template_path.exists():
        return template_path.read_text(encoding="utf-8")
    return ""


# 深度分析 Prompt 模板（从文件加载，失败则使用内置模板）
DEEP_ANALYSIS_TEMPLATE = load_prompt_template("video_deep_analysis")


# 简单总结 Prompt（向后兼容）
VIDEO_SUMMARY_PROMPT = """
你是一个专业的内容提炼专家。
请基于这段视频的转录文本，运用 ECC Content Engine 的 "Repurposing Flow" 准则提取核心知识点：
1. 核心 Angle：用一句话概括视频的主旨。
2. 原子级想法 (Atomic Ideas)：提取 3-5条 具有独立价值、无废话的知识点或事实结论。
3. 专家视角的启发：这段视频对开发者、投资者或决策者有什么深层启发？

转录文本（部分截取）：
{transcript}

请直接输出格式优美的 Markdown：
# 视频摘要: [标题]
## 🎙️ 核心主旨
...
## 💡 原子级事实
- ...
## 🧠 深度启发
- ...
"""


def extract_keywords(text: str, max_keywords: int = 10) -> list[str]:
    """
    从文本中提取关键词

    简单实现：提取中文词组和英文术语
    """
    import re

    # 提取中文词组（2-6字）
    chinese_pattern = r'[\u4e00-\u9fa5]{2,6}'
    chinese_words = re.findall(chinese_pattern, text)

    # 提取英文术语（大写字母开头的词组）
    english_pattern = r'[A-Z][a-z]+(?:[A-Z][a-z]+)*|[A-Z]{2,}'
    english_words = re.findall(english_pattern, text)

    # 合并去重
    keywords = list(set(chinese_words + english_words))

    # 按出现频率排序（简单实现：按长度）
    keywords.sort(key=len, reverse=True)

    return keywords[:max_keywords]


def enrich_with_context(transcript: str, top_k: int = 5) -> str:
    """
    从知识库检索相关背景知识

    Args:
        transcript: 转录文本
        top_k: 检索数量

    Returns:
        检索到的背景知识文本，失败返回空字符串
    """
    if not rag_available() or get_context is None:
        logger.debug("RAG 服务不可用，跳过背景知识检索")
        return ""

    # 从转录文本前 1000 字提取关键词
    keywords = extract_keywords(transcript[:1000])
    if not keywords:
        return ""

    # 构建查询
    query = " ".join(keywords[:5])
    logger.info(f"RAG 检索关键词: {query}")

    try:
        context = get_context(query, top_k=top_k)
        if context:
            logger.info(f"RAG 检索成功: {len(context)} 字符")
        return context
    except Exception as e:
        logger.error(f"RAG 检索失败: {e}")
        return ""


def transcribe_audio(audio_path: Path) -> str:
    """使用 faster-whisper 将音频转换为文本"""
    # 此处需要延迟导入，以防止环境未就绪时报错
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        logger.error("尚未安装 faster-whisper，请先执行环境部署。")
        raise

    logger.info(f"开始转录音频: {audio_path.name}")

    # 默认使用 base 模型以兼顾速度和准确度，在 Mac M系列芯片上建议使用 cpu (int8) 或更好的
    model_size = "base"
    try:
        model = WhisperModel(model_size, device="cpu", compute_type="int8")
        segments, info = model.transcribe(str(audio_path), beam_size=5)

        logger.info(f"检测到语言: {info.language} (概率: {info.language_probability:.2f})")

        full_text = []
        for segment in segments:
            full_text.append(segment.text)

        return "".join(full_text)
    except Exception as e:
        logger.error(f"转录过程中发生错误: {e}")
        raise


def process_video_content(
    media_path: Path,
    deep_analysis: bool = True,
    source_info: str = "",
    duration: str = ""
) -> str:
    """
    处理视频/音频：转录 -> RAG 增强 -> 深度分析 -> 返回 Markdown

    Args:
        media_path: 音频文件路径
        deep_analysis: 是否启用深度分析（RAG 增强），默认 True
        source_info: 来源信息（URL、标题等）
        duration: 时长信息

    Returns:
        Markdown 格式的分析报告
    """
    if not media_path.exists():
        raise FileNotFoundError(f"媒体文件不存在: {media_path}")

    # 1. 转录音频
    transcript = transcribe_audio(media_path)

    # 2. 检索背景知识（深度分析模式）
    background_context = ""
    if deep_analysis:
        logger.info("深度分析模式：检索背景知识...")
        background_context = enrich_with_context(transcript, top_k=5)

    # 3. 调用大模型生成报告
    if call_llm_direct:
        if deep_analysis and DEEP_ANALYSIS_TEMPLATE:
            # 深度分析模式
            logger.info("生成深度分析报告...")
            prompt = DEEP_ANALYSIS_TEMPLATE.format(
                background_context=background_context or "（未检索到相关背景知识）",
                transcript=transcript[:30000],  # 扩大上下文窗口
                source_info=source_info or media_path.stem,
                duration=duration or "未知"
            )
        else:
            # 简单总结模式（向后兼容）
            logger.info("生成视频摘要...")
            prompt = VIDEO_SUMMARY_PROMPT.format(transcript=transcript[:20000])

        summary_md = call_llm_direct(prompt)
    else:
        logger.warning("未找到 call_llm_direct，仅保存原始内容。")
        summary_md = f"# 原始转录\n\n{transcript}"

    return summary_md


def process_video_simple(media_path: Path) -> str:
    """
    简单处理模式（向后兼容）

    仅转录 + 简单总结，不启用深度分析
    """
    return process_video_content(media_path, deep_analysis=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import sys
    if len(sys.argv) > 1:
        test_file = Path(sys.argv[1])

        # 支持命令行参数控制模式
        deep_mode = "--simple" not in sys.argv
        if not deep_mode:
            print("使用简单总结模式")

        res = process_video_content(test_file, deep_analysis=deep_mode)
        print("\n=== 生成的报告预览 ===")
        print(res[:1500])
    else:
        print("用法: python video_processor.py <音频文件> [--simple]")
        print("  --simple: 使用简单总结模式，不启用深度分析")
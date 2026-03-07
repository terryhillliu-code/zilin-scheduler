#!/usr/bin/env python3
"""
视频内容处理器 (Phase 4b - T-402, T-403)
1. 使用 faster-whisper 提取音频文本 (Transcript)
2. 调用大模型基于 content-engine 准则生成核心摘要点
"""

import os
import json
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

# 尝试引入调度器的大模型调用能力
try:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from scheduler import call_llm_direct
except ImportError:
    call_llm_direct = None

logger = logging.getLogger(__name__)

# 配置视频工作路径
VIDEO_INBOX = Path.home() / "knowledge-inbox" / "videos"

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

def process_video_content(media_path: Path) -> str:
    """处理视频/音频：转录 -> 总结 -> 返回 Markdown"""
    if not media_path.exists():
        raise FileNotFoundError(f"媒体文件不存在: {media_path}")

    # 1. 如果是视频，可能需要 ffmpeg 转音频？yt-dlp 已经可以帮我们做 extract_audio
    # 我们假设 media_path 已经是音频或转录所需的文件
    
    transcript = transcribe_audio(media_path)
    
    # 2. 调用大模型提炼知识点 (截取前 20000 字符防止溢出)
    summary_md = ""
    if call_llm_direct:
        logger.info("调用大模型生成视频精简摘要...")
        prompt = VIDEO_SUMMARY_PROMPT.format(transcript=transcript[:20000])
        summary_md = call_llm_direct(prompt)
    else:
        logger.warning("未找到 call_llm_direct，仅保存原始内容。")
        summary_md = f"# 原始转录\n\n{transcript}"

    return summary_md

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import sys
    if len(sys.argv) > 1:
        test_file = Path(sys.argv[1])
        res = process_video_content(test_file)
        print("\n=== 生成的摘要预览 ===")
        print(res[:1000])

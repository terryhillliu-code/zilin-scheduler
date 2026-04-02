#!/usr/bin/env python3
"""
音频转录脚本 - 支持多种 ASR 方式
作为 subprocess 调用，避免 Python 3.14 async 兼容性问题

用法:
    python transcribe_audio.py --audio path/to/audio.mp3 --output result.json

支持方式（优先级）:
1. OpenAI Whisper API (需要 OPENAI_API_KEY)
2. 本地 MLX Whisper (Mac 优化)
3. 本地 openai-whisper
"""

import argparse
import json
import sys
import time
import tempfile
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path.home()))


def transcribe_with_openai_api(audio_path: Path) -> str:
    """使用 OpenAI Whisper API 转录"""
    try:
        from zhiwei_common.secrets import get_api_key
        api_key = get_api_key(["OPENAI_API_KEY", "CODING_PLAN_API_KEY"])
        if not api_key:
            print("未配置 OPENAI_API_KEY", file=sys.stderr)
            return ""
    except Exception as e:
        print(f"获取密钥失败: {e}", file=sys.stderr)
        return ""

    import requests

    url = "https://api.openai.com/v1/audio/transcriptions"

    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    print(f"OpenAI Whisper API 转录: {audio_path.name}", file=sys.stderr)

    try:
        with open(audio_path, 'rb') as f:
            files = {'file': (audio_path.name, f)}
            data = {'model': 'whisper-1'}

            response = requests.post(
                url,
                headers=headers,
                files=files,
                data=data,
                timeout=600  # 10分钟超时
            )

        if response.status_code == 200:
            result = response.json()
            text = result.get('text', '')
            print(f"转录成功: {len(text)} 字符", file=sys.stderr)
            return text
        else:
            print(f"API 错误: {response.status_code} - {response.text[:200]}", file=sys.stderr)
            return ""

    except requests.exceptions.Timeout:
        print("OpenAI API 超时", file=sys.stderr)
        return ""
    except Exception as e:
        print(f"OpenAI API 异常: {e}", file=sys.stderr)
        return ""


def transcribe_with_mlx_whisper(audio_path: Path) -> str:
    """使用 MLX Whisper（Mac 优化，快速）"""
    try:
        from mlx_whisper import transcribe
    except ImportError:
        print("MLX Whisper 未安装", file=sys.stderr)
        return ""

    print(f"MLX Whisper 转录: {audio_path.name}", file=sys.stderr)

    try:
        result = transcribe(str(audio_path), word_timestamps=False)
        text = result.get('text', '')
        print(f"转录成功: {len(text)} 字符", file=sys.stderr)
        return text
    except Exception as e:
        print(f"MLX Whisper 异常: {e}", file=sys.stderr)
        return ""


def transcribe_with_local_whisper(audio_path: Path) -> str:
    """使用本地 openai-whisper"""
    try:
        import whisper
    except ImportError:
        print("本地 Whisper 未安装", file=sys.stderr)
        return ""

    print(f"本地 Whisper 转录: {audio_path.name}", file=sys.stderr)

    try:
        model = whisper.load_model("base")
        result = model.transcribe(str(audio_path))
        text = result["text"]
        print(f"转录成功: {len(text)} 字符", file=sys.stderr)
        return text
    except Exception as e:
        print(f"Whisper 异常: {e}", file=sys.stderr)
        return ""


def main():
    parser = argparse.ArgumentParser(description="音频转录")
    parser.add_argument("--audio", required=True, help="音频文件路径")
    parser.add_argument("--output", required=True, help="输出 JSON 文件路径")

    args = parser.parse_args()

    audio_path = Path(args.audio).expanduser()

    if not audio_path.exists():
        print(f"文件不存在: {audio_path}", file=sys.stderr)
        json.dump({"success": False, "error": "file_not_found", "text": ""},
                  open(args.output, 'w'))
        sys.exit(1)

    # 优先级：OpenAI API > MLX Whisper > 本地 Whisper
    text = transcribe_with_openai_api(audio_path)

    if not text:
        text = transcribe_with_mlx_whisper(audio_path)

    if not text:
        text = transcribe_with_local_whisper(audio_path)

    # 写入结果
    result = {
        "success": bool(text),
        "text": text,
        "length": len(text) if text else 0,
        "audio_file": str(audio_path)
    }

    with open(args.output, 'w') as f:
        json.dump(result, f, indent=2)

    if text:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
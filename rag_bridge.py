"""
zhiwei-rag 桥接模块
通过子进程调用 zhiwei-rag封装的 bridge.py，避免依赖冲突
"""
import os
import sys
import json
import subprocess
from pathlib import Path
from typing import Optional

# zhiwei-rag 路径
RAG_DIR = Path.home() / "zhiwei-rag"
RAG_BRIDGE = RAG_DIR / "bridge.py"
RAG_VENV_PYTHON = RAG_DIR / "venv" / "bin" / "python"


def _call_bridge(command: str, query: str, top_k: int = 5, **kwargs) -> Optional[str]:
    """
    调用 zhiwei-rag bridge.py
    
    Args:
        command: retrieve / context / prompt
        query: 查询文本
        top_k: 返回数量
        **kwargs: 额外参数
        
    Returns:
        命令输出（成功）或 None（失败）
    """
    if not RAG_BRIDGE.exists():
        print(f"[RAG Bridge] bridge.py 不存在: {RAG_BRIDGE}", file=sys.stderr)
        return None
    
    # 构建命令
    cmd = [str(RAG_VENV_PYTHON), str(RAG_BRIDGE), command, query, "--top-k", str(top_k)]
    
    # 添加额外参数
    for key, value in kwargs.items():
        cmd.extend([f"--{key.replace('_', '-')}", str(value)])
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,  # 2 分钟超时
            cwd=str(RAG_DIR)
        )
        
        if result.returncode != 0:
            print(f"[RAG Bridge] 命令失败: {result.stderr}", file=sys.stderr)
            return None
        
        return result.stdout
        
    except subprocess.TimeoutExpired:
        print("[RAG Bridge] 命令超时", file=sys.stderr)
        return None
    except Exception as e:
        print(f"[RAG Bridge] 调用异常: {e}", file=sys.stderr)
        return None


def retrieve(query: str, top_k: int = 5) -> list[dict]:
    """
    执行检索，返回结构化结果
    
    Returns:
        检索结果列表，每项包含 text, source, score 等
    """
    output = _call_bridge("retrieve", query, top_k=top_k)
    
    if not output:
        return []
    
    try:
        return json.loads(output)
    except json.JSONDecodeError as e:
        print(f"[RAG Bridge] JSON 解析失败: {e}", file=sys.stderr)
        return []


def get_context(query: str, top_k: int = 5) -> str:
    """
    获取检索上下文（纯文本）
    
    用于注入到 Prompt 中
    """
    output = _call_bridge("context", query, top_k=top_k)
    return output or ""


def get_prompt(query: str, top_k: int = 5, template: str = "qa") -> str:
    """
    获取完整 Prompt（含模板）
    """
    output = _call_bridge("prompt", query, top_k=top_k, template=template)
    return output or ""


def is_available() -> bool:
    """检查 zhiwei-rag 是否可用"""
    return RAG_BRIDGE.exists() and RAG_VENV_PYTHON.exists()


# 快捷函数：兼容旧 API
def enrich_with_rag(query: str, top_k: int = 5) -> str:
    """
    RAG 增强（替代 enrich_with_klib）
    
    Returns:
        检索上下文字符串
    """
    return get_context(query, top_k=top_k)

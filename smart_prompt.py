#!/usr/bin/env python3
"""
智能提示词系统
结合代码骨架和开发经验构建动态上下文
"""

import os
import sys
import re
from pathlib import Path
from typing import List, Dict, Any, Optional

# 添加项目路径以便导入模块
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent / "scripts"))

import dev_memory
from scripts.code_skeleton import scan_directories, generate_skeleton


def inject_context(prompt: str, focus_files: Optional[List[str]] = None) -> str:
    """
    将相关代码骨架和开发经验注入提示词

    Args:
        prompt: 原始提示词
        focus_files: 关注的文件列表，如果提供则仅包含这些文件的骨架

    Returns:
        包含上下文的增强提示词
    """
    # 获取相关的代码骨架
    skeleton_context = get_relevant_skeleton(prompt, focus_files)

    # 获取相关的开发经验
    memory_context = get_relevant_memories(prompt)

    # 构建完整上下文
    context_prompt = build_context_prompt(
        task_desc=prompt,
        skeleton_context=skeleton_context,
        memory_context=memory_context
    )

    return context_prompt


def get_relevant_skeleton(query: str, focus_files: Optional[List[str]] = None) -> str:
    """
    根据查询获取相关代码结构

    Args:
        query: 查询字符串
        focus_files: 关注的文件列表，如果提供则仅扫描这些文件

    Returns:
        相关代码骨架文本
    """
    skeleton_file = Path.home() / "zhiwei-scheduler" / "skeleton.md"

    if not skeleton_file.exists():
        return "# 代码骨架\n未找到代码骨架文件。\n"

    # 读取现有骨架
    skeleton_content = skeleton_file.read_text(encoding="utf-8")

    if focus_files:
        # 如果指定了关注文件，则只返回这些文件的内容
        relevant_parts = []
        for line in skeleton_content.split('\n'):
            if any(file_path in line for file_path in focus_files):
                relevant_parts.append(line)

        # 添加相关部分的后续内容直到遇到下一个文件头
        result_lines = []
        include_section = False

        for line in skeleton_content.split('\n'):
            if any(f"## {file_path}" in line for file_path in focus_files):
                include_section = True
                result_lines.append(line)
            elif line.startswith("## ") and include_section:
                # 遇到下一个文件头，停止添加
                break
            elif include_section:
                result_lines.append(line)

        return "\n".join(result_lines) if result_lines else "# 代码骨架\n未找到相关代码结构。\n"

    # 检查骨架是否已经包含相关内容
    query_lower = query.lower()
    relevant_parts = []

    # 按行分割，查找可能相关的部分
    lines = skeleton_content.split('\n')
    current_section = ""
    current_content = []

    for line in lines:
        if line.startswith("## "):
            # 检查当前部分是否与查询相关
            if current_section and (
                query_lower in current_section.lower() or
                any(token in current_content_str.lower() for token in query_lower.split() if len(token) > 2)
            ):
                relevant_parts.extend([current_section] + current_content)

            # 开始新部分
            current_section = line
            current_content = []
        else:
            current_content.append(line)

        # 临时用于匹配的字符串
        current_content_str = ' '.join(current_content)

    # 处理最后一部分
    if current_section and (
        query_lower in current_section.lower() or
        any(token in current_content_str.lower() for token in query_lower.split() if len(token) > 2)
    ):
        relevant_parts.extend([current_section] + current_content)

    if relevant_parts:
        return "\n".join(relevant_parts)
    else:
        return skeleton_content  # 如果没找到特定相关部分，返回完整骨架


def get_relevant_memories(query: str) -> str:
    """
    根据查询获取相关开发经验

    Args:
        query: 查询字符串

    Returns:
        相关开发经验文本
    """
    # 使用dev_memory模块搜索相关经验
    memories = dev_memory.search(query, top_k=3)
    return dev_memory.format_for_prompt(memories)


def build_context_prompt(
    task_desc: str,
    skeleton_context: str = "",
    memory_context: str = ""
) -> str:
    """
    构建完整的上下文提示词

    Args:
        task_desc: 任务描述
        skeleton_context: 代码骨架上下文
        memory_context: 开发经验上下文

    Returns:
        完整的提示词
    """
    header = "# 知微系统开发助手\n\n"
    task_section = f"## 当前任务\n{task_desc}\n\n"
    skeleton_section = f"## 相关代码结构\n{skeleton_context}\n\n" if skeleton_context.strip() else ""
    memory_section = f"## 相关开发经验\n{memory_context}\n\n" if memory_context.strip() else ""

    footer = (
        "## 指令\n"
        "请基于以上代码结构和开发经验完成当前任务。注意：\n"
        "- 保持与现有代码风格的一致性\n"
        "- 遵循已有的开发模式和最佳实践\n"
        "- 考虑已知的问题和解决方案\n"
        "- 注意文件路径和函数签名\n"
    )

    return header + task_section + skeleton_section + memory_section + footer


def refresh_skeleton() -> str:
    """
    刷新代码骨架，重新扫描项目并生成新的骨架

    Returns:
        新的骨架内容
    """
    project_root = Path.home()

    # 定义需要扫描的目录
    dirs_to_scan = [
        project_root / "zhiwei-bot",
        project_root / "zhiwei-scheduler",
        project_root / "clawdbot-docker",
        project_root / "Documents" / "Library"
    ]

    # 过滤存在的目录
    existing_dirs = [d for d in dirs_to_scan if d.exists()]

    # 扫描目录获取文件信息
    files_info = scan_directories(existing_dirs)

    # 生成骨架
    skeleton_content = generate_skeleton(files_info)

    # 保存到文件
    skeleton_file = Path.home() / "zhiwei-scheduler" / "skeleton.md"
    skeleton_file.write_text(skeleton_content, encoding="utf-8")

    return skeleton_content


def get_smart_prompt(task_desc: str, focus_files: Optional[List[str]] = None) -> str:
    """
    获取智能增强后的提示词

    Args:
        task_desc: 任务描述
        focus_files: 关注的文件列表

    Returns:
        增强后的提示词
    """
    return inject_context(task_desc, focus_files)


# 示例使用方法
if __name__ == "__main__":
    sample_task = "修复 ws_client.py 中的 WebSocket 断连问题"
    enhanced_prompt = get_smart_prompt(sample_task)
    print(enhanced_prompt)
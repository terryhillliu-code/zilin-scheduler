#!/usr/bin/env python3
"""
Obsidian wikilinks 自动关联工具
Phase 3: 实现新内容与现有笔记的自动关联功能
"""

import json
import subprocess
import urllib.request
import time
from pathlib import Path
from datetime import datetime


# 配置
CHROMA_PATH = "/root/downloads/knowledge-library/chromadb"
CHROMA_COLLECTION = "knowledge_base"
CONTAINER_NAME = "clawdbot"

# DashScope API 配置（用于云端 embedding）
DASHSCOPE_API_KEY = "sk-70d377bd717b4f8abe405bff72427147"
DASHSCOPE_API_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings"


def find_related_notes(content: str, top_k: int = 5) -> list:
    """
    用 ChromaDB 检索相似笔记

    Args:
        content: 新笔记内容
        top_k: 返回最相似的笔记数量

    Returns:
        list: 相似笔记列表，每个元素包含 {id, title, content, score}
    """
    # 步骤 1: 使用 DashScope API 生成 embedding
    embed_data = json.dumps({
        "model": "text-embedding-v3",
        "input": content[:4096],  # 限制内容长度
        "dimension": 1024  # 匹配 ChromaDB 中已有数据的维度
    }).encode()

    embed_req = urllib.request.Request(
        DASHSCOPE_API_URL,
        data=embed_data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {DASHSCOPE_API_KEY}"
        }
    )

    try:
        with urllib.request.urlopen(embed_req, timeout=30) as resp:
            embed_result = json.loads(resp.read())
            embedding = embed_result["data"][0]["embedding"]
            print(f"   📡 内容 embedding 成功, 维度: {len(embedding)}")
    except Exception as e:
        print(f"   ❌ embedding 生成失败: {e}")
        return []

    # 步骤 2: 在容器内查询 ChromaDB
    embed_str = json.dumps(embedding)

    cmd = f"""
import chromadb
import json

client = chromadb.PersistentClient(path="{CHROMA_PATH}")
collection = client.get_or_create_collection("{CHROMA_COLLECTION}")

query_embedding = {embed_str}

results = collection.query(
    query_embeddings=[query_embedding],
    n_results={top_k}
)

# 输出结果
output_data = []
if 'documents' in results and 'metadatas' in results and 'distances' in results:
    for i in range(len(results['documents'][0])):
        doc_data = {{
            'id': results['ids'][0][i] if i < len(results['ids'][0]) else '',
            'title': results['metadatas'][0][i].get('title', ''),
            'content': results['documents'][0][i][:200] if i < len(results['documents'][0]) else '',
            'score': results['distances'][0][i] if i < len(results['distances'][0]) else 0.0,
            'source': results['metadatas'][0][i].get('source', '')
        }}
        output_data.append(doc_data)

print(json.dumps(output_data))
"""

    try:
        result = subprocess.run(
            ["docker", "exec", CONTAINER_NAME, "python3", "-c", cmd],
            capture_output=True, text=True, timeout=30
        )

        if result.returncode != 0:
            print(f"ChromaDB 查询失败: {result.stderr[:200]}")
            return []

        related_notes = json.loads(result.stdout.strip())
        print(f"🔍 找到 {len(related_notes)} 个潜在相关笔记")
        return related_notes

    except subprocess.TimeoutExpired:
        print("ChromaDB 查询超时", file=sys.stderr)
        return []
    except Exception as e:
        print(f"向量搜索异常: {e}", file=sys.stderr)
        return []


def confirm_relations(new_note: str, candidates: list) -> list:
    """
    调用百炼确认哪些候选笔记真正相关

    Args:
        new_note: 新笔记内容
        candidates: 候选笔记列表

    Returns:
        list: 确认相关的笔记列表
    """
    if not candidates:
        return []

    # 构建Prompt给百炼确认关联性
    candidate_info = ""
    for i, note in enumerate(candidates):
        candidate_info += f"{i+1}. 标题: {note['title']}\n"
        candidate_info += f"   内容片段: {note['content'][:100]}...\n"
        candidate_info += f"   相似度得分: {note['score']:.3f}\n\n"

    prompt = f"""
你是一个智能知识图谱构建助手，需要分析一篇新笔记与现有笔记之间的关联性。

## 新笔记内容:
{new_note[:1000]}

## 候选关联笔记:
{candidate_info}

## 任务
请分析新笔记与哪些候选笔记真正具有实质性关联。关联指的是:
- 主题相关或属于同一领域
- 内容相互补充或呼应
- 概念、事件、人物、技术等存在交集
- 具备知识图谱中的连接价值

## 输出要求
请输出 JSON 格式:
{{
    "relations": [
        {{
            "candidate_index": 数组索引(从0开始),
            "relation_type": "conceptual|domain|event|tech|other",
            "confidence": 0.0-1.0的置信度,
            "explanation": "为什么这两篇笔记相关"
        }}
    ]
}}

只返回JSON，不要其他内容。
"""

    # 调用百炼 API
    try:
        import urllib.request
        import json

        # 使用百炼 API 进行关联判断
        api_url = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
        api_headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {DASHSCOPE_API_KEY}"
        }

        api_data = json.dumps({
            "model": "qwen3.5-plus",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1
        }).encode()

        req = urllib.request.Request(api_url, data=api_data, headers=api_headers)
        with urllib.request.urlopen(req, timeout=60) as response:
            response_data = json.loads(response.read())

        ai_response = response_data["choices"][0]["message"]["content"].strip()

        # 提取JSON部分
        import re
        json_match = re.search(r'\{.*\}', ai_response, re.DOTALL)
        if json_match:
            relations_json = json_match.group()
            relations = json.loads(relations_json)
            confirmed_relations = relations.get("relations", [])

            # 将候选索引转换为实际的笔记信息
            confirmed_notes = []
            for rel in confirmed_relations:
                idx = rel["candidate_index"]
                if 0 <= idx < len(candidates):
                    note = candidates[idx].copy()
                    note["relation_info"] = {
                        "type": rel["relation_type"],
                        "confidence": rel["confidence"],
                        "explanation": rel["explanation"]
                    }
                    confirmed_notes.append(note)

            print(f"✅ 确认了 {len(confirmed_notes)} 个相关笔记")
            return confirmed_notes
        else:
            print("❌ 未能从百炼响应中提取JSON")
            return []

    except Exception as e:
        print(f"❌ 百炼关联分析失败: {e}")
        return []


def generate_wikilinks(relations: list) -> str:
    """
    生成 wikilinks 文本

    Args:
        relations: 确认的关联笔记列表

    Returns:
        str: 格式化的 wikilinks 字符串
    """
    if not relations:
        return ""

    wikilinks = []
    for note in relations:
        title = note['title']
        # 如果标题包含空格或其他特殊字符，保持不变以匹配Obsidian格式
        wikilinks.append(f"[[{title}]]")

    return " " + " ".join(wikilinks)


def update_note_with_links(note_path: Path, links: str):
    """
    更新笔记添加链接

    Args:
        note_path: 笔记文件路径
        links: 要添加的wikilinks字符串
    """
    if not note_path.exists():
        print(f"❌ 笔记文件不存在: {note_path}")
        return

    # 读取现有内容
    content = note_path.read_text(encoding='utf-8')

    # 检查是否已经有wikilinks，避免重复
    if links.strip() in content:
        print(f"📝 笔记已有相关链接，跳过: {note_path.name}")
        return

    # 在标题行后添加wikilinks，或者在文件开头添加
    lines = content.split('\n')
    updated_lines = []

    if lines and lines[0].startswith('# '):  # 如果第一行是标题
        updated_lines.append(lines[0])  # 保留标题
        updated_lines.append(links)      # 添加链接行
        updated_lines.extend(lines[1:])  # 添加其余内容
    else:
        updated_lines.append(links)      # 添加链接行
        updated_lines.extend(lines)      # 添加原有内容

    # 写回文件
    new_content = '\n'.join(updated_lines)
    note_path.write_text(new_content, encoding='utf-8')
    print(f"✅ 成功更新笔记: {note_path.name}，添加了 {len(links.split(' [['))-1} 个链接")


def create_bidirectional_links(new_note_path: Path, relations: list):
    """
    创建双向链接：不仅更新新笔记，还更新相关笔记添加反向链接

    Args:
        new_note_path: 新创建的笔记路径
        relations: 相关笔记列表
    """
    if not relations:
        return

    # 获取新笔记的标题
    new_title = new_note_path.stem

    # 为新笔记生成并添加正向链接
    forward_links = generate_wikilinks(relations)
    if forward_links.strip():
        update_note_with_links(new_note_path, forward_links)

    # 为相关笔记添加反向链接到新笔记
    for note in relations:
        # 从source路径获取实际的笔记文件路径
        try:
            # note['source'] 应该类似 "Inbox/xxx.md" 这样的相对路径
            related_path = Path.home() / "Documents" / "ZhiweiVault" / note['source']
            if related_path.exists():
                # 为相关笔记添加指向新笔记的链接
                reverse_link = f" [[{new_title}]]"
                update_note_with_links(related_path, reverse_link)
            else:
                print(f"⚠️ 相关笔记路径不存在: {related_path}")
        except Exception as e:
            print(f"⚠️ 更新相关笔记失败: {e}")


def link_new_note(note_path: Path, note_content: str) -> list:
    """
    为新笔记创建自动关联链接的主函数

    Args:
        note_path: 新笔记路径
        note_content: 新笔记内容

    Returns:
        list: 确认的关联笔记列表
    """
    print(f"🔗 开始为笔记建立自动关联: {note_path.name}")

    # 步骤1: 使用 ChromaDB 检索相似笔记
    print("🔍 步骤1: 检索相似笔记...")
    candidates = find_related_notes(note_content, top_k=5)

    if not candidates:
        print("📝 未找到相似笔记，无需关联")
        return []

    # 步骤2: 调用百炼确认真正相关的内容
    print("🧠 步骤2: 调用百炼确认关联...")
    confirmed_relations = confirm_relations(note_content, candidates)

    if not confirmed_relations:
        print("📝 未确认到真正相关的笔记")
        return []

    # 步骤3: 创建双向链接
    print("🔄 步骤3: 创建双向链接...")
    create_bidirectional_links(note_path, confirmed_relations)

    print(f"✅ 自动关联完成，建立了 {len(confirmed_relations)} 个关联")
    return confirmed_relations


def main():
    """主入口 - 用于测试"""
    print("🧪 Obsidian Linker 测试模式")

    # 示例使用
    # note_path = Path("/path/to/new/note.md")
    # note_content = "your note content here"
    # relations = link_new_note(note_path, note_content)


if __name__ == "__main__":
    main()
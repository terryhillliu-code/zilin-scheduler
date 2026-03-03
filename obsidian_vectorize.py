#!/usr/bin/env python3
"""
Obsidian 笔记自动向量化入 ChromaDB
Phase 2: 实现 Obsidian 笔记与 ChromaDB 的自动同步
"""

import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime
import urllib.request
import time

# 配置
OBSIDIAN_VAULT_PATH = Path.home() / "Documents" / "ZhiweiVault"  # 根据实际路径调整
CHROMA_PATH = "/root/downloads/knowledge-library/chromadb"
CHROMA_COLLECTION = "knowledge_base"
CONTAINER_NAME = "clawdbot"

# DashScope API 配置（用于云端 embedding）
DASHSCOPE_API_KEY = "sk-70d377bd717b4f8abe405bff72427147"
DASHSCOPE_API_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings"

# 向量数据库连接
def get_chromadb_client():
    """获取 ChromaDB 客户端"""
    import chromadb
    client = chromadb.PersistentClient(path=CHROMA_PATH)
    return client


def scan_vault(vault_path: Path = OBSIDIAN_VAULT_PATH) -> list:
    """
    扫描 Obsidian Vault 获取所有 .md 文件

    Args:
        vault_path: Obsidian 库路径

    Returns:
        list: 包含 .md 文件路径的列表
    """
    md_files = []

    if not vault_path.exists():
        print(f"⚠️  Obsidian Vault 路径不存在: {vault_path}")
        return md_files

    for md_file in vault_path.rglob("*.md"):
        if md_file.is_file():
            md_files.append(md_file)

    print(f"🔍 扫描到 {len(md_files)} 个 Markdown 文件")
    return md_files


def compute_hash(content: str) -> str:
    """
    计算文件内容的 hash，用于增量检测

    Args:
        content: 文件内容

    Returns:
        str: SHA256 hash 值
    """
    return hashlib.sha256(content.encode('utf-8')).hexdigest()


def vectorize_note(note_path: Path) -> tuple[list, dict]:
    """
    调用百炼 Embedding 生成向量

    Args:
        note_path: 笔记文件路径

    Returns:
        tuple: (embeddings, metadata)
    """
    try:
        # 读取笔记内容
        content = note_path.read_text(encoding='utf-8', errors='ignore')

        # 如果内容为空或太短，跳过
        if not content.strip() or len(content.strip()) < 10:
            print(f"⚠️  笔记内容为空或过短，跳过: {note_path}")
            return [], {}

        # 生成文档 ID
        doc_id = f"obsidian_{compute_hash(str(note_path))[:16]}"

        # 文档元数据
        metadata = {
            "doc_id": doc_id,
            "title": note_path.stem,
            "category": "obsidian_note",
            "source": str(note_path.relative_to(OBSIDIAN_VAULT_PATH)),
            "created_at": str(datetime.fromtimestamp(note_path.stat().st_ctime)),
            "modified_at": str(datetime.fromtimestamp(note_path.stat().st_mtime)),
            "file_path": str(note_path)
        }

        # 调用百炼 Embedding API
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

        with urllib.request.urlopen(embed_req, timeout=30) as resp:
            embed_result = json.loads(resp.read())
            embedding = embed_result["data"][0]["embedding"]
            print(f"   📡 {note_path.name} embedding 成功, 维度: {len(embedding)}")

        return [embedding], metadata

    except Exception as e:
        print(f"❌ 生成笔记向量失败 {note_path}: {e}")
        return [], {}


def get_existing_docs_in_chroma() -> set:
    """
    获取 ChromaDB 中已存在的文档 ID 集合

    Returns:
        set: 已存在文档 ID 的集合
    """
    try:
        cmd = f"""
import chromadb
import json

client = chromadb.PersistentClient(path="{CHROMA_PATH}")
collection = client.get_or_create_collection("{CHROMA_COLLECTION}")

# 获取所有文档的元数据
results = collection.peek(limit=10000)  # 假设不超过10000个文档

existing_ids = set()
for meta in results.get("metadatas", []):
    if "doc_id" in meta:
        existing_ids.add(meta["doc_id"])
    elif "book_id" in meta:
        existing_ids.add(meta["book_id"])

print(json.dumps(list(existing_ids)))
"""

        result = subprocess.run(
            ["docker", "exec", CONTAINER_NAME, "python3", "-c", cmd],
            capture_output=True, text=True, timeout=30
        )

        if result.returncode == 0:
            existing_ids = set(json.loads(result.stdout.strip()))
            print(f"📊 ChromaDB 中已有 {len(existing_ids)} 个文档")
            return existing_ids
        else:
            print(f"⚠️  获取现有文档失败: {result.stderr[:200]}")
            return set()

    except Exception as e:
        print(f"⚠️  获取现有文档时出错: {e}")
        return set()


def sync_to_chromadb(vault_path: Path = OBSIDIAN_VAULT_PATH):
    """
    主函数，增量同步 Obsidian 笔记到 ChromaDB

    Args:
        vault_path: Obsidian 库路径
    """
    print("🔄 开始同步 Obsidian 笔记到 ChromaDB...")

    # 获取已存在的文档 ID
    existing_doc_ids = get_existing_docs_in_chroma()

    # 扫描 Vault 中的 Markdown 文件
    md_files = scan_vault(vault_path)

    # 过滤出需要处理的文件
    files_to_process = []
    for md_file in md_files:
        doc_id = f"obsidian_{compute_hash(str(md_file))[:16]}"
        if doc_id not in existing_doc_ids:
            files_to_process.append((md_file, doc_id))

    print(f"📝 需要处理 {len(files_to_process)} 个新/更新的笔记")

    if not files_to_process:
        print("✅ 所有笔记均已同步，无需处理")
        return

    # 批量处理文件
    processed_count = 0
    for note_path, expected_doc_id in files_to_process:
        try:
            print(f"📦 处理: {note_path.name}")

            # 生成向量
            embeddings, metadata = vectorize_note(note_path)

            if not embeddings:
                print(f"   ⚠️  未能生成向量，跳过: {note_path.name}")
                continue

            # 保存到 ChromaDB
            embed_str = json.dumps(embeddings[0])
            content = note_path.read_text(encoding='utf-8', errors='ignore')
            # 对内容进行转义，使其可以在Python代码中使用
            escaped_content = content.replace('\\', '\\\\').replace('"', '\\"').replace("'", "\\'").replace('\n', '\\n')

            cmd = f"""
import chromadb
import json

client = chromadb.PersistentClient(path="{CHROMA_PATH}")
collection = client.get_or_create_collection("{CHROMA_COLLECTION}")

# 添加文档到集合
collection.add(
    embeddings=[{embed_str}],
    metadatas=[{json.dumps(metadata, ensure_ascii=False)}],
    documents=["{escaped_content[:4000]}"],  # 限制长度
    ids=["{expected_doc_id}"]
)

print("Added: {expected_doc_id}")
"""

            result = subprocess.run(
                ["docker", "exec", CONTAINER_NAME, "python3", "-c", cmd],
                capture_output=True, text=True, timeout=30
            )

            if result.returncode == 0:
                print(f"   ✅ 成功添加: {note_path.name}")
                processed_count += 1
            else:
                print(f"   ❌ 添加失败: {result.stderr[:200]}")

            # 避免 API 请求过于频繁
            time.sleep(0.5)

        except Exception as e:
            print(f"   ❌ 处理失败 {note_path.name}: {e}")

    print(f"\n🎉 同步完成! 新增/更新了 {processed_count} 个笔记到 ChromaDB")
    return processed_count


def main():
    """主入口"""
    print("🚀 Obsidian 笔记向量化同步工具")
    print(f"Vault 路径: {OBSIDIAN_VAULT_PATH}")
    print(f"ChromaDB 路径: {CHROMA_PATH}")

    sync_to_chromadb()


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
研报处理端到端流水线 (T-410, T-411)
负责扫描 reports 目录中的 PDF，调用 pdf_processor 进行摘要和提纯，
然后计算 Embedding 并存入 ChromaDB，完成 RAG 知识入库。
"""

import os
import json
import hashlib
import tempfile
import urllib.request
import subprocess
import logging
from pathlib import Path
from datetime import datetime

# 引入已有的模块
from pdf_processor import process_research_report
from obsidian_vectorize import DASHSCOPE_API_URL, DASHSCOPE_API_KEY, CHROMA_PATH, CHROMA_COLLECTION, CONTAINER_NAME, get_existing_docs_in_chroma

logger = logging.getLogger(__name__)

REPORTS_DIR = Path.home() / "knowledge-inbox" / "reports"

def compute_hash(filepath: Path) -> str:
    """计算文件 hash 作为文档 ID"""
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        buf = f.read(65536)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(65536)
    return hasher.hexdigest()

def vectorize_text(text: str) -> list:
    """调用百炼 API 获取 Embedding 向量"""
    try:
        embed_data = json.dumps({
            "model": "text-embedding-v3",
            "input": text[:4096],  # 截断以符合 API 限制
            "dimension": 1024
        }).encode("utf-8")

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
            return [embedding]
    except Exception as e:
        logger.error(f"Embedding 生成失败: {e}")
        return []

def scan_and_process_reports():
    """主扫描循环"""
    if not REPORTS_DIR.exists():
        logger.info(f"研报目录不存在，跳过: {REPORTS_DIR}")
        return
        
    logger.info("=== 启动研报流水线扫描 ===")
    existing_ids = get_existing_docs_in_chroma()
    
    # 获取所有的 pdf，包括二级目录（机构目录）
    pdf_files = list(REPORTS_DIR.rglob("*.pdf"))
    logger.info(f"发现 {len(pdf_files)} 份研报文件")
    
    processed_count = 0
    
    for pdf_path in pdf_files:
        doc_id = f"report_{compute_hash(pdf_path)[:16]}"
        
        if doc_id in existing_ids:
            logger.debug(f"研报已存在于知识库中，跳过: {pdf_path.name}")
            continue
            
        logger.info(f"--- 开始处理新研报: {pdf_path.name} ---")
        try:
            # 1. 解析与提纯
            extracted_data = process_research_report(pdf_path)
            markdown_content = extracted_data["markdown"]
            meta_json = extracted_data["metadata"]
            
            # 2. 生成 Embedding
            # 将摘要和关键洞察拼接到一段文字供 Embedding 检索使用
            focus_text = f"【标题】{pdf_path.name}\n" \
                         f"【摘要】{meta_json.get('summary', '')}\n" \
                         f"【指标】{', '.join(meta_json.get('key_metrics', []))}\n" \
                         f"【洞察】{', '.join(meta_json.get('key_insights', []))}"
            
            embeddings = vectorize_text(focus_text)
            if not embeddings:
                logger.warning(f"未能生成向量，跳过 {pdf_path.name}")
                continue
                
            # 3. 组织 Metadata
            # ChromaDB 的 metadata 值必须是 string/int/float/bool，不能是复杂对象
            final_metadata = {
                "category": "research_report",
                "doc_id": doc_id,
                "title": pdf_path.stem,
                "institution": pdf_path.parent.name if pdf_path.parent != REPORTS_DIR else "Unclassified",
                "summary": str(meta_json.get("summary", ""))[:500],
                "metrics": ", ".join(meta_json.get("key_metrics", []))[:200],
                "created_at": str(datetime.fromtimestamp(pdf_path.stat().st_ctime)),
                "file_path": str(pdf_path)
            }
            
            # 4. 写入 ChromaDB (通过容器层安全写入)
            embed_str = json.dumps(embeddings[0])
            escaped_content = markdown_content.replace('\\', '\\\\').replace('"', '\\"').replace("'", "\\'").replace('\n', '\\n')
            
            # 截取前 8000 字符作为 Document 本文存储
            doc_to_store = escaped_content[:8000]
            
            cmd = f"""
import chromadb
import json

client = chromadb.PersistentClient(path="{CHROMA_PATH}")
collection = client.get_or_create_collection("{CHROMA_COLLECTION}")

collection.add(
    embeddings=[{embed_str}],
    metadatas=[{json.dumps(final_metadata, ensure_ascii=False)}],
    documents=["{doc_to_store}"],
    ids=["{doc_id}"]
)
print("SUCCESS")
"""
            result = subprocess.run(
                ["docker", "exec", CONTAINER_NAME, "python3", "-c", cmd],
                capture_output=True, text=True, timeout=60
            )

            if result.returncode == 0 and "SUCCESS" in result.stdout:
                logger.info(f"✅ 入库成功: {pdf_path.name}")
                processed_count += 1
            else:
                logger.error(f"❌ 入库失败: {result.stderr}")
                
        except Exception as e:
            logger.error(f"处理研报 {pdf_path.name} 时发生致命错误: {e}")

    logger.info(f"=== 流水线完成，本次新入库 {processed_count} 份研报 ===")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    scan_and_process_reports()

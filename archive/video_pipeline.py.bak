#!/usr/bin/env python3
"""
视频知识增强管线 (Phase 4b - T-404)
串联下载、转录、总结、存盘（Obsidian）及向量化（ChromaDB）。
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime

# 导入业务模块
from video_downloader import extract_audio_only
from video_processor import process_video_content
# 借用研报和 Obsidian 同步的配置/接口
from research_pipeline import compute_hash, vectorize_text
from obsidian_vectorize import (
    OBSIDIAN_VAULT_PATH, CHROMA_PATH, CHROMA_COLLECTION, CONTAINER_NAME,
    get_existing_docs_in_chroma
)

import subprocess

def run_docker_python(cmd: str):
    """辅助函数：在 Docker 容器中执行 Python 代码"""
    try:
        result = subprocess.run(
            ["docker", "exec", CONTAINER_NAME, "python3", "-c", cmd],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            return result.stderr.strip()
    except Exception as e:
        return str(e)

logger = logging.getLogger(__name__)

# 配置 Obsidian 存储路径
VIDEO_NOTES_DIR = Path(OBSIDIAN_VAULT_PATH) / "10_Knowledge_Base" / "Video_Summaries"
VIDEO_NOTES_DIR.mkdir(parents=True, exist_ok=True)

def process_single_video(url: str):
    """处理单个视频链接"""
    logger.info(f"=== 启动视频自动化处理流: {url} ===")
    
    try:
        # 1. 抓取音频
        audio_path = extract_audio_only(url)
        doc_id = f"video_{compute_hash(audio_path)[:16]}"
        
        # 2. 查重
        existing_ids = get_existing_docs_in_chroma()
        if doc_id in existing_ids:
            logger.info(f"该视频已在知识库中 (ID: {doc_id})，跳过处理。")
            return
            
        # 3. 转录与总结（深度分析模式）
        logger.info("启用深度分析模式...")
        summary_md = process_video_content(
            audio_path,
            deep_analysis=True,
            source_info=url,
            duration=""  # 时长信息可从下载器获取
        )
        
        # 4. 存入 Obsidian
        # 清理非法文件名字符
        safe_title = audio_path.stem.replace("/", "_").replace("\\", "_")[:50]
        note_path = VIDEO_NOTES_DIR / f"{safe_title}.md"
        
        with open(note_path, "w", encoding="utf-8") as f:
            f.write(f"---\ntags: #video_summary #knowledge\nsource: {url}\ndate: {datetime.now().isoformat()}\nid: {doc_id}\n---\n\n")
            f.write(summary_md)
        logger.info(f"Obsidian 笔记已生成: {note_path}")
        
        # 5. 向量化入库 ChromaDB
        # 提取核心文本块进行 Embedding
        # 假设 summary_md 包含了我们提炼的“原子级事实”
        embeddings = vectorize_text(summary_md[:4000])
        if not embeddings:
            logger.warning("未能生成向量，跳过库入库。")
            return
            
        metadata = {
            "category": "video_summary",
            "doc_id": doc_id,
            "title": audio_path.stem,
            "source_url": url,
            "created_at": str(datetime.now())
        }
        
        embed_str = json.dumps(embeddings[0])
        doc_to_store = summary_md.replace('\\', '\\\\').replace('"', '\\"').replace("'", "\\'").replace('\n', '\\n')[:8000]
        
        cmd = f"""
import chromadb
import json
client = chromadb.PersistentClient(path="{CHROMA_PATH}")
collection = client.get_or_create_collection("{CHROMA_COLLECTION}")
collection.add(
    embeddings=[{embed_str}],
    metadatas=[{json.dumps(metadata, ensure_ascii=False)}],
    documents=["{doc_to_store}"],
    ids=["{doc_id}"]
)
print("SUCCESS")
"""
        result = run_docker_python(cmd)
        if "SUCCESS" in result:
            logger.info(f"✅ 视频内容入库 ChromaDB 成功: {audio_path.name}")
        else:
            logger.error(f"❌ 向量入库失败: {result}")
            
    except Exception as e:
        logger.error(f"处理视频链接时发生错误: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    import sys
    if len(sys.argv) > 1:
        process_single_video(sys.argv[1])
    else:
        logger.warning("请提供视频 URL 作为参数。")

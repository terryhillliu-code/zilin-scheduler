#!/usr/bin/env python3
"""
OBS-002: Obsidian 笔记索引器
- 复用 zhiwei-rag 的 LanceStore 和 EmbeddingManager
- 增量检测（基于 content hash）
- 支持 Obsidian frontmatter 解析
"""

import hashlib
import json
import os
import sys
import time
from pathlib import Path
from dataclasses import asdict
from typing import Optional
from datetime import datetime

# 添加 zhiwei-rag 路径
sys.path.insert(0, str(Path.home() / "zhiwei-rag"))

from ingest.lance_store import LanceStore, Document, call_embed_service
from ingest.semantic_splitter import SemanticSplitter
from retrieve.embedding_manager import get_embedding_manager


# ==================== 配置 ====================

VAULT_PATH = Path.home() / "Documents" / "ZhiweiVault"
TRACKER_FILE = Path.home() / ".obsidian_index_tracker.json"
BATCH_SIZE = 50  # 每批处理数量


# ==================== 工具函数 ====================

def compute_content_hash(content: str) -> str:
    """计算内容 hash"""
    return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]


def load_tracker() -> dict:
    """加载索引追踪记录"""
    if not TRACKER_FILE.exists():
        return {"files": {}, "last_scan": None}

    try:
        return json.loads(TRACKER_FILE.read_text())
    except Exception:
        return {"files": {}, "last_scan": None}


def save_tracker(tracker: dict):
    """保存索引追踪记录"""
    tracker["last_scan"] = datetime.now().isoformat()
    TRACKER_FILE.write_text(json.dumps(tracker, indent=2, ensure_ascii=False))


# ==================== 主类 ====================

class ObsidianIndexer:
    """
    Obsidian 笔记索引器

    使用方式:
        indexer = ObsidianIndexer()
        indexer.scan_and_index()  # 全量扫描+增量索引
        indexer.index_note(path)  # 索引单个笔记
        indexer.remove_note(path) # 删除笔记索引
    """

    def __init__(
        self,
        vault_path: Path = VAULT_PATH,
        db_path: str = "~/zhiwei-rag/data/lance_db"
    ):
        self.vault_path = vault_path
        self.store = LanceStore(db_path=db_path)
        self.splitter = SemanticSplitter(max_chunk_tokens=480)
        self.tracker = load_tracker()

    def scan_and_index(self, force: bool = False) -> dict:
        """
        扫描 Vault 并索引所有变更的笔记

        Args:
            force: 是否强制重新索引所有文件

        Returns:
            统计信息 {"indexed": N, "skipped": N, "failed": N, "removed": N}
        """
        print(f"🔍 扫描 Obsidian Vault: {self.vault_path}")
        start_time = time.time()

        stats = {"indexed": 0, "skipped": 0, "failed": 0, "removed": 0}

        # 1. 收集所有 .md 文件
        md_files = {}
        for md_file in self.vault_path.rglob("*.md"):
            # 跳过 .obsidian 目录
            if ".obsidian" in str(md_file):
                continue
            rel_path = str(md_file.relative_to(self.vault_path))
            md_files[rel_path] = md_file

        print(f"   发现 {len(md_files)} 个 Markdown 文件")

        # 2. 检测需要删除的文件（在 tracker 中但不在文件系统中）
        tracker_files = set(self.tracker.get("files", {}).keys())
        current_files = set(md_files.keys())

        deleted_files = tracker_files - current_files
        for rel_path in deleted_files:
            self._remove_from_index(rel_path)
            stats["removed"] += 1

        # 3. 增量检测
        files_to_index = []
        for rel_path, md_file in md_files.items():
            if force:
                files_to_index.append((rel_path, md_file))
                continue

            # 读取内容并计算 hash
            try:
                content = md_file.read_text(encoding='utf-8', errors='ignore')
                content_hash = compute_content_hash(content)

                # 检查是否需要索引
                tracked = self.tracker.get("files", {}).get(rel_path, {})
                if tracked.get("hash") != content_hash:
                    files_to_index.append((rel_path, md_file))
                else:
                    stats["skipped"] += 1
            except Exception as e:
                print(f"   ⚠️ 读取文件失败 {rel_path}: {e}")
                stats["failed"] += 1

        print(f"   需要索引: {len(files_to_index)} 个文件")
        print(f"   已跳过（未变更）: {stats['skipped']} 个文件")

        # 4. 批量索引
        for i, (rel_path, md_file) in enumerate(files_to_index):
            try:
                self.index_note(md_file, rel_path)
                stats["indexed"] += 1

                if (i + 1) % 100 == 0:
                    print(f"   进度: {i + 1}/{len(files_to_index)}")

            except Exception as e:
                print(f"   ❌ 索引失败 {rel_path}: {e}")
                stats["failed"] += 1

        # 5. 保存 tracker
        save_tracker(self.tracker)

        elapsed = time.time() - start_time
        print(f"\n✅ 索引完成!")
        print(f"   - 新增/更新: {stats['indexed']}")
        print(f"   - 跳过（未变更）: {stats['skipped']}")
        print(f"   - 删除: {stats['removed']}")
        print(f"   - 失败: {stats['failed']}")
        print(f"   - 耗时: {elapsed:.1f}s")

        return stats

    def index_note(self, note_path: Path, rel_path: Optional[str] = None) -> bool:
        """
        索引单个笔记

        Args:
            note_path: 笔记文件路径
            rel_path: 相对于 Vault 的路径（可选，用于 tracker）

        Returns:
            是否成功
        """
        if rel_path is None:
            rel_path = str(note_path.relative_to(self.vault_path))

        # 1. 读取内容
        content = note_path.read_text(encoding='utf-8', errors='ignore')

        # 内容为空或太短，跳过
        if not content.strip() or len(content.strip()) < 10:
            return False

        # 2. 删除旧索引
        self.store.delete_by_source(str(note_path))

        # 3. 切分文档
        chunks = self.splitter.split_file(note_path)

        if not chunks:
            return False

        # 4. 批量获取向量
        texts = [chunk.text for chunk in chunks]

        # 优先使用常驻 Embedding 服务
        embeddings = call_embed_service(texts)

        if embeddings is None:
            # 降级到本地
            print(f"   [Indexer] 使用本地 Embedding: {note_path.name}", file=sys.stderr)
            embedder = get_embedding_manager()
            embeddings = embedder.encode(texts).tolist()

        # 5. 构建 Document 并写入
        docs = []
        for i, (chunk, vector) in enumerate(zip(chunks, embeddings)):
            doc = Document(
                id=f"obsidian:{rel_path}:{i}",
                text=chunk.text,
                raw_text=chunk.raw_text,
                source=str(note_path),
                filename=chunk.filename,
                h1=chunk.h1,
                h2=chunk.h2,
                category=chunk.metadata.get("category", "obsidian"),
                tags=chunk.metadata.get("tags", ""),
                char_count=chunk.char_count,
                vector=vector
            )
            docs.append(doc)

        self.store.add_documents(docs, batch_size=BATCH_SIZE)

        # 6. 更新 tracker
        content_hash = compute_content_hash(content)
        self.tracker.setdefault("files", {})[rel_path] = {
            "hash": content_hash,
            "chunks": len(docs),
            "indexed_at": datetime.now().isoformat()
        }

        print(f"   📝 索引: {note_path.name} ({len(docs)} chunks)")
        return True

    def remove_note(self, note_path: Path):
        """
        删除笔记索引

        Args:
            note_path: 笔记文件路径
        """
        rel_path = str(note_path.relative_to(self.vault_path))
        self._remove_from_index(rel_path)

    def _remove_from_index(self, rel_path: str):
        """从索引中删除（内部方法）"""
        # 从 LanceDB 删除
        source_prefix = str(self.vault_path / rel_path)
        # LanceDB 不支持前缀删除，需要逐个删除
        # 这里我们删除整个 source 的记录
        self.store.delete_by_source(source_prefix)

        # 从 tracker 删除
        if rel_path in self.tracker.get("files", {}):
            del self.tracker["files"][rel_path]

        print(f"   🗑️ 删除索引: {rel_path}")

    def get_stats(self) -> dict:
        """获取索引统计"""
        tracker_files = self.tracker.get("files", {})
        total_chunks = sum(f.get("chunks", 0) for f in tracker_files.values())

        return {
            "indexed_files": len(tracker_files),
            "total_chunks": total_chunks,
            "last_scan": self.tracker.get("last_scan"),
            "lance_db_count": self.store.count()
        }


# ==================== CLI ====================

def main():
    """命令行入口"""
    import argparse

    parser = argparse.ArgumentParser(description="Obsidian 笔记索引器")
    parser.add_argument("--force", "-f", action="store_true", help="强制重新索引所有文件")
    parser.add_argument("--stats", action="store_true", help="显示索引统计")
    parser.add_argument("--file", type=str, help="索引单个文件")
    parser.add_argument("--vault", type=str, help="指定 Vault 路径")

    args = parser.parse_args()

    vault_path = Path(args.vault) if args.vault else VAULT_PATH

    if not vault_path.exists():
        print(f"❌ Vault 路径不存在: {vault_path}")
        return

    indexer = ObsidianIndexer(vault_path=vault_path)

    if args.stats:
        stats = indexer.get_stats()
        print("📊 索引统计:")
        print(f"   - 已索引文件: {stats['indexed_files']}")
        print(f"   - 总 chunk 数: {stats['total_chunks']}")
        print(f"   - LanceDB 文档数: {stats['lance_db_count']}")
        print(f"   - 最后扫描: {stats['last_scan'] or '从未'}")
        return

    if args.file:
        file_path = Path(args.file)
        if not file_path.is_absolute():
            file_path = vault_path / args.file

        if not file_path.exists():
            print(f"❌ 文件不存在: {file_path}")
            return

        success = indexer.index_note(file_path)
        if success:
            save_tracker(indexer.tracker)
            print("✅ 索引成功")
        else:
            print("❌ 索引失败")
        return

    # 全量扫描
    indexer.scan_and_index(force=args.force)


if __name__ == "__main__":
    main()
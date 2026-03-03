#!/usr/bin/env python3
"""
Obsidian 文件监听器
Phase 4: 实现 Obsidian Vault 文件的实时监听与同步
"""

import time
import json
import urllib.request
import subprocess
from pathlib import Path
from threading import Timer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


# 配置
OBSIDIAN_VAULT_PATH = Path.home() / "Documents" / "ZhiweiVault"
CHROMA_PATH = "/root/downloads/knowledge-library/chromadb"
CHROMA_COLLECTION = "knowledge_base"
CONTAINER_NAME = "clawdbot"

# DashScope API 配置（用于云端 embedding）
DASHSCOPE_API_KEY = "sk-70d377bd717b4f8abe405bff72427147"
DASHSCOPE_API_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings"


class ObsidianEventHandler(FileSystemEventHandler):
    """
    Obsidian 文件事件处理器
    """
    def __init__(self):
        super().__init__()
        self.debounce_timers = {}  # 防抖计时器

    def debounce(self, key, func, delay=2.0):
        """
        防抖处理：避免连续修改触发多次同步
        """
        if key in self.debounce_timers:
            self.debounce_timers[key].cancel()

        timer = Timer(delay, func)
        self.debounce_timers[key] = timer
        timer.start()

    def on_created(self, event):
        """
        文件创建事件处理
        """
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.suffix.lower() == '.md':
            print(f"🆕 检测到新文件: {file_path}")
            # 防抖处理
            key = f"create_{file_path}"
            self.debounce(key, lambda: self._handle_file_created(file_path))

    def on_modified(self, event):
        """
        文件修改事件处理
        """
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.suffix.lower() == '.md':
            print(f"✏️ 检测到文件修改: {file_path}")
            # 防抖处理
            key = f"modify_{file_path}"
            self.debounce(key, lambda: self._handle_file_modified(file_path))

    def on_deleted(self, event):
        """
        文件删除事件处理
        """
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.suffix.lower() == '.md':
            print(f"🗑️ 检测到文件删除: {file_path}")
            # 防抖处理
            key = f"delete_{file_path}"
            self.debounce(key, lambda: self._handle_file_deleted(file_path))

    def _generate_doc_id(self, file_path: Path) -> str:
        """
        生成文档 ID
        """
        import hashlib
        file_str = str(file_path.relative_to(OBSIDIAN_VAULT_PATH))
        return f"obsidian_{hashlib.sha256(file_str.encode()).hexdigest()[:16]}"

    def _vectorize_note(self, file_path: Path) -> tuple[list, dict]:
        """
        向量化单个笔记
        """
        try:
            # 读取笔记内容
            content = file_path.read_text(encoding='utf-8', errors='ignore')

            # 如果内容为空或太短，跳过
            if not content.strip() or len(content.strip()) < 10:
                print(f"⚠️  笔记内容为空或过短，跳过: {file_path}")
                return [], {}

            # 生成文档 ID
            doc_id = self._generate_doc_id(file_path)

            # 文档元数据
            metadata = {
                "doc_id": doc_id,
                "title": file_path.stem,
                "category": "obsidian_note",
                "source": str(file_path.relative_to(OBSIDIAN_VAULT_PATH)),
                "created_at": str(file_path.stat().st_ctime),
                "modified_at": str(file_path.stat().st_mtime),
                "file_path": str(file_path)
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
                print(f"   📡 {file_path.name} embedding 成功, 维度: {len(embedding)}")

            return [embedding], metadata

        except Exception as e:
            print(f"❌ 生成笔记向量失败 {file_path}: {e}")
            return [], {}

    def _handle_file_created(self, file_path: Path):
        """
        处理文件创建
        """
        print(f"🔄 同步新文件到 ChromaDB: {file_path.name}")

        # 生成向量
        embeddings, metadata = self._vectorize_note(file_path)

        if not embeddings:
            print(f"   ⚠️  未能生成向量，跳过: {file_path.name}")
            return

        # 保存到 ChromaDB
        embed_str = json.dumps(embeddings[0])
        content = file_path.read_text(encoding='utf-8', errors='ignore')
        # 对内容进行转义，使其可以在Python代码中使用
        escaped_content = content.replace('\\', '\\\\').replace('"', '\\"').replace("'", "\\'").replace('\n', '\\n')
        doc_id = metadata['doc_id']

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
    ids=["{doc_id}"]
)

print("Added: {doc_id}")
"""

        result = subprocess.run(
            ["docker", "exec", CONTAINER_NAME, "python3", "-c", cmd],
            capture_output=True, text=True, timeout=30
        )

        if result.returncode == 0:
            print(f"   ✅ 成功添加: {file_path.name}")

            # 调用自动链接功能（异步）
            try:
                from obsidian_linker import link_new_note
                import threading
                link_thread = threading.Thread(target=lambda: link_new_note(file_path, content), daemon=True)
                link_thread.start()
            except ImportError:
                print("⚠️ 未能导入 obsidian_linker，跳过自动链接")
            except Exception as e:
                print(f"❌ 自动链接过程中出错: {e}")
        else:
            print(f"   ❌ 添加失败: {result.stderr[:200]}")

    def _handle_file_modified(self, file_path: Path):
        """
        处理文件修改
        """
        print(f"🔄 更新修改文件到 ChromaDB: {file_path.name}")

        # 删除旧记录（如果存在）
        doc_id = self._generate_doc_id(file_path)
        self._remove_from_chromadb(doc_id)

        # 添加新记录
        self._handle_file_created(file_path)

    def _handle_file_deleted(self, file_path: Path):
        """
        处理文件删除
        """
        print(f"🔄 从 ChromaDB 删除文件: {file_path.name}")

        # 生成对应的 doc_id
        doc_id = self._generate_doc_id(file_path)

        # 从 ChromaDB 中移除
        self._remove_from_chromadb(doc_id)

    def _remove_from_chromadb(self, doc_id: str):
        """
        从 ChromaDB 中移除文档
        """
        cmd = f"""
import chromadb
import json

client = chromadb.PersistentClient(path="{CHROMA_PATH}")
collection = client.get_or_create_collection("{CHROMA_COLLECTION}")

# 删除指定 ID 的文档
try:
    collection.delete(ids=["{doc_id}"])
    print("Deleted: {doc_id}")
except Exception as e:
    print(f"Delete error for {doc_id}: " + str(e))
"""

        result = subprocess.run(
            ["docker", "exec", CONTAINER_NAME, "python3", "-c", cmd],
            capture_output=True, text=True, timeout=30
        )

        if result.returncode == 0:
            print(f"   ✅ 从 ChromaDB 成功删除: {doc_id}")
        else:
            print(f"   ❌ 删除失败: {result.stderr[:200]}")


def start_watching(vault_path: Path = OBSIDIAN_VAULT_PATH):
    """
    启动监听主函数
    """
    print(f"👀 开始监听 Obsidian Vault: {vault_path}")

    event_handler = ObsidianEventHandler()
    observer = Observer()
    observer.schedule(event_handler, str(vault_path), recursive=True)

    observer.start()
    print("✅ Obsidian 监听器已启动")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 监听器已停止")
        observer.stop()

    observer.join()


def main():
    """主入口"""
    print("🚀 Obsidian 文件监听器")
    print(f"Vault 路径: {OBSIDIAN_VAULT_PATH}")
    print(f"ChromaDB 路径: {CHROMA_PATH}")

    start_watching()


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
OBS-002: Obsidian 文件监听器（重构版）
- 使用新的 ObsidianIndexer
- 复用 watchdog 监听框架
- 支持增量更新

启动方式:
    # 前台运行（测试）
    python obsidian_watcher.py

    # launchd 服务（后台）
    # 配置文件: ~/Library/LaunchAgents/com.zhiwei.obsidian.plist
"""

import os
import sys
import time
import signal
import subprocess
from pathlib import Path
from threading import Timer

# 使用 zhiwei-rag 的 Python 环境
RAG_VENV = Path.home() / "zhiwei-rag" / "venv" / "bin" / "python"
INDEXER_SCRIPT = Path.home() / "zhiwei-scheduler" / "obsidian_indexer.py"

# 配置
OBSIDIAN_VAULT_PATH = Path.home() / "Documents" / "ZhiweiVault"


class ObsidianEventHandler:
    """
    Obsidian 文件事件处理器
    通过子进程调用 obsidian_indexer.py
    """

    def __init__(self):
        self.debounce_timers = {}
        self.python = str(RAG_VENV) if RAG_VENV.exists() else sys.executable

    def debounce(self, key, func, delay=2.0):
        """防抖处理"""
        if key in self.debounce_timers:
            self.debounce_timers[key].cancel()

        timer = Timer(delay, func)
        self.debounce_timers[key] = timer
        timer.start()

    def on_created(self, event):
        """文件创建事件"""
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.suffix.lower() == '.md':
            print(f"🆕 检测到新文件: {file_path.name}")
            key = f"create_{file_path}"
            self.debounce(key, lambda: self._index_file(file_path))

    def on_modified(self, event):
        """文件修改事件"""
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.suffix.lower() == '.md':
            print(f"✏️ 检测到文件修改: {file_path.name}")
            key = f"modify_{file_path}"
            self.debounce(key, lambda: self._index_file(file_path))

    def on_deleted(self, event):
        """文件删除事件"""
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if file_path.suffix.lower() == '.md':
            print(f"🗑️ 检测到文件删除: {file_path.name}")
            key = f"delete_{file_path}"
            self.debounce(key, lambda: self._remove_file(file_path))

    def _index_file(self, file_path: Path):
        """索引单个文件"""
        try:
            rel_path = file_path.relative_to(OBSIDIAN_VAULT_PATH)
            result = subprocess.run(
                [self.python, str(INDEXER_SCRIPT), "--file", str(file_path)],
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode == 0:
                print(f"   ✅ 索引成功: {file_path.name}")
            else:
                print(f"   ❌ 索引失败: {result.stderr[:200]}")

        except Exception as e:
            print(f"   ❌ 索引异常: {e}")

    def _remove_file(self, file_path: Path):
        """删除文件索引"""
        try:
            # 从 LanceDB 删除需要使用 indexer 的 remove_note 方法
            # 这里我们通过子进程调用 Python 代码
            code = f'''
import sys
sys.path.insert(0, "{Path.home() / "zhiwei-rag"}")
from obsidian_indexer import ObsidianIndexer
from pathlib import Path

indexer = ObsidianIndexer()
indexer.remove_note(Path("{file_path}"))
'''

            result = subprocess.run(
                [self.python, "-c", code],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                print(f"   ✅ 删除索引成功: {file_path.name}")
            else:
                print(f"   ⚠️ 删除索引失败: {result.stderr[:200]}")

        except Exception as e:
            print(f"   ❌ 删除索引异常: {e}")


def start_watching(vault_path: Path = OBSIDIAN_VAULT_PATH):
    """启动监听"""
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

    print(f"👀 开始监听 Obsidian Vault: {vault_path}")

    # 包装 Event Handler
    class EventHandlerWrapper(FileSystemEventHandler):
        def __init__(self, handler):
            self.handler = handler

        def on_created(self, event):
            self.handler.on_created(event)

        def on_modified(self, event):
            self.handler.on_modified(event)

        def on_deleted(self, event):
            self.handler.on_deleted(event)

    event_handler = ObsidianEventHandler()
    wrapper = EventHandlerWrapper(event_handler)

    observer = Observer()
    observer.schedule(wrapper, str(vault_path), recursive=True)

    observer.start()
    print("✅ Obsidian 监听器已启动")

    # 信号处理
    def signal_handler(signum, frame):
        print("\n🛑 收到停止信号")
        observer.stop()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        while observer.is_alive():
            observer.join(1)
    except KeyboardInterrupt:
        print("\n🛑 监听器已停止")
        observer.stop()

    observer.join()


def main():
    """主入口"""
    print("=" * 60)
    print("🚀 Obsidian 文件监听器 (OBS-002)")
    print("=" * 60)
    print(f"Vault 路径: {OBSIDIAN_VAULT_PATH}")
    print(f"Python: {RAG_VENV if RAG_VENV.exists() else sys.executable}")
    print(f"索引器: {INDEXER_SCRIPT}")
    print()

    if not OBSIDIAN_VAULT_PATH.exists():
        print(f"❌ Vault 路径不存在: {OBSIDIAN_VAULT_PATH}")
        return

    start_watching()


if __name__ == "__main__":
    main()
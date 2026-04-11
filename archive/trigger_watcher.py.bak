#!/usr/bin/env python3
"""
触发器监听器 (Trigger Watcher)
功能：监听 triggers/ 目录，检测到触发文件后立即执行对应任务

用法：
  1. 创建触发文件: touch ~/zhiwei-scheduler/triggers/tanwei.run
  2. 调度器检测到后自动执行探微任务
  3. 执行完成后自动删除触发文件
"""

import os
import sys
import time
import threading
import subprocess
from pathlib import Path
from datetime import datetime

# V2-103: 导入 LLM 客户端替代 OpenClaw
sys.path.insert(0, str(Path.home() / "zhiwei-bot"))
try:
    from llm_client import llm_client
    LLM_CLIENT_AVAILABLE = True
except ImportError:
    LLM_CLIENT_AVAILABLE = False
    llm_client = None

TRIGGER_DIR = Path(__file__).parent / "triggers"

# 触发器配置
TRIGGER_JOBS = {
    "tanwei.run": {
        "agent": "researcher",
        "prompt": "执行一次即时信息采集任务",
        "job_name": "manual_brief"
    },
    "arxiv.run": {
        "agent": "researcher",
        "prompt": "执行 arxiv 论文采集",
        "job_name": "job_arxiv"
    },
    "system.run": {
        "agent": "operator",
        "prompt": "执行系统巡检",
        "job_name": "job_system_check"
    }
}

def call_agent(agent_id: str, message: str, timeout: int = 300) -> tuple[bool, str]:
    """
    调用 LLM Agent（直连百炼 API，V2-103 重构）
    """
    if LLM_CLIENT_AVAILABLE and llm_client:
        try:
            success, content = llm_client.call(agent_id, message, timeout=timeout)
            if success:
                return True, content
            else:
                return False, content
        except Exception as e:
            return False, str(e)

    # 降级：返回错误
    return False, "LLM 客户端不可用"


def execute_trigger(trigger_file: str):
    """执行触发器对应的任务"""
    trigger_name = Path(trigger_file).name
    config = TRIGGER_JOBS.get(trigger_name)

    if not config:
        print(f"⚠️  未知的触发器: {trigger_name}")
        return False

    print(f"🔥 检测到触发器: {trigger_name}")
    print(f"   Agent: {config['agent']}")
    print(f"   任务: {config['job_name']}")

    # 执行任务
    success, result = call_agent(config['agent'], config['prompt'])

    if success:
        print(f"✅ 触发任务完成: {config['job_name']}")

        # 删除触发文件
        trigger_path = TRIGGER_DIR / trigger_file
        if trigger_path.exists():
            trigger_path.unlink()
            print(f"🗑 触发文件已清理: {trigger_file}")
    else:
        print(f"❌ 触发任务失败: {result[:200]}")

    return success


def watch_triggers(interval: int = 5):
    """监听触发器目录"""
    print(f"👀 触发器监听器已启动")
    print(f"   监控目录: {TRIGGER_DIR}")
    print(f"   轮询间隔: {interval}秒")
    print(f"   可用触发器: {list(TRIGGER_JOBS.keys())}")
    print()

    processed = set()  # 已处理的触发文件

    while True:
        try:
            # 检查触发目录
            if not TRIGGER_DIR.exists():
                TRIGGER_DIR.mkdir(exist_ok=True)

            # 遍历所有 .run 文件
            for trigger_file in TRIGGER_DIR.glob("*.run"):
                filename = trigger_file.name

                # 跳过已处理的文件
                if filename in processed:
                    continue

                # 标记为已处理
                processed.add(filename)

                # 执行触发任务
                execute_trigger(filename)

                # 从已处理集合中移除（如果文件被删除）
                if not trigger_file.exists():
                    processed.discard(filename)

        except Exception as e:
            print(f"❌ 监听错误: {e}")

        time.sleep(interval)


def start_watcher_daemon():
    """作为后台线程启动监听器"""
    watcher_thread = threading.Thread(target=watch_triggers, daemon=True)
    watcher_thread.start()
    return watcher_thread


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--daemon":
        watch_triggers()
    else:
        # 一次性检查
        print("🔍 触发器检查...")
        for trigger_file in TRIGGER_DIR.glob("*.run"):
            execute_trigger(trigger_file.name)
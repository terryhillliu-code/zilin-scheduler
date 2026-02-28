#!/usr/bin/env python3
"""
触发器监听器 (Trigger Listener)
功能：监听 triggers/ 目录，检测到触发文件后立即执行对应任务
特点：异常隔离设计，不会影响主调度器
"""

import os
import sys
import time
import threading
import subprocess
from pathlib import Path
from datetime import datetime

# 默认触发器目录
TRIGGER_DIR = Path("/Users/liufang/zhiwei-scheduler/triggers")

# 触发器配置
TRIGGER_JOBS = {
    "tanwei.run": {
        "agent": "researcher",
        "prompt": "执行一次即时信息采集任务，生成简要报告",
        "job_name": "manual_brief",
        "description": "手动触发探微采集"
    },
    "arxiv.run": {
        "agent": "researcher",
        "prompt": "执行 arxiv 论文采集",
        "job_name": "job_arxiv",
        "description": "手动触发 arxiv 采集"
    },
    "system.run": {
        "agent": "operator",
        "prompt": "执行系统巡检",
        "job_name": "job_system_check",
        "description": "手动触发系统巡检"
    },
    "morning.run": {
        "agent": "researcher",
        "prompt": "生成早报",
        "job_name": "job_morning_brief",
        "description": "手动触发早报"
    },
    "noon.run": {
        "agent": "researcher",
        "prompt": "生成午报",
        "job_name": "job_noon_brief",
        "description": "手动触发午报"
    }
}

CONTAINER = "clawdbot"

# 全局状态
_scheduler = None
_logger = None
_running = False


def init(scheduler, logger, trigger_dir: str = None):
    """初始化触发器监听器"""
    global _scheduler, _logger, TRIGGER_DIR

    _scheduler = scheduler
    _logger = logger

    if trigger_dir:
        TRIGGER_DIR = Path(trigger_dir)

    # 确保目录存在
    TRIGGER_DIR.mkdir(parents=True, exist_ok=True)


def call_agent(agent_id: str, message: str, timeout: int = 300) -> tuple[bool, str]:
    """调用 OpenClaw Agent"""
    cmd = [
        "docker", "exec", CONTAINER,
        "openclaw", "agent",
        "--agent", agent_id,
        "--message", message,
        "--json",
        "--timeout", str(timeout)
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 30)
        if result.returncode == 0:
            return True, result.stdout
        return False, result.stderr[:500]
    except Exception as e:
        return False, str(e)


def execute_trigger(trigger_file: Path):
    """执行触发器对应的任务"""
    global _scheduler, _logger

    trigger_name = trigger_file.name
    config = TRIGGER_JOBS.get(trigger_name)

    if not config:
        if _logger:
            _logger.warning(f"⚠️ 未知触发器: {trigger_name}")
        print(f"未知触发器: {trigger_name}")
        return False

    if _logger:
        _logger.info(f"🔥 检测到触发器: {trigger_name} ({config['description']})")
    print(f"🔥 触发: {trigger_name} -> {config['job_name']}")

    # 直接执行任务
    success, result = call_agent(config['agent'], config['prompt'])

    if success:
        if _logger:
            _logger.info(f"✅ 触发任务完成: {config['job_name']}")
        print(f"✅ 完成: {config['job_name']}")

        # 删除触发文件
        try:
            trigger_file.unlink()
            if _logger:
                _logger.debug(f"🗑 触发文件已清理: {trigger_name}")
        except Exception as e:
            if _logger:
                _logger.warning(f"⚠️ 清理触发文件失败: {e}")
    else:
        if _logger:
            _logger.error(f"❌ 触发任务失败: {result[:200]}")
        print(f"❌ 失败: {result[:200]}")

    return success


def watch_loop(check_interval: int = 5):
    """监听循环"""
    global _running, _logger

    processed = set()

    while _running:
        try:
            # 确保目录存在
            TRIGGER_DIR.mkdir(parents=True, exist_ok=True)

            # 遍历所有 .run 文件
            for trigger_file in TRIGGER_DIR.glob("*.run"):
                filename = trigger_file.name

                # 跳过已处理的文件
                if filename in processed:
                    continue

                # 标记为已处理
                processed.add(filename)

                # 执行触发任务
                try:
                    execute_trigger(trigger_file)
                except Exception as e:
                    if _logger:
                        _logger.error(f"❌ 执行触发器异常 [{filename}]: {e}")

                # 从已处理集合中移除（如果文件被删除）
                if not trigger_file.exists():
                    processed.discard(filename)

        except Exception as e:
            if _logger:
                _logger.error(f"❌ 监听循环异常: {e}")
            print(f"监听错误: {e}")

        time.sleep(check_interval)


def start(check_interval: int = 5):
    """启动监听器（后台线程）"""
    global _running

    if _running:
        if _logger:
            _logger.warning("⚠️ 触发器监听器已在运行")
        return

    _running = True
    thread = threading.Thread(target=watch_loop, args=(check_interval,), daemon=True)
    thread.start()

    if _logger:
        _logger.info(f"👀 触发器监听器已启动 (间隔 {check_interval}s)")
        _logger.info(f"   监控目录: {TRIGGER_DIR}")
        _logger.info(f"   支持触发器: {list(TRIGGER_JOBS.keys())}")


def stop():
    """停止监听器"""
    global _running
    _running = False


# CLI 测试
if __name__ == "__main__":
    print("🧪 触发器监听器测试模式")
    print(f"   监控目录: {TRIGGER_DIR}")

    # 一次性检查
    for trigger_file in TRIGGER_DIR.glob("*.run"):
        print(f"   发现: {trigger_file.name}")
        execute_trigger(trigger_file)
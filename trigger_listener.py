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

# V2-103: 导入 LLM 客户端替代 OpenClaw
sys.path.insert(0, str(Path.home() / "zhiwei-bot"))
try:
    from llm_client import llm_client
    LLM_CLIENT_AVAILABLE = True
except ImportError:
    LLM_CLIENT_AVAILABLE = False
    llm_client = None

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

    # 引入任务队列与消息总线
    sys.path.insert(0, str(Path.home() / "zhiwei-dev"))
    from task_store import TaskStore
    from message_bus import MessageBus
    store = TaskStore()
    mb = MessageBus()

    # 入队并标记为运行中
    task_id = store.enqueue(config['description'], backend='trigger')
    with store._connect() as conn:
        conn.execute("UPDATE tasks SET status='running', started_at=datetime('now', 'localtime') WHERE id=?", (task_id,))

    # 飞书：任务开始通知
    mb.publish("trigger_listener", "feishu_notification", f"⏳ 开始执行定制任务：{config['description']} (Task #{task_id})", {"targets": ["feishu"]})

    # 映射到 scheduler_jobs 中真正包含完整业务逻辑的函数
    sys.path.insert(0, str(Path.home() / "zhiwei-scheduler"))
    import scheduler_jobs
    
    success = False
    result_text = "成功发出指令"

    try:
        if trigger_name == "morning.run":
            scheduler_jobs.job_morning_brief()
            success = True
        elif trigger_name == "noon.run":
            scheduler_jobs.job_noon_brief()
            success = True
        elif trigger_name == "arxiv.run":
            scheduler_jobs.job_arxiv()
            success = True
        elif trigger_name == "system.run":
            scheduler_jobs.job_system_check()
            success = True
        elif trigger_name == "tanwei.run":
            # 兼容独立的 agent 调用
            succ, result_text = call_agent(config['agent'], config['prompt'])
            success = succ
            if success:
                from scheduler_queue import try_push, save_result_safe
                save_result_safe("manual_brief", result_text, ["feishu"])
                try_push(Path.home() / "zhiwei-scheduler/outputs/artifacts/pending/manual_brief.json")
        else:
            success = False
            result_text = "未实现的触发器函数映射"
    except Exception as e:
        success = False
        result_text = str(e)

    if success:
        if _logger:
            _logger.info(f"✅ 触发任务完成: {config['job_name']}")
        print(f"✅ 完成: {config['job_name']}")

        store.complete(task_id, branch="trigger", result="Success")
        mb.publish("trigger_listener", "feishu_notification", f"✅ 定制任务执行完毕：{config['description']} (Task #{task_id})", {"targets": ["feishu"]})

        # 删除触发文件
        try:
            trigger_file.unlink()
        except Exception as e:
            pass
    else:
        if _logger:
            _logger.error(f"❌ 触发任务失败: {result_text[:200]}")
        print(f"❌ 失败: {result_text[:200]}")
        
        store.fail(task_id, result_text)
        mb.publish("trigger_listener", "feishu_notification", f"❌ 定制任务执行异常：{config['description']} (Task #{task_id})\n\n{result_text[:200]}", {"targets": ["feishu"]})

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
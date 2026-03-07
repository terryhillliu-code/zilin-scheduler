#!/usr/bin/env python3
"""
任务调度器 v2 — 支持优先级队列和多 Worker 并行
T-079: Phase 2 任务调度器重构
"""

import os
import json
import time
import logging
import threading
from pathlib import Path
from queue import PriorityQueue
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Callable
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, Future

# 配置
TASKS_DIR = Path.home() / "tasks"
PENDING_DIR = TASKS_DIR / "pending"
RUNNING_DIR = TASKS_DIR / "running"
DONE_DIR = TASKS_DIR / "done"
FAILED_DIR = TASKS_DIR / "failed"
REVIEW_DIR = TASKS_DIR / "review"

POLL_INTERVAL = 5  # 轮询间隔（秒）
MAX_WORKERS = 3    # 最大并行 Worker 数

# 优先级映射（数字越小优先级越高）
PRIORITY_MAP = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
    "default": 2
}

# 日志
LOG_DIR = Path.home() / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [orchestrator] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "task_orchestrator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass(order=True)
class PrioritizedTask:
    """带优先级的任务包装"""
    priority: int
    timestamp: float = field(compare=False)
    task_id: str = field(compare=False)
    task_data: dict = field(compare=False, repr=False)
    
    @classmethod
    def from_json(cls, task_data: dict) -> "PrioritizedTask":
        risk_level = task_data.get("risk_level", "medium")
        priority = PRIORITY_MAP.get(risk_level, PRIORITY_MAP["default"])
        return cls(
            priority=priority,
            timestamp=time.time(),
            task_id=task_data.get("task_id", "unknown"),
            task_data=task_data
        )


class TaskOrchestrator:
    """
    任务调度器
    - 优先级队列
    - 多 Worker 并行执行
    - 任务状态追踪
    """
    
    def __init__(self, executor_func: Callable, max_workers: int = MAX_WORKERS):
        self.queue = PriorityQueue()
        self.executor_func = executor_func
        self.max_workers = max_workers
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self.running_tasks: Dict[str, Future] = {}
        self.lock = threading.Lock()
        self._stop_event = threading.Event()
        
        # 确保目录存在
        for d in [PENDING_DIR, RUNNING_DIR, DONE_DIR, FAILED_DIR, REVIEW_DIR]:
            d.mkdir(parents=True, exist_ok=True)
    
    def scan_pending(self) -> List[dict]:
        """扫描 pending 目录"""
        tasks = []
        for f in PENDING_DIR.glob("*.json"):
            try:
                task = json.loads(f.read_text())
                task["_source_file"] = str(f)
                tasks.append(task)
            except Exception as e:
                logger.error(f"读取任务失败 {f}: {e}")
        return tasks
    
    def enqueue(self, task: dict):
        """将任务加入优先级队列"""
        ptask = PrioritizedTask.from_json(task)
        self.queue.put(ptask)
        logger.info(f"入队: {ptask.task_id} (优先级: {ptask.priority})")
    
    def enqueue_batch(self, tasks: List[dict]):
        """批量入队"""
        for task in tasks:
            self.enqueue(task)
    
    def _execute_wrapper(self, ptask: PrioritizedTask) -> dict:
        """执行任务的包装器"""
        task_id = ptask.task_id
        task_data = ptask.task_data
        
        try:
            # 移动到 running
            src = Path(task_data.get("_source_file", ""))
            if src.exists():
                dst = RUNNING_DIR / src.name
                src.rename(dst)
                task_data["_source_file"] = str(dst)
            
            logger.info(f"开始执行: {task_id}")
            result = self.executor_func(task_data)
            logger.info(f"执行完成: {task_id}")
            return result
            
        except Exception as e:
            logger.error(f"执行失败: {task_id} - {e}")
            return {"success": False, "error": str(e)}
        finally:
            with self.lock:
                self.running_tasks.pop(task_id, None)
    
    def process_queue(self):
        """处理队列中的任务"""
        while not self.queue.empty():
            # 检查运行中任务数
            with self.lock:
                running_count = len(self.running_tasks)
            
            if running_count >= self.max_workers:
                time.sleep(1)
                continue
            
            try:
                ptask = self.queue.get_nowait()
            except:
                break
            
            # 提交到线程池
            future = self.thread_pool.submit(self._execute_wrapper, ptask)
            with self.lock:
                self.running_tasks[ptask.task_id] = future
    
    def run_once(self):
        """单次扫描并处理"""
        # 扫描新任务
        new_tasks = self.scan_pending()
        if new_tasks:
            logger.info(f"发现 {len(new_tasks)} 个新任务")
            self.enqueue_batch(new_tasks)
        
        # 处理队列
        self.process_queue()
    
    def run_loop(self):
        """主循环"""
        logger.info(f"调度器启动，Workers: {self.max_workers}")
        
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"循环异常: {e}")
            
            time.sleep(POLL_INTERVAL)
    
    def stop(self):
        """停止调度器"""
        self._stop_event.set()
        self.thread_pool.shutdown(wait=True)
        logger.info("调度器已停止")
    
    def status(self) -> dict:
        """获取状态"""
        return {
            "queue_size": self.queue.qsize(),
            "running": list(self.running_tasks.keys()),
            "max_workers": self.max_workers,
            "pending_files": len(list(PENDING_DIR.glob("*.json"))),
            "done_files": len(list(DONE_DIR.glob("*.json"))),
            "failed_files": len(list(FAILED_DIR.glob("*.json")))
        }


# 测试用简单执行器
def dummy_executor(task: dict) -> dict:
    """测试用执行器"""
    time.sleep(2)  # 模拟执行
    return {"success": True, "message": f"Task {task.get('task_id')} completed"}


if __name__ == "__main__":
    print("=== TaskOrchestrator 测试 ===")
    
    orchestrator = TaskOrchestrator(dummy_executor, max_workers=2)
    print(f"状态: {orchestrator.status()}")
    
    # 模拟任务
    test_tasks = [
        {"task_id": "test-1", "risk_level": "high"},
        {"task_id": "test-2", "risk_level": "low"},
        {"task_id": "test-3", "risk_level": "critical"},
    ]
    
    for t in test_tasks:
        orchestrator.enqueue(t)
    
    print(f"入队后状态: {orchestrator.status()}")
    print("✅ TaskOrchestrator 模块正常")

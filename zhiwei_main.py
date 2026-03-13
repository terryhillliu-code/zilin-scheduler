#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
知微系统 Phase 1 - 极简核心实现
架构师：谋微
原则：仅使用Python标准库 + SQLite
"""

import sqlite3
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any


class ZhiweiCore:
    """知微系统核心智能体"""
    
    def __init__(self, db_path: str = "/Users/liufang/zhiwei-scheduler/zhiwei.db"):
        """初始化知微系统核心"""
        self.db_path = db_path
        self.msg_bus_path = "/Users/liufang/zhiwei-dev/messages.db"
        self.conn = None
        self._init_database()
        
    def _init_database(self):
        """初始化SQLite数据库"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        
        # 优化数据库性能
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        
        # 创建任务表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                context TEXT,
                result TEXT
            )
        """)
        
        # 创建系统配置表
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS system_config (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建记忆表（用于存储关键信息）
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT NOT NULL UNIQUE,
                value TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.conn.commit()
        
    def add_task(self, task_name: str, context: Optional[str] = None) -> int:
        """添加新任务"""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO tasks (task_name, context) VALUES (?, ?)",
            (task_name, context)
        )
        self.conn.commit()
        return cursor.lastrowid
        
    def update_task_status(self, task_id: int, status: str, result: Optional[str] = None):
        """更新任务状态"""
        cursor = self.conn.cursor()
        if result is not None:
            cursor.execute(
                "UPDATE tasks SET status = ?, result = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, result, task_id)
            )
        else:
            cursor.execute(
                "UPDATE tasks SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (status, task_id)
            )
        self.conn.commit()
        
    def get_pending_tasks(self) -> List[Dict]:
        """获取待处理任务"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM tasks WHERE status = 'pending'")
        return [dict(row) for row in cursor.fetchall()]
        
    def publish_message(self, topic: str, content: str, sender: str = "謀微", metadata: dict = None):
        """向消息中台投递消息"""
        import json
        import time
        meta_str = json.dumps(metadata, ensure_ascii=False) if metadata else None

        # 重试机制
        MAX_RETRIES = 3
        RETRY_DELAY = 0.5
        BUSY_TIMEOUT = 30000

        for attempt in range(MAX_RETRIES):
            try:
                with sqlite3.connect(self.msg_bus_path, timeout=30.0) as m_conn:
                    m_conn.execute("PRAGMA journal_mode=WAL;")
                    m_conn.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT};")
                    m_conn.execute("""
                        INSERT INTO messages (sender, topic, content, metadata)
                        VALUES (?, ?, ?, ?)
                    """, (sender, topic, content, meta_str))
                    m_conn.commit()
                return  # 成功则返回
            except sqlite3.OperationalError as e:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    raise
            
    def store_memory(self, key: str, value: Any):
        """存储记忆"""
        serialized_value = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO memory (key, value, updated_at) 
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """, (key, serialized_value))
        self.conn.commit()
        
    def retrieve_memory(self, key: str) -> Optional[Any]:
        """检索记忆"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT value FROM memory WHERE key = ?", (key,))
        row = cursor.fetchone()
        if row:
            try:
                return json.loads(row['value'])
            except json.JSONDecodeError:
                return row['value']
        return None
        
    def set_config(self, key: str, value: Any):
        """设置系统配置"""
        serialized_value = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO system_config (key, value, updated_at) 
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """, (key, serialized_value))
        self.conn.commit()
        
    def get_config(self, key: str) -> Optional[Any]:
        """获取系统配置"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT value FROM system_config WHERE key = ?", (key,))
        row = cursor.fetchone()
        if row:
            try:
                return json.loads(row['value'])
            except json.JSONDecodeError:
                return row['value']
        return None
        
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


def main():
    """主函数 - 演示知微系统基本功能"""
    print("🚀 知微系统 Phase 1 启动中...")
    print("架构原则：Python标准库 + SQLite")
    print("-" * 40)
    
    # 初始化核心系统
    core = ZhiweiCore()
    
    # 设置系统标识
    core.set_config("system_name", "知微系统")
    core.set_config("version", "Phase 1.0 (Integrated)")
    core.set_config("architect", "谋微")
    
    # 存储初始记忆
    core.store_memory("system_initialized", True)
    core.store_memory("initialization_time", datetime.now().isoformat())
    
    # 演示任务流：创建 -> 执行 -> 推送
    task_id = core.add_task("系统自检", "验证核心功能与消息中台连接")
    print(f"✅ 已创建任务 ID: {task_id}")
    
    # 检索记忆
    init_time = core.retrieve_memory("initialization_time")
    print(f"🕒 系统初始化时间: {init_time}")
    
    # 模拟执行成功并投递消息
    print("📢 正在向中台投递通知...")
    core.publish_message(
        topic="notification",
        content="� 知微极简核心 (Phase 8) 已成功上线并完成首次自检。",
        metadata={"title": "系统上线通知", "targets": ["feishu", "dingtalk"]}
    )
    
    # 更新任务状态
    core.update_task_status(task_id, "completed", "自检通过，推送已投递至 MessageBus")
    print(f"✅ 任务 {task_id} 已完成")
    
    # 关闭系统
    core.close()
    print("-" * 40)
    print("✨ 知微系统 Phase 1 运行完成！")
    print(f"核心数据库: {core.db_path}")
    print(f"消息数据库: {core.msg_bus_path}")


if __name__ == "__main__":
    main()

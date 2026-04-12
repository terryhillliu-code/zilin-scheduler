#!/usr/bin/env python3
"""
消息总线 — Agent 间通信
T-080: Phase 3 Agent 协同
"""

import json
import time
import threading
import logging
from pathlib import Path
from typing import Dict, List, Callable, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
from queue import Queue
from enum import Enum

# 配置
MESSAGE_DIR = Path.home() / "tasks" / "agent-messages"
MESSAGE_DIR.mkdir(parents=True, exist_ok=True)

# 日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s [event_bus] %(message)s')
logger = logging.getLogger(__name__)


class EventType(Enum):
    """事件类型"""
    REQUEST = "request"       # 请求其他 Agent 处理
    RESPONSE = "response"     # 返回处理结果
    DELEGATE = "delegate"     # 委派子任务
    COMPLETE = "complete"     # 任务完成通知
    BROADCAST = "broadcast"   # 广播消息


@dataclass
class AgentMessage:
    """Agent 间消息格式"""
    msg_id: str
    from_agent: str
    to_agent: str
    event_type: str
    task_id: str
    payload: dict
    context: dict
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
    
    @classmethod
    def from_dict(cls, data: dict) -> "AgentMessage":
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> "AgentMessage":
        return cls.from_dict(json.loads(json_str))


class EventBus:
    """
    事件总线
    - 发布/订阅模式
    - 支持同步请求/响应
    - 文件持久化（用于跨进程）
    """
    
    def __init__(self):
        self.subscribers: Dict[str, List[Callable]] = {}
        self.pending_responses: Dict[str, Queue] = {}
        self.lock = threading.Lock()
        self._poll_thread = None
        self._stop_event = threading.Event()
    
    def subscribe(self, agent_id: str, handler: Callable[[AgentMessage], None]):
        """订阅消息"""
        with self.lock:
            if agent_id not in self.subscribers:
                self.subscribers[agent_id] = []
            self.subscribers[agent_id].append(handler)
        logger.info(f"Agent '{agent_id}' 已订阅")
    
    def unsubscribe(self, agent_id: str):
        """取消订阅"""
        with self.lock:
            self.subscribers.pop(agent_id, None)
    
    def publish(self, message: AgentMessage):
        """发布消息"""
        # 写入文件（持久化）
        filename = f"{message.from_agent}_{message.to_agent}_{message.msg_id}.json"
        filepath = MESSAGE_DIR / filename
        filepath.write_text(message.to_json())
        logger.info(f"消息发布: {message.from_agent} → {message.to_agent} ({message.event_type})")
        
        # 内存通知（同进程）
        with self.lock:
            handlers = self.subscribers.get(message.to_agent, [])
            for handler in handlers:
                try:
                    threading.Thread(target=handler, args=(message,), daemon=True).start()
                except Exception as e:
                    logger.error(f"Handler 执行失败: {e}")
    
    def request(self, from_agent: str, to_agent: str, task_id: str, 
                payload: dict, context: dict = None, timeout: float = 60) -> Optional[AgentMessage]:
        """
        同步请求（等待响应）
        """
        msg_id = f"{task_id}_{int(time.time()*1000)}"
        
        # 创建响应队列
        response_queue = Queue()
        with self.lock:
            self.pending_responses[msg_id] = response_queue
        
        # 发送请求
        request_msg = AgentMessage(
            msg_id=msg_id,
            from_agent=from_agent,
            to_agent=to_agent,
            event_type=EventType.REQUEST.value,
            task_id=task_id,
            payload=payload,
            context=context or {}
        )
        self.publish(request_msg)
        
        # 等待响应
        try:
            response = response_queue.get(timeout=timeout)
            return response
        except Exception:
            logger.warning(f"请求超时: {msg_id}")
            return None
        finally:
            with self.lock:
                self.pending_responses.pop(msg_id, None)
    
    def respond(self, original_msg: AgentMessage, payload: dict):
        """响应请求"""
        response = AgentMessage(
            msg_id=original_msg.msg_id,
            from_agent=original_msg.to_agent,
            to_agent=original_msg.from_agent,
            event_type=EventType.RESPONSE.value,
            task_id=original_msg.task_id,
            payload=payload,
            context=original_msg.context
        )
        self.publish(response)
        
        # 通知等待的请求
        with self.lock:
            queue = self.pending_responses.get(original_msg.msg_id)
            if queue:
                queue.put(response)
    
    def poll_messages(self, agent_id: str) -> List[AgentMessage]:
        """轮询文件系统中的消息（原子读取+删除）"""
        messages = []
        pattern = f"*_{agent_id}_*.json"

        for f in MESSAGE_DIR.glob(pattern):
            try:
                data = f.read_text()
                msg = AgentMessage.from_json(data)
                f.unlink()  # 解析成功后删除
                messages.append(msg)
            except FileNotFoundError:
                pass  # 已被其他进程取走
            except Exception as e:
                logger.error(f"读取消息失败 {f}: {e}")

        return messages
    
    def start_polling(self, agent_id: str, handler: Callable):
        """启动轮询线程"""
        def _poll():
            while not self._stop_event.is_set():
                messages = self.poll_messages(agent_id)
                for msg in messages:
                    try:
                        handler(msg)
                    except Exception as e:
                        logger.error(f"处理消息失败: {e}")
                time.sleep(1)
        
        self._poll_thread = threading.Thread(target=_poll, daemon=True)
        self._poll_thread.start()
    
    def stop(self):
        """停止轮询"""
        self._stop_event.set()


# 全局单例
_event_bus = None

def get_event_bus() -> EventBus:
    global _event_bus
    if _event_bus is None:
        _event_bus = EventBus()
    return _event_bus


if __name__ == "__main__":
    print("=== EventBus 测试 ===")
    
    bus = get_event_bus()
    
    # 测试消息
    msg = AgentMessage(
        msg_id="test-001",
        from_agent="知微",
        to_agent="探微",
        event_type=EventType.REQUEST.value,
        task_id="T-TEST",
        payload={"query": "分析 RISC-V 架构"},
        context={"user_id": "test_user"}
    )
    
    print(f"消息结构:\n{msg.to_json()}")
    
    # 测试发布
    bus.publish(msg)
    print(f"\n✅ 消息已发布到 {MESSAGE_DIR}")
    
    # 检查文件
    files = list(MESSAGE_DIR.glob("*.json"))
    print(f"消息文件数: {len(files)}")

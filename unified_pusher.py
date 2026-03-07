#!/usr/bin/env python3
"""
知微统一推送引擎 (UnifiedPusher) v1.0
特性: 
1. 统一消费 MessageBus (SQLite)
2. 联动 Operator Agent 进入自动内容润色
3. 支持动态渠道分发 (钉钉/飞书)
4. 具备自愈降级能力
"""

import os
import sys
import json
import time
import logging
import yaml
import subprocess
from pathlib import Path
from datetime import datetime

# 环境准备
BASE_DIR = Path(__file__).parent
DEV_DIR = Path("/Users/liufang/zhiwei-dev")
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(DEV_DIR))

from pusher import PushManager
from message_bus import MessageBus

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [UnifiedPusher] - %(levelname)s - %(message)s'
)
logger = logging.getLogger("unified-pusher")

# 配置常量
CONFIG_PATH = BASE_DIR / "config" / "settings.yaml"
POLL_INTERVAL = 10  # 秒
CONTAINER = "clawdbot"

class UnifiedPusher:
    def __init__(self):
        self.config = self._load_config()
        self.bus = MessageBus()
        self.pm = PushManager(self.config)
        self._running = True

    def _load_config(self):
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def call_llm_direct(self, message: str, timeout: int = 180) -> tuple[bool, str]:
        """降级方案：直接调用本地模型代理"""
        import http.client
        try:
            payload = json.dumps({
                "model": "gpt-4o",
                "messages": [{"role": "user", "content": message}],
                "temperature": 0.7
            })
            conn = http.client.HTTPConnection("127.0.0.1", 8045, timeout=timeout)
            conn.request("POST", "/v1/chat/completions", body=payload, headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            data = json.loads(resp.read().decode())
            conn.close()
            if resp.status == 200:
                return True, data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return False, f"HTTP {resp.status}"
        except Exception as e:
            return False, str(e)

    def refine_content(self, title: str, content: str, task_name: str = "") -> str:
        """调用 Operator Agent 进行内容润色"""
        # 提取任务类型描述
        category = "系统通知"
        if "morning" in task_name: category = "每日早报"
        elif "noon" in task_name: category = "每日午报"
        elif "info_brief" in task_name: category = "信息流简报"
        elif "arxiv" in task_name: category = "论文精选"

        prompt = (
            f"你是一个专业的知微运营助手。请将以下原始信息润色为一份排版精美、语气正式的 Markdown 简报。\n"
            f"**重要提示**：当前任务类型为【{category}】，请在标题和正文中保持这一分类，不要混淆为其他类型的报告。\n"
            f"原始标题: {title}\n"
            f"原始内容:\n{content}"
        )
        
        cmd = [
            "docker", "exec", CONTAINER,
            "openclaw", "agent", "--agent", "operator",
            "--message", prompt, "--json", "--timeout", "120"
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=150)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                if data.get("status") == "ok":
                    text = data.get("result", {}).get("payloads", [{}])[0].get("text", "")
                    if "Multiple tools" not in text and "400 {" not in text:
                        return text
            
            logger.warning("⚠️ Agent 调用异常或工具冲突，执行降级润色...")
            ok, text = self.call_llm_direct(prompt)
            return text if ok else content
        except Exception as e:
            logger.error(f"❌ 润色失败: {e}")
            return content

    def process_message(self, msg: dict):
        msg_id = msg['id']
        topic = msg['topic']
        content = msg['content']
        metadata = json.loads(msg['metadata'] or '{}')
        
        title = metadata.get("title", "系统通知")
        targets = metadata.get("targets", [])
        
        # 自动推断 targets
        if not targets:
            if "dingtalk" in topic: targets.append("dingtalk")
            if "feishu" in topic: targets.append("feishu")
        if not targets: targets = ["feishu"] # 默认飞书

        logger.info(f"📦 正在处理消息 #{msg_id} | Topic: {topic} | Title: {title}")

        # 是否需要润色
        final_content = content
        if metadata.get("refine") is True:
            logger.info(f"✨ 正在启用 Agent 润色...")
            final_content = self.refine_content(title, content, task_name=metadata.get("task", ""))

        # 执行推送
        results = self.pm.push(title, final_content, targets, force=True)
        
        # 结果判定
        success = True
        err_msgs = []
        for channel, res in results.items():
            if res.get("errcode", 0) != 0 and res.get("code", 0) != 0:
                success = False
                err_msgs.append(f"{channel}: {res}")

        if success:
            self.bus.mark_sent(msg_id)
            logger.info(f"✅ 消息 #{msg_id} 投递成功")
        else:
            self.bus.mark_failed(msg_id, "; ".join(err_msgs))
            logger.error(f"❌ 消息 #{msg_id} 投递部分失败: {err_msgs}")

    def run(self):
        logger.info("🚀 UnifiedPusher 引擎已启动，正在监听 MessageBus...")
        while self._running:
            try:
                # 消费所有待处理 topic
                messages = self.bus.consume_pending(limit=5)
                if not messages:
                    time.sleep(POLL_INTERVAL)
                    continue
                
                for msg in messages:
                    self.process_message(msg)
                    
            except KeyboardInterrupt:
                self._running = False
            except Exception as e:
                logger.error(f"💥 运行异常: {e}")
                time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    pusher = UnifiedPusher()
    pusher.run()

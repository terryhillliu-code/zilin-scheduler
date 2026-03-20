#!/usr/bin/env python3
"""
知微调度器核心模块
包含：工具函数、Agent调用、Prompt加载、日志配置

从 scheduler.py 拆分 (v47.0)
"""

import os
import sys
import json
import logging
import logging.handlers
import time
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

# 公共模块
from llm_proxy import call_llm_direct

# 新增业务跳过异常类
class TaskSkippedException(Exception):
    pass


# 直接初始化 logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 模块级全局变量
config = None
push_manager = None

# ============ 指数退避重试配置 ============
RETRY_DELAYS = [120, 300, 600]  # 2min, 5min, 10min

# JSONL 日志路径
JSON_LOG_PATH = Path.home() / "logs" / "scheduler.jsonl"


def is_quiet_hours(now: datetime = None) -> bool:
    """
    检查当前时间是否在静默时段（23:00-06:30）
    重试任务在静默时段不推送
    """
    if now is None:
        now = datetime.now()
    hour = now.hour
    minute = now.minute

    # 23:00-06:30 为静默时段
    if hour >= 23 or hour < 6:
        return True
    if hour == 6 and minute < 30:
        return True
    return False


def get_retry_delay(attempt: int) -> int:
    """获取重试延迟（秒）"""
    if attempt < len(RETRY_DELAYS):
        return RETRY_DELAYS[attempt]
    return RETRY_DELAYS[-1]


# ============ 结构化 JSONL 日志 ============

def log_task_metrics(
    task_name: str,
    status: str,
    duration_ms: int = None,
    error: str = None,
    extra: dict = None
):
    """
    将任务执行指标写入 JSONL 日志

    Args:
        task_name: 任务名称
        status: success / failure / skipped
        duration_ms: 执行时长（毫秒）
        error: 错误信息（可选）
        extra: 额外元数据（可选）
    """
    record = {
        "timestamp": datetime.now().isoformat(),
        "task": task_name,
        "status": status,
    }

    if duration_ms is not None:
        record["duration_ms"] = duration_ms
    if error:
        record["error"] = error[:500]  # 截断过长的错误信息
    if extra:
        record["extra"] = extra

    try:
        JSON_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(JSON_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning(f"JSONL 日志写入失败: {e}")


# ============ 任务失败告警 (T-016.3) ============

def send_failure_alert(task_name: str, error_msg: str = None):
    """
    发送任务失败告警（钉钉/飞书）

    Args:
        task_name: 任务名称
        error_msg: 错误信息
    """
    alert_msg = f"⚠️ 定时任务失败\n\n任务: {task_name}\n时间: {datetime.now().strftime('%H:%M:%S')}"
    if error_msg:
        alert_msg += f"\n错误: {error_msg[:200]}"

    # 尝试发送告警
    try:
        # 导入推送模块
        sys.path.insert(0, str(Path(__file__).parent))
        from scheduler_queue import try_push

        # 发送到飞书
        try_push(
            title="⚠️ 定时任务失败告警",
            content=alert_msg,
            channels=["feishu"],
            silent=True  # 告警消息不记录发送状态
        )
        logger.info(f"📤 失败告警已发送: {task_name}")
    except Exception as e:
        logger.warning(f"发送告警失败: {e}")


# ============ 日志 ============

def setup_logging(log_dir: str = "logs", retention_days: int = 30) -> logging.Logger:
    """
    配置日志轮转

    Args:
        log_dir: 日志目录
        retention_days: 保留天数
    """
    log_path = Path.home() / log_dir
    log_path.mkdir(parents=True, exist_ok=True)

    log_file = log_path / "scheduler.log"

    # 创建文件 handler（轮转）
    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))

    # 控制台 handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))

    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return logging.getLogger(__name__)


# ============ Prompt 加载器 ============

def load_prompt(template_name: str, **kwargs) -> str:
    """
    从外部文件加载 Prompt 模板

    Args:
        template_name: 模板名称（不含 .txt）
        **kwargs: 模板变量

    Returns:
        渲染后的 Prompt 字符串
    """
    prompt_dir = Path(__file__).parent / "prompts"
    template_path = prompt_dir / f"{template_name}.txt"

    if not template_path.exists():
        logger.warning(f"Prompt 模板不存在: {template_path}")
        return ""

    try:
        with open(template_path, "r", encoding="utf-8") as f:
            template = f.read()

        # 简单变量替换
        for key, value in kwargs.items():
            template = template.replace(f"{{{key}}}", str(value))

        return template.strip()
    except Exception as e:
        logger.error(f"加载 Prompt 失败: {e}")
        return ""


# ============ Agent 调用 ============

def call_agent(agent_id: str, message: str, timeout: int = 180) -> tuple[bool, str]:
    """
    调用 LLM Agent（直连百炼 API，不再依赖 OpenClaw/Docker）

    V2-102 重构：
    - 使用 llm_client 直连百炼 API
    - 移除 Docker 依赖

    Args:
        agent_id: Agent ID (main/researcher/operator)
        message: 消息内容
        timeout: 超时时间（秒）

    Returns:
        (success, response) 元组
    """
    # 尝试使用 llm_client
    if LLM_CLIENT_AVAILABLE and llm_client:
        try:
            success, text = llm_client.call(agent_id, message, timeout=timeout)
            if success:
                return True, text
            else:
                logger.warning(f"LLM 客户端调用失败: {text}")
        except Exception as e:
            logger.error(f"LLM 客户端异常: {e}")

    # 降级：使用本地代理
    logger.warning("🔄 LLM 客户端不可用，降级到本地代理...")
    return call_llm_direct(message, timeout)


# ============ LLM 客户端导入 ============

# 导入 LLM 客户端 (V2-102: 替代 OpenClaw)
sys.path.insert(0, str(Path.home() / "zhiwei-bot"))
try:
    from llm_client import llm_client, LLMClient
    LLM_CLIENT_AVAILABLE = True
except ImportError:
    LLM_CLIENT_AVAILABLE = False
    llm_client = None
    logger.warning("⚠️ llm_client 导入失败，将使用降级模式")


# ============ RAG 相关 ============

# 尝试导入 RAG 桥接
try:
    from rag_bridge import enrich_with_rag, is_available as rag_is_available
    RAG_AVAILABLE = rag_is_available()
except ImportError:
    RAG_AVAILABLE = False
    def enrich_with_rag(query, top_k=5):
        return ""


# ============ 配置加载 ============

def load_config() -> dict:
    """加载配置文件"""
    config_path = Path(__file__).parent / "config" / "settings.yaml"

    if not config_path.exists():
        logger.warning(f"配置文件不存在: {config_path}")
        return {"jobs": {}, "system": {}}

    try:
        import yaml
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error(f"加载配置失败: {e}")
        return {"jobs": {}, "system": {}}


# ============ 输出保存 ============

def save_output(job_name: str, content: str):
    """保存任务输出到文件"""
    output_dir = Path.home() / "logs" / "scheduler_output"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{job_name}_{datetime.now().strftime('%Y%m%d')}.md"

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"# {job_name}\n\n")
            f.write(f"生成时间: {datetime.now().isoformat()}\n\n")
            f.write(content)
        logger.info(f"📄 输出已保存: {output_file}")
    except Exception as e:
        logger.warning(f"保存输出失败: {e}")


# ============ 导出 ============

__all__ = [
    # 异常
    'TaskSkippedException',
    # 工具函数
    'is_quiet_hours',
    'get_retry_delay',
    'log_task_metrics',
    'send_failure_alert',
    'setup_logging',
    'load_prompt',
    'load_config',
    'save_output',
    # Agent 调用
    'call_agent',
    'enrich_with_rag',
    # 常量
    'RETRY_DELAYS',
    'JSON_LOG_PATH',
    'RAG_AVAILABLE',
    'LLM_CLIENT_AVAILABLE',
    # 全局变量
    'config',
    'push_manager',
    'logger',
]
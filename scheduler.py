#!/usr/bin/env python3
"""
知微定时任务调度器 v3.4 (2026-03-01 Phase 2)
特性: Prompt 外部化、支持热更新、重试机制、触发器监听
"""

import os
import sys
import json
import signal
import subprocess
import logging
import logging.handlers
import time
import threading
from datetime import datetime, timedelta
from pathlib import Path

# 直接初始化 logger（不再是 None）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

BASE_DIR = Path(__file__).parent
PROMPT_DIR = BASE_DIR / "prompts"
TRIGGER_DIR = BASE_DIR / "triggers"
sys.path.insert(0, str(BASE_DIR))

# 初始化模块级别的全局变量，防止未定义错误
config = None
push_manager = None

from pusher import PushManager
from scheduler_queue import save_result, try_push, save_result_safe

# 新增：新闻去重模块
from news_dedup import should_push, load_sent_today, get_sent_titles, record_sent, extract_titles_from_content

# 新增：锁管理、缓存、触发器
from lock_manager import acquire_lock
from price_cache import has_price_changed, update_price_cache
import trigger_listener

CONTAINER = "clawdbot"

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
    """获取指数退避延迟"""
    if attempt <= len(RETRY_DELAYS):
        return RETRY_DELAYS[attempt - 1]
    return RETRY_DELAYS[-1]  # 最大40分钟


# ============ 结构化 JSONL 日志 ============

def log_task_metrics(
    task_name: str,
    start_time: float,
    end_time: float,
    success: bool,
    push_status: dict = None,
    token_usage: dict = None,
    error_msg: str = None
):
    """
    将任务指标写入 JSONL 日志
    """
    import json

    log_entry = {
        "task_name": task_name,
        "start_time": datetime.fromtimestamp(start_time).isoformat(),
        "end_time": datetime.fromtimestamp(end_time).isoformat(),
        "latency_seconds": round(end_time - start_time, 2),
        "success": success,
        "push_status": push_status or {},
        "token_usage": token_usage or {},
        "error_msg": error_msg
    }

    JSON_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(JSON_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    # 安全地记录调试日志
    global logger
    if logger:
        logger.debug(f"📊 任务指标已记录: {task_name}")

    # T-016.3: 如果任务失败，发送告警
    if not success:
        send_failure_alert(task_name, error_msg)


# ============ 任务失败告警 (T-016.3) ============

def send_failure_alert(task_name: str, error_msg: str = None):
    """
    发送任务失败告警到钉钉/飞书
    """
    global logger  # 声明使用全局 logger（必须在函数开头）
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 截取错误信息
        error_summary = "未知异常"
        if error_msg:
            error_summary = str(error_msg)[:100]
            if len(str(error_msg)) > 100:
                error_summary += "..."

        # 构建告警消息
        alert_content = f"""## 🚨 系统任务执行异常 (Immediate Alert)

**任务名称**: {task_name}

**发生时间**: {current_time}

**异常摘要**:
```
{error_summary}
```

**建议**: 请查看 ~/logs/scheduler.jsonl 获取详细堆栈信息。
"""

        # 保存告警到临时文件
        alert_dir = BASE_DIR / "outputs"
        alert_dir.mkdir(exist_ok=True)
        alert_file = alert_dir / f"alert_{task_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

        with open(alert_file, "w", encoding="utf-8") as f:
            f.write(alert_content)

        # 推送到钉钉（紧急告警使用钉钉）
        channels = ["dingtalk"]
        file_path = save_result(
            task=f"alert_{task_name}",
            content=alert_content,
            targets=channels,
            metadata={"type": "alert", "level": "critical"}
        )

        # 立即推送
        success = try_push(file_path, push_manager, logger, return_status=True)
        if success and success.get("dingtalk"):
            if logger:
                logger.info(f"✅ 告警已发送: {task_name}")
        else:
            if logger:
                logger.warning(f"⚠️ 告警发送失败: {task_name}")

    except Exception as e:
        if logger:
            logger.error(f"❌ 告警发送异常: {e}")


# ============ 日志 ============

def setup_logging(log_dir: str = "logs", retention_days: int = 30):
    global logger  # 声明使用全局 logger
    log_path = BASE_DIR / log_dir
    log_path.mkdir(exist_ok=True)

    logger = logging.getLogger("zhiwei-scheduler")
    logger.setLevel(logging.DEBUG)

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    ))
    logger.addHandler(console)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_path / "scheduler.log",
        when="midnight", backupCount=retention_days, encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    ))
    logger.addHandler(file_handler)

    return logger


# ============ Prompt 加载器 (Phase 2 新增) ============

def load_prompt(template_name: str, **kwargs) -> str:
    """
    加载 Markdown 模板并填充变量
    """
    path = PROMPT_DIR / f"{template_name}.md"
    if not path.exists():
        logger.error(f"❌ 找不到 Prompt 模板: {path}")
        return f"Error: Prompt template {template_name} missing."

    try:
        content = path.read_text(encoding="utf-8")
        return content.format(**kwargs)
    except KeyError as e:
        logger.error(f"❌ Prompt 渲染失败 [{template_name}]: 缺少变量 {e}")
        # 返回错误信息，包含缺失的变量名
        return f"Error: Missing variable {e} in template {template_name}."
    except Exception as e:
        logger.error(f"❌ Prompt 渲染失败 [{template_name}]: {e}")
        # 返回错误信息而不是原始模板
        return f"Error: Failed to render template {template_name}: {e}"

# ============ Agent 调用 ============

def call_agent(agent_id: str, message: str, timeout: int = 180) -> tuple[bool, str]:
    """
    通过 OpenClaw 调用 Agent
    """
    cmd = [
        "docker", "exec", CONTAINER,
        "openclaw", "agent",
        "--agent", agent_id,
        "--message", message,
        "--json",
        "--timeout", str(timeout)
    ]

    # 简单的内部重试，防止网络抖动
    for attempt in range(2):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 30)
            if result.returncode != 0:
                err = result.stderr.strip()[:300]
                logger.warning(f"⚠️ Agent 调用警告 (第{attempt+1}次): {err}")
                continue

            # 解析 JSON 响应
            data = json.loads(result.stdout)
            if data.get("status") == "ok":
                payloads = data.get("result", {}).get("payloads", [])
                if payloads:
                    text = payloads[0].get("text", "")
                    return True, text

            logger.warning(f"⚠️ Agent 返回异常 (第{attempt+1}次): {result.stdout[:100]}")

        except subprocess.TimeoutExpired:
            logger.error(f"❌ Agent 调用超时")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Agent 响应解析失败: {e}")
        except Exception as e:
            logger.error(f"❌ Agent 调用异常: {e}")

    logger.error(f"❌ Agent {agent_id} 2次尝试均失败，放弃")
    return False, "Agent 调用失败，请检查 Docker 或网络连接"



# ============ RAG 上下文获取 (T-017) ============

# 任务名称到关键词的映射表
KLIB_ENRICHMENT = {
    "arxiv_papers": ["芯片架构", "HBM", "CoWoS", "MoE", "分布式训练"],
    "morning_brief": ["半导体", "封装", "推理加速"],
    "noon_brief": ["AI", "大模型", "GPU"],
    "info_brief": ["全球资讯", "经济动态", "科技前沿", "市场观察"],
}


# ============ A/B 测试配置 (T-019) ============
# 使用方式：在 AB_TESTS 中添加测试条目，然后手动运行 run_ab_test('test_id')
# 示例：
# AB_TESTS = {
#     "morning_v2_test": {
#         "task_name": "morning_brief",
#         "prompt_a": "morning_brief",
#         "prompt_b": "morning_brief_v2",
#         "agent": "researcher",
#         "timeout": 240,
#         "push_to": ["dingtalk"],
#     },
# }
AB_TESTS = {}


def run_ab_test(test_id: str):
    """
    运行 A/B 测试：用两个 prompt 版本各调用一次 agent，
    将结果并排推送给用户对比。
    """
    if test_id not in AB_TESTS:
        logger.warning(f"A/B 测试 [{test_id}] 不存在，可用测试: {list(AB_TESTS.keys())}")
        return

    config = AB_TESTS[test_id]
    task_name = config["task_name"]
    agent = config.get("agent", "researcher")
    timeout = config.get("timeout", 240)
    push_to = config.get("push_to", ["dingtalk"])

    now = datetime.now()
    date_str = now.strftime('%Y年%m月%d日 %A')
    results = {}

    for version_key in ["prompt_a", "prompt_b"]:
        template_name = config[version_key]
        label = "A" if version_key == "prompt_a" else "B"

        try:
            # 加载 prompt 模板
            prompt_kwargs = {"date": date_str}
            # 根据任务类型添加额外参数
            if "arxiv" in template_name:
                prompt_kwargs = {"categories": "cs.AI,cs.LG,cs.CL,cs.CV", "min_score": 2, "limit": 10}
            elif "noon" in template_name:
                prompt_kwargs = {"time": now.strftime('%m月%d日 %H:%M')}
            elif "crypto" in template_name:
                prompt_kwargs = {"period": "morning"}

            prompt = load_prompt(template_name, **prompt_kwargs)
            prompt = enrich_with_klib(task_name, prompt)

            ok, content = call_agent(agent, prompt, timeout=timeout)

            if ok:
                results[label] = content
            else:
                results[label] = f"[调用失败] {content}"

        except Exception as e:
            results[label] = f"[异常] {str(e)}"

    # 组装对比报告
    report = f"## 📊 A/B 测试报告 [{test_id}]\n\n"
    report += f"- **时间**: {now.strftime('%Y-%m-%d %H:%M')}\n"
    report += f"- **任务**: {task_name}\n"
    report += f"- **Prompt A**: {config['prompt_a']}\n"
    report += f"- **Prompt B**: {config['prompt_b']}\n\n"
    report += "---\n\n"
    report += "### 【版本 A】\n\n"
    report += results.get("A", "[无结果]") + "\n\n"
    report += "---\n\n"
    report += "### 【版本 B】\n\n"
    report += results.get("B", "[无结果]") + "\n"

    # 保存并推送对比报告
    try:
        file_path = save_result(
            task=f"ab_test_{test_id}",
            content=report,
            targets=push_to,
            metadata={"prompt_a": config["prompt_a"], "prompt_b": config["prompt_b"]}
        )
        success = try_push(file_path, push_manager, logger, return_status=True)
        logger.info(f"✅ A/B 测试 [{test_id}] 已推送")
    except Exception as e:
        logger.error(f"❌ A/B 测试推送失败 [{test_id}]: {e}")

    # 记录到 jsonl
    log_task_metrics(
        task_name=f"ab_test_{test_id}",
        success=all(not v.startswith("[") for v in results.values()),
        latency_seconds=0,
        token_usage={}
    )


def enrich_with_klib(task_name: str, prompt_text: str) -> str:
    """
    使用本地知识库增强 prompt (T-017)
    根据任务名称查询相关关键词，将结果注入 prompt
    """
    # 对于 info_brief_XX 类型的任务，使用 info_brief 作为键
    lookup_key = task_name
    if task_name.startswith("info_brief_"):
        lookup_key = "info_brief"

    if lookup_key not in KLIB_ENRICHMENT:
        return prompt_text

    try:
        keywords = KLIB_ENRICHMENT[lookup_key]
        results = []

        for kw in keywords:
            cmd = [
                "docker", "exec", "clawdbot", "python3",
                "/root/workspace/skills/knowledge-search/search.py",
                "keyword", "--query", kw, "--top_k", "2"
            ]
            output = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30
            )
            if output.returncode == 0 and output.stdout.strip():
                stdout = output.stdout.strip()
                # 过滤无效结果：包含"未找到"或结果过短（仅状态提示）
                if "未找到" not in stdout and len(stdout) > 50:
                    results.append(stdout)

        if not results:
            return prompt_text

        enrichment = "\n---\n[本地知识库参考]\n以下是用户知识库中与本次主题相关的内容，请在分析中适当引用：\n" + "\n".join(results) + "\n---\n"
        logger.info(f"✅ 已注入知识库上下文 (task: {task_name}, keywords: {keywords})")
        return prompt_text + enrichment

    except Exception as e:
        logger.warning(f"⚠️ 知识库增强失败 ({task_name}): {e}")
        return prompt_text


def fetch_rag_context(query: str, top_k: int = 2) -> str:
    """
    从本地知识库获取相关背景知识
    """
    import subprocess

    cmd = [
        "docker", "exec", "clawdbot",
        "python3", "/root/workspace/skills/knowledge-search/search.py",
        "vector", "--query", query, "--top_k", str(top_k)
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            logger.warning(f"RAG 查询失败: {result.stderr.strip()}")
            return ""

        output = result.stdout
        # 解析输出，提取相关片段
        lines = output.split('\n')
        context_parts = []
        capture = False

        for line in lines:
            if line.startswith('[1]') or line.startswith('[2]'):
                capture = True
                context_parts.append(line)
            elif capture and line.strip():
                context_parts.append(line)
            elif line.strip() == '':
                capture = False

        return '\n'.join(context_parts[:6])  # 限制上下文长度

    except subprocess.TimeoutExpired:
        logger.warning("RAG 查询超时")
        return ""
    except Exception as e:
        logger.warning(f"RAG 上下文获取异常: {e}")
        return ""


# ============ 任务定义 ============

def job_morning_brief():
    """每日早报 09:30"""
    global logger, config, push_manager  # 声明使用全局变量
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    if logger:
        logger.info("📰 === 每日早报 ===")
    task_name = "morning_brief"
    start_time = time.time()
    push_status = {"dingtalk": False, "feishu": False}

    def _run():
        now = datetime.now()
        # Phase 2: 从文件加载 Prompt
        prompt = load_prompt("morning_brief", date=now.strftime('%Y年%m月%d日 %A'))

        # ============ RAG 增强 (T-017) ============
        prompt = enrich_with_klib("morning_brief", prompt)

        ok, content = call_agent("researcher", prompt, timeout=240)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        save_output("morning_brief", content)
        channels = config["jobs"]["morning_brief"].get("push_to", ["dingtalk", "feishu"])

        file_path = save_result(
            task="morning_brief",
            content=content,
            targets=channels,
            metadata={"agent": "researcher"}
        )
        logger.info(f"📦 结果已保存: {file_path}")

        success = try_push(file_path, push_manager, logger, return_status=True)
        push_status.update(success or {})
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info("✅ 早报推送完成")

    try:
        _run()
    finally:
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=push_status.get("dingtalk") or push_status.get("feishu"),
            push_status=push_status
        )


def job_noon_brief():
    """每日午报 14:30"""
    global logger, config, push_manager  # 声明使用全局变量
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    if logger:
        logger.info("🌤 === 每日午报 ===")

    def _run():
        now = datetime.now()
        # Phase 2: 从文件加载 Prompt
        prompt = load_prompt("noon_brief", time=now.strftime('%m月%d日 %H:%M'))

        # ============ RAG 增强 (T-017) ============
        prompt = enrich_with_klib("noon_brief", prompt)

        ok, content = call_agent("researcher", prompt, timeout=180)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        save_output("noon_brief", content)
        channels = config["jobs"]["noon_brief"].get("push_to", ["dingtalk", "feishu"])

        file_path = save_result(
            task="noon_brief",
            content=content,
            targets=channels,
            metadata={"agent": "researcher"}
        )
        if logger:
            logger.info(f"📦 结果已保存: {file_path}")

        success = try_push(file_path, push_manager, logger)
        if not success:
            if logger:
                logger.warning("推送失败，已进入重试队列")
        else:
            if logger:
                logger.info("✅ 午报推送完成")

    _run()


def job_info_brief(hour: int):
    """信息流简报 (每2小时: 07, 09, 11, 13, 15, 17, 19, 21)"""
    global logger, config, push_manager  # 声明使用全局变量
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    if logger:
        logger.info(f"📰 === 信息流简报 {hour:02d}:00 ===")
    task_name = f"info_brief_{hour:02d}"
    start_time = time.time()
    push_status = {"dingtalk": False, "feishu": False}

    # 检查是否在静默时段（23:00-06:30）
    now = datetime.now()
    if is_quiet_hours(now):
        if logger:
            logger.info(f"🛑 当前在静默时段（23:00-06:30），跳过推送")
        return

    def _run():
        # Phase 2: 从文件加载 Prompt
        date_str = now.strftime('%Y年%m月%d日 %A')
        weekday_str = now.strftime('%A')
        sent_news = get_sent_titles()

        # 天气和加密货币部分由 Agent 在 prompt 中通过 exec 命令生成
        weather_section = "{weather_section}"
        crypto_section = "{crypto_section}"

        prompt = load_prompt(
            "info_brief",
            date=date_str,
            weekday=weekday_str,
            sent_news=sent_news,
            weather_section=weather_section,
            crypto_section=crypto_section
        )

        # ============ RAG 增强 (T-017) ============
        prompt = enrich_with_klib(task_name, prompt)

        ok, content = call_agent("researcher", prompt, timeout=600)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        # 检查是否所有数据源都失败
        if "EXEC_ALL_FAILED" in content:
            logger.warning("⚠️ 所有数据源获取失败，跳过本次推送")
            save_output(task_name, content)
            return

        # ============ 新闻去重检查 ============
        # 检查是否有新内容（至少2条新的）
        if "NO_NEW_CONTENT" in content:
            logger.info(f"📋 无新内容，跳过推送")
            save_output(task_name, content)
            return

        # 检查是否应该推送
        if not should_push(content):
            logger.info(f"📋 新闻无变化，跳过推送")
            save_output(task_name, content)
            return

        save_output(task_name, content)
        channels = config["jobs"].get(task_name, {}).get("push_to", ["dingtalk", "feishu"])

        file_path = save_result(
            task=task_name,
            content=content,
            targets=channels,
            metadata={"agent": "researcher", "hour": hour}
        )
        logger.info(f"📦 结果已保存: {file_path}")

        success = try_push(file_path, push_manager, logger, return_status=True)
        push_status.update(success or {})

        if success and (push_status.get("dingtalk") or push_status.get("feishu")):
            # 记录推送的新闻标题（用于下次去重）
            titles = extract_titles_from_content(content)
            if titles:
                record_sent(titles)
                if logger:
                    logger.info(f"✅ 记录 {len(titles)} 条已推送新闻")
            if logger:
                logger.info("✅ 信息流简报推送完成")
        else:
            if logger:
                logger.warning("推送失败，已进入重试队列")

    error_msg = None
    try:
        _run()
    except Exception as e:
        error_msg = str(e)
        if logger:
            logger.error(f"❌ info_brief_{hour:02d} 失败: {error_msg}")
    finally:
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=push_status.get("dingtalk") or push_status.get("feishu"),
            push_status=push_status,
            error_msg=error_msg
        )


def job_us_market_open():
    """美股开盘 21:30（工作日）"""
    global logger, config, push_manager  # 声明使用全局变量
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    if logger:
        logger.info("🔔 === 美股开盘 ===")
    
    def _run():
        prompt = load_prompt("us_market_open")

        ok, content = call_agent("researcher", prompt, timeout=180)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        save_output("us_market_open", content)
        channels = config["jobs"]["us_market_open"].get("push_to", ["dingtalk", "feishu"])
        
        file_path = save_result(
            task="us_market_open",
            content=content,
            targets=channels,
            metadata={"agent": "researcher"}
        )
        logger.info(f"📦 结果已保存: {file_path}")
        
        success = try_push(file_path, push_manager, logger)
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info("✅ 美股开盘推送完成")

    _run()


def job_us_market_close():
    """美股收盘复盘 07:30（次日推送）"""
    global logger, config, push_manager  # 声明使用全局变量
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    if logger:
        logger.info("📊 === 美股收盘复盘 ===")
    
    def _run():
        prompt = load_prompt("us_market_close")

        ok, content = call_agent("researcher", prompt, timeout=180)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        save_output("us_market_close", content)
        channels = config["jobs"]["us_market_close"].get("push_to", ["dingtalk", "feishu"])
        
        file_path = save_result(
            task="us_market_close",
            content=content,
            targets=channels,
            metadata={"agent": "researcher"}
        )
        logger.info(f"📦 结果已保存: {file_path}")
        
        success = try_push(file_path, push_manager, logger)
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info("✅ 美股收盘推送完成")

    _run()


def job_crypto(period: str = "morning"):
    """加密货币播报 08:00/20:00"""
    global logger, config, push_manager  # 声明使用全局变量
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    label = "早" if period == "morning" else "晚"
    if logger:
        logger.info(f"🪙 === 加密货币{label}报 ===")
    
    def _run():
        ds = config.get("data_sources", {}).get("crypto", {})
        threshold = ds.get("alert_threshold", 5)
        
        # Phase 2: 从文件加载 Prompt
        prompt = load_prompt("crypto", label=label, threshold=threshold)

        ok, content = call_agent("researcher", prompt, timeout=120)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        save_output(f"crypto_{period}", content)
        
        job_key = f"crypto_{period}"
        channels = config["jobs"].get(job_key, {}).get("push_to", ["dingtalk", "feishu"])
        
        file_path = save_result(
            task=f"crypto_{period}",
            content=content,
            targets=channels,
            metadata={"agent": "researcher", "period": period}
        )
        logger.info(f"📦 结果已保存: {file_path}")
        
        success = try_push(file_path, push_manager, logger)
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info(f"✅ 加密货币{label}报推送完成")

    _run()


def job_arxiv():
    """arXiv 论文追踪 10:30"""
    global logger  # 声明使用全局 logger 变量
    logger.info("📄 === arXiv 论文精选 ===")
    task_name = "arxiv_papers"
    start_time = time.time()
    push_status = {"dingtalk": False, "feishu": False}

    def _run():
        ds = config.get("data_sources", {}).get("arxiv", {})
        categories = ",".join(ds.get("categories", ["cs.AI", "cs.LG"]))
        min_score = ds.get("min_score", 2)
        limit = ds.get("max_results", 10)

        # Phase 2: 从文件加载 Prompt
        prompt = load_prompt("arxiv", categories=categories, min_score=min_score, limit=limit)

        # ============ RAG 增强 (T-017) - enrich_with_klib ============
        prompt = enrich_with_klib("arxiv_papers", prompt)

        ok, content = call_agent("researcher", prompt, timeout=240)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        save_output("arxiv", content)
        channels = config["jobs"]["arxiv_papers"].get("push_to", ["dingtalk", "feishu"])

        file_path = save_result(
            task="arxiv_papers",
            content=content,
            targets=channels,
            metadata={"agent": "researcher"}
        )
        logger.info(f"📦 结果已保存: {file_path}")

        success = try_push(file_path, push_manager, logger, return_status=True)
        push_status.update(success or {})
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info("✅ arXiv 论文推送完成")

    try:
        _run()
    finally:
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=push_status.get("dingtalk") or push_status.get("feishu"),
            push_status=push_status
        )


def log_health_status():
    """记录系统健康状况"""
    global logger
    try:
        import psutil
        import platform

        # 获取系统信息
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk_usage = psutil.disk_usage('/')

        # 获取进程信息
        current_process = psutil.Process()
        process_memory = current_process.memory_info().rss / 1024 / 1024  # MB

        health_info = (
            f"🖥️ 系统健康状况 - "
            f"CPU: {cpu_percent}%, "
            f"内存: {memory.percent}%, "
            f"磁盘: {disk_usage.percent}%, "
            f"调度器进程内存: {process_memory:.2f}MB"
        )

        logger.info(f"🏥 系统健康检查: {health_info}")

        # 检查是否有异常值
        if cpu_percent > 90 or memory.percent > 90 or disk_usage.percent > 90:
            logger.warning(f"⚠️ 系统资源使用率过高: CPU={cpu_percent}%, 内存={memory.percent}%, 磁盘={disk_usage.percent}%")

    except ImportError:
        logger.info("🏥 系统健康检查: 未安装psutil模块，无法获取详细系统信息")
    except Exception as e:
        logger.error(f"🏥 系统健康检查失败: {e}")


def job_system_check():
    """系统巡检 09:00"""
    global logger  # 声明使用全局 logger 变量
    logger.info("🔧 === 系统巡检 ===")

    def _run():
        # 添加系统健康检查日志
        log_health_status()
        prompt = load_prompt("system_check")

        # 系统巡检用 operator Agent，失败则用 main
        ok, content = call_agent("operator", prompt, timeout=90)
        if not ok:
            ok, content = call_agent("main", prompt, timeout=90)
            if not ok:
                raise Exception(f"Agent 执行失败: {content}")

        save_output("system_check", content)
        
        has_alert = "🚨" in content
        overall = "🚨" if has_alert else "✅"
        
        channels = config["jobs"]["system_check"].get("push_to", ["dingtalk"])
        
        file_path = save_result(
            task="system_check",
            content=content,
            targets=channels,
            metadata={"type": "system", "has_alert": has_alert}
        )
        logger.info(f"📦 结果已保存: {file_path}")
        
        success = try_push(file_path, push_manager, logger)
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info(f"✅ 系统巡检完成 {overall}")

    _run()


# ============ 运维报告 (T-016.2) ============

def job_system_metrics_report():
    """每日运维指标报告 10:35"""
    global logger  # 声明使用全局 logger 变量
    logger.info("📊 === 运维指标报告 ===")
    task_name = "system_metrics"
    start_time = time.time()
    push_status = {"dingtalk": False, "feishu": False}

    def _run():
        # 调用 analyze_metrics.py 获取数据
        import subprocess

        try:
            result = subprocess.run(
                ["python3", str(Path.home() / "scripts" / "analyze_metrics.py"), "--hours", "24"],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(BASE_DIR)
            )

            if result.returncode != 0:
                logger.warning(f"⚠️ 指标分析失败: {result.stderr}")
                report_content = "⚠️ 运维指标报告生成失败，请检查日志。"
            else:
                report_content = result.stdout

        except subprocess.TimeoutExpired:
            logger.warning("⚠️ 指标分析超时")
            report_content = "⚠️ 运维指标报告生成超时。"
        except Exception as e:
            logger.warning(f"⚠️ 指标分析异常: {e}")
            report_content = "⚠️ 运维指标报告生成异常。"

        # 保存输出
        save_output("system_metrics", report_content)

        # 推送（仅推送到钉钉，避免打扰）
        channels = config["jobs"]["system_metrics"].get("push_to", ["dingtalk"])

        file_path = save_result(
            task="system_metrics",
            content=report_content,
            targets=channels,
            metadata={"type": "metrics", "period": "24h"}
        )
        logger.info(f"📦 结果已保存: {file_path}")

        success = try_push(file_path, push_manager, logger, return_status=True)
        push_status.update(success or {})
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info("✅ 运维指标报告推送完成")

    try:
        _run()
    finally:
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=push_status.get("dingtalk") or push_status.get("feishu"),
            push_status=push_status
        )


def job_obsidian_sync():
    """Obsidian 笔记同步到 ChromaDB 02:00"""
    global logger  # 声明使用全局 logger 变量
    logger.info("🔄 === Obsidian 笔记同步 ===")

    def _run():
        from obsidian_vectorize import sync_to_chromadb

        # 执行同步
        sync_to_chromadb()

        logger.info("✅ Obsidian 笔记同步完成")

    _run()


# ============ 测试用故障注入任务 (T-016.3) ============

def job_fail_test():
    """T-016.3 测试用 - 故意抛异常验证告警"""
    global logger  # 声明使用全局 logger 变量
    logger.info("🧪 === 故障注入测试 ===")

    # 故意抛出异常
    raise Exception("T-016.3 模拟任务执行失败 - 这是一条测试异常")


def job_log_rotate():
    """T-016.5 日志滚动任务"""
    global logger  # 声明使用全局 logger 变量
    logger.info("📦 === 日志滚动 ===")
    import subprocess
    script = Path.home() / "scripts" / "rotate_logs.sh"
    result = subprocess.run(["bash", str(script)], capture_output=True, text=True)
    logger.info(f"📦 日志滚动结果:\n{result.stdout}")
    if result.returncode != 0:
        logger.error(f"📦 日志滚动失败: {result.stderr}")


def job_knowledge_classify():
    """T-076 知识管线分类任务"""
    global logger  # 声明使用全局 logger 变量
    logger.info("📚 === 知识分类 ===")
    import subprocess
    script = BASE_DIR / "knowledge_pipeline.py"
    if not script.exists():
        logger.warning("📚 knowledge_pipeline.py 不存在，跳过")
        return

    result = subprocess.run(
        ["python3", str(script)],
        capture_output=True, text=True, timeout=300
    )
    if result.returncode == 0:
        logger.info(f"📚 知识分类完成:\n{result.stdout}")
    else:
        logger.error(f"📚 知识分类失败: {result.stderr}")


def save_output(job_name: str, content: str):
    output_dir = BASE_DIR / config.get("system", {}).get("output_dir", "outputs")
    output_dir.mkdir(exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    output_file = output_dir / f"{job_name}_{today}.md"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(content)
    logger.info(f"💾 输出已保存: {output_file.name}")


def load_config() -> dict:
    """加载配置，敏感信息从环境变量读取"""
    config = {}
    
    # 加载 YAML 配置
    config_path = BASE_DIR / "config" / "settings.yaml"
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

    # 优先加载 .secrets 环境变量文件
    env_file = Path.home() / ".secrets" / "zhiwei.env"
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key.strip(), value.strip())
    
    # 确保 push 结构存在
    if "push" not in config:
        config["push"] = {"dingtalk": {}, "feishu": {}}

    # 从环境变量覆盖敏感配置
    if os.getenv("DINGTALK_WEBHOOK"):
        config["push"]["dingtalk"]["webhook"] = os.getenv("DINGTALK_WEBHOOK")
    if os.getenv("DINGTALK_SECRET"):
        config["push"]["dingtalk"]["secret"] = os.getenv("DINGTALK_SECRET")
    if os.getenv("FEISHU_APP_ID"):
        config["push"]["feishu"]["app_id"] = os.getenv("FEISHU_APP_ID")
    if os.getenv("FEISHU_APP_SECRET"):
        config["push"]["feishu"]["app_secret"] = os.getenv("FEISHU_APP_SECRET")
    if os.getenv("FEISHU_CHAT_ID"):
        config["push"]["feishu"]["chat_id"] = os.getenv("FEISHU_CHAT_ID")
    
    return config


# ============ 主程序 ============

def main():
    global config, push_manager, logger

    config = load_config()
    logger = setup_logging(
        config.get("system", {}).get("log_dir", "logs"),
        config.get("system", {}).get("log_retention_days", 30)
    )

    push_manager = PushManager(config)

    logger.info("=" * 50)
    logger.info("🤖 知微定时任务系统 v3.4 启动")
    logger.info("   架构: 调度器 → Agent(LLM) → 推送")
    logger.info("   特性: Prompt 外部化、重试机制、触发器监听 (v3.4)")
    logger.info("=" * 50)

    # 记录启动时的健康状态
    log_health_status()

    tz = config.get("system", {}).get("timezone", "Asia/Shanghai")
    scheduler = BlockingScheduler(timezone=tz)

    # ============ 启动触发器监听器 (隔离保护) ============
    try:
        trigger_listener.init(scheduler, logger)
        trigger_listener.start()
        logger.info("👀 触发器监听器已启动")
    except Exception as e:
        logger.warning(f"⚠️ 触发器监听器启动失败: {e}")
        logger.warning("   主定时任务不受影响")

    job_map = {
        "morning_brief":   job_morning_brief,
        "noon_brief":      job_noon_brief,
        "us_market_open":  job_us_market_open,
        "us_market_close": job_us_market_close,
        "crypto_morning":  lambda: job_crypto("morning"),
        "crypto_evening":  lambda: job_crypto("evening"),
        "arxiv_papers":    job_arxiv,
        "system_check":    job_system_check,
        "system_metrics":  job_system_metrics_report,  # T-016.2
        "obsidian_sync":   job_obsidian_sync,  # Phase 2: Obsidian 笔记同步
        "fail_test":       job_fail_test,  # T-016.3 (test only)
        "log_rotate":      job_log_rotate,  # T-016.5
        "knowledge_classify": job_knowledge_classify,  # T-076
    }

    # 动态添加 info_brief_XX 任务映射 (07, 09, 11, 13, 15, 17, 19, 21)
    for hour in [7, 9, 11, 13, 15, 17, 19, 21]:
        job_name = f"info_brief_{hour:02d}"
        job_map[job_name] = lambda h=hour: job_info_brief(h)

    for job_name, job_conf in config.get("jobs", {}).items():
        if not job_conf.get("enabled", False):
            continue
        func = job_map.get(job_name)
        if not func:
            continue

        trigger = CronTrigger(
            hour=job_conf["hour"],
            minute=job_conf["minute"],
            day_of_week=job_conf.get("day_of_week", "mon-sun"),
            timezone=tz
        )
        scheduler.add_job(
            func, trigger=trigger,
            id=job_name,
            name=job_conf.get("description", job_name),
            misfire_grace_time=300
        )
        logger.info(f"   📅 {job_conf.get('description', job_name)} "
                    f"[{job_conf['hour']:02d}:{job_conf['minute']:02d} "
                    f"{job_conf.get('day_of_week', '每天')}]")

    # 日志清理
    def cleanup_old_files():
        retention = config.get("system", {}).get("log_retention_days", 30)
        cutoff = datetime.now() - timedelta(days=retention)
        for d in ["logs", config.get("system", {}).get("output_dir", "outputs")]:
            dir_path = BASE_DIR / d
            if dir_path.exists():
                for f in dir_path.iterdir():
                    if f.stat().st_mtime < cutoff.timestamp():
                        f.unlink()
                        logger.info(f"🗑 清理: {f.name}")

    scheduler.add_job(
        cleanup_old_files,
        CronTrigger(hour=3, minute=0, timezone=tz),
        id="cleanup", name="日志清理"
    )

    # ============ 失败重试机制 (max_retries=3, 2分钟后首次重试) ============
    MAX_RETRIES = 3
    RETRY_DELAY_MINUTES = 2  # 首次重试2分钟，后续指数增长

    # 添加定期健康检查
    def health_check_job():
        log_health_status()

    # 每小时执行一次健康检查
    scheduler.add_job(
        health_check_job,
        CronTrigger(minute=0, timezone=tz),  # 每小时整点执行
        id="health_check",
        name="系统健康检查"
    )
    logger.info("   💚 系统健康检查已启用 [每小时]")

    def schedule_retry(job_id: str):
        """安排重试任务（遵守 quiet_hours）"""
        retry_count = job_retries.get(job_id, 0) + 1
        job_retries[job_id] = retry_count

        if retry_count < MAX_RETRIES:
            logger.warning(f"🔄 安排重试 [{job_id}]: {retry_count}/{MAX_RETRIES}")

            # 首次重试用 RETRY_DELAY_MINUTES，后续用 RETRY_DELAYS
            if retry_count == 1:
                delay_minutes = RETRY_DELAY_MINUTES
            else:
                delay_minutes = get_retry_delay(retry_count) // 60

            # 检查是否在静默时段
            run_time = datetime.now() + timedelta(minutes=delay_minutes)
            while is_quiet_hours(run_time):
                # 跳过静默时段，从 07:00 开始
                run_time = run_time.replace(hour=7, minute=0, second=0, microsecond=0)
                run_time += timedelta(days=1)

            scheduler.add_job(
                lambda: scheduler.print_jobs(job_id),  # 重新触发原任务
                trigger=DateTrigger(run_date=run_time),
                id=f"{job_id}_retry_{retry_count}",
                name=f"{job_id} 重试 {retry_count}"
            )
        else:
            logger.error(f"❌ 任务彻底失败 [{job_id}]，已达最大重试次数")
            job_retries.pop(job_id, None)

    job_retries = {}  # 重试计数

    def listener(event):
        if event.exception:
            logger.error(f"❌ 任务异常: {event.job_id}")
            # T-016.3: 发送告警
            original_job_id = event.job_id.rsplit('_retry_', 1)[0]
            error_msg = str(event.exception)[:200]
            send_failure_alert(original_job_id, f"任务执行异常: {error_msg}")
            # 安排重试
            schedule_retry(original_job_id)
        else:
            logger.info(f"✅ 任务完成: {event.job_id}")
            # 清除重试计数
            job_retries.pop(event.job_id, None)
            job_retries.pop(event.job_id.rsplit('_retry_', 1)[0], None)

    scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    def signal_handler(signum, frame):
        logger.info("📛 收到退出信号")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("📛 调度器已关闭")


if __name__ == "__main__":
    main()

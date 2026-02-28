from openclaw_api import OpenClawClient
#!/usr/bin/env python3
"""
知微定时任务调度器 v3.3 (2026-03-01 Phase 2)
特性: Prompt 外部化、支持热更新
"""

import os
import sys
import json
import signal
import subprocess
import logging
import logging.handlers
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

BASE_DIR = Path(__file__).parent
PROMPT_DIR = BASE_DIR / "prompts"
sys.path.insert(0, str(BASE_DIR))

from pusher import PushManager
from scheduler_queue import save_result, try_push

CONTAINER = "clawdbot"


# ============ 日志 ============

def setup_logging(log_dir: str = "logs", retention_days: int = 30):
    log_path = BASE_DIR / log_dir
    log_path.mkdir(exist_ok=True)

    logger = logging.getLogger("zhiwei-scheduler")
    logger.setLevel(logging.INFO)

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
    except Exception as e:
        logger.error(f"❌ Prompt 渲染失败 [{template_name}]: {e}")
        return content  # 降级返回原始模板

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

    return False, "Agent 调用失败，请检查 Docker 或网络连接"


# ============ 任务定义 ============

def job_morning_brief():
    """每日早报 09:30"""
    logger.info("📰 === 每日早报 ===")
    
    def _run():
        now = datetime.now()
        # Phase 2: 从文件加载 Prompt
        prompt = load_prompt("morning_brief", date=now.strftime('%Y年%m月%d日 %A'))

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
        
        success = try_push(file_path, push_manager, logger)
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info("✅ 早报推送完成")

    _run()


def job_noon_brief():
    """每日午报 14:30"""
    logger.info("🌤 === 每日午报 ===")
    
    def _run():
        now = datetime.now()
        # Phase 2: 从文件加载 Prompt
        prompt = load_prompt("noon_brief", time=now.strftime('%m月%d日 %H:%M'))

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
        logger.info(f"📦 结果已保存: {file_path}")
        
        success = try_push(file_path, push_manager, logger)
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info("✅ 午报推送完成")

    _run()


def job_us_market_open():
    """美股开盘 21:30（工作日）"""
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
    label = "早" if period == "morning" else "晚"
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
    logger.info("📄 === arXiv 论文精选 ===")
    
    def _run():
        ds = config.get("data_sources", {}).get("arxiv", {})
        categories = ",".join(ds.get("categories", ["cs.AI", "cs.LG"]))
        min_score = ds.get("min_score", 2)
        limit = ds.get("max_results", 10)
        
        # Phase 2: 从文件加载 Prompt
        prompt = load_prompt("arxiv", categories=categories, min_score=min_score, limit=limit)

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
        
        success = try_push(file_path, push_manager, logger)
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info("✅ arXiv 论文推送完成")

    _run()


def job_system_check():
    """系统巡检 09:00"""
    logger.info("🔧 === 系统巡检 ===")
    
    def _run():
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


# ============ 工具函数 ============

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
    logger.info("🤖 知微定时任务系统 v3.3 启动")
    logger.info("   架构: 调度器 → Agent(LLM) → 推送")
    logger.info("   特性: Prompt 外部化 (v3.3 Phase 2)")
    logger.info("=" * 50)

    tz = config.get("system", {}).get("timezone", "Asia/Shanghai")
    scheduler = BlockingScheduler(timezone=tz)

    job_map = {
        "morning_brief":   job_morning_brief,
        "noon_brief":      job_noon_brief,
        "us_market_open":  job_us_market_open,
        "us_market_close": job_us_market_close,
        "crypto_morning":  lambda: job_crypto("morning"),
        "crypto_evening":  lambda: job_crypto("evening"),
        "arxiv_papers":    job_arxiv,
        "system_check":    job_system_check,
    }

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

    def listener(event):
        if event.exception:
            logger.error(f"❌ 任务异常: {event.job_id}")
        else:
            logger.info(f"✅ 任务完成: {event.job_id}")

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

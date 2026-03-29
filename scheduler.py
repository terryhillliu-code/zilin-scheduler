#!/usr/bin/env python3
"""
知微定时任务调度器 v3.5 (2026-03-20)
特性: Prompt 外部化、支持热更新、重试机制、触发器监听

v47.0 重构: 拆分为 scheduler_core.py + scheduler_jobs.py + scheduler.py
"""

import os
import sys
import signal
from datetime import datetime
from pathlib import Path

# 导入核心模块
from scheduler_core import (
    logger, config, push_manager,
    setup_logging, load_config, log_task_metrics
)

# 导入任务模块
from scheduler_jobs import (
    job_morning_brief,
    job_noon_brief,
    job_info_brief,
    job_us_market_open,
    job_us_market_close,
    job_crypto,
    job_arxiv,
    job_system_check,
    job_system_metrics_report,
    job_obsidian_sync,
    job_fail_test,
    job_log_rotate,
    job_knowledge_classify,
    job_klib_sync,
    job_video_notes_organize,
    job_video_retry,
    job_asr_health_check,
    job_research_pipeline,
    job_graph_maintenance,
    job_daily_voice_task_summary,
    job_ws_health_check,
    job_vault_sync_master,
    job_intel_sync,
    job_intel_report,
    log_health_status,
)

# APScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

# 其他模块
import trigger_listener
from zhiwei_common import PushManager


# ============ 任务结果回调 ============

def job_result_listener(event):
    """任务执行结果监听器"""
    if event.exception:
        logger.error(f"❌ 任务失败: {event.job_id} - {event.exception}")
    else:
        logger.info(f"✅ 任务完成: {event.job_id}")


# ============ 主程序 ============

def main():
    global config, push_manager, logger

    # 加载配置
    config = load_config()

    # 设置日志
    logger = setup_logging(
        config.get("system", {}).get("log_dir", "logs"),
        config.get("system", {}).get("log_retention_days", 30)
    )

    # 初始化推送管理器
    push_manager = PushManager(config)

    logger.info("=" * 50)
    logger.info("🤖 知微定时任务系统 v3.5 启动")
    logger.info("   架构: 调度器 → Agent(LLM) → 推送")
    logger.info("   特性: Prompt 外部化、重试机制、触发器监听")
    logger.info("   v47.0: 模块化拆分 (core + jobs + main)")
    logger.info("=" * 50)

    # 记录启动健康状态
    log_health_status()

    # 创建调度器
    tz = config.get("system", {}).get("timezone", "Asia/Shanghai")
    scheduler = BlockingScheduler(timezone=tz)

    # 添加事件监听器
    scheduler.add_listener(job_result_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    # ============ 启动触发器监听器 ============
    try:
        trigger_listener.init(scheduler, logger)
        trigger_listener.start()
        logger.info("👀 触发器监听器已启动")
    except Exception as e:
        logger.warning(f"⚠️ 触发器监听器启动失败: {e}")
        logger.warning("   主定时任务不受影响")

    # ============ 任务映射表 ============
    job_map = {
        "morning_brief": job_morning_brief,
        "noon_brief": job_noon_brief,
        "us_market_open": job_us_market_open,
        "us_market_close": job_us_market_close,
        "crypto_morning": lambda: job_crypto("morning"),
        "crypto_evening": lambda: job_crypto("evening"),
        "arxiv_papers": job_arxiv,
        "system_check": job_system_check,
        "system_metrics": job_system_metrics_report,
        "obsidian_sync": job_obsidian_sync,
        "klib_sync": job_klib_sync,
        "video_notes_organize": job_video_notes_organize,
        "video_retry": job_video_retry,
        "asr_health_check": job_asr_health_check,
        "fail_test": job_fail_test,
        "log_rotate": job_log_rotate,
        "knowledge_classify": job_knowledge_classify,
        "research_pipeline": job_research_pipeline,
        "graph_maintenance": job_graph_maintenance,
        "daily_voice_task_summary": job_daily_voice_task_summary,
        "vault_sync_master": job_vault_sync_master,
        "intelligence_sync": job_intel_sync,
        "intelligence_report": job_intel_report,
        # ai_source_reminder 和 podcast_update 无对应函数，已禁用
    }

    # 动态添加 info_brief_XX 任务映射
    for hour in [7, 9, 11, 13, 15, 17, 19, 21]:
        job_name = f"info_brief_{hour:02d}"
        job_map[job_name] = lambda h=hour: job_info_brief(h)

    # ============ 注册定时任务 ============
    registered_jobs = []

    for job_name, job_conf in config.get("jobs", {}).items():
        if not job_conf.get("enabled", True):
            logger.info(f"⏭️ 任务已禁用: {job_name}")
            continue

        if job_name not in job_map:
            logger.warning(f"⚠️ 未知任务: {job_name}")
            continue

        # 获取触发器配置
        trigger_type = job_conf.get("trigger", "cron")
        trigger_args = job_conf.get("trigger_args", {})

        # 修复 (T-016.4): 显式合并顶层调度参数到 trigger_args
        for key in ["hour", "minute", "second", "day_of_week", "day", "month"]:
            if key in job_conf and key not in trigger_args:
                trigger_args[key] = job_conf[key]

        try:
            if trigger_type == "cron":
                trigger = CronTrigger(**trigger_args, timezone=tz)
            elif trigger_type == "date":
                trigger = DateTrigger(**trigger_args, timezone=tz)
            else:
                logger.warning(f"⚠️ 未知触发器类型: {trigger_type}")
                continue

            job_func = job_map[job_name]
            scheduler.add_job(
                job_func, 
                trigger, 
                id=job_name, 
                name=job_name,
                misfire_grace_time=7200  # 容忍 2 小时的错过执行（用于 Mac 唤醒后补执行）
            )
            registered_jobs.append(job_name)
            logger.info(f"✅ 已注册: {job_name}")

        except Exception as e:
            logger.error(f"❌ 注册任务失败 {job_name}: {e}")

    # ============ 信号处理 ============
    def signal_handler(signum, frame):
        logger.info(f"🛑 收到信号 {signum}，正在关闭...")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # ============ 启动调度器 ============
    logger.info(f"🚀 调度器启动，共 {len(registered_jobs)} 个任务")
    logger.info(f"   任务列表: {', '.join(registered_jobs)}")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("🛑 用户中断，正在关闭...")
        scheduler.shutdown(wait=False)
    except Exception as e:
        logger.error(f"❌ 调度器异常: {e}")
        raise


if __name__ == "__main__":
    main()
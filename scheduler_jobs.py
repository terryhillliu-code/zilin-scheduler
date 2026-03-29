#!/usr/bin/env python3
"""
知微调度器定时任务定义
包含所有 job_* 函数

从 scheduler.py 拆分 (v47.0)
"""

import os
import sys
import json
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

# 导入核心模块
from scheduler_core import (
    logger, config, push_manager,
    is_quiet_hours, get_retry_delay, log_task_metrics, send_failure_alert,
    load_prompt, call_agent, enrich_with_rag, save_output
)

# 导入其他模块
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from pusher import PushManager
from scheduler_queue import save_result, try_push, save_result_safe
from news_dedup import should_push, load_sent_today, get_sent_titles, record_sent, extract_titles_from_content
from lock_manager import acquire_lock, try_acquire_lock, release_lock
from price_cache import has_price_changed, update_price_cache
import trigger_listener

CONTAINER = "clawdbot"


# ============ GraphRAG (已禁用) ============
# LightRAG 已禁用，使用 LanceDB RAG 替代 (scheduler_core.enrich_with_rag)

def enrich_with_graphrag(task_name: str, prompt_text: str) -> str:
    """
    使用 GraphRAG 增强上下文 - 已禁用

    LightRAG 已禁用，使用 LanceDB RAG 替代。
    此函数保留作为占位符，避免调用方报错。
    """
    return ""


def enrich_with_klib(task_name: str, prompt_text: str, top_k: int = 5) -> str:
    """
    使用 klib.db 进行全文检索增强

    Args:
        task_name: 任务名称
        prompt_text: 原始 Prompt
        top_k: 返回数量

    Returns:
        增强后的上下文字符串
    """
    try:
        import sqlite3
        klib_path = Path.home() / "Documents" / "Library" / "klib.db"

        if not klib_path.exists():
            return ""

        # 简单关键词提取
        keywords = prompt_text[:100]

        conn = sqlite3.connect(str(klib_path))
        cursor = conn.execute("""
            SELECT title, summary FROM books
            WHERE title LIKE ? OR summary LIKE ?
            LIMIT ?
        """, (f"%{keywords}%", f"%{keywords}%", top_k))

        results = cursor.fetchall()
        conn.close()

        if results:
            context = "\n\n【知识库参考】\n"
            for title, summary in results:
                context += f"- {title}: {summary[:200] if summary else ''}\n"
            logger.info(f"📚 klib 增强: {len(results)} 条结果")
            return context
        return ""
    except Exception as e:
        logger.warning(f"klib 检索失败: {e}")
        return ""


# ============ 定时任务定义 ============

def job_morning_brief():
    """早报任务 (09:30)"""
    task_name = "morning_brief"
    start_time = time.time()

    try:
        logger.info(f"🌅 开始执行: {task_name}")

        # 加载 Prompt (修复: 注入 mode 变量)
        prompt = load_prompt("morning_brief", mode="晨间精选", date=datetime.now().strftime("%Y-%m-%d"))

        if not prompt:
            logger.warning("早报 Prompt 加载失败")
            return

        # RAG 增强
        if enrich_with_rag:
            rag_context = enrich_with_rag("早报", top_k=5)
            if rag_context:
                prompt = f"{rag_context}\n\n{prompt}"

        # 调用 Agent
        success, content = call_agent("researcher", prompt, timeout=600)

        if success:
            # 保存结果 (安全落盘：若今日已发则跳过)
            file_path, skipped = save_result_safe(task_name, content, targets=["feishu"])

            # 尝试推送
            if not skipped and not is_quiet_hours():
                try_push(file_path)

            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
        else:
            logger.error(f"早报生成失败: {content}")
            log_task_metrics(task_name, "failure", error=content)
            send_failure_alert(task_name, content)

    except Exception as e:
        logger.error(f"早报任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))
        send_failure_alert(task_name, str(e))


def job_noon_brief():
    """午报任务 (14:30)"""
    task_name = "noon_brief"
    start_time = time.time()

    try:
        logger.info(f"🌞 开始执行: {task_name}")

        prompt = load_prompt("noon_brief", date=datetime.now().strftime("%Y-%m-%d"))

        if not prompt:
            logger.warning("午报 Prompt 加载失败")
            return

        if enrich_with_rag:
            rag_context = enrich_with_rag("午报", top_k=5)
            if rag_context:
                prompt = f"{rag_context}\n\n{prompt}"

        success, content = call_agent("researcher", prompt, timeout=600)

        if success:
            file_path, skipped = save_result_safe(task_name, content, targets=["feishu"])
            if not skipped and not is_quiet_hours():
                try_push(file_path)
            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
        else:
            logger.error(f"午报生成失败: {content}")
            log_task_metrics(task_name, "failure", error=content)

    except Exception as e:
        logger.error(f"午报任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_info_brief(hour: int):
    """信息流简报 (每2小时)"""
    task_name = f"info_brief_{hour:02d}"
    start_time = time.time()

    try:
        logger.info(f"📡 开始执行: {task_name}")

        prompt = load_prompt("info_brief", hour=hour, date=datetime.now().strftime("%Y-%m-%d"))

        if not prompt:
            logger.warning("信息流 Prompt 加载失败")
            return

        # RAG 增强
        rag_context = enrich_with_rag("信息流", top_k=5)
        if rag_context:
            prompt = f"{rag_context}\n\n{prompt}"

        # 调用 Agent
        success, content = call_agent("researcher", prompt, timeout=600)

        if success:
            # 新闻去重检查
            titles = extract_titles_from_content(content)
            if not should_push(titles):
                logger.info(f"📭 {task_name}: 新闻已发送过，跳过")
                log_task_metrics(task_name, "skipped", extra={"reason": "duplicate"})
                return

            # 保存结果 (安全落盘：若今日已发则跳过)
            file_path, skipped = save_result_safe(task_name, content, targets=["feishu"])


            # 推送
            if not skipped and not is_quiet_hours():
                try_push(file_path)
                # 记录已发送
                for title in titles:
                    record_sent(title)

            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
        else:
            logger.error(f"信息流生成失败: {content}")
            log_task_metrics(task_name, "failure", error=content)

    except Exception as e:
        logger.error(f"信息流任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_us_market_open():
    """美股开盘提醒 (21:00)"""
    task_name = "us_market_open"
    start_time = time.time()

    try:
        logger.info(f"📈 开始执行: {task_name}")

        prompt = load_prompt("us_market_open")

        if not prompt:
            logger.warning("美股开盘 Prompt 加载失败")
            return

        success, content = call_agent("researcher", prompt, timeout=300)

        if success:
            file_path, skipped = save_result_safe(task_name, content, targets=["feishu"])
            if not skipped:
                try_push(file_path)
            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
        else:
            logger.error(f"美股开盘提醒失败: {content}")
            log_task_metrics(task_name, "failure", error=content)

    except Exception as e:
        logger.error(f"美股开盘任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_us_market_close():
    """美股收盘摘要 (08:00)"""
    task_name = "us_market_close"
    start_time = time.time()

    try:
        logger.info(f"📉 开始执行: {task_name}")

        prompt = load_prompt("us_market_close")

        if not prompt:
            logger.warning("美股收盘 Prompt 加载失败")
            return

        success, content = call_agent("researcher", prompt, timeout=300)

        if success:
            file_path, skipped = save_result_safe(task_name, content, targets=["feishu"])
            if not skipped:
                try_push(file_path)
            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
        else:
            logger.error(f"美股收盘摘要失败: {content}")
            log_task_metrics(task_name, "failure", error=content)

    except Exception as e:
        logger.error(f"美股收盘任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_crypto(period: str = "morning"):
    """加密货币行情"""
    task_name = f"crypto_{period}"
    start_time = time.time()

    try:
        logger.info(f"🪙 开始执行: {task_name}")

        prompt = load_prompt(f"crypto_{period}")

        if not prompt:
            logger.warning(f"加密货币 Prompt 加载失败: {period}")
            return

        success, content = call_agent("researcher", prompt, timeout=180)

        if success:
            file_path, skipped = save_result_safe(task_name, content, targets=["feishu"])
            if not skipped:
                try_push(file_path)
            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
        else:
            logger.error(f"加密货币行情失败: {content}")
            log_task_metrics(task_name, "failure", error=content)

    except Exception as e:
        logger.error(f"加密货币任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_arxiv():
    """ArXiv 论文精选 (07:00)"""
    task_name = "arxiv_papers"
    start_time = time.time()

    try:
        logger.info(f"📄 开始执行: {task_name}")

        # 调用 arxiv 处理脚本
        arxiv_script = BASE_DIR / "arxiv_processor.py"

        if arxiv_script.exists():
            result = subprocess.run(
                [sys.executable, str(arxiv_script)],
                capture_output=True,
                text=True,
                timeout=600
            )

            if result.returncode == 0:
                content = result.stdout
                file_path, skipped = save_result_safe(task_name, content, targets=["feishu"])
                if not skipped:
                    try_push(file_path)
                log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
            else:
                logger.error(f"ArXiv 处理失败: {result.stderr}")
                log_task_metrics(task_name, "failure", error=result.stderr)
        else:
            logger.warning("ArXiv 处理脚本不存在")

    except Exception as e:
        logger.error(f"ArXiv 任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def log_health_status():
    """记录系统健康状态"""
    try:
        # 检查关键服务
        services = {
            "llm_client": False,
            "rag": False,
            "pusher": False,
        }

        # 检查 LLM 客户端
        try:
            from scheduler_core import LLM_CLIENT_AVAILABLE
            services["llm_client"] = LLM_CLIENT_AVAILABLE
        except:
            pass

        # 检查 RAG
        try:
            from scheduler_core import RAG_AVAILABLE
            services["rag"] = RAG_AVAILABLE
        except:
            pass

        # 检查推送
        try:
            services["pusher"] = push_manager is not None
        except:
            pass

        status_str = " | ".join(f"{k}: {'✅' if v else '❌'}" for k, v in services.items())
        logger.info(f"💚 系统健康状态: {status_str}")

    except Exception as e:
        logger.warning(f"健康状态检查失败: {e}")


def job_system_check():
    """系统健康检查 (06:30)"""
    task_name = "system_check"
    start_time = time.time()

    try:
        logger.info(f"🔍 开始执行: {task_name}")

        # 执行健康检查脚本
        health_script = Path.home() / "zhiwei-health.sh"

        if health_script.exists():
            result = subprocess.run(
                ["bash", str(health_script)],
                capture_output=True,
                text=True,
                timeout=120
            )

            content = result.stdout if result.returncode == 0 else result.stderr
            save_result_safe(task_name, content, targets=["feishu"])
            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
        else:
            logger.warning("健康检查脚本不存在")
            log_health_status()

    except Exception as e:
        logger.error(f"系统检查异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_system_metrics_report():
    """运维报告 (每周一 09:00)"""
    task_name = "system_metrics"
    start_time = time.time()

    try:
        logger.info(f"📊 开始执行: {task_name}")

        # 生成运维报告
        prompt = load_prompt("system_metrics")

        if not prompt:
            logger.warning("运维报告 Prompt 加载失败")
            return

        success, content = call_agent("operator", prompt, timeout=300)

        if success:
            file_path, skipped = save_result_safe(task_name, content, targets=["feishu"])
            if not skipped:
                try_push(file_path)
            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
        else:
            logger.error(f"运维报告生成失败: {content}")

    except Exception as e:
        logger.error(f"运维报告异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_obsidian_sync():
    """Obsidian 笔记同步 (02:00)"""
    task_name = "obsidian_sync"
    start_time = time.time()

    try:
        logger.info(f"📝 开始执行: {task_name}")

        # 调用向量化脚本
        sync_script = BASE_DIR.parent / "zhiwei-rag" / "ingest" / "ingest_obsidian.py"

        if sync_script.exists():
            result = subprocess.run(
                [sys.executable, str(sync_script)],
                capture_output=True,
                text=True,
                timeout=1800  # 30 分钟
            )

            if result.returncode == 0:
                logger.info(f"Obsidian 同步完成")
                log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
            else:
                logger.error(f"Obsidian 同步失败: {result.stderr}")
                log_task_metrics(task_name, "failure", error=result.stderr)
        else:
            logger.warning("Obsidian 同步脚本不存在")

    except Exception as e:
        logger.error(f"Obsidian 同步异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_fail_test():
    """测试用故障注入任务"""
    logger.info("🧪 故障注入测试")
    raise Exception("测试故障")


def job_log_rotate():
    """日志轮转 (03:00)"""
    task_name = "log_rotate"
    start_time = time.time()

    try:
        logger.info(f"📋 开始执行: {task_name}")

        log_dir = Path.home() / "logs"

        # 清理 30 天前的日志
        cutoff = datetime.now() - timedelta(days=30)
        cleaned = 0

        for log_file in log_dir.glob("*.log.*"):
            try:
                mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                if mtime < cutoff:
                    log_file.unlink()
                    cleaned += 1
            except:
                pass

        logger.info(f"日志清理完成: {cleaned} 个文件")
        log_task_metrics(task_name, "success", extra={"cleaned": cleaned})

    except Exception as e:
        logger.error(f"日志轮转异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_knowledge_classify():
    """知识管线分类 (23:00)"""
    task_name = "knowledge_classify"
    start_time = time.time()

    try:
        logger.info(f"📚 开始执行: {task_name}")

        # 调用分类脚本
        classify_script = BASE_DIR / "knowledge_pipeline.py"

        if classify_script.exists():
            result = subprocess.run(
                [sys.executable, str(classify_script), "--classify"],
                capture_output=True,
                text=True,
                timeout=600
            )

            if result.returncode == 0:
                logger.info(f"知识分类完成")
                log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
            else:
                logger.error(f"知识分类失败: {result.stderr}")
                log_task_metrics(task_name, "failure", error=result.stderr)
        else:
            logger.warning("知识分类脚本不存在")

    except Exception as e:
        logger.error(f"知识分类异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_klib_sync():
    """klib 同步 (03:00)"""
    task_name = "klib_sync"
    start_time = time.time()

    try:
        logger.info(f"📖 开始执行: {task_name}")

        # klib 同步逻辑
        klib_path = Path.home() / "Documents" / "Library" / "klib.db"

        if klib_path.exists():
            # klib 向量化逻辑 (实质性同步)
            vectorize_script = BASE_DIR / "tasks" / "klib_vectorize.py"
            if vectorize_script.exists():
                logger.info("📖 启动 klib 向量化同步...")
                result = subprocess.run(
                    [sys.executable, str(vectorize_script)],
                    capture_output=True,
                    text=True,
                    timeout=1800
                )
                if result.returncode == 0:
                    logger.info("✅ klib 同步与向量化完成")
                    log_task_metrics(task_name, "success")
                else:
                    logger.error(f"❌ klib 同步失败: {result.stderr}")
                    log_task_metrics(task_name, "failure", error=result.stderr)
            else:
                logger.warning(f"⚠️ 向量化脚本不存在: {vectorize_script}")
        else:
            logger.warning("klib 数据库不存在")

    except Exception as e:
        logger.error(f"klib 同步异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_video_notes_organize():
    """视频笔记整理"""
    task_name = "video_notes_organize"
    start_time = time.time()

    try:
        logger.info(f"🎬 开始执行: {task_name}")

        # 视频笔记目录
        video_dir = Path.home() / "Documents" / "ZhiweiVault" / "40-49_视频笔记"

        if video_dir.exists():
            # 统计笔记数量
            notes = list(video_dir.glob("**/*.md"))
            logger.info(f"视频笔记数量: {len(notes)}")
            log_task_metrics(task_name, "success", extra={"notes": len(notes)})
        else:
            logger.warning("视频笔记目录不存在")

    except Exception as e:
        logger.error(f"视频笔记整理异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_research_pipeline():
    """研报扫描与向量化 (23:30)"""
    task_name = "research_pipeline"
    start_time = time.time()

    try:
        logger.info(f"📊 开始执行: {task_name}")

        # 调用研报处理脚本
        pipeline_script = BASE_DIR / "research_processor.py"

        if pipeline_script.exists():
            result = subprocess.run(
                [sys.executable, str(pipeline_script)],
                capture_output=True,
                text=True,
                timeout=1800
            )

            if result.returncode == 0:
                logger.info(f"研报处理完成")
                log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
            else:
                logger.error(f"研报处理失败: {result.stderr}")
                log_task_metrics(task_name, "failure", error=result.stderr)
        else:
            logger.warning("研报处理脚本不存在")

    except Exception as e:
        logger.error(f"研报管线异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_vault_sync_master():
    """
    ArXiv-Obsidian 搜索完备性对齐 (Research V4.4)
    调用 reconcile_obsidian.py v3.0 进行全库同步
    """
    task_name = "vault_sync_master"
    start_time = time.time()

    try:
        logger.info("开始执行 [VaultSyncMaster 全量同步] 任务...")

        # 脚本路径
        script_path = Path.home() / "zhiwei-rag" / "scripts" / "reconcile_obsidian.py"

        if script_path.exists():
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=1800  # 30 分钟
            )

            if result.returncode == 0:
                logger.info(f"VaultSyncMaster 全量同步完成:\n{result.stdout}")
                log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
            else:
                logger.error(f"VaultSyncMaster 全量同步失败: {result.stderr}")
                log_task_metrics(task_name, "failure", error=result.stderr)
        else:
            logger.warning("VaultSyncMaster 全量同步脚本不存在")

    except Exception as e:
        logger.error(f"VaultSyncMaster 全量同步任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_graph_maintenance():
    """GraphRAG 图谱维护 (02:30)"""
    task_name = "graph_maintenance"
    start_time = time.time()

    try:
        logger.info(f"🕸️ 开始执行: {task_name}")

        # GraphRAG 维护脚本
        graph_script = BASE_DIR / "graph_maintenance.py"

        if graph_script.exists():
            result = subprocess.run(
                [sys.executable, str(graph_script)],
                capture_output=True,
                text=True,
                timeout=3600
            )

            if result.returncode == 0:
                logger.info(f"图谱维护完成")
                log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
            else:
                logger.error(f"图谱维护失败: {result.stderr}")
                log_task_metrics(task_name, "failure", error=result.stderr)
        else:
            logger.warning("图谱维护脚本不存在")

    except Exception as e:
        logger.error(f"图谱维护异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_daily_voice_task_summary():
    """每日语音任务汇总"""
    task_name = "daily_voice_task_summary"
    start_time = time.time()

    try:
        logger.info(f"🎤 开始执行: {task_name}")

        # 调用语音任务汇总脚本
        summary_script = BASE_DIR.parent / "zhiwei-bot" / "voice_task_summary.py"

        if summary_script.exists():
            result = subprocess.run(
                [sys.executable, str(summary_script)],
                capture_output=True,
                text=True,
                timeout=300
            )

            if result.returncode == 0:
                content = result.stdout
                if content:
                    file_path, _ = save_result_safe(task_name, content, targets=["feishu"], force=True)
                    try_push(file_path)
                log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
            else:
                logger.error(f"语音任务汇总失败: {result.stderr}")
                log_task_metrics(task_name, "failure", error=result.stderr)
        else:
            logger.warning("语音任务汇总脚本不存在")

    except Exception as e:
        logger.error(f"语音任务汇总异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_ws_health_check():
    """WebSocket 健康检查 (每5分钟)"""
    task_name = "ws_health_check"

    try:
        # 检查飞书 WebSocket 进程
        result = subprocess.run(
            ["launchctl", "list"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if "zhiwei.bot" in result.stdout:
            logger.debug("💚 飞书服务正常")
        else:
            logger.error("⚠️ 飞书服务未运行")

        # 检查日志活跃度
        log_file = Path.home() / "logs" / "feishu_bot.log"
        if log_file.exists():
            mtime = os.path.getmtime(log_file)
            age_seconds = time.time() - mtime

            if age_seconds > 600:
                logger.warning(f"⚠️ 飞书日志 {int(age_seconds/60)} 分钟无更新")

    except subprocess.TimeoutExpired:
        logger.error("⚠️ WebSocket 健康检查超时")
    except Exception as e:
        logger.error(f"⚠️ WebSocket 健康检查失败: {e}")


# ============ 导出 ============

def job_intel_sync():
    """情报中心自动化同步 (v5.5)"""
    task_name = "intel_sync"
    start_time = time.time()

    try:
        logger.info(f"📡 开始执行: {task_name}")
        script_path = Path.home() / "zhiwei-rag" / "scripts" / "intel_sync.py"
        python_executable = Path.home() / "zhiwei-rag" / "venv" / "bin" / "python3"

        if script_path.exists():
            result = subprocess.run(
                [str(python_executable), str(script_path)],
                capture_output=True,
                text=True,
                timeout=1200 # 20 分钟
            )

            if result.returncode == 0:
                logger.info(f"情报同步完成: {result.stdout.splitlines()[-1] if result.stdout else ''}")
                log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
            else:
                logger.error(f"情报同步失败: {result.stderr}")
                log_task_metrics(task_name, "failure", error=result.stderr)
        else:
            logger.warning("情报同步脚本不存在")

    except Exception as e:
        logger.error(f"情报同步任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_intel_report():
    """情报中心周期性简报生成 (v5.5)"""
    task_name = "intel_report"
    start_time = time.time()

    try:
        logger.info(f"📊 开始执行: {task_name}")
        script_path = Path.home() / "zhiwei-rag" / "scripts" / "intel_reporter.py"
        python_executable = Path.home() / "zhiwei-rag" / "venv" / "bin" / "python3"

        if script_path.exists():
            result = subprocess.run(
                [str(python_executable), str(script_path)],
                capture_output=True,
                text=True,
                timeout=300
            )

            if result.returncode == 0:
                logger.info(f"情报简报生成完成")
                # 尝试从输出中提取报告路径
                for line in result.stdout.splitlines():
                    if "情报简报已生成并存入" in line:
                        report_path_str = line.split(":")[-1].strip()
                        if os.path.exists(report_path_str):
                            try_push(Path(report_path_str))
                
                log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
            else:
                logger.error(f"情报简报生成失败: {result.stderr}")
                log_task_metrics(task_name, "failure", error=result.stderr)
        else:
            logger.warning("情报简报生成脚本不存在")

    except Exception as e:
        logger.error(f"情报简报任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


# ============ 视频处理重试任务 ============

def job_video_retry():
    """重试失败的视频处理任务

    检查 video_history.db 中可重试的失败记录，
    自动重新处理。
    """
    task_name = "video_retry"
    start_time = time.time()

    logger.info("📹 开始检查失败的视频任务...")

    try:
        # 导入 video_history 模块
        zhiwei_bot_dir = Path.home() / "zhiwei-bot"
        if str(zhiwei_bot_dir) not in sys.path:
            sys.path.insert(0, str(zhiwei_bot_dir))

        from video_history import get_video_history, RETRYABLE_ERRORS, MAX_RETRIES

        history = get_video_history()
        failed_records = history.get_failed_for_retry(limit=5)

        if not failed_records:
            logger.info("没有可重试的失败视频")
            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
            return

        logger.info(f"发现 {len(failed_records)} 个可重试的失败视频")

        # 导入 media_handler (函数名修正: process_video 不是 process_video_url)
        from media_handler import process_video

        success_count = 0
        for record in failed_records:
            url = record['url']
            retry_count = record['retry_count']

            logger.info(f"重试视频 ({retry_count + 1}/{MAX_RETRIES}): {url[:60]}...")

            try:
                # process_video 需要 text 参数，URL 本身即可作为 text
                result = process_video(url)
                if "✅" in result:
                    success_count += 1
                    logger.info(f"视频重试成功: {url[:50]}...")
                else:
                    logger.warning(f"视频重试失败: {result[:100]}")
            except Exception as e:
                logger.error(f"视频重试异常: {e}")

        logger.info(f"视频重试完成: {success_count}/{len(failed_records)} 成功")
        log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))

    except ImportError as e:
        logger.warning(f"无法导入视频处理模块: {e}")
        log_task_metrics(task_name, "failure", error=str(e))
    except Exception as e:
        logger.error(f"视频重试任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


# ============ ASR 服务健康检查 ============

def job_asr_health_check():
    """ASR 服务健康检查

    定期检查 DashScope ASR 和本地 Whisper 可用性，
    发现问题记录到日志。
    """
    task_name = "asr_health_check"
    start_time = time.time()

    logger.info("🏥 开始 ASR 服务健康检查...")

    try:
        zhiwei_bot_dir = Path.home() / "zhiwei-bot"
        script_path = zhiwei_bot_dir / "scripts" / "asr_health_check.py"
        # 使用共享 venv (v2.0 合并后)
        venv_python = Path.home() / "zhiwei-shared-venv" / "bin" / "python"

        if not script_path.exists():
            logger.warning(f"健康检查脚本不存在: {script_path}")
            return

        if not venv_python.exists():
            logger.warning(f"共享 venv 不存在: {venv_python}")
            return

        # 运行健康检查
        result = subprocess.run(
            [str(venv_python), str(script_path), "--json"],
            capture_output=True,
            text=True,
            timeout=60,
            env={**os.environ, "PYTHONPATH": str(zhiwei_bot_dir)}
        )

        if result.returncode == 0:
            import json
            health_data = json.loads(result.stdout)
            status = health_data.get("status", "unknown")

            if status == "healthy":
                logger.info("✅ ASR 服务健康检查通过")
            else:
                logger.warning(f"⚠️ ASR 服务状态: {status}")
                # 检查具体问题
                for check in health_data.get("checks", []):
                    if check.get("error"):
                        logger.warning(f"   {check.get('service', 'check')}: {check['error']}")

            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
        else:
            logger.error(f"健康检查执行失败: {result.stderr}")
            log_task_metrics(task_name, "failure", error=result.stderr[:200])

    except subprocess.TimeoutExpired:
        logger.error("健康检查超时")
        log_task_metrics(task_name, "failure", error="timeout")
    except Exception as e:
        logger.error(f"健康检查异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


__all__ = [
    # 任务函数
    'job_morning_brief',
    'job_noon_brief',
    'job_info_brief',
    'job_us_market_open',
    'job_us_market_close',
    'job_crypto',
    'job_arxiv',
    'job_system_check',
    'job_system_metrics_report',
    'job_fail_test',
    'job_log_rotate',
    'job_knowledge_classify',
    'job_klib_sync',
    'job_video_notes_organize',
    'job_video_retry',
    'job_asr_health_check',
    'job_research_pipeline',
    'job_vault_sync_master',
    'job_graph_maintenance',
    'job_daily_voice_task_summary',
    'job_ws_health_check',
    'job_intel_sync',
    'job_intel_report',
    # 辅助函数
    'enrich_with_graphrag',
    'enrich_with_klib',
    'log_health_status',
]
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
import scheduler_core
from scheduler_core import (
    logger, config,
    is_quiet_hours, get_retry_delay, log_task_metrics, send_failure_alert,
    load_prompt, call_agent, enrich_with_rag, save_output
)

# 导入其他模块
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(Path.home()))  # ⭐ v65.2: 添加 HOME 目录以支持 zhiwei_agent 绝对导入
sys.path.insert(0, str(Path.home() / "zhiwei_agent"))

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


def _collect_real_news_sources() -> str:
    """收集真实数据源用于早报生成 - v66.1 多源聚合

    数据源：
    1. 实时搜索：当天 AI/科技新闻
    2. Hacker News Top 5
    3. GitHub Trending
    4. 情报中心
    5. 最新论文
    """
    sources = []
    today = datetime.now().strftime("%Y-%m-%d")
    inbox_path = Path.home() / "Documents" / "ZhiweiVault" / "Inbox"

    # 1. RSS 实时新闻（替代 DuckDuckGo 搜索）
    RSS_FEEDS = [
        ("TechCrunch AI", "https://techcrunch.com/category/artificial-intelligence/feed/"),
        ("MIT Tech Review", "https://www.technologyreview.com/feed/"),
        ("Ars Technica", "https://feeds.arstechnica.com/arstechnica/technology-lab"),
    ]

    rss_news = []
    for name, url in RSS_FEEDS:
        try:
            from tools.rss_feed import RSSFeedTool
            rss_tool = RSSFeedTool()
            result = rss_tool.execute(url=url, limit=5)
            if result.success and result.data.get("articles"):
                for article in result.data["articles"][:3]:
                    title = article.get("title", "")[:60]
                    link = article.get("link", "")
                    summary = article.get("summary", "")[:80]
                    rss_news.append(f"- [{name}] **{title}**\n  {summary}\n  > [链接]({link})")
        except Exception as e:
            logger.warning(f"RSS {name} 获取失败: {e}")

    if rss_news:
        sources.append(f"### 🔴 实时新闻\n" + "\n".join(rss_news[:10]))
        logger.info(f"🔍 已获取 RSS 新闻 {len(rss_news)} 条")

    # 2. Hacker News Top 5
    hn_file = inbox_path / f"NEWS_{today}_Hacker-News-Top5.md"
    if not hn_file.exists():
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        hn_file = inbox_path / f"NEWS_{yesterday}_Hacker-News-Top5.md"

    if hn_file.exists():
        hn_content = hn_file.read_text(encoding="utf-8")
        if "## 内容" in hn_content:
            sources.append(f"### 📰 Hacker News\n{hn_content.split('## 内容')[1].strip()[:1000]}")
            logger.info(f"📰 已加载 HN")

    # 3. GitHub Trending
    github_file = inbox_path / f"NEWS_{today}_GitHub-Trending-AI.md"
    if not github_file.exists():
        for i in range(1, 8):
            past_date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            github_file = inbox_path / f"NEWS_{past_date}_GitHub-Trending-AI.md"
            if github_file.exists():
                break

    if github_file.exists():
        github_content = github_file.read_text(encoding="utf-8")
        if "## 内容" in github_content:
            sources.append(f"### 🐙 GitHub Trending\n{github_content.split('## 内容')[1].strip()[:800]}")
            logger.info(f"🐙 已加载 GitHub")

    # 4. 情报中心（近3天）
    intel_files = sorted(inbox_path.glob("2026-*_深度情报*.md"), reverse=True)[:3]
    if intel_files:
        intel_items = []
        for f in intel_files:
            content = f.read_text(encoding="utf-8")
            title = f.stem.replace("深度情报：", "")[:50]
            source_url = ""
            if "source_url:" in content:
                import re
                url_match = re.search(r'source_url:\s*"([^"]+)"', content)
                if url_match:
                    source_url = url_match.group(1)
            if source_url:
                intel_items.append(f"- **{title}**\n  > [来源]({source_url})")
            else:
                intel_items.append(f"- **{title}**")
        sources.append(f"### 📡 情报中心\n" + "\n".join(intel_items))
        logger.info(f"📡 已加载情报 {len(intel_files)} 条")

    # 5. 最新论文
    paper_files = sorted(inbox_path.glob("PAPER_*.md"), reverse=True)[:3]
    if paper_files:
        paper_items = [f"- {f.stem.split('_', 2)[-1][:50] if '_' in f.stem else f.stem[:50]}" for f in paper_files]
        sources.append(f"### 📄 最新论文\n" + "\n".join(paper_items))

    if not sources:
        return "⚠️ 今日暂无数据源"

    return "\n\n".join(sources)


# ============ 定时任务定义 ============

def job_morning_brief():
    """早报任务 (09:30) - v66.0: 只使用真实数据源，禁止幻觉"""
    task_name = "morning_brief"
    start_time = time.time()

    try:
        logger.info(f"🌅 开始执行: {task_name}")

        # 收集真实数据源
        real_data = _collect_real_news_sources()

        # 加载 Prompt
        prompt = load_prompt("morning_brief",
                            date=datetime.now().strftime("%Y-%m-%d"),
                            time=datetime.now().strftime("%H:%M"),
                            real_data=real_data)

        if not prompt:
            logger.warning("早报 Prompt 加载失败")
            return

        # 调用 Agent
        success, content = call_agent("researcher", prompt, timeout=300)

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
    """午报任务 (14:30) - v66.0: 只使用真实数据源，禁止幻觉"""
    task_name = "noon_brief"
    start_time = time.time()

    try:
        logger.info(f"🌞 开始执行: {task_name}")

        # 收集真实数据源
        real_data = _collect_real_news_sources()

        # 加载 Prompt
        prompt = load_prompt("noon_brief",
                            date=datetime.now().strftime("%Y-%m-%d"),
                            time=datetime.now().strftime("%H:%M"),
                            real_data=real_data)

        if not prompt:
            logger.warning("午报 Prompt 加载失败")
            return

        success, content = call_agent("researcher", prompt, timeout=300)

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


def _collect_us_market_news() -> str:
    """收集美股相关新闻 - v66.1 RSS 数据源"""
    sources = []

    # 财经 RSS 源
    FINANCE_FEEDS = [
        ("Yahoo Finance", "https://finance.yahoo.com/news/rssindex"),
        ("MarketWatch", "https://www.marketwatch.com/rss/topstories"),
        ("SeekingAlpha", "https://seekingalpha.com/market_currents.xml"),
    ]

    finance_news = []
    for name, url in FINANCE_FEEDS:
        try:
            from tools.rss_feed import RSSFeedTool
            rss_tool = RSSFeedTool()
            result = rss_tool.execute(url=url, limit=5)
            if result.success and result.data.get("articles"):
                for article in result.data["articles"][:3]:
                    title = article.get("title", "")[:60]
                    link = article.get("link", "")
                    finance_news.append(f"- [{name}] **{title}**\n  > [链接]({link})")
        except Exception as e:
            logger.warning(f"RSS {name} 获取失败: {e}")

    if finance_news:
        sources.append(f"### 📊 财经新闻\n" + "\n".join(finance_news[:9]))
        logger.info(f"📊 已获取财经新闻 {len(finance_news)} 条")

    if not sources:
        return "⚠️ 暂无财经数据"

    return "\n\n".join(sources)


def job_us_market_open():
    """美股开盘提醒 (21:00) - v66.0: 只使用真实数据源"""
    task_name = "us_market_open"
    start_time = time.time()

    try:
        logger.info(f"📈 开始执行: {task_name}")

        # 收集真实数据源
        real_data = _collect_us_market_news()

        prompt = load_prompt("us_market_open",
                        date=datetime.now().strftime("%Y-%m-%d"),
                        time=datetime.now().strftime("%H:%M"),
                        real_data=real_data)

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
    """美股收盘摘要 (08:00) - v66.0: 只使用真实数据源"""
    task_name = "us_market_close"
    start_time = time.time()

    try:
        logger.info(f"📉 开始执行: {task_name}")

        # 收集真实数据源
        real_data = _collect_us_market_news()

        prompt = load_prompt("us_market_close",
                        date=datetime.now().strftime("%Y-%m-%d"),
                        time=datetime.now().strftime("%H:%M"),
                        real_data=real_data)

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


def job_arxiv():
    """ArXiv 论文精选 (07:00) - v65.0 改造：使用 ArxivSearchTool"""
    task_name = "arxiv_papers"
    start_time = time.time()

    try:
        logger.info(f"📄 开始执行: {task_name}")

        # 使用 ArxivSearchTool 获取论文
        from tools.arxiv_search import ArxivSearchTool

        tool = ArxivSearchTool()
        result = tool.execute(
            query="LLM inference distributed training accelerator",
            category="cs.DC",  # 分布式计算
            limit=8,
            with_summary=True  # 调用 LLM 总结
        )

        if result.success and result.data["papers"]:
            papers = result.data["papers"]
            trend = result.data.get("trend", "")

            # 格式化输出
            content = f"""# 📚 arXiv 论文精选

> 生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}

## 论文推荐

"""
            for i, p in enumerate(papers, 1):
                content += f"""### {i}. [{p['title']}]({p['url']})

- **核心贡献**: {p.get('core_contribution', '暂无总结')}
- **作者**: {', '.join(p['authors'][:3])}
- **分类**: {', '.join(p['categories'][:2])}

"""

            if trend:
                content += f"""## 🔬 趋势分析

{trend}
"""

            # 保存并推送
            file_path, skipped = save_result_safe(task_name, content, targets=["feishu"])
            if not skipped:
                try_push(file_path)
            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
        else:
            logger.warning(f"ArXiv 搜索无结果: {result.error or '未找到论文'}")
            log_task_metrics(task_name, "skipped", extra={"reason": "no_papers"})

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
            services["pusher"] = scheduler_core.push_manager is not None
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


def job_video_notes_organize():
    """视频笔记整理 - v65.0 改造：归档+分类+重命名+索引"""
    task_name = "video_notes_organize"
    start_time = time.time()

    # 主题关键词配置（v65.0 扩展）
    THEME_KEYWORDS = {
        "AI": ["LLM", "GPT", "Claude", "transformer", "AI", "人工智能", "机器学习", "深度学习", "神经网络", "ChatGPT", "大模型", "AIGC", "RAG", "Agent"],
        "半导体": ["芯片", "GPU", "NVIDIA", "制程", "半导体", "晶圆", "芯片设计", "EDA", "光刻", "封装", "AMD", "Intel", "台积电"],
        "编程": ["Python", "代码", "开发", "编程", "算法", "软件工程", "GitHub", "API", "框架", "调试", "测试"],
        "创业": ["创业", "融资", "商业", "产品", "用户", "增长", "投资人", "赛道", "PMF", "MVP"],
        "投资": ["股票", "基金", "投资", "理财", "交易", "财报", "估值", "市场", "加密货币", "BTC", "ETH"],
        "效率": ["效率", "工具", "工作流", "自动化", "笔记", "Obsidian", "时间管理", "GTD"],
    }

    try:
        logger.info(f"🎬 开始执行: {task_name}")

        video_dir = Path.home() / "Documents" / "ZhiweiVault" / "40-49_视频笔记"

        if not video_dir.exists():
            logger.warning("视频笔记目录不存在")
            return

        notes = list(video_dir.glob("*.md"))  # 只处理顶层笔记
        organized_count = 0

        for note in notes:
            # 跳过索引文件
            if note.name.lower() in ["readme.md", "index.md"]:
                continue

            content = note.read_text(encoding="utf-8")
            stat = note.stat()

            # 1. 确定主题
            theme = "其他"
            for t, keywords in THEME_KEYWORDS.items():
                if any(kw.lower() in content.lower() for kw in keywords):
                    theme = t
                    break

            # 2. 确定日期（使用修改时间）
            mtime = datetime.fromtimestamp(stat.st_mtime)
            year_month = mtime.strftime("%Y-%m")

            # 3. 创建目标目录
            theme_dir = video_dir / theme
            date_dir = theme_dir / year_month
            date_dir.mkdir(parents=True, exist_ok=True)

            # 4. 新文件名
            date_prefix = mtime.strftime("%Y-%m-%d")
            new_name = f"VIDEO_{date_prefix}_{note.stem[:30]}.md"
            new_path = date_dir / new_name

            # 5. 移动文件（如果目标不同）
            if note.resolve() != new_path.resolve():
                note.rename(new_path)
                organized_count += 1
                logger.info(f"整理: {note.name} → {theme}/{year_month}/{new_name}")

        # 6. 生成索引文件
        index_path = video_dir / "README.md"
        index_content = f"""# 视频笔记索引

> 更新时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}

## 目录结构

"""
        for theme_dir in sorted(video_dir.iterdir()):
            if theme_dir.is_dir() and not theme_dir.name.startswith("."):
                note_count = len(list(theme_dir.glob("**/*.md")))
                index_content += f"- **{theme_dir.name}** ({note_count} 篇)\n"

        index_path.write_text(index_content, encoding="utf-8")

        logger.info(f"✅ 视频笔记整理完成: 整理 {organized_count} 篇，索引已更新")
        log_task_metrics(task_name, "success",
                        duration_ms=int((time.time() - start_time) * 1000),
                        extra={"organized": organized_count})

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
        # ⭐ v62.0 修复：使用 zhiwei-rag 的 venv（lancedb 在此安装）
        python_executable = Path.home() / "zhiwei-rag" / "venv" / "bin" / "python3"

        if script_path.exists() and python_executable.exists():
            result = subprocess.run(
                [str(python_executable), str(script_path)],
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
            missing = []
            if not script_path.exists():
                missing.append(f"脚本: {script_path}")
            if not python_executable.exists():
                missing.append(f"Python: {python_executable}")
            logger.warning(f"VaultSyncMaster 缺失: {', '.join(missing)}")
            log_task_metrics(task_name, "skipped", extra={"missing": missing})

    except Exception as e:
        logger.error(f"VaultSyncMaster 全量同步任务异常: {e}")
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


def job_douyin_health_check():
    """Douyin API 健康检查

    定期检查 Douyin API 服务可用性，
    发现问题自动重启并记录到日志。
    """
    task_name = "douyin_health_check"
    start_time = time.time()

    logger.info("🎬 开始 Douyin API 健康检查...")

    try:
        zhiwei_bot_dir = Path.home() / "zhiwei-bot"
        script_path = zhiwei_bot_dir / "scripts" / "douyin_health_check.py"

        # 使用系统 Python 或共享 venv
        venv_python = Path.home() / "zhiwei-shared-venv" / "bin" / "python"
        python_cmd = str(venv_python) if venv_python.exists() else "python3"

        if not script_path.exists():
            logger.warning(f"健康检查脚本不存在: {script_path}")
            return

        # 运行健康检查（带自动重启）
        result = subprocess.run(
            [python_cmd, str(script_path), "--json", "--restart"],
            capture_output=True,
            text=True,
            timeout=30,
            env={**os.environ, "PYTHONPATH": str(zhiwei_bot_dir)}
        )

        if result.returncode == 0:
            import json
            health_data = json.loads(result.stdout)
            status = health_data.get("status", "unknown")

            if status == "healthy":
                logger.info("✅ Douyin API 健康检查通过")
            elif status == "recovered":
                logger.info("✅ Douyin API 已自动恢复")
                # 发送恢复通知
                _send_douyin_recovery_notification()
            else:
                logger.warning(f"⚠️ Douyin API 状态: {status}")
                # 检查具体问题
                for check in health_data.get("checks", []):
                    if check.get("error"):
                        logger.warning(f"   {check.get('service', check.get('check', 'check'))}: {check['error']}")

            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
        else:
            logger.error(f"Douyin 健康检查执行失败: {result.stderr}")
            log_task_metrics(task_name, "failure", error=result.stderr[:200])

    except subprocess.TimeoutExpired:
        logger.error("Douyin 健康检查超时")
        log_task_metrics(task_name, "failure", error="timeout")
    except Exception as e:
        logger.error(f"Douyin 健康检查异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_llm_health_check():
    """LLM API 健康检查 ⭐ v68.0

    定期检查三层 API (Coding Plan, DashScope, OpenRouter) 可用性，
    发现问题自动告警到钉钉，并记录统计信息。
    支持报告归档，便于历史追溯。
    """
    task_name = "llm_health_check"
    start_time = time.time()

    logger.info("🤖 开始 LLM API 健康检查...")

    try:
        script_path = BASE_DIR / "scripts" / "llm_health_check.py"
        venv_python = Path.home() / "zhiwei-shared-venv" / "bin" / "python"
        python_cmd = str(venv_python) if venv_python.exists() else "python3"

        if not script_path.exists():
            logger.warning(f"健康检查脚本不存在: {script_path}")
            return

        # 运行健康检查（JSON 格式输出，自动告警，保存报告）
        result = subprocess.run(
            [python_cmd, str(script_path), "--json", "--alert", "--save"],
            capture_output=True,
            text=True,
            timeout=120,
            env={**os.environ, "PYTHONPATH": str(Path.home() / "zhiwei-common")}
        )

        if result.returncode == 0:
            health_data = json.loads(result.stdout)
            status = health_data.get("status", "unknown")

            if status == "healthy":
                logger.info("✅ LLM API 三层全部可用")
            elif status == "warning":
                logger.warning("⚠️ LLM API 部分层不可用")
                for issue in health_data.get("issues", []):
                    logger.warning(f"   - {issue}")
            elif status == "critical":
                logger.error("🔴 LLM API 全层不可用")

            # 记录统计信息
            stats = health_data.get("stats", {})
            for api_name, api_stats in stats.items():
                logger.info(f"   {api_name}: 成功 {api_stats['success']}, 失败 {api_stats['fail']}")

            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))

        elif result.returncode == 1:
            logger.warning("⚠️ LLM 健康检查发现警告级问题")
            try:
                health_data = json.loads(result.stdout)
                for issue in health_data.get("issues", []):
                    logger.warning(f"   - {issue}")
            except:
                pass
            log_task_metrics(task_name, "warning", duration_ms=int((time.time() - start_time) * 1000))

        elif result.returncode == 2:
            logger.error("🔴 LLM 健康检查发现严重问题")
            try:
                health_data = json.loads(result.stdout)
                for issue in health_data.get("issues", []):
                    logger.error(f"   - {issue}")
            except:
                pass
            log_task_metrics(task_name, "critical", duration_ms=int((time.time() - start_time) * 1000))

        else:
            logger.warning(f"LLM 健康检查异常: {result.stderr}")
            log_task_metrics(task_name, "failure", error=result.stderr[:200])

    except subprocess.TimeoutExpired:
        logger.error("❌ LLM 健康检查超时 (120s)")
        log_task_metrics(task_name, "failure", error="timeout")
    except json.JSONDecodeError as e:
        logger.error(f"❌ LLM 健康检查输出解析失败: {e}")
        log_task_metrics(task_name, "failure", error="json_parse_error")
    except Exception as e:
        logger.error(f"❌ LLM 健康检查执行失败: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def _send_douyin_recovery_notification():
    """发送 Douyin API 服务恢复通知到飞书"""
    try:
        zhiwei_bot_dir = Path.home() / "zhiwei-bot"
        if str(zhiwei_bot_dir) not in sys.path:
            sys.path.insert(0, str(zhiwei_bot_dir))

        from video_history import ALERT_USER_ID

        if ALERT_USER_ID:
            from feishu_api import send_direct_message
            msg = """✅ Douyin API 服务已恢复

服务健康检查发现 Douyin API 不可运行，已自动重启。

请确认视频处理功能正常。"""
            send_direct_message(ALERT_USER_ID, msg)
            logger.info("Douyin API 恢复通知已发送")
    except Exception as e:
        logger.warning(f"发送恢复通知失败: {e}")


# ============ 信息源同步任务 (v62.0) ============

def job_sync_hn_daily():
    """同步 Hacker News Top 5 到 Obsidian Inbox

    ⭐ v62.0 新增：定时同步优质内容
    """
    task_name = "sync_hn_daily"
    start_time = time.time()

    try:
        logger.info(f"📰 开始执行: {task_name}")

        # 使用 zhiwei_agent 的 trending_discover 工具
        from tools.trending_discover import TrendingDiscoverTool

        tool = TrendingDiscoverTool()
        result = tool.execute(platform="hn", limit=5)

        if not result.success:
            logger.error(f"HN 获取失败: {result.error}")
            log_task_metrics(task_name, "failure", error=result.error)
            return

        items = result.data.get("items", [])
        if not items:
            logger.warning("HN 无热门内容")
            log_task_metrics(task_name, "skipped", extra={"reason": "no_items"})
            return

        # 生成 Markdown 内容
        today = datetime.now().strftime("%Y-%m-%d")
        content = f"""# NEWS_{today}_Hacker-News-Top5

## 来源
- Hacker News Top Stories
- 抓取时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}

## 内容

"""
        for i, item in enumerate(items, 1):
            title = item.get("title", "无标题")
            url = item.get("url", "")
            score = item.get("score", 0)
            comments = item.get("comments", 0)
            by = item.get("by", "")

            content += f"""### {i}. {title}

- URL: {url}
- 分数: {score} | 评论: {comments} | 作者: {by}

"""

        # 保存到 Obsidian Inbox
        inbox_path = Path.home() / "Documents" / "ZhiweiVault" / "Inbox"
        inbox_path.mkdir(parents=True, exist_ok=True)

        output_file = inbox_path / f"NEWS_{today}_Hacker-News-Top5.md"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info(f"✅ HN Top 5 已保存到: {output_file}")
        log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000),
                        extra={"items": len(items), "file": str(output_file)})

    except Exception as e:
        logger.error(f"HN 同步任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def job_sync_github_weekly():
    """同步 GitHub Trending (AI) 到 Obsidian Inbox

    ⭐ v62.0 新增：每周同步 GitHub AI 趋势
    """
    task_name = "sync_github_weekly"
    start_time = time.time()

    try:
        logger.info(f"🐙 开始执行: {task_name}")

        # 使用 zhiwei_agent 的 trending_discover 工具
        from tools.trending_discover import TrendingDiscoverTool

        tool = TrendingDiscoverTool()
        result = tool.execute(platform="github", limit=10)

        if not result.success:
            logger.error(f"GitHub Trending 获取失败: {result.error}")
            log_task_metrics(task_name, "failure", error=result.error)
            return

        items = result.data.get("items", [])
        if not items:
            logger.warning("GitHub Trending 无内容")
            log_task_metrics(task_name, "skipped", extra={"reason": "no_items"})
            return

        # 生成 Markdown 内容
        today = datetime.now().strftime("%Y-%m-%d")
        content = f"""# NEWS_{today}_GitHub-Trending-AI

## 来源
- GitHub Trending (Daily)
- 抓取时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}

## 内容

"""
        for i, item in enumerate(items, 1):
            title = item.get("title", item.get("name", "无名称"))
            url = item.get("url", "")
            desc = item.get("description", "无描述")
            stars = item.get("stars", 0)
            lang = item.get("language", "Unknown")
            author = item.get("author", "")

            content += f"""### {i}. {title}

- URL: {url}
- 描述: {desc}
- Stars: {stars} | 语言: {lang} | 作者: {author}

"""

        # 保存到 Obsidian Inbox
        inbox_path = Path.home() / "Documents" / "ZhiweiVault" / "Inbox"
        inbox_path.mkdir(parents=True, exist_ok=True)

        output_file = inbox_path / f"NEWS_{today}_GitHub-Trending-AI.md"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info(f"✅ GitHub Trending 已保存到: {output_file}")
        log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000),
                        extra={"items": len(items), "file": str(output_file)})

    except Exception as e:
        logger.error(f"GitHub 同步任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


# ============ 播客更新任务 (v65.0 新增) ============

def job_podcast_update():
    """播客更新检查与下载 - v65.1 增强"""
    task_name = "podcast_update"
    start_time = time.time()

    try:
        logger.info(f"🎧 开始执行: {task_name}")

        # 读取播客配置
        podcasts_config = config.get("podcasts", {}) if config else {}
        download_dir = Path(podcasts_config.get("download_dir", "~/Documents/ZhiweiVault/70-79_个人笔记/播客")).expanduser()
        feeds = podcasts_config.get("feeds", [])

        if not feeds:
            logger.info("📢 播客订阅列表为空")
            logger.info("   请在 settings.yaml 的 podcasts.feeds 中添加订阅源")
            logger.info("   获取RSS地址: 小宇宙App → 播客页面 → 分享 → RSS")
            log_task_metrics(task_name, "skipped", extra={"reason": "no_feeds"})
            return

        download_dir.mkdir(parents=True, exist_ok=True)

        # 记录统计
        stats = {
            "checked": 0,
            "new_downloads": 0,
            "already_exists": 0,
            "errors": 0
        }
        new_episodes = []

        for feed in feeds:
            feed_name = feed.get("name", "未知播客")
            feed_url = feed.get("url")

            if not feed_url:
                logger.warning(f"⚠️ {feed_name}: 未配置RSS地址")
                continue

            try:
                import feedparser
                parsed = feedparser.parse(feed_url)

                if not parsed.entries:
                    logger.warning(f"⚠️ {feed_name}: RSS无内容，请检查地址是否正确")
                    stats["errors"] += 1
                    continue

                stats["checked"] += 1
                logger.info(f"📻 {feed_name}: {len(parsed.entries)} 个节目")

                # 检查最近3个episode
                for entry in parsed.entries[:3]:
                    title = entry.get("title", "无标题")
                    audio_url = None

                    # 查找音频链接
                    for enclosure in entry.get("enclosures", []):
                        if "audio" in enclosure.get("type", ""):
                            audio_url = enclosure.get("href")
                            break

                    # 备选：检查链接
                    if not audio_url and entry.get("link"):
                        link = entry.get("link", "")
                        if link.endswith((".mp3", ".m4a", ".mp4")):
                            audio_url = link

                    if audio_url:
                        # 安全文件名
                        safe_title = "".join(c for c in title[:40] if c.isalnum() or c in " -_").strip()
                        ext = audio_url.split(".")[-1][:4] if "." in audio_url else "mp3"
                        audio_file = download_dir / f"{feed_name}_{safe_title}.{ext}"

                        if not audio_file.exists():
                            # 下载音频
                            logger.info(f"  ⬇️ 下载: {title[:40]}")
                            import urllib.request
                            urllib.request.urlretrieve(audio_url, audio_file)
                            new_episodes.append({
                                "podcast": feed_name,
                                "title": title,
                                "file": str(audio_file)
                            })
                            stats["new_downloads"] += 1
                        else:
                            stats["already_exists"] += 1

            except Exception as e:
                logger.error(f"❌ {feed_name}: {e}")
                stats["errors"] += 1

        # 生成更新简报
        if new_episodes:
            content = f"""# 🎧 播客更新

> 检查时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}

## 新下载的节目

"""
            for ep in new_episodes:
                content += f"- **{ep['podcast']}**: {ep['title']}\n"

            file_path, _ = save_result_safe(task_name, content, targets=["feishu"])
            try_push(file_path)

        # 统计日志
        logger.info(f"✅ 播客更新完成: 检查{stats['checked']}个源，新下载{stats['new_downloads']}个，已存在{stats['already_exists']}个")
        log_task_metrics(task_name, "success",
                        duration_ms=int((time.time() - start_time) * 1000),
                        extra=stats)

    except Exception as e:
        logger.error(f"播客更新任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


__all__ = [
    # 任务函数
    'job_morning_brief',
    'job_noon_brief',
    'job_us_market_open',
    'job_us_market_close',
    'job_arxiv',
    'job_system_check',
    'job_system_metrics_report',
    'job_log_rotate',
    'job_knowledge_classify',
    'job_video_notes_organize',
    'job_video_retry',
    'job_asr_health_check',
    'job_douyin_health_check',
    'job_research_pipeline',
    'job_vault_sync_master',
    'job_daily_voice_task_summary',
    'job_ws_health_check',
    'job_intel_sync',
    'job_intel_report',
    'job_sync_hn_daily',
    'job_sync_github_weekly',
    'job_podcast_update',  # ⭐ v65.0 新增
    'job_llm_health_check',  # ⭐ v67.0 新增
    # 辅助函数
    'enrich_with_graphrag',
    'enrich_with_klib',
    'log_health_status',
]
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
    logger,
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


def _fetch_rss_articles(feeds: list, max_per_feed: int = 3, max_total: int = 10) -> list:
    """通用 RSS 文章获取函数 - v66.2

    Args:
        feeds: [(name, url), ...] 格式的 RSS 源列表
        max_per_feed: 每个 RSS 源最多获取的文章数
        max_total: 总共最多返回的文章数

    Returns:
        格式化的文章列表
    """
    articles = []
    for name, url in feeds:
        try:
            from tools.rss_feed import RSSFeedTool
            rss_tool = RSSFeedTool()
            result = rss_tool.execute(url=url, limit=5)
            if result.success and result.data.get("articles"):
                for article in result.data["articles"][:max_per_feed]:
                    title = article.get("title", "")[:60]
                    link = article.get("link", "")
                    summary = article.get("summary", "")[:80]
                    articles.append(f"- [{name}] **{title}**\n  {summary}\n  > [链接]({link})")
        except Exception as e:
            logger.warning(f"RSS {name} 获取失败: {e}")
    return articles[:max_total]


def _collect_real_news_sources() -> str:
    """收集真实数据源用于早报生成 - v68.3 多源聚合

    数据源：
    1. RSS 科技新闻
    2. Hacker News Top 5
    3. GitHub Trending
    4. 情报中心
    5. 最新论文
    6. 国际动态 (新增)
    7. 国内动态 (新增)
    8. 加密货币 (新增)
    """
    sources = []
    today = datetime.now().strftime("%Y-%m-%d")
    inbox_path = Path.home() / "Documents" / "ZhiweiVault" / "Inbox"

    # 1. RSS 实时新闻
    TECH_FEEDS = [
        ("TechCrunch AI", "https://techcrunch.com/category/artificial-intelligence/feed/"),
        ("MIT Tech Review", "https://www.technologyreview.com/feed/"),
        ("Ars Technica", "https://feeds.arstechnica.com/arstechnica/technology-lab"),
        ("Wired AI", "https://www.wired.com/feed/tag/ai/latest/rss"),
        ("VentureBeat AI", "https://venturebeat.com/category/ai/feed/"),
    ]

    rss_news = _fetch_rss_articles(TECH_FEEDS, max_per_feed=3, max_total=10)
    if rss_news:
        sources.append(f"### 🔴 实时新闻\n" + "\n".join(rss_news))
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

    # 6. 国际动态 (v68.3 新增)
    intl_news = _collect_international_news()
    if intl_news:
        sources.append(intl_news)

    # 7. 国内动态 (v68.3 新增)
    domestic_news = _collect_domestic_news()
    if domestic_news:
        sources.append(domestic_news)

    # 8. 加密货币 (v68.3 新增)
    crypto_info = _collect_crypto_info()
    if crypto_info:
        sources.append(crypto_info)

    # 9. 天气 (v68.5 新增)
    weather = _collect_weather()
    if weather:
        sources.append(weather)

    # 10. 行业洞察 (v68.5 新增)
    insights = _collect_industry_insights()
    if insights:
        sources.append(insights)

    # 11. 开发者资讯 (v68.5 新增)
    dev_news = _collect_dev_news()
    if dev_news:
        sources.append(dev_news)

    # 12. 科学前沿 (v68.5 新增)
    science = _collect_science_news()
    if science:
        sources.append(science)

    # 13. 产品发现 (v68.5 新增)
    products = _collect_product_discovery()
    if products:
        sources.append(products)

    if not sources:
        return "⚠️ 今日暂无数据源"

    return "\n\n".join(sources)


def _collect_international_news() -> str:
    """收集国际新闻 - v68.4: 官方/主流来源

    来源：
    - TechCrunch (科技)
    - Ars Technica (科技)
    - The Verge (科技)
    - WSJ Tech (华尔街日报科技)
    - MarketWatch (财经)
    """
    INTERNATIONAL_FEEDS = [
        ("WSJ Tech", "https://feeds.a.dj.com/rss/RSSWSJD.xml"),
        ("TechCrunch", "https://techcrunch.com/feed/"),
        ("Ars Technica", "https://feeds.arstechnica.com/arstechnica/index"),
        ("The Verge", "https://www.theverge.com/rss/index.xml"),
        ("MarketWatch", "https://www.marketwatch.com/rss/topstories"),
    ]

    articles = _fetch_rss_articles(INTERNATIONAL_FEEDS, max_per_feed=2, max_total=10)
    if articles:
        logger.info(f"🌍 已获取国际新闻 {len(articles)} 条")
        return "### 🌍 国际动态\n" + "\n".join(articles)
    return ""


def _collect_domestic_news() -> str:
    """收集国内新闻 - v68.3: 官方来源

    来源：
    - 新华网 (官方)
    - 人民网 (官方)
    """
    DOMESTIC_FEEDS = [
        ("新华网", "http://www.news.cn/politics/news_politics.xml"),
        ("人民网", "http://www.people.com.cn/rss/politics.xml"),
    ]

    articles = _fetch_rss_articles(DOMESTIC_FEEDS, max_per_feed=4, max_total=8)
    if articles:
        logger.info(f"🇨🇳 已获取国内新闻 {len(articles)} 条")
        return "### 🇨🇳 国内动态\n" + "\n".join(articles)
    return ""


def _collect_crypto_info() -> str:
    """收集加密货币动态 - v68.4: 改用RSS新闻（API被墙）

    来源：
    - Cointelegraph (加密货币新闻)
    - CryptoNews (加密货币新闻)
    """
    CRYPTO_FEEDS = [
        ("Cointelegraph", "https://cointelegraph.com/rss"),
        ("CryptoNews", "https://cryptonews.com/news/feed/"),
    ]

    articles = _fetch_rss_articles(CRYPTO_FEEDS, max_per_feed=2, max_total=4)
    if articles:
        logger.info(f"💰 已获取加密货币动态 {len(articles)} 条")
        return "### 💰 加密货币动态\n" + "\n".join(articles)
    return ""


def _collect_weather() -> str:
    """获取杭州天气 - v68.5: wttr.in API"""
    try:
        import urllib.request
        url = "https://wttr.in/Hangzhou?format=%t+%C+%h+%w&lang=zh"
        req = urllib.request.Request(url, headers={'User-Agent': 'curl'})
        resp = urllib.request.urlopen(req, timeout=10)
        weather = resp.read().decode().strip()
        if weather:
            logger.info(f"🌤️ 已获取天气: {weather}")
            return f"### 🌤️ 杭州天气\n{weather}\n\n> 出行建议：根据天气情况合理安排行程"
    except Exception as e:
        logger.warning(f"天气获取失败: {e}")
    return ""


def _collect_industry_insights() -> str:
    """收集行业洞察 - v68.5: McKinsey, 商业分析"""
    INSIGHT_FEEDS = [
        ("McKinsey", "https://www.mckinsey.com/rss"),
    ]
    articles = _fetch_rss_articles(INSIGHT_FEEDS, max_per_feed=3, max_total=3)
    if articles:
        logger.info(f"📊 已获取行业洞察 {len(articles)} 条")
        return "### 📊 行业洞察\n" + "\n".join(articles)
    return ""


def _collect_dev_news() -> str:
    """收集开发者资讯 - v68.5: Dev.to, 技术社区"""
    DEV_FEEDS = [
        ("Dev.to", "https://dev.to/feed"),
        ("Smashing Mag", "https://www.smashingmagazine.com/feed/"),
    ]
    articles = _fetch_rss_articles(DEV_FEEDS, max_per_feed=2, max_total=4)
    if articles:
        logger.info(f"👨‍💻 已获取开发者资讯 {len(articles)} 条")
        return "### 👨‍💻 开发者资讯\n" + "\n".join(articles)
    return ""


def _collect_science_news() -> str:
    """收集科学前沿 - v68.5: Science Daily"""
    SCIENCE_FEEDS = [
        ("Science Daily AI", "https://www.sciencedaily.com/rss/computers_math/artificial_intelligence.xml"),
    ]
    articles = _fetch_rss_articles(SCIENCE_FEEDS, max_per_feed=3, max_total=3)
    if articles:
        logger.info(f"🔬 已获取科学前沿 {len(articles)} 条")
        return "### 🔬 科学前沿\n" + "\n".join(articles)
    return ""


def _collect_product_discovery() -> str:
    """收集产品发现 - v68.5: Product Hunt"""
    PRODUCT_FEEDS = [
        ("Product Hunt", "https://www.producthunt.com/feed"),
    ]
    articles = _fetch_rss_articles(PRODUCT_FEEDS, max_per_feed=3, max_total=3)
    if articles:
        logger.info(f"🚀 已获取产品发现 {len(articles)} 条")
        return "### 🚀 产品发现\n" + "\n".join(articles)
    return ""


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
            # v68.2: 从配置读取 push_to
            push_targets = scheduler_core.config.get("jobs", {}).get("morning_brief", {}).get("push_to", ["feishu"])
            file_path, skipped = save_result_safe(task_name, content, targets=push_targets)

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
            # v68.2: 从配置读取 push_to
            push_targets = scheduler_core.config.get("jobs", {}).get("noon_brief", {}).get("push_to", ["feishu"])
            file_path, skipped = save_result_safe(task_name, content, targets=push_targets)
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
    """收集美股相关新闻 - v66.2 使用公共 RSS 函数"""
    FINANCE_FEEDS = [
        ("Yahoo Finance", "https://finance.yahoo.com/news/rssindex"),
        ("MarketWatch", "https://www.marketwatch.com/rss/topstories"),
        ("SeekingAlpha", "https://seekingalpha.com/market_currents.xml"),
        ("CNBC Markets", "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
    ]

    finance_news = _fetch_rss_articles(FINANCE_FEEDS, max_per_feed=3, max_total=9)
    if finance_news:
        logger.info(f"📊 已获取财经新闻 {len(finance_news)} 条")
        return f"### 📊 财经新闻\n" + "\n".join(finance_news)

    return "⚠️ 暂无财经数据"


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
            # v68.2: 从配置读取 push_to
            push_targets = scheduler_core.config.get("jobs", {}).get("us_market_open", {}).get("push_to", ["feishu"])
            file_path, skipped = save_result_safe(task_name, content, targets=push_targets)
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
            # v68.2: 从配置读取 push_to
            push_targets = scheduler_core.config.get("jobs", {}).get("us_market_close", {}).get("push_to", ["feishu"])
            file_path, skipped = save_result_safe(task_name, content, targets=push_targets)
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
    """ArXiv 论文精选 (07:00) - v66.2: 使用已配置的 sys.path"""
    task_name = "arxiv_papers"
    start_time = time.time()

    try:
        logger.info(f"📄 开始执行: {task_name}")

        # 使用 ArxivSearchTool 获取论文（依赖文件开头的 sys.path 配置）
        from zhiwei_agent.tools.arxiv_search import ArxivSearchTool

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
            # v68.2: 从配置读取 push_to
            push_targets = scheduler_core.config.get("jobs", {}).get("arxiv_papers", {}).get("push_to", ["feishu"])
            file_path, skipped = save_result_safe(task_name, content, targets=push_targets)
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
            # v68.1: 从配置读取 push_to
            push_targets = scheduler_core.config.get("jobs", {}).get("system_check", {}).get("push_to", ["feishu"])
            save_result_safe(task_name, content, targets=push_targets)
            log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
        else:
            logger.warning("健康检查脚本不存在")
            log_health_status()

    except Exception as e:
        logger.error(f"系统检查异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def _collect_system_metrics() -> str:
    """收集系统运维指标 - v66.0 真实数据"""
    import subprocess

    metrics = []

    # 1. 服务状态
    try:
        result = subprocess.run(
            ["launchctl", "list"],
            capture_output=True, text=True, timeout=30
        )
        services = []
        for line in result.stdout.split('\n'):
            if 'zhiwei' in line.lower():
                parts = line.split()
                if len(parts) >= 3:
                    status = "✅" if parts[0] != "0" else "❌"
                    services.append(f"{status} {parts[2]}")
        metrics.append(f"### 服务状态\n" + "\n".join(services[:6]))
    except Exception as e:
        metrics.append(f"### 服务状态\n查询失败: {e}")

    # 2. 磁盘使用
    try:
        result = subprocess.run(
            ["df", "-h", "/"],
            capture_output=True, text=True, timeout=10
        )
        lines = result.stdout.strip().split('\n')
        if len(lines) >= 2:
            parts = lines[1].split()
            metrics.append(f"### 磁盘使用\n- 总量: {parts[1]}\n- 已用: {parts[2]}\n- 可用: {parts[3]}\n- 使用率: {parts[4]}")
    except Exception as e:
        metrics.append(f"### 磁盘使用\n查询失败: {e}")

    # 3. Docker 容器
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0 and result.stdout.strip():
            containers = [f"- {line}" for line in result.stdout.strip().split('\n')[:5]]
            metrics.append(f"### Docker 容器\n" + "\n".join(containers))
    except Exception:
        pass  # Docker 可能未安装

    # 4. 日志大小
    try:
        log_dir = Path.home() / "logs"
        if log_dir.exists():
            total_size = sum(f.stat().st_size for f in log_dir.rglob("*") if f.is_file())
            metrics.append(f"### 日志\n- 总大小: {total_size / 1024 / 1024:.1f} MB")
    except Exception:
        pass

    return "\n\n".join(metrics)


def job_system_metrics_report():
    """运维报告 (10:35) - v66.0: 基于真实系统指标"""
    task_name = "system_metrics"
    start_time = time.time()

    try:
        logger.info(f"📊 开始执行: {task_name}")

        # 收集真实系统指标
        real_metrics = _collect_system_metrics()

        # 加载 Prompt
        prompt = load_prompt("system_metrics", real_data=real_metrics)

        if not prompt:
            logger.warning("运维报告 Prompt 加载失败")
            return

        success, content = call_agent("operator", prompt, timeout=300)

        if success:
            # v68.1: 从配置读取 push_to
            push_targets = scheduler_core.config.get("jobs", {}).get("system_metrics", {}).get("push_to", ["feishu"])
            file_path, skipped = save_result_safe(task_name, content, targets=push_targets)
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
    调用 reconcile_obsidian.py v3.0 进行增量同步
    ⭐ v69.0 优化：增加 --limit 200 加速同步，--skip-chroma 跳过 Docker
    """
    task_name = "vault_sync_master"
    start_time = time.time()

    try:
        logger.info("开始执行 [VaultSyncMaster 增量同步] 任务...")

        # 脚本路径
        script_path = Path.home() / "zhiwei-rag" / "scripts" / "reconcile_obsidian.py"
        # ⭐ v62.0 修复：使用 zhiwei-rag 的 venv（lancedb 在此安装）
        python_executable = Path.home() / "zhiwei-rag" / "venv" / "bin" / "python3"

        if script_path.exists() and python_executable.exists():
            result = subprocess.run(
                [str(python_executable), str(script_path), "--limit", "200", "--skip-chroma"],
                capture_output=True,
                text=True,
                timeout=900  # 15 分钟
            )

            if result.returncode == 0:
                content = result.stdout
                logger.info(f"VaultSyncMaster 增量同步完成:\n{content[:500]}...")
                # v68.2: 从配置读取 push_to 并推送
                push_targets = scheduler_core.config.get("jobs", {}).get("vault_sync_master", {}).get("push_to", ["feishu"])
                if push_targets:
                    file_path, _ = save_result_safe(task_name, content, targets=push_targets)
                    try_push(file_path)
                log_task_metrics(task_name, "success", duration_ms=int((time.time() - start_time) * 1000))
            else:
                logger.error(f"VaultSyncMaster 增量同步失败: {result.stderr}")
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
        logger.error(f"VaultSyncMaster 增量同步任务异常: {e}")
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
                # v68.2: 从输出提取报告路径，走队列推送
                for line in result.stdout.splitlines():
                    if "情报简报已生成并存入" in line:
                        report_path_str = line.split(":")[-1].strip()
                        if os.path.exists(report_path_str):
                            # 读取报告内容
                            report_content = Path(report_path_str).read_text(encoding="utf-8")
                            push_targets = scheduler_core.config.get("jobs", {}).get("intelligence_report", {}).get("push_to", ["feishu"])
                            file_path, _ = save_result_safe(task_name, report_content, targets=push_targets)
                            try_push(file_path)
                            break

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
        from zhiwei_agent.tools.trending_discover import TrendingDiscoverTool

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
        from zhiwei_agent.tools.trending_discover import TrendingDiscoverTool

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
    """播客更新检查与下载 - v69.0 增强：转录 + 知识蒸馏

    流程：
    1. 检查 RSS 更新
    2. 下载新音频
    3. ASR 转录
    4. LLM 知识蒸馏
    5. 保存到 Obsidian Vault
    6. 推送更新通知
    """
    task_name = "podcast_update"
    start_time = time.time()

    try:
        logger.info(f"🎧 开始执行: {task_name}")

        # 读取播客配置
        podcasts_config = scheduler_core.config.get("podcasts", {}) if scheduler_core.config else {}
        download_dir = Path(podcasts_config.get("download_dir", "~/Documents/ZhiweiVault/70-79_个人笔记/播客")).expanduser()
        feeds = podcasts_config.get("feeds", [])

        if not feeds:
            logger.info("📢 播客订阅列表为空")
            logger.info("   请在 settings.yaml 的 podcasts.feeds 中添加订阅源")
            logger.info("   获取RSS地址: 小宇宙App → 播客页面 → 分享 → RSS")
            log_task_metrics(task_name, "skipped", extra={"reason": "no_feeds"})
            return

        download_dir.mkdir(parents=True, exist_ok=True)

        # 笔记输出目录
        notes_dir = Path.home() / "Documents" / "ZhiweiVault" / "70-79_个人笔记" / "播客笔记"
        notes_dir.mkdir(parents=True, exist_ok=True)

        # 记录统计
        stats = {
            "checked": 0,
            "new_downloads": 0,
            "new_notes": 0,
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

                # 只处理最新的 1 个 episode（避免处理时间过长）
                entry = parsed.entries[0]
                title = entry.get("title", "无标题")
                audio_url = None

                # 查找音频链接
                for enclosure in entry.get("enclosures", []):
                    if "audio" in enclosure.get("type", ""):
                        audio_url = enclosure.get("href")
                        break

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
                        stats["new_downloads"] += 1

                    # 检查是否已有笔记
                    note_file = notes_dir / f"PODCAST_{datetime.now().strftime('%Y-%m-%d')}_{safe_title[:30]}.md"
                    if not note_file.exists():
                        # ⭐ v69.5: 转录 + 蒸馏（失败时生成基本笔记）
                        logger.info(f"  🎤 转录中: {title[:30]}")
                        transcript = _transcribe_podcast(audio_file)

                        if transcript:
                            logger.info(f"  🧠 蒸馏中: {len(transcript)} 字")
                            note_content = _distill_podcast(title, transcript)
                        else:
                            # ⭐ 转录失败时，使用 RSS 元数据生成基本笔记
                            logger.info(f"  ⚠️ 转录失败，生成基本笔记")
                            summary = entry.get("summary", entry.get("description", ""))
                            note_content = _generate_basic_podcast_note(feed_name, title, summary, audio_file)

                        if note_content:
                            _save_podcast_note(title, note_content, note_file)
                            stats["new_notes"] += 1
                            new_episodes.append({
                                "podcast": feed_name,
                                "title": title,
                                "file": str(audio_file),
                                "note": str(note_file),
                                "transcribed": bool(transcript)
                            })
                    else:
                        stats["already_exists"] += 1

            except Exception as e:
                logger.error(f"❌ {feed_name}: {e}")
                stats["errors"] += 1

        # 生成更新简报
        if new_episodes:
            content = f"""# 🎧 播客更新与知识蒸馏

> 处理时间: {datetime.now().strftime("%Y-%m-%d %H:%M")}

## 新处理的节目

"""
            for ep in new_episodes:
                content += f"""### {ep['podcast']}: {ep['title']}

- 音频: `{Path(ep['file']).name}`
- 笔记: `{Path(ep['note']).name}`

"""
            # v68.2: 从配置读取 push_to
            push_targets = scheduler_core.config.get("jobs", {}).get("podcast_update", {}).get("push_to", ["feishu"])
            file_path, _ = save_result_safe(task_name, content, targets=push_targets)
            try_push(file_path)

        # 统计日志
        logger.info(f"✅ 播客更新完成: 检查{stats['checked']}个源，下载{stats['new_downloads']}个，生成笔记{stats['new_notes']}个")
        log_task_metrics(task_name, "success",
                        duration_ms=int((time.time() - start_time) * 1000),
                        extra=stats)

    except Exception as e:
        logger.error(f"播客更新任务异常: {e}")
        log_task_metrics(task_name, "failure", error=str(e))


def _transcribe_podcast(audio_path: Path) -> str:
    """播客 ASR 转录 - v69.4: 使用 subprocess 调用，避免 Python 3.14 async 兼容性问题"""
    try:
        import subprocess
        import tempfile
        import json

        # 创建临时输出文件
        output_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        output_path = output_file.name
        output_file.close()

        # 调用转录脚本
        script_path = BASE_DIR / "scripts" / "transcribe_audio.py"
        if not script_path.exists():
            logger.warning(f"转录脚本不存在: {script_path}")
            return ""

        logger.info(f"🎤 转录中（subprocess）...")

        result = subprocess.run(
            [
                sys.executable,
                str(script_path),
                "--audio", str(audio_path.absolute()),
                "--output", output_path
            ],
            capture_output=True,
            text=True,
            timeout=300,  # 5分钟超时
            cwd=str(BASE_DIR)
        )

        if result.returncode == 0:
            # 读取结果
            try:
                with open(output_path, 'r') as f:
                    data = json.load(f)
                text = data.get('text', '')
                if text:
                    logger.info(f"✅ 转录完成: {len(text)} 字符")
                    return text
                else:
                    logger.warning("转录结果为空")
                    return ""
            except Exception as e:
                logger.error(f"读取转录结果失败: {e}")
                return ""
        else:
            logger.error(f"转录脚本失败: {result.stderr[:200]}")
            return ""

    except subprocess.TimeoutExpired:
        logger.error("转录超时（300s）")
        return ""
    except Exception as e:
        logger.error(f"转录异常: {e}")
        return ""
    finally:
        # 清理临时文件
        try:
            import os
            if output_path and os.path.exists(output_path):
                os.unlink(output_path)
        except:
            pass


DISTILL_PROMPT = """你是一个顶级的技术研究员与情报分析师，擅长从播客内容中提取高密度知识。

**任务**：从播客转录文本中蒸馏出核心知识，生成结构化的 Markdown 笔记。

**输出格式**：

## 🎧 核心观点

> 一句话总结本期核心价值

## 📌 关键要点

1. 要点1
2. 要点2
3. 要点3

## 🔍 深度解读

### 技术要点
- 要点1：详细解释
- 要点2：详细解释

### 工具/资源
- 工具名：用途说明

### 实践建议
- 建议1
- 建议2

## 💡 启发与思考

- 启发1
- 启发2

## 📚 延伸阅读

- 相关主题

---

**播客标题**: {title}
**转录字数**: {word_count} 字

**转录文本**：
{transcript}
"""


def _distill_podcast(title: str, transcript: str) -> str:
    """LLM 知识蒸馏"""
    try:
        from zhiwei_common.llm import llm_client

        word_count = len(transcript)
        prompt = DISTILL_PROMPT.format(
            title=title,
            word_count=word_count,
            transcript=transcript[:15000]  # 限制长度
        )

        # 使用 llm_client.call() 方法
        success, response = llm_client.call("researcher", prompt, timeout=120)
        if success and response:
            logger.info(f"✅ 蒸馏完成: {len(response)} 字符")
            return response
        else:
            logger.error(f"蒸馏失败: LLM 调用不成功")
            return f"# {title}\n\n转录文本：\n\n{transcript[:2000]}"

    except Exception as e:
        logger.error(f"蒸馏失败: {e}")
        return f"# {title}\n\n转录文本：\n\n{transcript[:2000]}"


def _generate_basic_podcast_note(podcast_name: str, title: str, summary: str, audio_file: Path) -> str:
    """生成基本播客笔记（转录失败时的降级方案）"""
    # 清理 summary
    if summary:
        # 移除 HTML 标签
        import re
        summary = re.sub(r'<[^>]+>', '', summary)
        summary = summary[:500]  # 限制长度

    content = f"""## 🎧 播客信息

- **来源**: {podcast_name}
- **标题**: {title}
- **音频文件**: `{audio_file.name}`
- **生成时间**: {datetime.now().strftime("%Y-%m-%d %H:%M")}
- **转录状态**: ⚠️ 未转录（待手动处理）

## 📝 简介

{summary if summary else "无简介"}

---

> 💡 提示：此笔记为基本版本，待后续转录后可更新为深度分析版本。
"""

    return content


def _save_podcast_note(title: str, content: str, output_path: Path) -> None:
    """保存播客笔记"""
    import re

    full_content = f"""---
title: {title}
date: {datetime.now().strftime("%Y-%m-%d %H:%M")}
type: podcast
tags: [播客, 技术]
---

{content}
"""
    output_path.write_text(full_content, encoding="utf-8")
    logger.info(f"📝 笔记已保存: {output_path.name}")


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
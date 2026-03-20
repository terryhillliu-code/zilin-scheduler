#!/usr/bin/env python3
"""
Twitter/X 内容分析器 v1.0

功能：
1. 单条推文分析（URL 或文本）
2. 账号监控（定时检查新推文）
3. RAG 背景知识增强
4. 输出到 Obsidian + 飞书推送

使用方式：
    # 分析单条推文
    python twitter_processor.py analyze "https://twitter.com/user/status/xxx"

    # 分析文本内容
    python twitter_processor.py analyze "推文内容..."

    # 监控账号
    python twitter_processor.py monitor

    # 列出监控账号
    python twitter_processor.py accounts
"""

import os
import json
import logging
import hashlib
import re
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# 配置路径
SCRIPT_DIR = Path(__file__).parent
PROMPTS_DIR = SCRIPT_DIR / "prompts"
DATA_DIR = SCRIPT_DIR / "data"
OBSIDIAN_VAULT = Path.home() / "Documents" / "ZhiweiVault"

# Twitter 笔记存储目录
TWITTER_NOTES_DIR = OBSIDIAN_VAULT / "10-19_AI-Systems" / "12_多模态智能体_Multimodal-Agent" / "Twitter-Insights"
TWITTER_NOTES_DIR.mkdir(parents=True, exist_ok=True)

# 历史记录文件
HISTORY_FILE = DATA_DIR / "twitter_history.json"
ACCOUNTS_FILE = DATA_DIR / "twitter_accounts.txt"

# 默认监控账号
DEFAULT_ACCOUNTS = [
    {"handle": "karpathy", "name": "Andrej Karpathy", "category": "AI/教育", "priority": "high"},
    {"handle": "sama", "name": "Sam Altman", "category": "AI/创业", "priority": "high"},
    {"handle": "ylecun", "name": "Yann LeCun", "category": "AI/研究", "priority": "high"},
    {"handle": "jeffdean", "name": "Jeff Dean", "category": "AI/工程", "priority": "high"},
]


@dataclass
class Tweet:
    """推文数据结构"""
    id: str
    handle: str
    name: str
    content: str
    url: str
    timestamp: str
    likes: int = 0
    retweets: int = 0
    replies: int = 0


def load_prompt_template(template_name: str) -> str:
    """加载 prompt 模板"""
    template_path = PROMPTS_DIR / f"{template_name}.txt"
    if template_path.exists():
        return template_path.read_text(encoding="utf-8")
    return ""


def extract_keywords(text: str, max_keywords: int = 8) -> List[str]:
    """从文本中提取关键词"""
    # 提取中文词组
    chinese_pattern = r'[\u4e00-\u9fa5]{2,6}'
    chinese_words = re.findall(chinese_pattern, text)

    # 提取英文术语
    english_pattern = r'[A-Z][a-z]+(?:[A-Z][a-z]+)*|[A-Z]{2,}|[a-z]+'
    english_words = re.findall(english_pattern, text)

    # 过滤常见无意义词
    stop_words = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been',
                  'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
                  'would', 'could', 'should', 'may', 'might', 'must', 'shall',
                  'can', 'need', 'dare', 'ought', 'used', 'to', 'of', 'in',
                  'for', 'on', 'with', 'at', 'by', 'from', 'as', 'into',
                  'through', 'during', 'before', 'after', 'above', 'below',
                  'between', 'under', 'again', 'further', 'then', 'once', 'and',
                  'but', 'or', 'nor', 'so', 'yet', 'both', 'either', 'neither',
                  'not', 'only', 'own', 'same', 'than', 'too', 'very', 'just'}

    english_words = [w for w in english_words if w.lower() not in stop_words and len(w) > 2]

    keywords = list(set(chinese_words + english_words))
    keywords.sort(key=len, reverse=True)

    return keywords[:max_keywords]


def enrich_with_context(content: str, top_k: int = 5) -> str:
    """RAG 背景知识检索"""
    try:
        from rag_bridge import get_context, is_available as rag_available
        if not rag_available():
            return ""
    except ImportError:
        return ""

    keywords = extract_keywords(content)
    if not keywords:
        return ""

    query = " ".join(keywords[:5])
    logger.info(f"RAG 检索关键词: {query}")

    try:
        context = get_context(query, top_k=top_k)
        if context:
            logger.info(f"RAG 检索成功: {len(context)} 字符")
        return context
    except Exception as e:
        logger.error(f"RAG 检索失败: {e}")
        return ""


def call_llm(prompt: str) -> str:
    """调用 LLM"""
    try:
        from llm_proxy import call_llm_direct
        if call_llm_direct:
            return call_llm_direct(prompt)
    except ImportError:
        pass

    # 降级：尝试直接调用
    try:
        import os
        api_key = os.environ.get("DASHSCOPE_API_KEY", "")
        if api_key:
            from openai import OpenAI
            client = OpenAI(
                api_key=api_key,
                base_url="https://dashscope.aliyuncs.com/compatible-mode/v1"
            )
            response = client.chat.completions.create(
                model="qwen3.5-plus",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=4096
            )
            return response.choices[0].message.content or ""
    except Exception as e:
        logger.error(f"LLM 调用失败: {e}")

    return ""


def parse_tweet_url(url: str) -> Optional[Dict[str, str]]:
    """解析推文 URL"""
    # 匹配 twitter.com/user/status/id 或 x.com/user/status/id
    pattern = r'(?:twitter\.com|x\.com)/(\w+)/status/(\d+)'
    match = re.search(pattern, url)
    if match:
        return {
            "handle": match.group(1),
            "tweet_id": match.group(2),
            "url": url
        }
    return None


def fetch_tweet_content(url: str) -> Optional[Tweet]:
    """
    获取推文内容

    注意：由于 Twitter API 限制，这里返回 None
    实际使用时需要用户提供内容或使用第三方工具获取
    """
    parsed = parse_tweet_url(url)
    if not parsed:
        return None

    # 返回占位数据，实际内容需要用户提供
    return Tweet(
        id=parsed["tweet_id"],
        handle=parsed["handle"],
        name=parsed["handle"],  # 需要填充
        content="",  # 需要用户提供
        url=url,
        timestamp=datetime.now().isoformat()
    )


def analyze_tweet(
    content: str,
    handle: str = "unknown",
    name: str = "Unknown",
    url: str = "",
    timestamp: str = "",
    deep_analysis: bool = True
) -> str:
    """
    分析推文内容

    Args:
        content: 推文文本内容
        handle: 作者 handle
        name: 作者名称
        url: 原文链接
        timestamp: 时间戳
        deep_analysis: 是否启用深度分析

    Returns:
        Markdown 格式的分析报告
    """
    template = load_prompt_template("twitter_deep_analysis")
    if not template:
        logger.warning("模板未找到，使用默认格式")
        template = "分析以下推文内容：\n\n{content}"

    # RAG 检索背景知识
    background_context = ""
    if deep_analysis:
        logger.info("深度分析模式：检索背景知识...")
        background_context = enrich_with_context(content, top_k=5)

    # 构建 prompt
    prompt = template.format(
        background_context=background_context or "（未检索到相关背景知识）",
        content=content,
        handle=handle,
        name=name,
        url=url or f"https://twitter.com/{handle}",
        timestamp=timestamp or datetime.now().isoformat()
    )

    # 调用 LLM
    logger.info("生成分析报告...")
    report = call_llm(prompt)

    if not report:
        # 降级输出
        report = f"""# 🐦 @{handle} 的推文分析

## 原文内容

{content}

---
*分析生成失败，请检查 LLM 服务*
"""

    return report


def save_to_obsidian(report: str, handle: str, tweet_id: str) -> Path:
    """保存到 Obsidian"""
    # 生成文件名
    date_prefix = datetime.now().strftime("%Y-%m-%d")
    safe_handle = handle.replace("@", "").replace("/", "_")
    filename = f"TWITTER_{date_prefix}_{safe_handle}_{tweet_id[:8]}.md"
    filepath = TWITTER_NOTES_DIR / filename

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(report)

    logger.info(f"已保存到 Obsidian: {filepath}")
    return filepath


def load_history() -> Dict[str, Any]:
    """加载历史记录"""
    if HISTORY_FILE.exists():
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed": {}, "accounts": {}}


def save_history(history: Dict[str, Any]):
    """保存历史记录"""
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def load_accounts() -> List[Dict[str, str]]:
    """加载监控账号列表"""
    accounts = []

    # 从文件加载
    if ACCOUNTS_FILE.exists():
        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    parts = line.split("|")
                    if len(parts) >= 2:
                        accounts.append({
                            "handle": parts[0].strip().replace("@", ""),
                            "category": parts[1].strip() if len(parts) > 1 else "",
                            "priority": parts[2].strip() if len(parts) > 2 else "medium"
                        })

    # 如果文件为空，使用默认列表
    if not accounts:
        accounts = DEFAULT_ACCOUNTS
        # 保存默认列表
        save_accounts(accounts)

    return accounts


def save_accounts(accounts: List[Dict[str, str]]):
    """保存监控账号列表"""
    ACCOUNTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
        f.write("# Twitter 监控账号列表\n")
        f.write("# 格式: handle | 领域 | 优先级\n")
        f.write(f"# 最后更新: {datetime.now().strftime('%Y-%m-%d')}\n\n")
        for acc in accounts:
            f.write(f"{acc['handle']} | {acc.get('category', '')} | {acc.get('priority', 'medium')}\n")


def push_to_feishu(report: str, handle: str) -> bool:
    """推送到飞书"""
    try:
        from pusher import FeishuPusher
        pusher = FeishuPusher()
        # 截取前 1000 字符
        preview = report[:1000] + "..." if len(report) > 1000 else report
        message = f"# 🐦 Twitter 分析: @{handle}\n\n{preview}"
        return pusher.send_markdown(message)
    except Exception as e:
        logger.error(f"飞书推送失败: {e}")
        return False


def process_single_tweet(
    content: str,
    handle: str = "unknown",
    name: str = "Unknown",
    url: str = "",
    timestamp: str = "",
    save: bool = True,
    push: bool = True
) -> str:
    """
    处理单条推文

    Args:
        content: 推文内容
        handle: 作者 handle
        name: 作者名称
        url: 原文链接
        timestamp: 时间戳
        save: 是否保存到 Obsidian
        push: 是否推送到飞书

    Returns:
        分析报告
    """
    # 生成唯一 ID
    tweet_id = hashlib.md5(content.encode()).hexdigest()[:12]

    # 检查是否已处理
    history = load_history()
    if tweet_id in history.get("processed", {}):
        logger.info(f"推文已处理过: {tweet_id}")
        return history["processed"][tweet_id].get("report", "")

    # 分析
    report = analyze_tweet(content, handle, name, url, timestamp)

    # 保存
    if save:
        save_to_obsidian(report, handle, tweet_id)

    # 推送
    if push:
        push_to_feishu(report, handle)

    # 记录历史
    history.setdefault("processed", {})[tweet_id] = {
        "handle": handle,
        "timestamp": datetime.now().isoformat(),
        "url": url,
        "file": f"TWITTER_{datetime.now().strftime('%Y-%m-%d')}_{handle}_{tweet_id[:8]}.md"
    }
    save_history(history)

    return report


def monitor_accounts(max_tweets: int = 5) -> List[Dict[str, Any]]:
    """
    监控账号

    注意：由于 Twitter API 限制，此功能需要配合第三方工具使用
    实际使用时建议：
    1. 使用 RSS hub 获取账号 RSS
    2. 使用 nitter 实例
    3. 手动粘贴内容
    """
    accounts = load_accounts()
    logger.info(f"监控 {len(accounts)} 个账号...")

    results = []

    for account in accounts[:max_tweets]:
        handle = account["handle"]
        logger.info(f"检查 @{handle}...")

        # TODO: 实际获取推文的逻辑
        # 这里需要配置 API 或第三方工具
        logger.warning(f"⚠️ 需要配置 Twitter API 或第三方工具来获取 @{handle} 的推文")

        results.append({
            "handle": handle,
            "status": "pending",
            "message": "需要配置数据源"
        })

    return results


def main():
    import sys

    if len(sys.argv) < 2:
        print(__doc__)
        return

    command = sys.argv[1]

    if command == "analyze":
        if len(sys.argv) < 3:
            print("用法: python twitter_processor.py analyze <URL或内容>")
            return

        input_text = sys.argv[2]

        # 判断是 URL 还是内容
        if input_text.startswith("http"):
            parsed = parse_tweet_url(input_text)
            if parsed:
                print(f"检测到推文 URL: @{parsed['handle']}")
                print("请提供推文内容（粘贴后按 Ctrl+D）:")
                content = sys.stdin.read().strip()
                if not content:
                    print("❌ 需要提供推文内容")
                    return
                report = process_single_tweet(
                    content=content,
                    handle=parsed["handle"],
                    url=input_text
                )
            else:
                print("❌ 无法解析 URL，请确认格式正确")
                return
        else:
            # 直接作为内容分析
            report = process_single_tweet(content=input_text)

        print("\n" + "="*50)
        print(report[:1500])
        if len(report) > 1500:
            print("\n... (已截断)")

    elif command == "monitor":
        results = monitor_accounts()
        print(f"\n监控结果: {len(results)} 个账号")
        for r in results:
            print(f"  @{r['handle']}: {r['status']}")

    elif command == "accounts":
        accounts = load_accounts()
        print(f"\n📋 监控账号列表 ({len(accounts)} 个):\n")
        for acc in accounts:
            print(f"  @{acc['handle']:20} | {acc.get('category', ''):15} | {acc.get('priority', 'medium')}")

    else:
        print(f"未知命令: {command}")
        print(__doc__)


if __name__ == "__main__":
    main()
"""
arXiv 论文追踪模块
"""

import json
import http.client
import ssl
import xml.etree.ElementTree as ET
import logging
from datetime import datetime, timedelta

logger = logging.getLogger("zhiwei-scheduler")

# 关键词权重（用于相关性评分）
KEYWORDS = {
    "agent": 3, "agents": 3, "llm": 3, "large language model": 3,
    "reasoning": 2, "planning": 2, "tool use": 2,
    "retrieval": 2, "rag": 2, "knowledge": 2,
    "multimodal": 2, "vision": 1, "diffusion": 1,
    "transformer": 1, "attention": 1, "fine-tuning": 2,
    "reinforcement learning": 2, "alignment": 2,
    "code generation": 2, "coding": 2,
}


def fetch_arxiv(categories: list, max_results: int = 20) -> list:
    """
    获取最新 arXiv 论文
    """
    papers = []

    try:
        # 构建查询：多个分类用 OR 连接
        cat_query = "+OR+".join([f"cat:{c}" for c in categories])
        query = f"search_query={cat_query}&start=0&max_results={max_results}&sortBy=submittedDate&sortOrder=descending"

        ctx = ssl.create_default_context()
        conn = http.client.HTTPSConnection("export.arxiv.org", context=ctx)
        conn.request("GET", f"/api/query?{query}",
                     headers={"User-Agent": "Mozilla/5.0"})
        resp = conn.getresponse()

        if resp.status == 200:
            xml_data = resp.read().decode()
            root = ET.fromstring(xml_data)

            ns = {"atom": "http://www.w3.org/2005/Atom"}

            for entry in root.findall("atom:entry", ns):
                title = entry.find("atom:title", ns).text.strip().replace("\n", " ")
                summary = entry.find("atom:summary", ns).text.strip()[:300]
                published = entry.find("atom:published", ns).text[:10]
                link = entry.find("atom:id", ns).text

                authors = []
                for author in entry.findall("atom:author", ns):
                    name = author.find("atom:name", ns).text
                    authors.append(name)

                cats = []
                for cat in entry.findall("atom:category", ns):
                    cats.append(cat.get("term", ""))

                # 计算相关性评分
                score = 0
                text_lower = (title + " " + summary).lower()
                for keyword, weight in KEYWORDS.items():
                    if keyword in text_lower:
                        score += weight

                papers.append({
                    "title": title,
                    "authors": authors[:3],  # 最多3个作者
                    "published": published,
                    "url": link,
                    "summary": summary,
                    "categories": cats[:3],
                    "score": score
                })

        conn.close()

        # 按评分排序
        papers.sort(key=lambda x: x["score"], reverse=True)
        logger.info(f"✅ arXiv 获取成功: {len(papers)} 篇")

    except Exception as e:
        logger.error(f"❌ arXiv 获取失败: {e}")

    return papers


def format_arxiv_markdown(papers: list, min_score: int = 2, limit: int = 10) -> str:
    """格式化论文列表为 Markdown"""
    if not papers:
        return "⚠️ 暂无 arXiv 论文"

    # 过滤低分论文
    filtered = [p for p in papers if p["score"] >= min_score][:limit]
    if not filtered:
        filtered = papers[:5]  # 如果都低分，至少展示5篇

    lines = [f"### 📄 arXiv 论文精选 ({len(filtered)}篇)\n"]

    for i, p in enumerate(filtered, 1):
        stars = "⭐" * min(p["score"], 5)
        authors_str = ", ".join(p["authors"])
        lines.extend([
            f"**{i}. {p['title']}** {stars}",
            f"   👤 {authors_str} | 📅 {p['published']}",
            f"   🏷 {', '.join(p['categories'])}",
            f"   🔗 {p['url']}",
            f"   > {p['summary'][:150]}...\n"
        ])

    return "\n".join(lines)

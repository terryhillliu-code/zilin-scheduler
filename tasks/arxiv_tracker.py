"""
arXiv 论文追踪模块
"""

import json
import http.client
import ssl
import xml.etree.ElementTree as ET
import logging
from datetime import datetime, timedelta

import xml.etree.ElementTree as ET
import logging
import os
import re
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger("zhiwei-scheduler")
SCHEDULER_ROOT = Path("/Users/liufang/zhiwei-scheduler")

# 默认全局权重 (作为兜底)
DEFAULT_KEYWORDS = {
    "agent": 3, "agents": 3, "llm": 3, "large language model": 3,
    "reasoning": 2, "planning": 2, "tool use": 2,
    "retrieval": 2, "rag": 2, "knowledge": 2,
    "multimodal": 2, "vision": 1, "diffusion": 1,
}

VAULT_PATH = Path.home() / "Documents" / "ZhiweiVault"


def fetch_vault_keywords() -> dict:
    """
    动态从 Vault 目录结构提取关键词倾向 (v2.0)
    """
    keywords = DEFAULT_KEYWORDS.copy()
    
    if not VAULT_PATH.exists():
        logger.warning(f"⚠️ Vault 路径不存在: {VAULT_PATH}")
        return keywords

    try:
        # 扫描核心 AI 系统目录 (10-19 码段)
        ai_systems = VAULT_PATH / "10-19_AI-Systems"
        if ai_systems.exists():
            for entry in ai_systems.iterdir():
                if entry.is_dir():
                    # 提取名称中的英文部分，如 "12_多模态智能体_Multimodal-Agent" -> "Multimodal-Agent"
                    parts = re.split(r'[_|-]', entry.name)
                    for p in parts:
                        if re.match(r'^[a-zA-Z]{2,}$', p):
                            kw = p.lower()
                            # 统计目录下文件数作为热点倍率
                            file_count = len(list(entry.glob("*.md")))
                            boost = 1 + (file_count // 5)  # 每5个文件 +1 权重
                            keywords[kw] = max(keywords.get(kw, 2), 3) * boost
                            
        logger.info(f"📊 动态权重加载完成: {len(keywords)} 个关键词")
    except Exception as e:
        logger.error(f"❌ 提取 Vault 权重失败: {e}")
        
    return keywords


def fetch_s2_data(arxiv_id: str) -> dict:
    """
    从 Semantic Scholar 获取论文影响力数据 (v2.0)
    """
    try:
        # 移除版本号 (e.g., 2403.12345v1 -> 2403.12345)
        clean_id = re.sub(r'v\d+$', '', arxiv_id)
        
        ctx = ssl.create_default_context()
        conn = http.client.HTTPSConnection("api.semanticscholar.org", context=ctx)
        url = f"/graph/v1/paper/ARXIV:{clean_id}?fields=citationCount,influentialCitationCount"
        
        conn.request("GET", url, headers={"User-Agent": "Mozilla/5.0"})
        resp = conn.getresponse()
        
        if resp.status == 200:
            data = json.loads(resp.read().decode())
            return {
                "citations": data.get("citationCount", 0),
                "influential": data.get("influentialCitationCount", 0)
            }
        conn.close()
    except Exception as e:
        logger.debug(f"⚠️ S2 数据获取跳过 ({arxiv_id}): {e}")
        
    return {"citations": 0, "influential": 0}


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

                # 计算相关性评分 (v2.0: 动态权重)
                keywords = fetch_vault_keywords()
                kw_score = 0
                text_lower = (title + " " + summary).lower()
                for keyword, weight in keywords.items():
                    if keyword in text_lower:
                        kw_score += weight
                
                # 初始化 paper 对象
                paper = {
                    "title": title,
                    "authors": authors[:3],
                    "published": published,
                    "url": link,
                    "summary": summary,
                    "categories": cats[:3],
                    "kw_score": kw_score,
                    "s2_data": {"citations": 0, "influential": 0},
                    "final_score": kw_score
                }
                papers.append(paper)

            # --- v2.0: 影响力增强 (仅对 Top 15 潜在候选进行分析) ---
            papers.sort(key=lambda x: x["kw_score"], reverse=True)
            for p in papers[:15]:
                if p["kw_score"] > 0:
                    arxiv_id = p["url"].split("/")[-1]
                    s2 = fetch_s2_data(arxiv_id)
                    p["s2_data"] = s2
                    
                    # 影响力评分公式
                    citation_score = (s2["citations"] // 10) + (s2["influential"] * 2)
                    p["final_score"] += citation_score
                    
                    # 时效性奖励 (+5)
                    try:
                        pub_date = datetime.strptime(p["published"], "%Y-%m-%d")
                        if (datetime.now() - pub_date).days <= 3:
                            p["final_score"] += 5
                    except: pass

        conn.close()

        # 按最终评分排序
        papers.sort(key=lambda x: x["final_score"], reverse=True)
        logger.info(f"✅ arXiv 获取成功: {len(papers)} 篇 (含 S2 增强)")
        
        # 将原始数据存入临时文件供周报引擎聚合
        raw_data = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "papers": papers
        }
        raw_file = SCHEDULER_ROOT / f"outputs/raw_arxiv_{raw_data['date']}.json"
        with open(raw_file, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
            
    except Exception as e:
        import traceback
        logger.error(f"❌ arXiv 获取失败: {e}")
        traceback.print_exc()

    return papers


def format_arxiv_markdown(papers: list, min_score: int = 5, limit: int = 10) -> str:
    """格式化论文列表为 Markdown (v2.0)"""
    if not papers:
        return "⚠️ 暂无 arXiv 论文"

    # 过滤低分论文
    filtered = [p for p in papers if p["final_score"] >= min_score][:limit]
    if not filtered:
        filtered = sorted(papers, key=lambda x: x["final_score"], reverse=True)[:5]

    lines = [f"### 📄 arXiv 论文情报 (v2.0 精选 {len(filtered)}篇)\n"]

    for i, p in enumerate(filtered, 1):
        # 动态星级
        score = p["final_score"]
        if score >= 20: stars = "🔥 **[爆款预警]** " + "⭐" * 5
        elif score >= 10: stars = "⭐" * 4
        elif score >= 5: stars = "⭐" * 3
        else: stars = "⭐" * 2
        
        s2 = p.get("s2_data", {"citations": 0, "influential": 0})
        stats = f"(Ref: {s2['citations']} | Inf: {s2['influential']})"
        
        authors_str = ", ".join(p["authors"])
        lines.extend([
            f"**{i}. {p['title']}**",
            f"   {stars} | {stats}",
            f"   👤 {authors_str} | 📅 {p['published']}",
            f"   🏷 {', '.join(p['categories'])}",
            f"   🔗 {p['url']}",
            f"   > {p['summary'][:180]}...\n"
        ])

    return "\n".join(lines)


if __name__ == "__main__":
    # 设置基本的日志格式
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 手动触发一次数据抓取与导出
    print("🚀 启动 ArXiv 追踪任务 (手动模式)...")
    papers_list = fetch_arxiv(["cs.AI", "cs.LG", "cs.CL", "cs.CV"], max_results=15)
    if papers_list:
        md = format_arxiv_markdown(papers_list)
        print("✅ 抓取与评分完成。")
        print(f"📊 原始数据已导出至: {SCHEDULER_ROOT}/outputs/raw_arxiv_{datetime.now().strftime('%Y-%m-%d')}.json")

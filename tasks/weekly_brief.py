"""
智能研究周报生成引擎 (v2.5)
功能：从每日简报中聚合 Top 论文，并生成深度洞察周报。
"""

import json
import os
import re
from pathlib import Path
from datetime import datetime, timedelta
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("weekly-brief")

SCHEDULER_ROOT = Path("/Users/liufang/zhiwei-scheduler")
DATA_DIR = SCHEDULER_ROOT / "outputs/artifacts/sent"
OBSIDIAN_ROOT = Path("/Users/liufang/Documents/ZhiweiVault")
BRIEF_STORE = OBSIDIAN_ROOT / "90-99_Archive/91_Weekly-Briefs"

class WeeklyBriefEngine:
    def __init__(self, days=7):
        self.days = days
        self.cutoff_date = datetime.now() - timedelta(days=days)
        self.papers = {}  # URL -> Paper Info

    def collect_incremental_data(self):
        """
        聚合过去 7 天的每日简报原始数据 (v2.0)
        """
        raw_files = list(SCHEDULER_ROOT.glob("outputs/raw_arxiv_*.json"))
        logger.info(f"📂 发现 {len(raw_files)} 个原始数据文件")
        
        for file_path in raw_files:
            try:
                # 日期过滤
                match = re.search(r'(\d{4}-\d{2}-\d{2})', file_path.name)
                if not match: continue
                file_date = datetime.strptime(match.group(1), "%Y-%m-%d")
                if file_date < self.cutoff_date: continue

                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for p in data.get("papers", []):
                        # 按 URL 去重，保留高分版本
                        url = p.get("url")
                        if url not in self.papers or p.get("final_score", 0) > self.papers[url].get("final_score", 0):
                            self.papers[url] = p
            except Exception as e:
                logger.error(f"❌ 解析 {file_path.name} 失败: {e}")

        logger.info(f"✨ 聚合完成，共 {len(self.papers)} 篇独立论文")

    def synthesize_with_llm(self):
        """
        调用 DashScope (Qwen) 进行主题聚类与洞察合成 (v2.0)
        """
        top_papers = sorted(self.papers.values(), key=lambda x: x.get("final_score", 0), reverse=True)[:15]
        if not top_papers:
            return "⚠️ 本周无可分析的论文数据"

        # 加载密钥
        api_key = os.getenv("DASHSCOPE_API_KEY")
        if not api_key:
            # 尝试从 global.env 加载
            from pathlib import Path
            env_path = Path.home() / ".secrets/global.env"
            if env_path.exists():
                with open(env_path) as f:
                    for line in f:
                        if line.startswith("DASHSCOPE_API_KEY="):
                            api_key = line.split("=")[1].strip().strip('"')
                            break

        if not api_key:
            logger.error("❌ 未找到 DASHSCOPE_API_KEY")
            return "❌ 授权失败"

        # 构建分析上下文
        context_lines = []
        for i, p in enumerate(top_papers, 1):
            context_lines.append(f"[{i}] {p['title']}\nSummary: {p['summary'][:300]}\n")

        prompt_path = SCHEDULER_ROOT / "prompts/weekly_brief.txt"
        system_prompt = open(prompt_path).read() if prompt_path.exists() else "You are a research analyst."
        user_content = f"以下是本周候选论文列表：\n\n" + "\n".join(context_lines)

        import http.client
        import json
        try:
            conn = http.client.HTTPSConnection("dashscope.aliyuncs.com")
            payload = json.dumps({
                "model": "qwen-max",
                "input": {
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_content}
                    ]
                },
                "parameters": {"result_format": "message"}
            })
            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            }
            conn.request("POST", "/api/v1/services/aigc/text-generation/generation", payload, headers)
            res = conn.getresponse()
            data = json.loads(res.read().decode("utf-8"))
            conn.close()
            
            return data["output"]["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"❌ LLM 调用失败: {e}")
            return "❌ LLM 合成失败"

    def save_to_obsidian(self, content: str):
        """
        将周报保存至 Obsidian
        """
        BRIEF_STORE.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y-%m-%W") # 年-月-周
        filename = f"Weekly_Insight_{date_str}.md"
        file_path = BRIEF_STORE / filename
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"✅ 周报已归档至 Obsidian: {filename}")

if __name__ == "__main__":
    engine = WeeklyBriefEngine()
    engine.collect_incremental_data()
    report = engine.synthesize_with_llm()
    engine.save_to_obsidian(report)

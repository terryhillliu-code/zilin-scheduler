请执行 exec python3 /root/workspace/skills/arxiv-tracker/arxiv.py daily --categories '{categories}' --min-score {min_score} --limit {limit} --timeout 60 获取今日 arXiv 论文。

生成论文精选报告：
- 每篇论文包含：标题、作者、日期、arXiv链接
- 一句话总结核心贡献
- 按相关性排序
- "今日趋势"分析

格式为 Markdown，链接必须完整。
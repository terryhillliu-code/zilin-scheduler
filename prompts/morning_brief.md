今天是 {date}，请生成每日早报。

请尝试执行以下命令获取数据（如果新闻获取失败，请基于已知信息生成）：
1. exec python3 /root/workspace/skills/daily-brief/brief.py weather --city 杭州
2. exec python3 /root/workspace/skills/situation-report/sitrep.py news --topic china --limit 8 --timeout 20
3. exec python3 /root/workspace/skills/situation-report/sitrep.py crypto --timeout 15

整合成一份早报，要求：
- 标题：📰 知微早报 + 日期
- 天气：杭州今日天气概况
- 新闻：挑选5条最重要的新闻（如获取失败，请注明"新闻源暂时不可用"并提供历史回顾）
- 加密货币：主流币简报
- 最后写一段"今日提示"

输出格式为 Markdown，适合钉钉/飞书阅读。
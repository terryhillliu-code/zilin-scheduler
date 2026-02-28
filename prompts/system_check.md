请执行系统巡检命令：
1. exec docker ps --format '{{.Names}}\t{{.Status}}'
2. exec df -h /
3. exec launchctl list | grep zhiwei

生成巡检报告：
- Docker 容器状态
- 磁盘使用率
- 关键服务状态
- 总体健康评分（异常项用 🚨 标注）

格式为 Markdown。
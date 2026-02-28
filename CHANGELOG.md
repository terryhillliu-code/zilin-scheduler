# 知微系统改造日志

## 2026-02-27 架构升级

### 阶段一：基础加固

#### 1. 密钥治理
- ✅ 创建统一密钥文件：`~/.secrets/zhiwei.env` 和 `~/.secrets/docker.env`
- ✅ scheduler.py 从环境变量加载配置
- ✅ docker-compose.yml 使用 env_file
- ✅ watchdog.sh 从环境变量读取飞书凭证

#### 2. SQLite 并发优化
- ✅ 启用 WAL 模式 (`journal_mode=WAL`)
- ✅ 设置 `busy_timeout=5000ms`
- ✅ 创建统一数据库连接模块：`~/Documents/Library/klib_db.py`
- ✅ 4 个 klib 脚本统一使用 `get_db_connection()`

#### 3. 向量化脚本优雅退出
- ✅ klib_vectorize.py 添加信号处理
- ✅ 收到 SIGTERM/SIGINT 时完成当前 chunk 后退出

#### 4. Docker 配置优化
- ✅ HuggingFace 缓存挂载改为只读 (`:ro`)
- ✅ 移除硬编码的 API Key，使用 env_file

---

### 阶段二：文件队列系统

#### 核心改造
文件：`scheduler_queue.py` (189 行)

**队列目录结构：**
~/zhiwei-scheduler/outputs/artifacts/
├── pending/ # 待推送
├── processing/ # 正在推送（原子锁）
├── sent/ # 推送成功
└── failed/ # 推送失败，等待重试

text


**核心函数：**
- `save_result()` - 原子落盘（tmp + fsync + os.replace）
- `try_push()` - 并发安全推送（claim_file 原子领取）
- `retry_failed()` - 重试失败任务
- `cleanup_old_files()` - 清理旧文件

#### Job 函数改造
改造了 7 个定时任务：
1. `job_morning_brief` - 每日早报
2. `job_noon_brief` - 每日午报
3. `job_us_market_open` - 美股开盘
4. `job_us_market_close` - 美股收盘
5. `job_crypto` - 加密货币播报
6. `job_arxiv` - arXiv 论文
7. `job_system_check` - 系统巡检

**改造模式：**
```python
# 原代码
push_manager.push(title, content, channels)

# 新代码
file_path = save_result(task="job_name", content=content, targets=channels)
success = try_push(file_path, push_manager, logger)
重试机制
✅ 创建 retry_failed.py
✅ Crontab 每小时自动重试（最多 3 次）
✅ 超过重试次数的任务保留在 failed/ 等待人工处理
优化项
1. 队列监控命令
文件：~/zhiwei-scheduler/queue

用法：

Bash

queue stats              # 队列统计
queue list [queue]       # 列出任务
queue inspect <job_id>   # 查看详情
queue cleanup [days]     # 清理旧文件
queue retry              # 手动重试
2. 失败告警增强
✅ check_and_alert() 函数
✅ failed 队列积压超过 3 个时自动发送飞书告警
✅ processing 任务卡住超过 30 分钟时告警
3. 队列自动清理
✅ Crontab 每天 04:30 清理 7 天前的 sent 文件
✅ 防止磁盘占用无限增长
4. 健康检查增强
✅ job_system_check 包含队列状态
✅ 每天 07:00 自动巡检时报告队列积压
5. 推送去重保护
✅ is_already_sent() 检查同一 job_id 当天是否已推送
✅ save_result_safe() 防止重复推送
6. Watchdog 增强
✅ 从环境变量读取飞书凭证（不再硬编码）
✅ 检查 processing 卡住的任务
✅ 检查 failed 队列积压
关键收益
收益点	改造前	改造后
LLM 生成结果丢失风险	推送失败即丢失	永久保存在队列中
推送失败处理	手动重跑	自动重试（最多3次）
同一任务重复推送	可能重复	去重保护
队列可观测性	无	queue stats 一目了然
密钥安全	硬编码在代码中	统一存储在 ~/.secrets/
SQLite 并发	delete 日志模式	WAL 模式 + 5s timeout
向量化中断	可能损坏数据	优雅退出
依赖关系
新增依赖
无（零新依赖，使用标准库）
新增文件
text

~/.secrets/zhiwei.env          # 密钥文件（不入库）
~/.secrets/docker.env          # Docker 专用密钥（无注释）
~/Documents/Library/klib_db.py # 数据库连接模块
~/zhiwei-scheduler/scheduler_queue.py  # 队列模块
~/zhiwei-scheduler/retry_failed.py     # 重试脚本
~/zhiwei-scheduler/queue               # 队列管理命令
Crontab 任务
cron

# 重试失败推送（每小时）
0 * * * *  cd ~/zhiwei-scheduler && source venv/bin/activate && python3 retry_failed.py >> ~/logs/retry.log 2>&1

# 清理旧队列文件（每天 04:30）
30 4 * * *  cd ~/zhiwei-scheduler && source venv/bin/activate && ./queue cleanup 7 >> ~/logs/retry.log 2>&1

# Watchdog 健康检查（每 5 分钟）
*/5 * * * * ~/scripts/watchdog.sh >> ~/logs/watchdog.log 2>&1
运维命令速查
查看队列状态
Bash

cd ~/zhiwei-scheduler && source venv/bin/activate && ./queue stats
查看失败任务
Bash

./queue list failed
./queue inspect <job_id>
手动重试
Bash

python3 retry_failed.py
清理旧文件
Bash

./queue cleanup 7
查看日志
Bash

tail -f ~/logs/zhiwei-scheduler.error.log
tail -f ~/logs/retry.log
明天观察要点
自动任务时间表
时间	任务	观察点
07:00	系统巡检	① 钉钉推送成功 ② 报告包含队列状态
07:30	arXiv 论文	钉钉+飞书双推送
08:30	美股收盘	钉钉+飞书双推送
09:30	每日早报	钉钉+飞书双推送
检查清单
 queue stats 显示今日成功数正确
 sent/ 目录有 4 个新文件
 没有任务进入 failed/
 watchdog.log 无告警
未来扩展触发条件
需要容器化时
场景：迁移到云服务器
工作量：1 天
准备：docker-compose.yml 已更新，可直接使用
需要 Redis/MQ 时
场景：任务量 >50/天 或需要多生产者
工作量：1.5 天
当前方案：文件队列足够
需要 knowledge-service 时
场景：书籍 >50 本或出现 SQLite 锁竞争
工作量：1 天
当前方案：WAL + busy_timeout 足够
需要 PostgreSQL/pgvector 时
场景：知识库数据 >10GB
工作量：2 天
当前方案：SQLite + ChromaDB 足够
技术债务
低优先级
 合并 knowledge-base 和 knowledge-library Skill
 State dir migration 警告处理
 其他定时任务 prompt 格式优化
不需要做
❌ 引入 Gemini API（成本 vs 收益不匹配）
❌ 立即容器化（当前规模不需要）
❌ 立即上 PostgreSQL（SQLite 足够）
改造统计
代码改动量: ~600 行（新增 + 修改）
实施时间: 5 小时
新增依赖: 0
新增容器: 0
新增服务: 0（复用现有 launchctl + crontab）
可靠性提升: ∞（推送失败不再丢失结果）
生成时间: 2026-02-27 23:59
改造人员: Claude + 用户
版本: v3.1

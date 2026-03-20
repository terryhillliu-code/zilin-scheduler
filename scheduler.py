#!/usr/bin/env python3
"""
知微定时任务调度器 v3.4 (2026-03-01 Phase 2)
特性: Prompt 外部化、支持热更新、重试机制、触发器监听
"""

import os
import sys
import json
import signal
import subprocess
import logging
import logging.handlers
import time
import threading
from datetime import datetime, timedelta
from pathlib import Path

# 新增业务跳过异常类
class TaskSkippedException(Exception):
    pass


# 直接初始化 logger（不再是 None）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

import yaml
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

BASE_DIR = Path(__file__).parent
PROMPT_DIR = BASE_DIR / "prompts"
TRIGGER_DIR = BASE_DIR / "triggers"
sys.path.insert(0, str(BASE_DIR))

# 初始化模块级别的全局变量，防止未定义错误
config = None
push_manager = None

from pusher import PushManager
from scheduler_queue import save_result, try_push, save_result_safe

# 新增：新闻去重模块
from news_dedup import should_push, load_sent_today, get_sent_titles, record_sent, extract_titles_from_content

# 新增：锁管理、缓存、触发器
from lock_manager import acquire_lock, try_acquire_lock, release_lock
from price_cache import has_price_changed, update_price_cache
import trigger_listener

CONTAINER = "clawdbot"

# 尝试导入新的 RAG 桥接
try:
    from rag_bridge import enrich_with_rag, is_available as rag_is_available
    RAG_AVAILABLE = rag_is_available()
except ImportError:
    RAG_AVAILABLE = False
    def enrich_with_rag(query, top_k=5):
        return ""

# 导入 LLM 客户端 (V2-102: 替代 OpenClaw)
sys.path.insert(0, str(Path.home() / "zhiwei-bot"))
try:
    from llm_client import llm_client, LLMClient
    LLM_CLIENT_AVAILABLE = True
except ImportError:
    LLM_CLIENT_AVAILABLE = False
    llm_client = None
    logger.warning("⚠️ llm_client 导入失败，将使用降级模式")

# ============ 指数退避重试配置 ============
RETRY_DELAYS = [120, 300, 600]  # 2min, 5min, 10min

# JSONL 日志路径
JSON_LOG_PATH = Path.home() / "logs" / "scheduler.jsonl"


def is_quiet_hours(now: datetime = None) -> bool:
    """
    检查当前时间是否在静默时段（23:00-06:30）
    重试任务在静默时段不推送
    """
    if now is None:
        now = datetime.now()
    hour = now.hour
    minute = now.minute

    # 23:00-06:30 为静默时段
    if hour >= 23 or hour < 6:
        return True
    if hour == 6 and minute < 30:
        return True
    return False


def get_retry_delay(attempt: int) -> int:
    """获取指数退避延迟"""
    if attempt <= len(RETRY_DELAYS):
        return RETRY_DELAYS[attempt - 1]
    return RETRY_DELAYS[-1]  # 最大40分钟


# ============ 结构化 JSONL 日志 ============

def log_task_metrics(
    task_name: str,
    start_time: float,
    end_time: float,
    success: bool,
    push_status: dict = None,
    token_usage: dict = None,
    error_msg: str = None,
    is_skipped: bool = False
):
    """
    将任务指标写入 JSONL 日志
    """
    import json

    log_entry = {
        "task_name": task_name,
        "start_time": datetime.fromtimestamp(start_time).isoformat(),
        "end_time": datetime.fromtimestamp(end_time).isoformat(),
        "latency_seconds": round(end_time - start_time, 2),
        "success": success,
        "is_skipped": is_skipped,
        "push_status": push_status or {},
        "token_usage": token_usage or {},
        "error_msg": error_msg
    }

    JSON_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(JSON_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    # 安全地记录调试日志
    global logger
    if logger:
        logger.debug(f"📊 任务指标已记录: {task_name}")

    # T-016.3: 如果任务失败（且不是跳过），发送告警
    if not success and not is_skipped:
        send_failure_alert(task_name, error_msg)


# ============ 任务失败告警 (T-016.3) ============

def send_failure_alert(task_name: str, error_msg: str = None):
    """
    发送任务失败告警到钉钉/飞书
    """
    global logger  # 声明使用全局 logger（必须在函数开头）
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 截取错误信息
        error_summary = "未知异常"
        if error_msg:
            error_summary = str(error_msg)[:100]
            if len(str(error_msg)) > 100:
                error_summary += "..."

        # 构建告警消息
        alert_content = f"""## 🚨 系统任务执行异常 (Immediate Alert)

**任务名称**: {task_name}

**发生时间**: {current_time}

**异常摘要**:
```
{error_summary}
```

**建议**: 请查看 ~/logs/scheduler.jsonl 获取详细堆栈信息。
"""

        # 保存告警到临时文件
        alert_dir = BASE_DIR / "outputs"
        alert_dir.mkdir(exist_ok=True)
        alert_file = alert_dir / f"alert_{task_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

        with open(alert_file, "w", encoding="utf-8") as f:
            f.write(alert_content)

        # 推送到钉钉（紧急告警使用钉钉）
        channels = ["dingtalk"]
        file_path = save_result(
            task=f"alert_{task_name}",
            content=alert_content,
            targets=channels,
            metadata={"type": "alert", "level": "critical"}
        )

        # 立即推送
        success = try_push(file_path, push_manager, logger, return_status=True)
        if success and success.get("dingtalk"):
            if logger:
                logger.info(f"✅ 告警已发送: {task_name}")
        else:
            if logger:
                logger.warning(f"⚠️ 告警发送失败: {task_name}")

    except Exception as e:
        if logger:
            logger.error(f"❌ 告警发送异常: {e}")


# ============ 日志 ============

def setup_logging(log_dir: str = "logs", retention_days: int = 30):
    global logger  # 声明使用全局 logger
    log_path = BASE_DIR / log_dir
    log_path.mkdir(exist_ok=True)

    logger = logging.getLogger("zhiwei-scheduler")
    logger.setLevel(logging.DEBUG)

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    ))
    logger.addHandler(console)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_path / "scheduler.log",
        when="midnight", backupCount=retention_days, encoding="utf-8"
    )
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    ))
    logger.addHandler(file_handler)

    return logger


# ============ Prompt 加载器 (Phase 2 新增) ============

def load_prompt(template_name: str, **kwargs) -> str:
    """
    加载 Markdown 模板并填充变量
    """
    path = PROMPT_DIR / f"{template_name}.md"
    if not path.exists():
        logger.error(f"❌ 找不到 Prompt 模板: {path}")
        return f"Error: Prompt template {template_name} missing."

    try:
        content = path.read_text(encoding="utf-8")
        return content.format(**kwargs)
    except KeyError as e:
        logger.error(f"❌ Prompt 渲染失败 [{template_name}]: 缺少变量 {e}")
        # 返回错误信息，包含缺失的变量名
        return f"Error: Missing variable {e} in template {template_name}."
    except Exception as e:
        logger.error(f"❌ Prompt 渲染失败 [{template_name}]: {e}")
        # 返回错误信息而不是原始模板
        return f"Error: Failed to render template {template_name}: {e}"

# ============ Agent 调用 ============

def call_llm_direct(message: str, timeout: int = 180) -> tuple[bool, str]:
    """
    绕过 OpenClaw，直接调用本地代理 (8045)
    用于解决 Agent/模型工具冲突时的降级方案
    """
    import http.client
    import json
    
    try:
        payload = json.dumps({
            "model": "qwen3.5-plus",
            "messages": [{"role": "user", "content": message}],
            "temperature": 0.7
        })
        
        conn = http.client.HTTPConnection("127.0.0.1", 8045, timeout=timeout)
        conn.request("POST", "/v1/chat/completions", body=payload, headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        data = json.loads(resp.read().decode())
        conn.close()
        
        if resp.status == 200:
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return True, content
        else:
            err = data.get("error", {}).get("message", "Unknown error")
            return False, f"LLM Direct Error {resp.status}: {err}"
    except Exception as e:
        return False, f"LLM Direct Exception: {e}"


def call_agent(agent_id: str, message: str, timeout: int = 180) -> tuple[bool, str]:
    """
    调用 LLM Agent（直连百炼 API，不再依赖 OpenClaw/Docker）

    V2-102 重构：
    - 使用 llm_client 直连百炼 API
    - 移除 Docker 依赖
    - 保留降级逻辑

    Args:
        agent_id: Agent ID (main/researcher/operator)
        message: 用户消息
        timeout: 超时时间（秒）

    Returns:
        (success, content) 元组
    """
    # 优先使用 llm_client（直连百炼）
    if LLM_CLIENT_AVAILABLE and llm_client:
        try:
            # agent_id 直接作为 role 使用（llm_client 已映射）
            success, content = llm_client.call(
                role=agent_id,
                message=message,
                timeout=timeout
            )
            if success:
                logger.info(f"✅ LLM 调用成功: {agent_id}, {len(content)} 字符")
                return True, content
            else:
                logger.warning(f"⚠️ LLM 调用失败: {content}")
                # 继续尝试降级
        except Exception as e:
            logger.error(f"❌ LLM 客户端异常: {e}")

    # 降级：使用原有的 call_llm_direct
    logger.warning("🔄 LLM 客户端不可用，降级到本地代理...")
    return call_llm_direct(message, timeout)



# ============ RAG 上下文获取 (T-017) ============

# 任务名称到关键词的映射表
KLIB_ENRICHMENT = {
    "arxiv_papers": ["芯片架构", "HBM", "CoWoS", "MoE", "分布式训练"],
    "morning_brief": ["半导体", "封装", "推理加速"],
    "noon_brief": ["AI", "大模型", "GPU"],
    "info_brief": ["全球资讯", "经济动态", "科技前沿", "市场观察"],
}


# ============ A/B 测试配置 (T-019) ============
# 使用方式：在 AB_TESTS 中添加测试条目，然后手动运行 run_ab_test('test_id')
# 示例：
# AB_TESTS = {
#     "morning_v2_test": {
#         "task_name": "morning_brief",
#         "prompt_a": "morning_brief",
#         "prompt_b": "morning_brief_v2",
#         "agent": "researcher",
#         "timeout": 240,
#         "push_to": ["dingtalk"],
#     },
# }
AB_TESTS = {}


def run_ab_test(test_id: str):
    """
    运行 A/B 测试：用两个 prompt 版本各调用一次 agent，
    将结果并排推送给用户对比。
    """
    if test_id not in AB_TESTS:
        logger.warning(f"A/B 测试 [{test_id}] 不存在，可用测试: {list(AB_TESTS.keys())}")
        return

    config = AB_TESTS[test_id]
    task_name = config["task_name"]
    agent = config.get("agent", "researcher")
    timeout = config.get("timeout", 240)
    push_to = config.get("push_to", ["dingtalk"])

    now = datetime.now()
    date_str = now.strftime('%Y年%m月%d日 %A')
    results = {}

    for version_key in ["prompt_a", "prompt_b"]:
        template_name = config[version_key]
        label = "A" if version_key == "prompt_a" else "B"

        try:
            # 加载 prompt 模板
            prompt_kwargs = {"date": date_str}
            # 根据任务类型添加额外参数
            if "arxiv" in template_name:
                prompt_kwargs = {"categories": "cs.AI,cs.LG,cs.CL,cs.CV", "min_score": 2, "limit": 10}
            elif "noon" in template_name:
                prompt_kwargs = {"time": now.strftime('%m月%d日 %H:%M')}
            elif "crypto" in template_name:
                prompt_kwargs = {"period": "morning"}

            prompt = load_prompt(template_name, **prompt_kwargs)
            prompt = enrich_with_klib(task_name, prompt)

            ok, content = call_agent(agent, prompt, timeout=timeout)

            if ok:
                results[label] = content
            else:
                results[label] = f"[调用失败] {content}"

        except Exception as e:
            results[label] = f"[异常] {str(e)}"

    # 组装对比报告
    report = f"## 📊 A/B 测试报告 [{test_id}]\n\n"
    report += f"- **时间**: {now.strftime('%Y-%m-%d %H:%M')}\n"
    report += f"- **任务**: {task_name}\n"
    report += f"- **Prompt A**: {config['prompt_a']}\n"
    report += f"- **Prompt B**: {config['prompt_b']}\n\n"
    report += "---\n\n"
    report += "### 【版本 A】\n\n"
    report += results.get("A", "[无结果]") + "\n\n"
    report += "---\n\n"
    report += "### 【版本 B】\n\n"
    report += results.get("B", "[无结果]") + "\n"

    # 保存并推送对比报告
    try:
        file_path = save_result(
            task=f"ab_test_{test_id}",
            content=report,
            targets=push_to,
            metadata={"prompt_a": config["prompt_a"], "prompt_b": config["prompt_b"]}
        )
        success = try_push(file_path, push_manager, logger, return_status=True)
        logger.info(f"✅ A/B 测试 [{test_id}] 已推送")
    except Exception as e:
        logger.error(f"❌ A/B 测试推送失败 [{test_id}]: {e}")

    # 记录到 jsonl
    log_task_metrics(
        task_name=f"ab_test_{test_id}",
        success=all(not v.startswith("[") for v in results.values()),
        latency_seconds=0,
        token_usage={}
    )


# ============ GraphRAG 并发保护 (Phase 5+) ============
# 限制同时运行的 GraphRAG 子进程数量，防止线程/内存压爆
graphrag_semaphore = threading.Semaphore(2)  # 最多允许 2 个并发查询

def enrich_with_graphrag(task_name: str, prompt_text: str) -> str:
    """
    使用 GraphRAG (LightRAG) 增强 prompt (Phase 4c - T-407)
    根据任务名称查询相关关键词，将结果注入 prompt
    """
    # Phase 4c: 使用子进程查询 GraphRAG，彻底解决事件循环冲突
    import subprocess
    import os
    import threading 
    from pathlib import Path

    cli_path = BASE_DIR / "graph_query_cli.py"
    if not cli_path.exists():
        logger.warning(f"⚠️ 找不到 GraphRAG CLI: {cli_path}")
        return prompt_text

    # 对于 info_brief_XX 类型的任务，使用 info_brief 作为键
    lookup_key = task_name
    if task_name.startswith("info_brief_"):
        lookup_key = "info_brief"

    if lookup_key not in KLIB_ENRICHMENT:
        return prompt_text

    try:
        keywords = KLIB_ENRICHMENT[lookup_key]
        results = []
        seen_paragraphs = set() # 用于语义去重（简单段落级）

        # 知识衰减：注入当前日期上下文
        current_date = datetime.now().strftime("%Y-%m-%d")
        decay_prompt = f"请优先参考2025-2026年的最新知识。当前日期是 {current_date}。"

        for kw in keywords:
            # 申请信号量，受限并发
            with graphrag_semaphore:
                try:
                    logger.info(f"🔍 正在通过 GraphRAG 子进程查询: {kw} (Semaphore 保护中)")
                    env = os.environ.copy()
                    python_exe = sys.executable or "python3"
                    
                    # 注入 user_prompt 实现知识衰减
                    cmd = [python_exe, str(cli_path), "--query", kw, "--mode", "hybrid", "--user_prompt", decay_prompt]
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60, env=env)
                    
                    if result.returncode == 0:
                        res = result.stdout.strip()
                        # 只有当结果足够丰富（非空且不是报错）时才注入
                        if res and len(res) > 50 and "抱歉" not in res and "ERROR" not in res:
                            # 简单的段落级去重逻辑
                            unique_paragraphs = []
                            for para in res.split("\n\n"):
                                para_stripped = para.strip()
                                if not para_stripped: continue
                                # 如果该段落已有 80% 相似（这里简化为全文匹配或包含）则跳过
                                if para_stripped[:100] not in seen_paragraphs:
                                    unique_paragraphs.append(para_stripped)
                                    seen_paragraphs.add(para_stripped[:100])
                            
                            if unique_paragraphs:
                                results.append(f"【{kw} 领域图谱分析】\n" + "\n\n".join(unique_paragraphs))
                    else:
                        logger.warning(f"⚠️ GraphRAG CLI 执行失败 ({kw}): {result.stderr.strip()[:100]}")
                except Exception as e:
                    logger.warning(f"⚠️ GraphRAG 进程调用异常 ({kw}): {e}")

        if not results:
            return prompt_text

        enrichment = "\n---\n[GraphRAG 深度知识库参考]\n以下是系统知识图谱中萃取的深层分析视角，已根据当前日期（{0}）进行时效性加权并去重：\n\n".format(current_date) + "\n\n".join(results) + "\n---\n"
        logger.info(f"✅ 已注入 GraphRAG 增强上下文 (task: {task_name}, keywords: {keywords})")
        return prompt_text + enrichment

    except Exception as e:
        logger.error(f"❌ enrich_with_graphrag 顶层异常: {e}")
        return prompt_text

# 兼容旧的方法调用名 (改为 legacy)
enrich_with_klib_legacy = enrich_with_graphrag

def enrich_with_klib(task_name: str, prompt_text: str, top_k: int = 5) -> str:
    """
    增强版知识库检索
    优先使用 zhiwei-rag (三轨融合+精排)，降级到 GraphRAG (旧)
    """
    # 尝试新 RAG
    if RAG_AVAILABLE:
        try:
            # 对于 info_brief_XX 类型的任务，提取关键词作为查询
            query = task_name
            if task_name.startswith("info_brief_"):
                query = "最近全球重要资讯和科技动态" # 默认查询
                if task_name in KLIB_ENRICHMENT:
                    query = ", ".join(KLIB_ENRICHMENT[task_name])
            elif task_name in KLIB_ENRICHMENT:
                query = ", ".join(KLIB_ENRICHMENT[task_name])
            
            context = enrich_with_rag(query, top_k=top_k)
            if context and len(context) > 100:
                logger.info(f"🚀 [Scheduler] 使用 zhiwei-rag 增强 (Recall/Rerank)，上下文 {len(context)} 字符")
                # 注入到 Prompt
                enrichment = f"\n---\n[zhiwei-rag 检索参考]\n以下是从 300+ 份研报及知识库中检索到的相关资料（已通过 Reranker 精排）：\n\n{context}\n---\n"
                return prompt_text + enrichment
        except Exception as e:
            logger.warning(f"⚠️ zhiwei-rag 失败，降级到 GraphRAG: {e}")
    
    # 降级到旧方案 (GraphRAG)
    return enrich_with_klib_legacy(task_name, prompt_text)





# ============ 任务定义 ============

def job_morning_brief():
    """每日早报 09:30"""
    global logger, config, push_manager  # 声明使用全局变量
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    if logger:
        logger.info("📰 === 每日早报 ===")
    task_name = "morning_brief"
    
    # T-016.4: 任务互斥锁
    if not try_acquire_lock(task_name):
        if logger:
            logger.warning(f"⏩ {task_name} 正在执行中，跳过本次触发")
        return
    start_time = time.time()
    push_status = {"dingtalk": False, "feishu": False}

    def _run():
        now = datetime.now()
        # Phase 2: 从文件加载 Prompt
        prompt = load_prompt("morning_brief", date=now.strftime('%Y年%m月%d日 %A'))

        # ============ RAG 增强 (T-017) ============
        prompt = enrich_with_klib("morning_brief", prompt)

        ok, content = call_agent("researcher", prompt, timeout=240)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        save_output("morning_brief", content)
        job_conf = config.get("jobs", {}).get("morning_brief", {})
        channels = job_conf.get("push_to", ["dingtalk", "feishu"])

        file_path = save_result(
            task="morning_brief",
            content=content,
            targets=channels,
            metadata={"agent": "researcher"}
        )
        logger.info(f"📦 结果已保存: {file_path}")

        # === Obsidian 归档 ===
        try:
            from obsidian_archive import archive_to_obsidian
            obs_path = archive_to_obsidian(
                content=content,
                note_type="brief",
                tags=["scheduler", "早报"],
                task_name=task_name
            )
            logger.info(f"📝 已归档到 Obsidian: {obs_path.name}")
        except Exception as e:
            logger.warning(f"Obsidian 归档失败（不影响推送）: {e}")

        success = try_push(file_path, push_manager, logger, return_status=True)
        push_status.update(success or {})
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info("✅ 早报推送完成")

    try:
        _run()
    finally:
        release_lock(task_name)
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=push_status.get("dingtalk") or 
                    push_status.get("feishu") or 
                    push_status.get("_skipped_by_concurrency"),
            push_status=push_status
        )


def job_noon_brief():
    """每日午报 14:30"""
    global logger, config, push_manager  # 声明使用全局变量
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    if logger:
        logger.info("🌤 === 每日午报 ===")
    task_name = "noon_brief"

    # T-016.4: 任务互斥锁
    if not try_acquire_lock(task_name):
        if logger:
            logger.warning(f"⏩ {task_name} 正在执行中，跳过本次触发")
        return
    start_time = time.time()
    push_status = {"dingtalk": False, "feishu": False}

    def _run():
        now = datetime.now()
        # Phase 2: 从文件加载 Prompt
        prompt = load_prompt("noon_brief", time=now.strftime('%m月%d日 %H:%M'))

        # ============ RAG 增强 (T-017) ============
        prompt = enrich_with_klib("noon_brief", prompt)

        ok, content = call_agent("researcher", prompt, timeout=180)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        save_output("noon_brief", content)
        job_conf = config.get("jobs", {}).get("noon_brief", {})
        channels = job_conf.get("push_to", ["dingtalk", "feishu"])

        file_path = save_result(
            task="noon_brief",
            content=content,
            targets=channels,
            metadata={"agent": "researcher"}
        )
        if logger:
            logger.info(f"📦 结果已保存: {file_path}")

        # === Obsidian 归档 ===
        try:
            from obsidian_archive import archive_to_obsidian
            obs_path = archive_to_obsidian(
                content=content,
                note_type="brief",
                tags=["scheduler", "午报"],
                task_name=task_name
            )
            if logger:
                logger.info(f"📝 已归档到 Obsidian: {obs_path.name}")
        except Exception as e:
            if logger:
                logger.warning(f"Obsidian 归档失败（不影响推送）: {e}")

        success = try_push(file_path, push_manager, logger, return_status=True)
        push_status.update(success or {})
        if not success:
            if logger:
                logger.warning("推送失败，已进入重试队列")
        else:
            if logger:
                logger.info("✅ 午报推送完成")

    try:
        _run()
    finally:
        release_lock(task_name)
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=push_status.get("dingtalk") or 
                    push_status.get("feishu") or 
                    push_status.get("_skipped_by_concurrency"),
            push_status=push_status
        )


def job_info_brief(hour: int):
    """信息流简报 (每2小时: 07, 09, 11, 13, 15, 17, 19, 21)"""
    global logger, config, push_manager  # 声明使用全局变量
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    if logger:
        logger.info(f"📰 === 信息流简报 {hour:02d}:00 ===")
    task_name = f"info_brief_{hour:02d}"

    # T-016.4: 任务互斥锁，防止并发执行
    if not try_acquire_lock(task_name):
        if logger:
            logger.warning(f"⏩ {task_name} 正在执行中，跳过本次触发")
        return
    start_time = time.time()
    push_status = {"dingtalk": False, "feishu": False}

    # 检查是否在静默时段（23:00-06:30）
    now = datetime.now()
    if is_quiet_hours(now):
        if logger:
            logger.info(f"🛑 当前在静默时段（23:00-06:30），跳过推送")
        return

    def _run():
        # Phase 2: 从文件加载 Prompt
        date_str = now.strftime('%Y年%m月%d日 %A')
        weekday_str = now.strftime('%A')
        sent_news = get_sent_titles()

        # 天气和加密货币部分由 Agent 在 prompt 中通过 exec 命令生成
        weather_section = "{weather_section}"
        crypto_section = "{crypto_section}"

        prompt = load_prompt(
            "info_brief",
            date=date_str,
            weekday=weekday_str,
            sent_news=sent_news,
            weather_section=weather_section,
            crypto_section=crypto_section
        )

        # ============ RAG 增强 (T-017) ============
        prompt = enrich_with_klib(task_name, prompt)

        ok, content = call_agent("researcher", prompt, timeout=600)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        # 检查是否所有数据源都失败
        if "EXEC_ALL_FAILED" in content:
            logger.warning("⚠️ 所有数据源获取失败，跳过本次推送")
            save_output(task_name, content)
            raise TaskSkippedException("EXEC_ALL_FAILED")

        # ============ 新闻去重检查 ============
        # 检查是否有新内容（至少2条新的）
        if "NO_NEW_CONTENT" in content:
            logger.info(f"📋 无新内容，跳过推送")
            save_output(task_name, content)
            raise TaskSkippedException("NO_NEW_CONTENT")

        # 检查是否应该推送
        if not should_push(content):
            logger.info(f"📋 新闻无变化，跳过推送")
            save_output(task_name, content)
            raise TaskSkippedException("NO_NEW_CONTENT")

        save_output(task_name, content)
        channels = config["jobs"].get(task_name, {}).get("push_to", ["dingtalk", "feishu"])

        file_path = save_result(
            task=task_name,
            content=content,
            targets=channels,
            metadata={"agent": "researcher", "hour": hour}
        )
        logger.info(f"📦 结果已保存: {file_path}")

        # === Obsidian 归档 ===
        try:
            from obsidian_archive import archive_to_obsidian
            obs_path = archive_to_obsidian(
                content=content,
                note_type="brief",
                tags=["scheduler", "信息流"],
                task_name=task_name
            )
            logger.info(f"📝 已归档到 Obsidian: {obs_path.name}")
        except Exception as e:
            logger.warning(f"Obsidian 归档失败（不影响推送）: {e}")

        success = try_push(file_path, push_manager, logger, return_status=True)
        push_status.update(success or {})

        if success and (push_status.get("dingtalk") or push_status.get("feishu")):
            # 记录推送的新闻标题（用于下次去重）
            titles = extract_titles_from_content(content)
            if titles:
                record_sent(titles)
                if logger:
                    logger.info(f"✅ 记录 {len(titles)} 条已推送新闻")
            if logger:
                logger.info("✅ 信息流简报推送完成")
        else:
            if logger:
                logger.warning("推送失败，已进入重试队列")

    error_msg = None
    is_skipped = False
    try:
        _run()
    except TaskSkippedException as e:
        is_skipped = True
        if logger:
            logger.info(f"ℹ️ info_brief_{hour:02d} 已确认业务跳过: {e}")
    except Exception as e:
        error_msg = str(e)
        if logger:
            logger.error(f"❌ info_brief_{hour:02d} 失败: {error_msg}")
        
        # 兼容旧代码里抛出普通Exception也可能带这些字眼的情况
        if "NO_NEW_CONTENT" in error_msg or "NO_NEWS" in error_msg.upper():
            is_skipped = True
            if logger:
                logger.info(f"ℹ️ info_brief_{hour:02d} 已确认业务跳过")

    finally:
        release_lock(task_name)
        end_time = time.time()
        # 只要不是 Exception 抛出且完成了推送，或者明确标记为 skipped，就不告警
        success_final = (
            push_status.get("dingtalk") or
            push_status.get("feishu") or
            push_status.get("_skipped_by_concurrency")
        )
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=success_final,
            push_status=push_status,
            error_msg=error_msg,
            is_skipped=is_skipped
        )


def job_us_market_open():
    """美股开盘 21:30（工作日）"""
    global logger, config, push_manager  # 声明使用全局变量
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    if logger:
        logger.info("🔔 === 美股开盘 ===")
    task_name = "us_market_open"
    
    # T-016.4: 任务互斥锁
    if not try_acquire_lock(task_name):
        if logger:
            logger.warning(f"⏩ {task_name} 正在执行中，跳过本次触发")
        return
    start_time = time.time()
    push_status = {"dingtalk": False, "feishu": False}

    def _run():
        prompt = load_prompt("us_market_open")
        ok, content = call_agent("researcher", prompt, timeout=180)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        save_output(task_name, content)
        channels = config["jobs"][task_name].get("push_to", ["dingtalk", "feishu"])

        file_path = save_result(
            task=task_name,
            content=content,
            targets=channels,
            metadata={"agent": "researcher"}
        )
        logger.info(f"📦 结果已保存: {file_path}")

        # === Obsidian 归档 ===
        try:
            from obsidian_archive import archive_to_obsidian
            obs_path = archive_to_obsidian(
                content=content,
                note_type="market",
                tags=["scheduler", "美股开盘"],
                task_name=task_name
            )
            logger.info(f"📝 已归档到 Obsidian: {obs_path.name}")
        except Exception as e:
            logger.warning(f"Obsidian 归档失败（不影响推送）: {e}")

        success = try_push(file_path, push_manager, logger, return_status=True)
        push_status.update(success or {})
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info(f"✅ {task_name} 推送成功")

    error_msg = None
    try:
        _run()
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ {task_name} 失败: {error_msg}")
    finally:
        release_lock(task_name)
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=push_status.get("dingtalk") or
                    push_status.get("feishu") or
                    push_status.get("_skipped_by_concurrency"),
            push_status=push_status,
            error_msg=error_msg
        )


def job_us_market_close():
    """美股收盘复盘 07:30（次日推送）"""
    global logger, config, push_manager  # 声明使用全局变量
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    if logger:
        logger.info("📊 === 美股收盘复盘 ===")
    task_name = "us_market_close"

    # T-016.4: 任务互斥锁
    if not try_acquire_lock(task_name):
        if logger:
            logger.warning(f"⏩ {task_name} 正在执行中，跳过本次触发")
        return
    start_time = time.time()
    push_status = {"dingtalk": False, "feishu": False}

    def _run():
        prompt = load_prompt("us_market_close")
        ok, content = call_agent("researcher", prompt, timeout=180)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        save_output(task_name, content)
        channels = config["jobs"][task_name].get("push_to", ["dingtalk", "feishu"])

        file_path = save_result(
            task=task_name,
            content=content,
            targets=channels,
            metadata={"agent": "researcher"}
        )
        logger.info(f"📦 结果已保存: {file_path}")

        # === Obsidian 归档 ===
        try:
            from obsidian_archive import archive_to_obsidian
            obs_path = archive_to_obsidian(
                content=content,
                note_type="market",
                tags=["scheduler", "美股收盘"],
                task_name=task_name
            )
            logger.info(f"📝 已归档到 Obsidian: {obs_path.name}")
        except Exception as e:
            logger.warning(f"Obsidian 归档失败（不影响推送）: {e}")

        success = try_push(file_path, push_manager, logger, return_status=True)
        push_status.update(success or {})
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info(f"✅ {task_name} 推送成功")

    error_msg = None
    try:
        _run()
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ {task_name} 失败: {error_msg}")
    finally:
        release_lock(task_name)
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=push_status.get("dingtalk") or
                    push_status.get("feishu") or
                    push_status.get("_skipped_by_concurrency"),
            push_status=push_status,
            error_msg=error_msg
        )


def job_crypto(period: str = "morning"):
    """加密货币播报 08:00/20:00"""
    global logger, config, push_manager  # 声明使用全局变量
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    label = "早" if period == "morning" else "晚"
    if logger:
        logger.info(f"🪙 === 加密货币{label}报 ===")
    task_name = f"crypto_{period}"

    # T-016.4: 任务互斥锁
    if not try_acquire_lock(task_name):
        if logger:
            logger.warning(f"⏩ {task_name} 正在执行中，跳过本次触发")
        return
    start_time = time.time()
    push_status = {"dingtalk": False, "feishu": False}

    def _run():
        ds = config.get("data_sources", {}).get("crypto", {})
        threshold = ds.get("alert_threshold", 5)
        
        # Phase 2: 从文件加载 Prompt
        prompt = load_prompt("crypto", label=label, threshold=threshold)
        ok, content = call_agent("researcher", prompt, timeout=120)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        save_output(task_name, content)
        channels = config["jobs"].get(task_name, {}).get("push_to", ["dingtalk", "feishu"])
        
        file_path = save_result(
            task=task_name,
            content=content,
            targets=channels,
            metadata={"agent": "researcher", "period": period}
        )
        logger.info(f"📦 结果已保存: {file_path}")
        
        success = try_push(file_path, push_manager, logger, return_status=True)
        push_status.update(success or {})
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info(f"✅ 加密货币{label}报推送完成")

    error_msg = None
    try:
        _run()
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ {task_name} 失败: {error_msg}")
    finally:
        release_lock(task_name)
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=push_status.get("dingtalk") or
                    push_status.get("feishu") or
                    push_status.get("_skipped_by_concurrency"),
            push_status=push_status,
            error_msg=error_msg
        )


def job_arxiv():
    """arXiv 论文追踪 10:30"""
    global logger, config, push_manager
    if not config or not push_manager:
        if logger:
            logger.error(f"❌ 配置未初始化，请先运行主程序")
        return

    if logger:
        logger.info("📄 === arXiv 论文精选 ===")
    task_name = "arxiv_papers"

    # T-016.4: 任务互斥锁
    if not try_acquire_lock(task_name):
        if logger:
            logger.warning(f"⏩ {task_name} 正在执行中，跳过本次触发")
        return
    start_time = time.time()
    push_status = {"dingtalk": False, "feishu": False}

    def _run():
        ds = config.get("data_sources", {}).get("arxiv", {})
        categories = ds.get("categories", ["cs.AI", "cs.LG", "cs.CL", "cs.CV"])
        min_score = ds.get("min_score", 2)
        limit = ds.get("max_results", 15)

        # Phase 2: 从文件加载 Prompt
        prompt = load_prompt(
            "arxiv",
            categories=",".join(categories),
            min_score=min_score,
            limit=limit
        )

        ok, content = call_agent("researcher", prompt, timeout=300)
        if not ok:
            raise Exception(f"Agent 执行失败: {content}")

        save_output(task_name, content)
        channels = config["jobs"].get(task_name, {}).get("push_to", ["dingtalk", "feishu"])

        file_path = save_result(
            task=task_name,
            content=content,
            targets=channels,
            metadata={"agent": "researcher"}
        )
        logger.info(f"📦 结果已保存: {file_path}")

        # === Obsidian 归档 ===
        try:
            from obsidian_archive import archive_to_obsidian
            obs_path = archive_to_obsidian(
                content=content,
                note_type="arxiv",
                tags=["scheduler", "论文"],
                task_name=task_name
            )
            logger.info(f"📝 已归档到 Obsidian: {obs_path.name}")
        except Exception as e:
            logger.warning(f"Obsidian 归档失败（不影响推送）: {e}")

        success = try_push(file_path, push_manager, logger, return_status=True)
        push_status.update(success or {})
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info(f"✅ {task_name} 推送成功")

    error_msg = None
    try:
        _run()
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ {task_name} 失败: {error_msg}")
    finally:
        release_lock(task_name)
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=push_status.get("dingtalk") or
                    push_status.get("feishu") or
                    push_status.get("_skipped_by_concurrency"),
            push_status=push_status,
            error_msg=error_msg
        )


def log_health_status():
    """记录系统健康状况"""
    global logger
    try:
        import psutil
        import platform

        # 获取系统信息
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk_usage = psutil.disk_usage('/')

        # 获取进程信息
        current_process = psutil.Process()
        process_memory = current_process.memory_info().rss / 1024 / 1024  # MB
        num_threads = current_process.num_threads()
        open_files = len(current_process.open_files())

        health_info = (
            f"🖥️ 系统健康状况 - "
            f"CPU: {cpu_percent}%, "
            f"内存: {memory.percent}%, "
            f"磁盘: {disk_usage.percent}%, "
            f"调度器进程内存: {process_memory:.2f}MB, "
            f"线程数: {num_threads}, "
            f"打开文件数: {open_files}"
        )

        logger.info(f"🏥 系统健康检查: {health_info}")

        # 检查是否有异常值
        if cpu_percent > 90 or memory.percent > 90 or disk_usage.percent > 90:
            logger.warning(f"⚠️ 系统资源使用率过高: CPU={cpu_percent}%, 内存={memory.percent}%, 磁盘={disk_usage.percent}%")

    except ImportError:
        logger.info("🏥 系统健康检查: 未安装psutil模块，无法获取详细系统信息")
    except Exception as e:
        logger.error(f"🏥 系统健康检查失败: {e}")


def job_system_check():
    """系统巡检 09:00"""
    global logger  # 声明使用全局 logger 变量
    logger.info("🔧 === 系统巡检 ===")

    def _run():
        # 添加系统健康检查日志
        log_health_status()
        prompt = load_prompt("system_check")

        # 系统巡检用 operator Agent，失败则用 main
        ok, content = call_agent("operator", prompt, timeout=90)
        if not ok:
            ok, content = call_agent("main", prompt, timeout=90)
            if not ok:
                raise Exception(f"Agent 执行失败: {content}")

        save_output("system_check", content)
        
        has_alert = "🚨" in content
        overall = "🚨" if has_alert else "✅"
        
        channels = config["jobs"]["system_check"].get("push_to", ["dingtalk"])
        
        file_path = save_result(
            task="system_check",
            content=content,
            targets=channels,
            metadata={"type": "system", "has_alert": has_alert}
        )
        logger.info(f"📦 结果已保存: {file_path}")
        
        success = try_push(file_path, push_manager, logger)
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info(f"✅ 系统巡检完成 {overall}")

    _run()


# ============ 运维报告 (T-016.2) ============

def job_system_metrics_report():
    """每日运维指标报告 10:35"""
    global logger  # 声明使用全局 logger 变量
    logger.info("📊 === 运维指标报告 ===")
    task_name = "system_metrics"
    start_time = time.time()
    push_status = {"dingtalk": False, "feishu": False}

    def _run():
        # 调用 analyze_metrics.py 获取数据
        import subprocess

        try:
            result = subprocess.run(
                ["python3", str(Path(__file__).resolve().parent / "scripts" / "analyze_metrics.py"), "--hours", "24"],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(BASE_DIR)
            )

            if result.returncode != 0:
                logger.warning(f"⚠️ 指标分析失败: {result.stderr}")
                report_content = "⚠️ 运维指标报告生成失败，请检查日志。"
            else:
                report_content = result.stdout

        except subprocess.TimeoutExpired:
            logger.warning("⚠️ 指标分析超时")
            report_content = "⚠️ 运维指标报告生成超时。"
        except Exception as e:
            logger.warning(f"⚠️ 指标分析异常: {e}")
            report_content = "⚠️ 运维指标报告生成异常。"

        # 保存输出
        save_output("system_metrics", report_content)

        # 推送（仅推送到钉钉，避免打扰）
        channels = config["jobs"]["system_metrics"].get("push_to", ["dingtalk"])

        file_path = save_result(
            task="system_metrics",
            content=report_content,
            targets=channels,
            metadata={"type": "metrics", "period": "24h"}
        )
        logger.info(f"📦 结果已保存: {file_path}")

        success = try_push(file_path, push_manager, logger, return_status=True)
        push_status.update(success or {})
        if not success:
            logger.warning("推送失败，已进入重试队列")
        else:
            logger.info("✅ 运维指标报告推送完成")

    try:
        _run()
    finally:
        release_lock(task_name)
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=push_status.get("dingtalk") or push_status.get("feishu"),
            push_status=push_status
        )


def job_obsidian_sync():
    """Obsidian 笔记同步到 ChromaDB 02:00"""
    global logger  # 声明使用全局 logger 变量
    logger.info("🔄 === Obsidian 笔记同步 ===")

    def _run():
        from obsidian_vectorize import sync_to_chromadb

        # 执行同步
        sync_to_chromadb()

        logger.info("✅ Obsidian 笔记同步完成")

    _run()


# ============ 测试用故障注入任务 (T-016.3) ============

def job_fail_test():
    """T-016.3 测试用 - 故意抛异常验证告警"""
    global logger  # 声明使用全局 logger 变量
    logger.info("🧪 === 故障注入测试 ===")

    # 故意抛出异常
    raise Exception("T-016.3 模拟任务执行失败 - 这是一条测试异常")


def job_log_rotate():
    """T-016.5 日志滚动任务"""
    global logger  # 声明使用全局 logger 变量
    logger.info("📦 === 日志滚动 ===")
    import subprocess
    script = Path(__file__).resolve().parent / "scripts" / "rotate_logs.sh"
    result = subprocess.run(["bash", str(script)], capture_output=True, text=True)
    logger.info(f"📦 日志滚动结果:\n{result.stdout}")
    if result.returncode != 0:
        logger.error(f"📦 日志滚动失败: {result.stderr}")


def job_knowledge_classify():
    """T-076 知识管线分类任务"""
    global logger  # 声明使用全局 logger 变量
    logger.info("📚 === 知识分类 ===")
    import subprocess
    script = BASE_DIR / "knowledge_pipeline.py"
    if not script.exists():
        logger.warning("📚 knowledge_pipeline.py 不存在，跳过")
        return

    result = subprocess.run(
        ["python3", str(script)],
        capture_output=True, text=True, timeout=300
    )
    if result.returncode == 0:
        logger.info(f"📚 知识分类完成:\n{result.stdout}")
    else:
        logger.error(f"📚 知识分类失败: {result.stderr}")


def job_klib_sync():
    """每日凌晨自动整理知识库: scan -> organize -> vectorize"""
    global logger
    task_name = "klib_sync"
    
    if not try_acquire_lock(task_name):
        logger.warning(f"⏩ {task_name} 正在执行中，跳过本次触发")
        return
        
    start_time = time.time()
    success = False
    logger.info("📚 === 知识库凌晨自动整理流水线 ===")
    
    try:
        base_dir = Path.home() / "Documents" / "Library"
        scripts = ["klib_scan.py", "klib_organize.py", "klib_sync_obsidian.py", "klib_vectorize.py"]
        
        for script_name in scripts:
            script_path = base_dir / script_name
            if not script_path.exists():
                logger.warning(f"⚠️ {script_name} 不存在，跳过")
                continue
                
            logger.info(f"⏳ 正在执行: {script_name} ...")
            
            # klib_organize.py 需要提供源目录参数
            cmd_args = ["python3", str(script_path)]
            if script_name == "klib_organize.py":
                cmd_args.append(str(base_dir / "【待整理】"))
                
            result = subprocess.run(
                cmd_args,
                cwd=str(base_dir),
                capture_output=True, text=True, timeout=1200 # 向量化可能耗时，给 20 分钟
            )
            if result.returncode == 0:
                logger.info(f"✅ {script_name} 执行成功:\n{result.stdout.strip()[:500]}")
            else:
                logger.error(f"❌ {script_name} 执行报错: {result.stderr.strip()}")
                return # 失败则立刻终端整个流水线
        
        success = True
        logger.info("📚 === 知识库自动整理完成 ===")
    except Exception as e:
        logger.error(f"❌ {task_name} 发生非预期异常: {e}")
    finally:
        release_lock(task_name)
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=success
        )


def job_video_notes_organize():
    """每日凌晨4点整理 Inbox 中的视频笔记"""
    global logger
    task_name = "video_notes_organize"

    if not try_acquire_lock(task_name):
        return

    start_time = time.time()
    success = False

    try:
        logger.info("🎬 === 视频笔记整理 ===")

        from video_notes_organize import organize_video_notes
        stats = organize_video_notes(dry_run=False)

        if stats:
            logger.info(f"🎬 整理完成: 移动 {stats['moved']} 个, 错误 {stats['errors']} 个")
            success = stats['errors'] == 0
        else:
            success = True

    except Exception as e:
        logger.error(f"❌ {task_name} 发生异常: {e}")
    finally:
        release_lock(task_name)
        end_time = time.time()
        log_task_metrics(
            task_name=task_name,
            start_time=start_time,
            end_time=end_time,
            success=success
        )


def job_research_pipeline():
    """Phase 4a: PDF研报扫描与向量化入库"""
    global logger
    logger.info("📄 === 研报扫描流水线 ===")
    import subprocess
    script = BASE_DIR / "research_pipeline.py"
    if not script.exists():
        logger.warning("📄 research_pipeline.py 不存在，跳过")
        return

    result = subprocess.run(
        ["python3", str(script)],
        capture_output=True, text=True, timeout=1200
    )
    if result.returncode == 0:
        logger.info(f"📄 研报流水线完成:\n{result.stdout[-1000:]}")
    else:
        logger.error(f"📄 研报流水线失败: {result.stderr}")


def save_output(job_name: str, content: str):
    output_dir = BASE_DIR / config.get("system", {}).get("output_dir", "outputs")
    output_dir.mkdir(exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    output_file = output_dir / f"{job_name}_{today}.md"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(content)
    logger.info(f"💾 输出已保存: {output_file.name}")


def load_config() -> dict:
    """加载配置，敏感信息从环境变量读取"""
    config = {}
    
    # 加载 YAML 配置
    config_path = BASE_DIR / "config" / "settings.yaml"
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

    # 优先加载 .secrets 环境变量文件
    env_file = Path.home() / ".secrets" / "zhiwei.env"
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ.setdefault(key.strip(), value.strip())
    
    # 确保 push 结构存在
    if "push" not in config:
        config["push"] = {"dingtalk": {}, "feishu": {}}

    # 从环境变量覆盖敏感配置
    if os.getenv("DINGTALK_WEBHOOK"):
        config["push"]["dingtalk"]["webhook"] = os.getenv("DINGTALK_WEBHOOK")
    if os.getenv("DINGTALK_SECRET"):
        config["push"]["dingtalk"]["secret"] = os.getenv("DINGTALK_SECRET")
    if os.getenv("FEISHU_APP_ID"):
        config["push"]["feishu"]["app_id"] = os.getenv("FEISHU_APP_ID")
    if os.getenv("FEISHU_APP_SECRET"):
        config["push"]["feishu"]["app_secret"] = os.getenv("FEISHU_APP_SECRET")
    if os.getenv("FEISHU_CHAT_ID"):
        config["push"]["feishu"]["chat_id"] = os.getenv("FEISHU_CHAT_ID")
    
    return config


# ============ 主程序 ============

def job_graph_maintenance():
    """知识图谱自动化维护任务 (Phase 5b)"""
    global logger
    task_name = "graph_maintenance"
    if not try_acquire_lock(task_name):
        logger.warning(f"⏩ {task_name} 正在执行中，跳过本次触发")
        return

    logger.info("🛠️ === 启动知识图谱自动化维护 (GraphRAG Indexing) ===")
    start_time = time.time()
    
    try:
        env = os.environ.copy()
        python_exe = sys.executable or "python3"
        cli_path = BASE_DIR / "graph_index_cli.py"
        
        if not cli_path.exists():
            raise FileNotFoundError(f"找不到索引 CLI: {cli_path}")
            
        # 执行索引子进程
        result = subprocess.run([python_exe, str(cli_path)], capture_output=True, text=True, timeout=1800, env=env)
        
        if result.returncode == 0:
            output = result.stdout.strip()
            if "SUCCESS:" in output:
                hotspots = output.split("SUCCESS:")[1]
                logger.info(f"✅ 知识图谱索引完成。热点更新数: {hotspots}")
            else:
                logger.info(f"✅ 知识图谱索引完成。{output}")
        else:
            logger.error(f"❌ 知识图谱索引失败: {result.stderr}")

    except Exception as e:
        logger.error(f"❌ 知识图谱维护异常: {e}")
    finally:
        release_lock(task_name)
        end_time = time.time()
        log_task_metrics(task_name, start_time, end_time, success=True) # 索引不影响推送，标记为成功


def job_daily_voice_task_summary():
    """每日语音任务汇总 20:00

    推送今日待办任务和已完成任务汇总
    """
    global logger
    task_name = "daily_voice_task_summary"
    logger.info("📋 === 每日任务汇总 ===")

    if not acquire_lock(task_name):
        logger.warning(f"⚠️ {task_name} 已在运行，跳过")
        return

    start_time = time.time()
    success = False

    try:
        # 导入语音任务模块
        sys.path.insert(0, str(Path.home() / "zhiwei-bot"))
        from voice_task_store import VoiceTaskStore, create_daily_note

        store = VoiceTaskStore()
        pending = store.list_pending(20)
        done_today = store.list_done_today(20)

        # 创建 Obsidian 笔记
        note_path = create_daily_note(pending, done_today)
        logger.info(f"📝 创建每日笔记: {note_path}")

        # 获取统计
        stats = store.stats()

        # 构建汇总消息
        today = datetime.now().strftime('%Y-%m-%d')
        lines = [
            f"📋 每日任务汇总 ({today})",
            "",
            f"**待办任务** ({stats['pending']} 项)"
        ]

        if pending:
            priority_icons = {"high": "🔴", "normal": "🟡", "low": "⚪"}
            for task in pending[:10]:
                icon = priority_icons.get(task.get('priority', 'normal'), '🟡')
                lines.append(f"{icon} {task['content']}")
            if len(pending) > 10:
                lines.append(f"... 还有 {len(pending) - 10} 项")
        else:
            lines.append("暂无待办任务 🎉")

        lines.append("")
        lines.append(f"**今日完成** ({stats['done_today']} 项)")

        if done_today:
            for task in done_today[:5]:
                lines.append(f"✅ {task['content']}")
            if len(done_today) > 5:
                lines.append(f"... 还有 {len(done_today) - 5} 项")
        else:
            lines.append("今日暂无完成任务")

        # 推送汇总
        summary = "\n".join(lines)

        # 获取活跃用户推送
        try:
            feishu_user_file = Path.home() / "tasks" / ".feishu_user_id"
            if feishu_user_file.exists():
                target_user = feishu_user_file.read_text().strip()
            else:
                target_user = None

            if target_user:
                from feishu_api import send_direct_message
                send_direct_message(target_user, summary)
                logger.info(f"✅ 任务汇总已推送给用户: {target_user[:10]}...")
            else:
                logger.warning("⚠️ 未找到活跃用户，跳过推送")

        except Exception as e:
            logger.error(f"❌ 推送失败: {e}")

        success = True

    except Exception as e:
        logger.error(f"❌ 每日任务汇总异常: {e}")
    finally:
        release_lock(task_name)
        end_time = time.time()
        log_task_metrics(task_name, start_time, end_time, success=success)


def job_ws_health_check():
    """WebSocket 健康检查（每 5 分钟）"""
    global logger
    task_name = "ws_health_check"

    try:
        # 检查飞书服务进程
        result = subprocess.run(
            ["launchctl", "list"],
            capture_output=True,
            text=True,
            timeout=5
        )

        bot_running = "com.zhiwei.bot" in result.stdout

        if not bot_running:
            logger.error("⚠️ 飞书服务掉线！com.zhiwei.bot 未运行")
            # TODO: 发送告警通知（钉钉/飞书）
            # 当前只记录日志，后续可接入推送
        else:
            logger.debug("💚 飞书服务正常")

        # 检查最近消息时间（可选增强）
        log_file = Path.home() / "logs" / "feishu_bot.log"
        if log_file.exists():
            import os
            mtime = os.path.getmtime(log_file)
            age_seconds = time.time() - mtime

            if age_seconds > 600:  # 10 分钟无更新
                logger.warning(f"⚠️ 飞书日志 {int(age_seconds/60)} 分钟无更新")

    except subprocess.TimeoutExpired:
        logger.error("⚠️ WebSocket 健康检查超时")
    except Exception as e:
        logger.error(f"⚠️ WebSocket 健康检查失败: {e}")


def main():
    global config, push_manager, logger

    config = load_config()
    logger = setup_logging(
        config.get("system", {}).get("log_dir", "logs"),
        config.get("system", {}).get("log_retention_days", 30)
    )

    push_manager = PushManager(config)

    logger.info("=" * 50)
    logger.info("🤖 知微定时任务系统 v3.4 启动")
    logger.info("   架构: 调度器 → Agent(LLM) → 推送")
    logger.info("   特性: Prompt 外部化、重试机制、触发器监听 (v3.4)")
    logger.info("=" * 50)

    # 记录启动时的健康状态
    log_health_status()

    tz = config.get("system", {}).get("timezone", "Asia/Shanghai")
    scheduler = BlockingScheduler(timezone=tz)

    # ============ 启动触发器监听器 (隔离保护) ============
    try:
        trigger_listener.init(scheduler, logger)
        trigger_listener.start()
        logger.info("👀 触发器监听器已启动")
    except Exception as e:
        logger.warning(f"⚠️ 触发器监听器启动失败: {e}")
        logger.warning("   主定时任务不受影响")

    job_map = {
        "morning_brief":   job_morning_brief,
        "noon_brief":      job_noon_brief,
        "us_market_open":  job_us_market_open,
        "us_market_close": job_us_market_close,
        "crypto_morning":  lambda: job_crypto("morning"),
        "crypto_evening":  lambda: job_crypto("evening"),
        "arxiv_papers":    job_arxiv,
        "system_check":    job_system_check,
        "system_metrics":  job_system_metrics_report,  # T-016.2
        "obsidian_sync":   job_obsidian_sync,  # Phase 2: Obsidian 笔记同步
        "klib_sync":       job_klib_sync, # KLib 每日自动整理知识库
        "video_notes_organize": job_video_notes_organize,  # 视频笔记整理
        "fail_test":       job_fail_test,  # T-016.3 (test only)
        "log_rotate":      job_log_rotate,  # T-016.5
        "knowledge_classify": job_knowledge_classify,  # T-076
        "research_pipeline": job_research_pipeline,  # Phase 4a (T-411)
        "graph_maintenance": job_graph_maintenance,  # Phase 5b (T-501)
        "daily_voice_task_summary": job_daily_voice_task_summary,  # 每日语音任务汇总
    }

    # 动态添加 info_brief_XX 任务映射 (07, 09, 11, 13, 15, 17, 19, 21)
    for hour in [7, 9, 11, 13, 15, 17, 19, 21]:
        job_name = f"info_brief_{hour:02d}"
        job_map[job_name] = lambda h=hour: job_info_brief(h)

    for job_name, job_conf in config.get("jobs", {}).items():
        if not job_conf.get("enabled", False):
            continue
        func = job_map.get(job_name)
        if not func:
            continue

        trigger = CronTrigger(
            hour=job_conf["hour"],
            minute=job_conf["minute"],
            day_of_week=job_conf.get("day_of_week", "mon-sun"),
            timezone=tz
        )
        scheduler.add_job(
            func, trigger=trigger,
            id=job_name,
            name=job_conf.get("description", job_name),
            misfire_grace_time=300
        )
        logger.info(f"   📅 {job_conf.get('description', job_name)} "
                    f"[{job_conf['hour']:02d}:{job_conf['minute']:02d} "
                    f"{job_conf.get('day_of_week', '每天')}]")

    # 日志清理
    def cleanup_old_files():
        retention = config.get("system", {}).get("log_retention_days", 30)
        cutoff = datetime.now() - timedelta(days=retention)
        for d in ["logs", config.get("system", {}).get("output_dir", "outputs")]:
            dir_path = BASE_DIR / d
            if dir_path.exists():
                for f in dir_path.iterdir():
                    if f.stat().st_mtime < cutoff.timestamp():
                        f.unlink()
                        logger.info(f"🗑 清理: {f.name}")

    scheduler.add_job(
        cleanup_old_files,
        CronTrigger(hour=3, minute=0, timezone=tz),
        id="cleanup", name="日志清理"
    )

    # ============ 失败重试机制 (max_retries=3, 2分钟后首次重试) ============
    MAX_RETRIES = 3
    RETRY_DELAY_MINUTES = 2  # 首次重试2分钟，后续指数增长

    # 添加定期健康检查
    def health_check_job():
        log_health_status()

    # 每小时执行一次健康检查
    scheduler.add_job(
        health_check_job,
        CronTrigger(minute=0, timezone=tz),  # 每小时整点执行
        id="health_check",
        name="系统健康检查"
    )
    logger.info("   💚 系统健康检查已启用 [每小时]")

    # WebSocket 健康检查（每 5 分钟）
    scheduler.add_job(
        job_ws_health_check,
        CronTrigger(minute="*/5", timezone=tz),
        id="ws_health_check",
        name="WebSocket 健康检查"
    )
    logger.info("   💚 WebSocket 健康检查已启用 [每 5 分钟]")

    def schedule_retry(job_id: str):
        """安排重试任务（遵守 quiet_hours）"""
        retry_count = job_retries.get(job_id, 0) + 1
        job_retries[job_id] = retry_count

        if retry_count < MAX_RETRIES:
            logger.warning(f"🔄 安排重试 [{job_id}]: {retry_count}/{MAX_RETRIES}")

            # 首次重试用 RETRY_DELAY_MINUTES，后续用 RETRY_DELAYS
            if retry_count == 1:
                delay_minutes = RETRY_DELAY_MINUTES
            else:
                delay_minutes = get_retry_delay(retry_count) // 60

            # 检查是否在静默时段
            run_time = datetime.now() + timedelta(minutes=delay_minutes)
            while is_quiet_hours(run_time):
                # 跳过静默时段，从 07:00 开始
                run_time = run_time.replace(hour=7, minute=0, second=0, microsecond=0)
                run_time += timedelta(days=1)

            scheduler.add_job(
                lambda: scheduler.print_jobs(job_id),  # 重新触发原任务
                trigger=DateTrigger(run_date=run_time),
                id=f"{job_id}_retry_{retry_count}",
                name=f"{job_id} 重试 {retry_count}"
            )
        else:
            logger.error(f"❌ 任务彻底失败 [{job_id}]，已达最大重试次数")
            job_retries.pop(job_id, None)

    job_retries = {}  # 重试计数

    def listener(event):
        if event.exception:
            logger.error(f"❌ 任务异常: {event.job_id}")
            # T-016.3: 发送告警
            original_job_id = event.job_id.rsplit('_retry_', 1)[0]
            error_msg = str(event.exception)[:200]
            send_failure_alert(original_job_id, f"任务执行异常: {error_msg}")
            # 安排重试
            schedule_retry(original_job_id)
        else:
            logger.info(f"✅ 任务完成: {event.job_id}")
            # 清除重试计数
            job_retries.pop(event.job_id, None)
            job_retries.pop(event.job_id.rsplit('_retry_', 1)[0], None)

    scheduler.add_listener(listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    def signal_handler(signum, frame):
        logger.info("📛 收到退出信号")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("📛 调度器已关闭")


if __name__ == "__main__":
    main()

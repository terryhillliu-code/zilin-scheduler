#!/usr/bin/env python3
"""
知微系统 - 任务执行器 (v22.0 T-041)
监听 ~/tasks/pending/，自动调用 Claude Code CLI 执行任务。

启动方式: python3 ~/zhiwei-scheduler/task_executor.py
守护方式: launchctl (com.zhiwei.executor.plist)
"""

import json
import os
import subprocess
import time
import shutil
import logging
from pathlib import Path
from datetime import datetime

# ============ 配置 ============
TASKS_DIR = Path.home() / "tasks"
PENDING = TASKS_DIR / "pending"
RUNNING = TASKS_DIR / "running"
DONE = TASKS_DIR / "done"
FAILED = TASKS_DIR / "failed"
REVIEW = TASKS_DIR / "review"

CLAUDE_CMD = "/opt/homebrew/bin/claude"
MAX_BUDGET = 1.0
POLL_INTERVAL = 10
WORKING_DIR = str(Path.home())

MAX_RETRIES = 1  # 遇到错误严禁反复唤醒死推演
RETRY_DELAY = 5

AUTO_EXECUTE_LEVELS = {"low"}
MANUAL_APPROVE_LEVELS = {"medium", "high", "architecture"}

PRE_CHECK_SCRIPT = str(Path.home() / "scripts" / "pre_check_v2.sh")
BACKUP_SCRIPT = str(Path.home() / "scripts" / "backup.sh")

# ===== 风险评估（T-056）=====

# 禁止自动执行的文件关键词（匹配 target_files 路径）
CRITICAL_FILES = [
    "ws_client.py",
    "task_executor.py",
    "pre_check",
    ".plist",
    "openclaw.json",
    "system-prompt.md",
    "CLAUDE.md",
    "docker-compose.yml",
    "entrypoint.sh",
]

# 需审批的文件关键词
SENSITIVE_FILES = [
    "scheduler.py",
    "dispatch.py",
    "task_builder.py",
    "backup.sh",
    "rotate_logs.sh",
    "intent_router.py",
    "memory_manager.py",
    "agent_chain.py",
]


def max_risk(a: str, b: str) -> str:
    """取两个风险等级中较高的"""
    order = {"low": 0, "medium": 1, "high": 2}
    return a if order.get(a, 1) >= order.get(b, 1) else b


def assess_risk(intent: dict) -> str:
    """确定性风险评估，返回 low / medium / high"""
    target_files = intent.get("target_files", [])
    summary = intent.get("summary", "").lower()
    description = intent.get("description", "").lower()
    llm_risk = intent.get("risk_level", "medium")
    combined_text = summary + " " + description

    # 兜底：无法判断影响范围
    if not target_files:
        return "high"

    # 文件敏感度检查
    rule_risk = "low"
    for f in target_files:
        f_lower = f.lower()
        for critical in CRITICAL_FILES:
            if critical in f_lower:
                rule_risk = "high"
                break
        if rule_risk == "high":
            break
        for sensitive in SENSITIVE_FILES:
            if sensitive in f_lower:
                rule_risk = max_risk(rule_risk, "medium")

    # 操作类型检查
    if any(kw in combined_text for kw in ["删除", "移除", "remove", "delete", "drop"]):
        rule_risk = max_risk(rule_risk, "high")
    if any(kw in combined_text for kw in ["重构", "重写", "refactor", "rewrite"]):
        rule_risk = max_risk(rule_risk, "medium")

    # 文件数量检查
    if len(target_files) >= 4:
        rule_risk = "high"
    elif len(target_files) >= 2:
        rule_risk = max_risk(rule_risk, "medium")

    # 就高不就低
    final = max_risk(rule_risk, llm_risk)
    return final

# ============ 日志 ============
LOG_DIR = Path.home() / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [task_executor] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "task_executor.log"),
    ]
)
logger = logging.getLogger("task_executor")

# ============ 推送 ============
def push_to_dingtalk(message: str):
    """推送消息到钉钉"""
    try:
        webhook = os.environ.get("DINGTALK_WEBHOOK", "")
        if not webhook:
            # 尝试从容器环境获取
            result = subprocess.run(
                ["docker", "exec", "clawdbot", "printenv", "DINGTALK_WEBHOOK"],
                capture_output=True, text=True, timeout=10
            )
            webhook = result.stdout.strip()

        if not webhook:
            logger.warning("DINGTALK_WEBHOOK 未配置")
            return

        import urllib.request
        data = json.dumps({
            "msgtype": "text",
            "text": {"content": f"[知微] {message}"}
        }).encode()
        req = urllib.request.Request(
            webhook,
            data=data,
            headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=10)
        logger.info("钉钉推送成功")
    except Exception as e:
        logger.error(f"钉钉推送失败: {e}")


def notify_review(task_id: str, task_data: dict, risk_level: str):
    """推送审批通知到飞书（通过写入通知文件，由 ws_client 读取推送）"""
    intent = task_data.get("intent", {})
    summary = intent.get("summary", "未知需求")
    target_files = intent.get("target_files", [])
    constraints = intent.get("constraints", [])
    scope = intent.get("scope_boundary", "")
    user_request = task_data.get("user_request", "")

    files_text = "\n".join(f"  • {f}" for f in target_files) if target_files else "  • 未指定"
    constraints_text = "\n".join(f"  • {c}" for c in constraints) if constraints else "  • 无"
    risk_emoji = "🔴" if risk_level == "high" else "🟡"

    message = f"""🔔 开发任务待审批

📌 需求：{summary}
📋 原始请求：{user_request}

📁 要改的文件：
{files_text}

🔒 约束：
{constraints_text}

{risk_emoji} 风险等级：{risk_level}
{'⚠️ 涉及系统关键文件' if risk_level == 'high' else '⚠️ 涉及业务逻辑文件'}

回复「批准 {task_id}」执行 | 回复「取消 {task_id}」放弃"""

    # 写入通知文件，供 ws_client.py 读取并推送
    notify_path = os.path.expanduser(f"~/tasks/review/{task_id}.notify")
    Path(notify_path).parent.mkdir(parents=True, exist_ok=True)
    with open(notify_path, "w") as f:
        json.dump({
            "task_id": task_id,
            "message": message,
            "risk_level": risk_level,
            "created_at": datetime.now().isoformat()
        }, f, ensure_ascii=False, indent=2)

    logger.info(f"{task_id} 审批通知已写入 {notify_path}")

    # 钉钉只做提醒，交互通过飞书
    push_to_dingtalk(f"任务 {task_id} 需审批（{risk_level}），请在飞书中回复「好」或「不要」")


# ============ 核心逻辑 ============
def setup_dirs():
    """确保目录存在"""
    for d in [PENDING, RUNNING, DONE, FAILED, REVIEW]:
        d.mkdir(parents=True, exist_ok=True)


def scan_pending():
    """扫描待执行任务"""
    tasks = []
    for f in sorted(PENDING.glob("*.json")):
        try:
            task = json.loads(f.read_text())
            if task.get("status") == "awaiting_approval":
                continue
            task["_file"] = str(f)
            tasks.append(task)
        except Exception as e:
            logger.error(f"读取失败 {f.name}: {e}")
    return tasks


def call_claude(task_text: str) -> dict:
    """调用 Claude Code CLI"""
    try:
        result = subprocess.run(
            [
                CLAUDE_CMD,
                "--print",
                "--output-format", "json",
                "--max-budget-usd", str(MAX_BUDGET),
                "--add-dir", WORKING_DIR,
                "--dangerously-skip-permissions",
            ],
            input=task_text,
            capture_output=True,
            text=True,
            timeout=600,
            cwd=WORKING_DIR,
        )

        if result.returncode == 0:
            try:
                output = json.loads(result.stdout)
            except json.JSONDecodeError:
                output = {"raw_output": result.stdout[:2000]}
            return {"success": True, "output": output}
        else:
            return {
                "success": False,
                "error": result.stderr[:1000] if result.stderr else "非零退出码",
                "stdout": result.stdout[:1000],
            }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "执行超时（600s）"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def run_pre_check() -> dict:
    """执行 pre_check_v2.sh"""
    try:
        result = subprocess.run(
            [PRE_CHECK_SCRIPT],
            capture_output=True, text=True, timeout=60
        )
        return {
            "passed": result.returncode == 0,
            "output": result.stdout[-500:] if result.stdout else "",
        }
    except Exception as e:
        return {"passed": False, "output": str(e)}


def run_backup() -> dict:
    """执行 backup.sh"""
    try:
        result = subprocess.run(
            [BACKUP_SCRIPT],
            capture_output=True, text=True, timeout=120
        )
        return {
            "success": result.returncode == 0,
            "output": result.stdout[-200:] if result.stdout else "",
        }
    except Exception as e:
        return {"success": False, "output": str(e)}


def execute_task(task: dict) -> dict:
    """执行单个任务（含重试 + 风险分级）"""
    task_id = task.get("task_id", "unknown")
    task_text = task.get("task_text", "")

    # ===== T-056: 风险分级检查 =====
    # 已审批的任务跳过风险评估
    if task.get("approved"):
        logger.info(f"{task_id} 已人工审批，跳过风险评估，直接执行")
        final_risk = "approved"
    else:
        intent = task.get("intent", {})
        final_risk = assess_risk(intent)
        logger.info(f"{task_id} 风险评估: rule+LLM → {final_risk}")

    # 先移入 running 目录
    src = Path(task["_file"])
    dst = RUNNING / src.name
    shutil.move(str(src), str(dst))
    task["_file"] = str(dst)
    running_path = dst

    # 中高风险移入 review 等待审批（已审批的任务不检查）
    if final_risk not in ("approved", "low") and final_risk in ("medium", "high"):
        # 移入 review 目录等待审批
        review_path = REVIEW / f"{task_id}.json"
        task["assessed_risk"] = final_risk
        task["status"] = "awaiting_approval"
        with open(review_path, "w") as f:
            json.dump(task, f, ensure_ascii=False, indent=2, default=str)
        # 从 running 中移除
        if running_path.exists():
            running_path.unlink()

        logger.info(f"{task_id} → review/ (风险: {final_risk})")

        # 推送审批通知
        notify_review(task_id, task, final_risk)

        return {
            "status": "needs_approval",
            "message": f"风险等级 {final_risk}，已移入待审批队列。",
            "risk_level": final_risk,
        }

    # ===== low 风险：自动执行 =====

    # 带重试的 Claude 调用
    claude_result = None
    for attempt in range(MAX_RETRIES):
        logger.info(f"执行 {task_id}（第 {attempt + 1} 次）")
        claude_result = call_claude(task_text)

        if claude_result["success"]:
            break

        if attempt < MAX_RETRIES - 1:
            logger.warning(f"{task_id} 第 {attempt + 1} 次失败: {claude_result.get('error', '')[:100]}")
            time.sleep(RETRY_DELAY)

    if not claude_result["success"]:
        return {
            "status": "failed",
            "error": claude_result.get("error", "未知错误"),
            "retries": MAX_RETRIES,
        }

    # 执行 pre_check
    pre_check = run_pre_check()

    # 执行 backup
    backup = run_backup()

    return {
        "status": "done",
        "claude_output": claude_result.get("output"),
        "pre_check": pre_check,
        "backup": backup,
    }


def build_result_message(task: dict, result: dict) -> str:
    """构建推送消息"""
    task_id = task.get("task_id", "unknown")
    user_request = task.get("user_request", "")
    status = result.get("status", "unknown")

    if status == "done":
        pre_check = result.get("pre_check", {})
        check_status = "全绿" if pre_check.get("passed") else "有失败项"
        return (
            f"任务完成 [{task_id}]\n"
            f"需求: {user_request}\n"
            f"pre_check: {check_status}\n"
            f"备份: 已完成"
        )
    elif status == "needs_approval":
        return (
            f"需要审批 [{task_id}]\n"
            f"需求: {user_request}\n"
            f"风险等级: {task.get('intent', {}).get('risk_level')}\n"
            f"回复「批准 {task_id}」以执行。"
        )
    else:
        return (
            f"任务失败 [{task_id}]\n"
            f"需求: {user_request}\n"
            f"错误: {result.get('error', '未知')[:200]}"
        )


def finalize_task(task: dict, result: dict):
    """移动任务到最终目录并保存结果"""
    # 如果任务已经移到 review 目录，不需要再处理
    src = Path(task["_file"]) if "_file" in task and task["_file"] else None
    status = result.get("status", "failed")

    # needs_approval 已在 execute_task 中处理到 review 目录
    if status == "needs_approval":
        logger.info(f"任务 {task.get('task_id')} 已在 review 目录等待审批")
        return

    # 更新任务状态
    task["status"] = status
    task["result"] = result
    task["completed_at"] = datetime.now().isoformat()

    # 清理内部字段
    task.pop("_file", None)

    if status == "done":
        dst = DONE / f"{task.get('task_id', 'unknown')}.json"
    else:
        dst = FAILED / f"{task.get('task_id', 'unknown')}.json"

    dst.write_text(json.dumps(task, ensure_ascii=False, indent=2, default=str))
    if src and src.exists():
        src.unlink()

    # 写入飞书通知文件
    try:
        task_id = task.get("task_id", "unknown")
        notify_dir = Path.home() / "tasks" / "notify"
        notify_dir.mkdir(parents=True, exist_ok=True)

        feishu_user_file = Path.home() / "tasks" / ".feishu_user_id"
        feishu_user_id = feishu_user_file.read_text().strip() if feishu_user_file.exists() else ""

        is_success = status == "done"

        # 提取结果摘要：从 claude_output.result 或 user_request 中获取
        summary = ""
        if is_success:
            claude_result = result.get("claude_output", {})
            raw_summary = claude_result.get("result", "")
            if raw_summary:
                # 提取验收表中的关键信息（取前200字）
                summary = raw_summary[:200].replace("\n", " ").strip()
            else:
                summary = task.get("user_request", "任务已完成")[:200]
        else:
            summary = result.get("error", "任务执行失败")[:200]

        notify_data = {
            "task_id": task_id,
            "feishu_user_id": feishu_user_id,
            "status": "success" if is_success else "failed",
            "title": task.get("user_request", "开发任务")[:50],
            "summary": summary,
            "created_at": datetime.now().isoformat()
        }

        notify_path = notify_dir / f"{task_id}.json"
        notify_path.write_text(json.dumps(notify_data, ensure_ascii=False, indent=2))
        logger.info(f"飞书通知文件已写入: {notify_path}")
    except Exception as e:
        logger.error(f"写入飞书通知文件失败: {e}")

    logger.info(f"任务 {task.get('task_id')} → {status}")


def main_loop():
    """主循环"""
    setup_dirs()
    logger.info("任务执行器启动，监听 ~/tasks/pending/")

    while True:
        try:
            tasks = scan_pending()

            for task in tasks:
                task_id = task.get("task_id", "unknown")
                logger.info(f"发现新任务: {task_id}")

                result = execute_task(task)

                # 推送结果
                message = build_result_message(task, result)
                push_to_dingtalk(message)

                # 移动到最终目录
                finalize_task(task, result)

        except Exception as e:
            logger.error(f"主循环异常: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main_loop()

# ========== Phase 2: Orchestrator 模式 ==========

def run_with_orchestrator():
    """使用 TaskOrchestrator 运行（支持并行）"""
    from task_orchestrator import TaskOrchestrator
    
    def executor_wrapper(task: dict) -> dict:
        """包装现有的 execute_task 函数"""
        return execute_task(task)
    
    orchestrator = TaskOrchestrator(
        executor_func=executor_wrapper,
        max_workers=2  # 保守设置，避免资源竞争
    )
    
    logger.info("=== 使用 Orchestrator 模式启动 ===")
    
    try:
        orchestrator.run_loop()
    except KeyboardInterrupt:
        orchestrator.stop()


if __name__ == "__main__":
    import sys
    
    # 支持命令行参数切换模式
    if len(sys.argv) > 1 and sys.argv[1] == "--orchestrator":
        run_with_orchestrator()
    else:
        # 默认使用原有串行模式（稳定）
        main_loop()

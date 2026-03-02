#!/usr/bin/env python3
"""
知微系统 - 任务单生成引擎 (T-028)
将结构化 JSON 转换为 Claude Code 可执行的任务单。

使用方式:
  # Python 调用
  from task_builder import build_task, needs_clarification
  task_text = build_task(intent_json)

  # 命令行调用
  echo '{"summary":"测试","description":"测试任务",...}' | python3 task_builder.py
"""

import json
import sys
from datetime import datetime

# ============ 系统路径注册表 ============
# 已知文件路径 → 对应的验证命令
VERIFICATION_REGISTRY = {
    "~/zhiwei-scheduler/scheduler.py": [
        ("语法验证", "python3 -m py_compile ~/zhiwei-scheduler/scheduler.py && echo '语法正确'"),
        ("pre_check", "~/scripts/pre_check_v2.sh"),
    ],
    "~/zhiwei-scheduler/prompts/": [
        ("Prompt无表格", 'grep -l "|.*---|" ~/zhiwei-scheduler/prompts/*.txt || echo "无表格标记"'),
        ("pre_check", "~/scripts/pre_check_v2.sh"),
    ],
    "~/clawdbot-docker/data/openclaw/openclaw.json": [
        ("JSON语法", "python3 -m json.tool ~/clawdbot-docker/data/openclaw/openclaw.json > /dev/null && echo '语法正确'"),
        ("无密钥泄露", 'grep -n "sk-" ~/clawdbot-docker/data/openclaw/openclaw.json || echo "无泄露"'),
        ("容器健康", "docker inspect -f '{{.State.Health.Status}}' clawdbot"),
        ("pre_check", "~/scripts/pre_check_v2.sh"),
    ],
    "~/clawdbot-docker/skills/": [
        ("容器健康", "docker inspect -f '{{.State.Health.Status}}' clawdbot"),
        ("pre_check", "~/scripts/pre_check_v2.sh"),
    ],
    "~/scripts/": [
        ("脚本可执行", "test -x {file} && echo '可执行'"),
        ("pre_check", "~/scripts/pre_check_v2.sh"),
    ],
}

# 所有任务都必须包含的约束
DEFAULT_CONSTRAINTS = [
    "不要引入新的第三方依赖",
    "不要修改任务单未提及的文件",
    "完成后运行 pre_check_v2.sh 确认全绿",
]

# 已知路径描述（帮助国产模型选择正确路径）
KNOWN_PATHS = {
    "~/zhiwei-scheduler/scheduler.py": "定时任务调度器主文件",
    "~/zhiwei-scheduler/prompts/": "各定时任务的 Prompt 模板目录",
    "~/clawdbot-docker/data/openclaw/openclaw.json": "OpenClaw 网关主配置",
    "~/clawdbot-docker/skills/": "Skill 代码目录（容器内）",
    "~/scripts/pre_check_v2.sh": "四层验证脚本",
    "~/scripts/backup.sh": "备份脚本",
    "~/scripts/rotate_logs.sh": "日志滚动脚本",
    "~/scripts/analyze_metrics.py": "指标看板脚本",
    "~/CLAUDE.md": "执微操作规范（宪法）",
    "~/Documents/Library/klib.db": "知识库 SQLite 数据库",
    "~/logs/scheduler.jsonl": "定时任务结构化日志",
}

# 风险等级 → 附加说明
RISK_WARNINGS = {
    "low": "",
    "medium": "",
    "high": "⚠️ 高风险变更。完成后等待用户确认再进行下一步。\n\n",
    "architecture": "🔴 架构级变更。必须等待用户审批后才能执行。本任务单仅供用户审阅。\n\n",
}


def needs_clarification(intent: dict) -> list:
    """
    检查意图 JSON 是否信息充分。
    返回需要追问用户的问题列表。空列表表示信息充分。
    """
    questions = list(intent.get("needs_clarification", []))

    if not intent.get("summary"):
        questions.append("请用一句话描述你想做什么。")

    if not intent.get("description"):
        questions.append("请详细描述具体要做什么修改。")

    if not intent.get("target_files"):
        path_list = "\n".join(f"  - {p}: {d}" for p, d in KNOWN_PATHS.items())
        questions.append(f"需要修改哪些文件？已知路径：\n{path_list}")

    return questions


def _match_verifications(target_files: list) -> list:
    """根据目标文件匹配验证命令，去重后返回。"""
    verifications = []
    seen_names = set()

    for f in target_files:
        for pattern, checks in VERIFICATION_REGISTRY.items():
            if f.startswith(pattern) or f == pattern:
                for name, cmd in checks:
                    if name not in seen_names:
                        seen_names.add(name)
                        actual_cmd = cmd.replace("{file}", f)
                        verifications.append((name, actual_cmd))
                break

    # 确保 pre_check 始终存在
    if "pre_check" not in seen_names:
        verifications.append(("pre_check", "~/scripts/pre_check_v2.sh"))

    return verifications


def build_task(intent: dict) -> str:
    """
    将结构化意图 JSON 转换为 Claude Code 可执行的任务单。

    intent 字段:
      summary: str           — 一句话描述
      description: str       — 详细说明
      target_files: list     — 要修改的文件路径
      scope_boundary: str    — 不要改什么（可选）
      constraints: list      — 额外禁止事项（可选）
      context: str           — 背景信息（可选）
      risk_level: str        — low/medium/high/architecture（可选，默认 medium）
      implementation: str    — 具体实现说明（可选）
    """
    task_id = f"T-{datetime.now().strftime('%m%d%H%M')}"
    summary = intent.get("summary", "未命名任务")
    description = intent.get("description", "")
    target_files = intent.get("target_files", [])
    scope_boundary = intent.get("scope_boundary", "")
    user_constraints = intent.get("constraints", [])
    context = intent.get("context", "")
    risk_level = intent.get("risk_level", "medium")
    implementation = intent.get("implementation", "")

    constraints = user_constraints + DEFAULT_CONSTRAINTS
    verifications = _match_verifications(target_files)
    risk_warning = RISK_WARNINGS.get(risk_level, "")

    # ---- 组装任务单 ----
    lines = []

    if risk_warning:
        lines.append(risk_warning)

    lines.append("执微，请执行以下任务。")
    lines.append("")
    lines.append(f"### 任务: {task_id} {summary}")
    lines.append("")

    # 背景
    lines.append("### 背景")
    if context:
        lines.append(context)
    if description:
        lines.append(description)
    lines.append("")

    # 改动范围
    lines.append("### 改动范围")
    for f in target_files:
        lines.append(f"- `{f}`")
    if scope_boundary:
        lines.append(f"\n{scope_boundary}")
    lines.append("")

    # 具体实现（如果有）
    if implementation:
        lines.append("### 具体实现")
        lines.append(implementation)
        lines.append("")

    # 禁止事项
    lines.append("### 禁止事项")
    for c in constraints:
        lines.append(f"- {c}")
    lines.append("")

    # 验收表
    lines.append("### 完成后提交验收表")
    lines.append("")
    lines.append(f"## 验收表 — {task_id}")
    lines.append("")

    # 变更文件
    lines.append("### 1. 变更文件")
    for f in target_files:
        lines.append(f"- [ ] `{f}`")
    lines.append("")

    # 验证项
    step = 2
    for name, cmd in verifications:
        lines.append(f"### {step}. {name}")
        lines.append("```bash")
        lines.append(cmd)
        lines.append("```")
        lines.append("")
        step += 1

    # 备份
    lines.append(f"### {step}. 备份")
    lines.append("```bash")
    lines.append("~/scripts/backup.sh")
    lines.append("```")
    lines.append("记录执行时间。")
    lines.append("")
    step += 1

    # 遇到的问题
    lines.append(f"### {step}. 遇到的问题")
    lines.append("（如实填写，即使没有也写「无」。）")

    return "\n".join(lines)


# ============ 命令行入口 ============
if __name__ == "__main__":
    if not sys.stdin.isatty():
        raw = sys.stdin.read().strip()
    elif len(sys.argv) > 1:
        raw = sys.argv[1]
    else:
        print("用法: echo '{...}' | python3 task_builder.py")
        print("  或: python3 task_builder.py '{...}'")
        sys.exit(1)

    try:
        intent = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"JSON 解析失败: {e}", file=sys.stderr)
        sys.exit(1)

    # 先检查是否需要追问
    questions = needs_clarification(intent)
    if questions:
        print("⚠️ 信息不足，需要向用户确认以下问题：")
        for i, q in enumerate(questions, 1):
            print(f"  {i}. {q}")
        sys.exit(2)

    # 生成任务单


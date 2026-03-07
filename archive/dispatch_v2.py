#!/usr/bin/env python3
"""
task-dispatch Skill v2 (T-042)
单阶段模式：内部调用 LLM 完成意图提取，直接生成任务单并写入任务队列。
"""

import json
import sys
import os
import subprocess
import requests
from datetime import datetime
from pathlib import Path

# 导入同目录下的 task_builder
# 由于容器内路径是 /root/workspace/skills/task-dispatch/
# 需要从正确的路径导入
TASK_BUILDER_PATH = "/root/workspace/skills/task-dispatch/task_builder.py"

# ============ 意图提取 Prompt ============
INTENT_PROMPT_TEMPLATE = """你是一个需求分析器。用户会用自然语言描述一个系统修改需求。
你的任务是从中提取结构化信息。

你必须输出一个 JSON 对象，包含以下字段：

{{
  "summary": "一句话描述，不超过20字",
  "description": "具体要做什么，2-3句话",
  "target_files": ["涉及的文件路径，使用绝对路径"],
  "scope_boundary": "明确不要改什么",
  "constraints": ["禁止事项，至少1条"],
  "context": "相关背景信息，当前状态是什么",
  "risk_level": "low | medium | high | architecture",
  "needs_clarification": ["如果信息不足，列出需要用户补充的问题"]
}}

规则：
1. target_files 中的路径必须从以下已知路径中选择：
{known_paths}

   如果你不确定路径，在 needs_clarification 中提问。

2. 如果用户的描述太模糊，不要猜测，把问题放在 needs_clarification 中。

3. risk_level 判断标准：
   - low: 只改 prompt 文本或文档
   - medium: 改 Skill 代码或定时任务参数
   - high: 改 openclaw.json 或 scheduler.py 核心逻辑
   - architecture: 新增/删除 Agent 或 Skill

4. 只输出 JSON，不要输出任何其他内容。不要用 markdown 代码块包裹。

用户需求：
{user_request}
"""

# 已知路径映射
KNOWN_PATHS = {
    "~/zhiwei-scheduler/": "定时任务调度",
    "~/zhiwei-bot/": "飞书机器人",
    "~/clawdbot-docker/": "Docker 配置",
    "~/Documents/Library/": "知识库",
    "~/CLAUDE.md": "执微规范",
    "~/SYSTEM_ARCH_V21.md": "架构文档",
    "~/scripts/": "脚本工具",
    "~/logs/": "日志目录",
}


def get_intent_prompt(user_request: str) -> str:
    """生成意图提取 prompt"""
    paths_text = "\n".join(f"   - {p}: {d}" for p, d in KNOWN_PATHS.items())
    return INTENT_PROMPT_TEMPLATE.format(
        known_paths=paths_text,
        user_request=user_request
    )


def call_llm(prompt: str) -> str:
    """
    调用 DashScope API (qwen3.5-plus) 获取响应。
    """
    api_key = os.environ.get("DASHSCOPE_API_KEY")
    if not api_key:
        return ""

    try:
        resp = requests.post(
            "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "qwen3.5-plus",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 2048,
                "temperature": 0.1,
            },
            timeout=90
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"LLM 调用失败: {e}", file=sys.stderr)
        return ""


def parse_intent(llm_response: str) -> dict:
    """解析 LLM 返回的 JSON"""
    text = llm_response.strip()

    # 清理 markdown 包裹
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines)

    # 尝试提取 JSON 块
    if "{" in text:
        start = text.index("{")
        # 找到最后一个 } 的位置
        end = text.rindex("}")
        text = text[start:end+1]

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        return {
            "needs_clarification": [f"无法解析意图，请重新描述你的需求。(错误: {e})"]
        }


def load_task_builder():
    """动态加载 task_builder 模块"""
    import importlib.util
    spec = importlib.util.spec_from_file_location("task_builder", TASK_BUILDER_PATH)
    task_builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(task_builder)
    return task_builder


def build_task_text(intent: dict) -> str:
    """生成任务单文本"""
    tb = load_task_builder()
    return tb.build_task(intent)


def needs_clarification(intent: dict) -> list:
    """检查是否需要追问"""
    tb = load_task_builder()
    return tb.needs_clarification(intent)


def save_task(user_request: str, intent: dict, task_text: str) -> str:
    """将任务保存到 pending 目录"""
    task_id = f"T-{datetime.now().strftime('%m%d%H%M')}"

    task_file = {
        "task_id": task_id,
        "created_at": datetime.now().isoformat(),
        "source": "feishu",
        "user_request": user_request,
        "intent": intent,
        "task_text": task_text,
        "status": "pending",
        "result": None,
    }

    tasks_dir = Path("/root/tasks/pending")
    tasks_dir.mkdir(parents=True, exist_ok=True)
    filepath = tasks_dir / f"{task_id}.json"

    # 原子写入：先写临时文件，再 rename
    tmp_path = filepath.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(task_file, ensure_ascii=False, indent=2))
    tmp_path.rename(filepath)

    return task_id


def dispatch(user_request: str) -> str:
    """
    单阶段任务分发入口。
    1. 调用 LLM 提取意图
    2. 生成任务单
    3. 写入任务队列
    4. 返回确认消息
    """
    # 阶段 1：意图提取
    prompt = get_intent_prompt(user_request)
    llm_response = call_llm(prompt)

    if not llm_response:
        return "抱歉，无法连接到 LLM 服务，请稍后重试。"

    intent = parse_intent(llm_response)

    # 检查追问
    questions = needs_clarification(intent)
    if questions:
        result = "需要补充以下信息：\n\n"
        for i, q in enumerate(questions, 1):
            result += f"{i}. {q}\n"
        return result

    # 阶段 2：生成任务单
    task_text = build_task_text(intent)

    # 阶段 3：写入任务队列
    task_id = save_task(user_request, intent, task_text)

    # 阶段 4：返回确认
    risk = intent.get("risk_level", "medium")
    if risk in ("high", "architecture"):
        return (
            f"已生成任务 {task_id}（风险等级: {risk}），等待你的审批。\n\n"
            f"任务概要: {intent.get('summary', user_request)}\n"
            f"涉及文件: {', '.join(intent.get('target_files', []))}\n\n"
            f"回复「批准 {task_id}」以执行。"
        )
    else:
        return (
            f"已提交任务 {task_id}，执微将自动执行。\n\n"
            f"任务概要: {intent.get('summary', user_request)}\n"
            f"涉及文件: {', '.join(intent.get('target_files', []))}\n"
            f"风险等级: {risk}\n\n"
            f"完成后会推送结果到飞书。"
        )


# ============ 命令行入口 ============
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python3 dispatch.py '<用户需求>'")
        sys.exit(1)

    user_request = sys.argv[1]
    print(dispatch(user_request))
"""
开发任务协调器
- 接收任务（飞书 /dev 或命令行）
- 架构师分析拆解
- 执微逐步执行
- 结果汇总推送
"""

import json
import time
import re
import os
import subprocess
from pathlib import Path
from datetime import datetime

# 导入新模块
import sys
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent / "scripts"))

from dev_state import DevLock, DevState, git_auto_commit, git_revert_last
from claude_runner import ClaudeRunner, run_architect, run_executor
from smart_prompt import inject_context
from dev_memory import record as record_memory, search as search_memory


class DevCoordinator:
    """开发任务协调器"""

    def __init__(self):
        self.architect = ClaudeRunner(role="architect")
        self.executor = ClaudeRunner(
            role="executor",
            work_dir=str(Path.home() / "zhiwei-bot")
        )
        self.task_log = Path.home() / "logs" / "dev_tasks.jsonl"
        self.task_log.parent.mkdir(parents=True, exist_ok=True)

        # 加载上下文
        self.context_file = Path.home() / "CONTEXT_RESUME.md"
        self.claude_md = Path.home() / "CLAUDE.md"

        # 会话目录
        self.session_dir = Path.home() / "zhiwei-scheduler" / "sessions"
        self.session_dir.mkdir(parents=True, exist_ok=True)

    def _extract_tags(self, task: str) -> list:
        """从任务描述提取标签"""
        keywords = ['websocket', 'timeout', 'async', 'obsidian', 'scheduler',
                    'agent', 'memory', 'feishu', 'dingtalk', 'error', 'fix',
                    'bug', 'optimization', 'refactor', 'upgrade', 'integration',
                    'performance', 'stability', 'reliability', 'security']
        return [k for k in keywords if k.lower() in task.lower()]

    def process_task(self, task: str, source: str = "cli") -> dict:
        """
        处理开发任务（v27.0 增强版）（带锁和状态管理）

        Args:
            task: 任务描述
            source: 来源 (cli/feishu)

        Returns:
            {
                "task_id": str,
                "status": "success" | "failed" | "partial" | "queued" | "context_low",
                "steps": [...],
                "summary": str,
                "duration_ms": int
            }
        """
        task_id = f"dev_{int(time.time())}"
        start_time = time.time()
        steps = []

        print(f"📋 任务 {task_id}: {task[:50]}...")

        # === 1. 获取锁 ===
        lock = DevLock()
        acquired, lock_msg = lock.acquire()

        if not acquired:
            return {
                "task_id": task_id,
                "status": "queued",
                "steps": [],
                "summary": f"⏳ 任务已排队：{lock_msg}",
                "duration_ms": 0,
                "timestamp": datetime.now().isoformat()
            }

        try:
            # === 2. 记录任务开始 + Git 快照 ===
            state = DevState()
            state.start_task(task_id, task)

            git_success, git_msg = git_auto_commit(f"[dev] 任务开始前快照: {task_id}")
            print(f"📸 Git 快照: {git_msg}")

            # === 3. 检索相关经验（新增）===
            print("🔍 检索相关开发经验...")
            memories = search_memory(task, top_k=3)
            if memories:
                print(f"📚 找到 {len(memories)} 条相关经验")
                # 记录到步骤日志
                steps.append({
                    "step": "memory_search",
                    "success": True,
                    "result": f"找到 {len(memories)} 条相关经验",
                    "details": memories
                })
            else:
                print("📝 未找到相关开发经验")
                steps.append({
                    "step": "memory_search",
                    "success": True,
                    "result": "未找到相关开发经验"
                })

            # === 4. 构建增强 Prompt（新增）===
            print("🔧 构建增强上下文...")
            enhanced_prompt = inject_context(task)

            # === 5. 获取原始上下文注入 ===
            context_prompt = state.get_context_for_prompt()

            # === 6. 架构师分析 ===
            print("🏗️  架构师分析中...")
            architect_prompt = f"""
{enhanced_prompt}

## 当前任务
{task}

请分析这个任务，输出 JSON 格式的执行计划。
"""
            analysis = self.architect.run(architect_prompt, timeout=300)

            if not analysis["success"]:
                steps.append({
                    "step": "architect_analysis",
                    "success": False,
                    "result": analysis.get("error", "分析失败")
                })
                state.end_task(task_id, False)
                return self._build_result(task_id, "failed", steps, start_time,
                                          f"架构师分析失败: {analysis['error']}")

            steps.append({
                "step": "architect_analysis",
                "success": True,
                "result": analysis["result"][:500]
            })

            # === 7. 解析执行计划 ===
            plan = self._parse_plan(analysis["result"])

            # 确保 plan 有必需的字段
            if not isinstance(plan, dict) or "action" not in plan:
                print(f"⚠️ 解析计划失败，使用默认计划: {plan}")
                plan = {
                    "action": "direct_execute",
                    "content": analysis["result"]
                }

            # === 8. 执行任务 ===
            if plan["action"] == "direct_execute":
                # 简单任务，直接执行
                print("⚙️  执微执行中...")
                exec_result = self.executor.run(
                    f"{enhanced_prompt}\n\n任务：{task}\n\n架构师指令：{plan.get('content', '')}",
                    timeout=600,
                    skip_permissions=True
                )

                # 检查上下文是否不足
                if exec_result.get("context_low", False):
                    print("⚠️  检测到执微上下文不足，正在保存进度...")
                    self._save_session_and_notify(task, steps, task_id)
                    return self._build_result(task_id, "context_low", steps, start_time,
                                              "执微上下文不足，已保存进度，请开新窗口继续")

                steps.append({
                    "step": "executor_run",
                    "success": exec_result["success"],
                    "result": exec_result.get("result", exec_result.get("error", ""))[:500]
                })

            # 验证multi_step计划的有效性
            elif plan["action"] == "multi_step":
                # 多步骤执行
                for i, step in enumerate(plan.get("steps", []), 1):
                    print(f"⚙️  执微执行步骤 {i}/{len(plan.get('steps', []))}...")
                    exec_result = self.executor.run(
                        f"{enhanced_prompt}\n\n当前步骤：{step}\n\n整体任务：{task}",
                        timeout=600,
                        skip_permissions=True
                    )

                    # 检查上下文是否不足
                    if exec_result.get("context_low", False):
                        print(f"⚠️  检测到执微上下文不足，正在保存进度... (执行到第{i}步)")
                        self._save_session_and_notify(task, steps, task_id, remaining_steps=plan.get("steps", [])[i:])
                        return self._build_result(task_id, "context_low", steps, start_time,
                                                  f"执微上下文不足，已保存进度到第{i}步，请开新窗口继续")

                    steps.append({
                        "step": f"executor_step_{i}",
                        "content": step,
                        "success": exec_result["success"],
                        "result": exec_result.get("result", exec_result.get("error", ""))[:500]
                    })
                    if not exec_result["success"]:
                        break

            # === 9. 任务完成后记录经验（新增）===
            success = all(s["success"] for s in steps if s["step"] not in ["memory_search"])
            # 收集修改的文件
            modified_files = self._get_modified_files()

            if success:
                print("✅ 任务成功完成，记录经验...")
                record_memory(
                    task=task,
                    tags=self._extract_tags(task),
                    problem="",  # 可能需要从结果中提取问题描述
                    solution="", # 可能需要从结果中提取解决方案
                    files=modified_files,
                    status='success'
                )
                print("📝 开发经验已记录")
            else:
                print("❌ 任务失败，仍记录经验...")
                record_memory(
                    task=task,
                    tags=self._extract_tags(task),
                    problem="任务执行失败",
                    solution="需要进一步排查问题",
                    files=modified_files,
                    status='failed'
                )
                print("📝 开发经验已记录（失败任务）")

            # === 10. 任务完成 + Git 提交 ===
            if success:
                git_auto_commit(f"[dev] 任务完成: {task_id} - {task[:30]}")

            state.end_task(task_id, success, modified_files)

            return self._build_result(task_id, "success" if success else "failed", steps, start_time)

        finally:
            lock.release()

    def _save_session_and_notify(self, task: str, completed_steps: list, task_id: str, remaining_steps: list = None):
        """
        保存当前会话并通知用户
        """
        # 生成 session 文件
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        session_filename = f"session_{timestamp}.md"
        session_path = self.session_dir / session_filename

        # 收集修改的文件
        modified_files = self._get_modified_files()

        # 创建会话内容
        session_content = f"""# 开发任务会话续接 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 原始任务
{task}

## 已完成步骤
{len(completed_steps)} 步骤已完成

{chr(10).join([f"- {step.get('step', 'Step')} - {'✓' if step.get('success', False) else '✗'}" for step in completed_steps])}

## 待续步骤
{len(remaining_steps) if remaining_steps else 0} 步骤待续

{chr(10).join([f"- {step}" for step in remaining_steps]) if remaining_steps else '无'}

## 已修改文件
{chr(10).join(['- ' + f for f in modified_files]) if modified_files else '无'}

## 当前时间
{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 说明
执微上下文不足，已自动保存当前进度。请新开窗口继续任务。
"""

        # 写入会话文件
        with open(session_path, 'w', encoding='utf-8') as f:
            f.write(session_content)

        print(f"💾 会话已保存至: {session_path}")

        # 发送通知到控制台和日志
        continuation_message = f"""⚠️ 执微上下文不足，已保存进度，请开新窗口继续

当前会话已保存到: {session_path}
请打开新窗口并继续处理剩余任务。
继续命令示例:
cd ~/zhiwei-scheduler && python3 dev_coordinator.py "{task}"
"""
        print(continuation_message)

        # 如果来源是飞书，也需要发送飞书通知（这里只打印，实际实现需要集成飞书推送）
        print("📝 通知: 执微上下文不足，进度已保存")

    def _get_modified_files(self) -> list:
        """获取最近修改的文件"""
        files = []
        for d in [os.path.expanduser("~/zhiwei-bot"), os.path.expanduser("~/zhiwei-scheduler")]:
            try:
                # 获取当前 HEAD
                head_result = subprocess.run(
                    ["git", "rev-parse", "HEAD"],
                    cwd=d, capture_output=True, text=True, timeout=10
                )
                if head_result.returncode != 0:
                    continue
                current_head = head_result.stdout.strip()

                # 获取变更的文件
                result = subprocess.run(
                    ["git", "diff", "--name-only", "HEAD"],
                    cwd=d, capture_output=True, text=True, timeout=10
                )
                if result.returncode == 0:
                    files.extend([os.path.join(d, f) for f in result.stdout.strip().split("\n") if f])
            except:
                pass
        return files

    def _build_architect_prompt(self, task: str) -> str:
        """构建架构师 prompt"""
        context = ""
        if self.context_file.exists():
            context = self.context_file.read_text()[:3000]

        return f"""你是知微系统架构师。分析以下任务，决定如何执行。

## 系统上下文
{context}

## 任务
{task}

## 输出要求
用 JSON 格式输出：
{{
    "action": "direct_execute" 或 "multi_step",
    "risk_level": "low" | "medium" | "high",
    "content": "如果 direct_execute，这里是具体执行指令",
    "steps": ["如果 multi_step，这里是步骤数组"]
}}

注意：
- 修改 CRITICAL 文件（ws_client.py 等）需要 pre_check + backup
- 简单任务用 direct_execute
- 复杂任务拆分为 multi_step
"""

    def _build_executor_prompt(self, instruction: str) -> str:
        """构建执微 prompt"""
        claude_md = ""
        if self.claude_md.exists():
            claude_md = self.claude_md.read_text()[:2000]

        return f"""你是执微，负责执行开发任务。

## 操作规范
{claude_md}

## 任务
{instruction}

## 要求
1. 执行任务
2. 输出执行结果
3. 如果修改了文件，说明改了什么
"""

    def _parse_plan(self, architect_output: str) -> dict:
        """解析架构师输出的计划"""
        try:
            # 尝试提取 JSON
            json_match = re.search(r'\{[\s\S]*\}', architect_output)
            if json_match:
                plan = json.loads(json_match.group())
                return plan
        except:
            pass

        # 默认直接执行
        return {
            "action": "direct_execute",
            "content": architect_output
        }

    def _summarize_results(self, results: list) -> str:
        """汇总执行结果"""
        success_count = sum(1 for r in results if r["success"])
        total = len(results)

        if success_count == total:
            return f"全部 {total} 个步骤执行成功"
        else:
            return f"{success_count}/{total} 个步骤成功"

    def _build_result(self, task_id: str, status: str, steps: list,
                      start_time: float) -> dict:
        """构建最终结果"""
        duration_ms = int((time.time() - start_time) * 1000)

        result = {
            "task_id": task_id,
            "status": status,
            "steps": steps,
            "summary": steps[-1]["result"][:200] if steps else "无输出",
            "duration_ms": duration_ms,
            "timestamp": datetime.now().isoformat()
        }

        # 记录日志
        with open(self.task_log, "a") as f:
            f.write(json.dumps(result, ensure_ascii=False) + "\n")

        return result


def main():
    """命令行入口"""
    if len(__import__('sys').argv) < 2:
        print("用法: python3 dev_coordinator.py <任务描述>")
        print("示例: python3 dev_coordinator.py '继续拆分 ws_client.py'")
        __import__('sys').exit(1)

    task = " ".join(__import__('sys').argv[1:])
    coordinator = DevCoordinator()
    result = coordinator.process_task(task, source="cli")

    print("\n" + "=" * 50)
    print(f"任务ID: {result['task_id']}")
    print(f"状态: {result['status']}")
    print(f"耗时: {result['duration_ms']/1000:.1f}s")
    print(f"摘要: {result['summary']}")


if __name__ == "__main__":
    main()

"""
Claude Code Headless 模式封装
直接调用 claude -p，返回结构化结果
"""

import subprocess
import json
import os
from pathlib import Path
from datetime import datetime


class ClaudeRunner:
    """Claude Code 运行器"""

    MODELS = {
        "architect": "qwen3.5-plus",      # 架构分析，1M 上下文
        "executor": "qwen3-coder-next",   # 代码执行，最强代码能力
        "researcher": "kimi-k2.5",        # 信息采集，128K 上下文
    }

    def __init__(self, role: str = "executor", work_dir: str = None):
        self.role = role
        self.model = self.MODELS.get(role, "qwen3-coder-next")
        self.work_dir = work_dir or str(Path.home())
        self.log_dir = Path.home() / "logs" / "claude_runner"
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def run(self, prompt: str, timeout: int = 600,
            skip_permissions: bool = True) -> dict:
        """
        执行 Claude Code 任务

        Args:
            prompt: 任务描述
            timeout: 超时秒数，默认 10 分钟
            skip_permissions: 是否跳过权限确认

        Returns:
            {
                "success": bool,
                "result": str,
                "error": str | None,
                "usage": dict,
                "duration_ms": int
            }
        """
        cmd = [
            "claude", "-p", prompt,
            "--model", self.model,
            "--output-format", "json"
        ]

        if skip_permissions:
            cmd.append("--dangerously-skip-permissions")

        # 清除 CLAUDECODE 环境变量，绕过嵌套会话检测
        env = os.environ.copy()
        env.pop("CLAUDECODE", None)

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=self.work_dir,
                env=env
            )

            # 解析 JSON 输出
            if result.stdout.strip():
                data = json.loads(result.stdout)
                self._log_run(prompt, data)
                return {
                    "success": not data.get("is_error", False),
                    "result": data.get("result", ""),
                    "error": None if not data.get("is_error") else data.get("result"),
                    "usage": data.get("usage", {}),
                    "duration_ms": data.get("duration_ms", 0),
                    "session_id": data.get("session_id", "")
                }
            else:
                return {
                    "success": False,
                    "result": "",
                    "error": result.stderr or "Empty response",
                    "usage": {},
                    "duration_ms": 0
                }

        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "result": "",
                "error": f"Timeout after {timeout}s",
                "usage": {},
                "duration_ms": timeout * 1000
            }
        except json.JSONDecodeError as e:
            return {
                "success": False,
                "result": result.stdout if result else "",
                "error": f"JSON parse error: {e}",
                "usage": {},
                "duration_ms": 0
            }
        except Exception as e:
            return {
                "success": False,
                "result": "",
                "error": str(e),
                "usage": {},
                "duration_ms": 0
            }

    def _log_run(self, prompt: str, data: dict):
        """记录运行日志"""
        log_file = self.log_dir / f"{datetime.now().strftime('%Y%m%d')}.jsonl"
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "role": self.role,
            "model": self.model,
            "prompt_preview": prompt[:200],
            "success": not data.get("is_error", False),
            "duration_ms": data.get("duration_ms", 0),
            "tokens": {
                "input": data.get("usage", {}).get("input_tokens", 0),
                "output": data.get("usage", {}).get("output_tokens", 0)
            }
        }
        with open(log_file, "a") as f:
            f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")


# 便捷函数
def run_architect(prompt: str, **kwargs) -> dict:
    """用架构师模型执行"""
    return ClaudeRunner(role="architect").run(prompt, **kwargs)


def run_executor(prompt: str, work_dir: str = None, **kwargs) -> dict:
    """用执行者模型执行"""
    runner = ClaudeRunner(role="executor", work_dir=work_dir)
    return runner.run(prompt, **kwargs)


if __name__ == "__main__":
    # 测试
    result = run_executor("echo 'Hello from Claude Runner'")
    print(json.dumps(result, indent=2, ensure_ascii=False))

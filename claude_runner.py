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
        "executor": "qwen3-coder-plus",   # 代码执行，最强代码能力
        "researcher": "kimi-k2.5",        # 信息采集，128K 上下文
    }

    def __init__(self, role: str = "executor", work_dir: str = None):
        self.role = role
        self.model = self.MODELS.get(role, "qwen3-coder-plus")
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
                "duration_ms": int,
                "context_low": bool  # 新增字段，表示上下文不足
            }
        """
        # 使用降级策略调用模型
        from model_fallback import call_with_fallback

        result = call_with_fallback(
            primary_model=self.model,
            message=prompt,
            timeout=timeout,
            skip_permissions=skip_permissions,
            work_dir=self.work_dir
        )

        # 保持原有的返回结构，同时添加降级信息
        enhanced_result = {
            "success": result["success"],
            "result": result["result"],
            "error": result["error"],
            "usage": result["usage"],
            "duration_ms": result["duration_ms"],
            "session_id": result.get("session_id", ""),
            "context_low": self._check_context_low(result["result"]),
            "model_used": result.get("model_used", self.model),
            "was_fallback": result.get("was_fallback", False)
        }

        self._log_run(prompt, enhanced_result)
        return enhanced_result

    def _check_context_low(self, result_text: str) -> bool:
        """
        检测输出中是否包含上下文不足信号

        Args:
            result_text: Claude 的输出文本

        Returns:
            bool: 如果检测到上下文不足则返回 True
        """
        if not result_text:
            return False

        # 检查是否存在上下文不足的信号
        # 如 "Context left until auto-compact: X%" 且 X < 10
        import re
        # 匹配 "Context left until auto-compact: X%" 格式，其中 X < 10
        pattern = r'Context left until auto-compact:\s*(\d+(?:\.\d+)?)%'
        match = re.search(pattern, result_text)

        if match:
            percent_left = float(match.group(1))
            if percent_left < 10:
                return True

        # 其他上下文不足的相关模式
        low_context_indicators = [
            "context limit",
            "context length exceeded",
            "out of context",
            "memory exhausted",
            "context window",
            "context capacity"
        ]

        result_lower = result_text.lower()
        for indicator in low_context_indicators:
            if indicator in result_lower:
                return True

        return False

    def _log_run(self, prompt: str, data: dict):
        """记录运行日志"""
        log_file = self.log_dir / f"{datetime.now().strftime('%Y%m%d')}.jsonl"
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "role": self.role,
            "model": data.get("model_used", self.model),
            "was_fallback": data.get("was_fallback", False),
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

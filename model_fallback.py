#!/usr/bin/env python3
"""
模型降级策略实现
实现 Agent 调用失败时的模型降级机制
"""

import subprocess
import json
import os
import time
from pathlib import Path
from datetime import datetime


# 降级配置表
FALLBACK_CONFIG = {
    "qwen3-coder-plus": ["qwen3.5-plus"],  # 执微降级到1M上下文模型
    "kimi-k2.5": ["qwen3.5-plus", "glm-5"],  # 探微降级到其他模型
    "qwen3.5-plus": ["glm-5"],  # 知微降级到备选模型
    "researcher": ["qwen3.5-plus", "glm-5"],
    "architect": ["qwen3.5-plus", "glm-5"],
    "executor": ["qwen3.5-plus", "glm-5"],
    "operator": ["qwen3.5-plus", "glm-5"],
    "main": ["qwen3.5-plus", "glm-5"],
    "developer": ["qwen3.5-plus", "glm-5"]
}


def call_with_fallback(primary_model: str, message: str, timeout: int = 180,
                     skip_permissions: bool = True, work_dir: str = None) -> dict:
    """
    带降级的模型调用

    Args:
        primary_model: 主模型
        message: 输入消息
        timeout: 超时时间
        skip_permissions: 是否跳过权限确认
        work_dir: 工作目录

    Returns:
        {
            "success": bool,
            "result": str,
            "error": str | None,
            "usage": dict,
            "duration_ms": int,
            "model_used": str,  # 实际使用的模型
            "was_fallback": bool  # 是否使用了降级
        }
    """
    work_dir = work_dir or str(Path.home())

    # 首先尝试主模型
    result = _call_single_model(primary_model, message, timeout, skip_permissions, work_dir)

    # 如果成功，直接返回
    if result["success"]:
        result["model_used"] = primary_model
        result["was_fallback"] = False
        return result

    # 如果失败，尝试降级模型
    fallback_models = FALLBACK_CONFIG.get(primary_model, [])

    for fallback_model in fallback_models:
        print(f"🔄 模型 {primary_model} 调用失败，尝试降级到 {fallback_model}")

        # 记录降级日志
        log_fallback(primary_model, fallback_model, result.get("error", "Primary model failed"))

        # 调用降级模型
        fallback_result = _call_single_model(fallback_model, message, timeout, skip_permissions, work_dir)

        if fallback_result["success"]:
            fallback_result["model_used"] = fallback_model
            fallback_result["was_fallback"] = True
            fallback_result["primary_model"] = primary_model
            return fallback_result

    # 所有模型都失败，返回主模型的结果
    result["model_used"] = primary_model
    result["was_fallback"] = False
    return result


def _call_single_model(model: str, message: str, timeout: int = 180,
                      skip_permissions: bool = True, work_dir: str = None) -> dict:
    """
    调用单个模型

    Args:
        model: 模型名称
        message: 输入消息
        timeout: 超时时间
        skip_permissions: 是否跳过权限确认
        work_dir: 工作目录

    Returns:
        {
            "success": bool,
            "result": str,
            "error": str | None,
            "usage": dict,
            "duration_ms": int
        }
    """
    work_dir = work_dir or str(Path.home())

    cmd = [
        "claude", "-p", message,
        "--model", model,
        "--output-format", "json"
    ]

    if skip_permissions:
        cmd.append("--dangerously-skip-permissions")

    # 清除 CLAUDECODE 环境变量，绕过嵌套会话检测
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)

    try:
        start_time = time.time()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=work_dir,
            env=env
        )
        duration_ms = int((time.time() - start_time) * 1000)

        # 解析 JSON 输出
        if result.stdout.strip():
            data = json.loads(result.stdout)
            return {
                "success": not data.get("is_error", False),
                "result": data.get("result", ""),
                "error": None if not data.get("is_error") else data.get("result"),
                "usage": data.get("usage", {}),
                "duration_ms": data.get("duration_ms", duration_ms),
                "session_id": data.get("session_id", "")
            }
        else:
            return {
                "success": False,
                "result": "",
                "error": result.stderr or "Empty response",
                "usage": {},
                "duration_ms": duration_ms
            }

    except subprocess.TimeoutExpired:
        duration_ms = timeout * 1000
        return {
            "success": False,
            "result": "",
            "error": f"Timeout after {timeout}s",
            "usage": {},
            "duration_ms": duration_ms
        }
    except json.JSONDecodeError as e:
        duration_ms = int((time.time() - start_time) * 1000) if 'start_time' in locals() else 0
        return {
            "success": False,
            "result": result.stdout if 'result' in locals() else "",
            "error": f"JSON parse error: {e}",
            "usage": {},
            "duration_ms": duration_ms
        }
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000) if 'start_time' in locals() else 0
        return {
            "success": False,
            "result": "",
            "error": str(e),
            "usage": {},
            "duration_ms": duration_ms
        }


def log_fallback(primary: str, fallback: str, reason: str):
    """
    记录降级日志，便于分析

    Args:
        primary: 主模型
        fallback: 降级模型
        reason: 降级原因
    """
    log_dir = Path.home() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "model_fallback.jsonl"

    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "primary_model": primary,
        "fallback_model": fallback,
        "reason": reason[:500],  # 限制长度
        "type": "model_fallback"
    }

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    print(f"🔄 降级记录: {primary} → {fallback}, 原因: {reason[:100]}")


def test_fallback():
    """测试降级功能"""
    print("🧪 测试模型降级功能...")

    # 模拟调用一个假想的失败模型
    result = call_with_fallback("non-existent-model", "Hello World", timeout=30)
    print(f"测试结果: success={result['success']}, model_used={result.get('model_used')}")

    # 测试正常的模型
    result = call_with_fallback("qwen3.5-plus", "Hello World", timeout=30)
    print(f"正常测试结果: success={result['success']}, model_used={result.get('model_used')}, was_fallback={result.get('was_fallback', False)}")


if __name__ == "__main__":
    test_fallback()
#!/usr/bin/env python3
"""
LLM 直连代理模块
提供统一的本地代理调用接口，绕过 OpenClaw/Docker
"""
import http.client
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# 默认配置
DEFAULT_MODEL = "qwen3.5-plus"
DEFAULT_TIMEOUT = 180
PROXY_HOST = "127.0.0.1"
PROXY_PORT = 8045


def call_llm_direct(
    message: str,
    timeout: int = DEFAULT_TIMEOUT,
    model: Optional[str] = None,
    temperature: float = 0.7
) -> tuple[bool, str]:
    """
    直接调用本地 LLM 代理 (8045)

    Args:
        message: 用户消息
        timeout: 超时时间（秒）
        model: 模型名称，默认 qwen3.5-plus
        temperature: 温度参数

    Returns:
        (success, content) 元组
        - success: 是否成功
        - content: 成功时为响应内容，失败时为错误信息
    """
    model = model or DEFAULT_MODEL

    try:
        payload = json.dumps({
            "model": model,
            "messages": [{"role": "user", "content": message}],
            "temperature": temperature
        })

        conn = http.client.HTTPConnection(PROXY_HOST, PROXY_PORT, timeout=timeout)
        conn.request(
            "POST",
            "/v1/chat/completions",
            body=payload,
            headers={"Content-Type": "application/json"}
        )
        resp = conn.getresponse()
        data = json.loads(resp.read().decode())
        conn.close()

        if resp.status == 200:
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return True, content
        else:
            err = data.get("error", {}).get("message", "Unknown error")
            return False, f"LLM Error {resp.status}: {err}"

    except http.client.HTTPException as e:
        logger.error(f"HTTP 连接错误: {e}")
        return False, f"HTTP Error: {e}"
    except json.JSONDecodeError as e:
        logger.error(f"JSON 解析错误: {e}")
        return False, f"JSON Error: {e}"
    except Exception as e:
        logger.error(f"LLM 调用异常: {e}")
        return False, f"Exception: {e}"


# 模块级单例导入兼容
__all__ = ["call_llm_direct"]


if __name__ == "__main__":
    # 测试
    import sys
    logging.basicConfig(level=logging.INFO)

    test_msg = "你好，请用一句话介绍自己。"

    print(f"测试 call_llm_direct (模型: {DEFAULT_MODEL})...")
    ok, result = call_llm_direct(test_msg, timeout=30)

    if ok:
        print(f"✅ 成功: {result[:100]}...")
    else:
        print(f"❌ 失败: {result}")
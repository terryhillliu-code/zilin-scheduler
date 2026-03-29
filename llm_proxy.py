#!/usr/bin/env python3
"""
LLM 直连代理模块 - 兼容层

已迁移到 zhiwei_common.llm，此文件保留作为兼容导入。
"""
# 兼容层：重定向到 zhiwei_common.llm
from zhiwei_common.llm import llm_client

def call_llm_direct(message: str, timeout: int = 180, model: str = None, temperature: float = 0.7) -> tuple:
    """
    直接调用 LLM（兼容接口）

    Args:
        message: 用户消息
        timeout: 超时时间（秒）
        model: 模型名称（已忽略，使用角色模型）
        temperature: 温度参数（已忽略）

    Returns:
        (success, content) 元组
    """
    return llm_client.call("chat", message, timeout=timeout)

__all__ = ["call_llm_direct"]
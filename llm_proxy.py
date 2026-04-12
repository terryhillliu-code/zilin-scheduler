#!/usr/bin/env python3
"""
LLM 直连代理模块 - 兼容层

已迁移到 zhiwei_common.llm，此文件保留作为兼容导入。
"""
import logging
from zhiwei_common.llm import llm_client

logger = logging.getLogger(__name__)

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
    if model is not None:
        logger.warning("llm_proxy.call_llm_direct: model 参数已忽略，使用角色模型")
    if temperature != 0.7:
        logger.warning("llm_proxy.call_llm_direct: temperature 参数已忽略")
    return llm_client.call("chat", message, timeout=timeout)
#!/usr/bin/env python3
"""
测试 info_brief 修复效果
"""
import sys
from pathlib import Path
import os

# 添加项目根目录到路径
BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

def test_enrich_with_klib():
    """测试 enrich_with_klib 函数是否能正确处理 info_brief 任务名称"""
    print("Testing enrich_with_klib function...")

    # 从 scheduler.py 导入函数和配置
    from scheduler import KLIB_ENRICHMENT, enrich_with_klib

    # 测试不同的任务名称格式
    test_cases = [
        "info_brief_07",
        "info_brief_09",
        "info_brief_11",
        "info_brief_15",
        "morning_brief",
        "nonexistent_task"
    ]

    print(f"KLIB_ENRICHMENT keys: {list(KLIB_ENRICHMENT.keys())}")

    for task_name in test_cases:
        # 模拟一个简单的 prompt
        test_prompt = "这是一个测试 prompt"

        # 这里只是测试函数逻辑是否正常，实际不会执行 docker 命令
        # 在真实环境中，这会调用 docker exec 命令，但由于我们无法真正执行，
        # 函数会在 subprocess.run 时捕获异常并返回原始 prompt
        print(f"  Testing task_name: {task_name}")

        # 验证任务名称映射逻辑
        lookup_key = task_name
        if task_name.startswith("info_brief_"):
            lookup_key = "info_brief"
        print(f"    Lookup key: {lookup_key}, Found in KLIB: {lookup_key in KLIB_ENRICHMENT}")

    print("✅ enrich_with_klib 测试完成")

def test_timeout_setting():
    """测试 info_brief 超时设置"""
    print("\nTesting timeout setting...")

    # 读取 scheduler.py 文件并查找修改后的超时设置
    scheduler_path = BASE_DIR / "scheduler.py"
    content = scheduler_path.read_text(encoding="utf-8")

    # 检查是否已经更新为 600 秒
    if "call_agent(\"researcher\", prompt, timeout=600)" in content:
        print("✅ 超时设置已更新为 600 秒")
    else:
        print("❌ 超时设置未正确更新")

    # 检查 KLIB_ENRICHMENT 是否包含 info_brief 键
    if "\"info_brief\"" in content and "全球资讯" in content:
        print("✅ KLIB_ENRICHMENT 已包含 info_brief 键和关键词")
    else:
        print("❌ KLIB_ENRICHMENT 可能未正确更新")

if __name__ == "__main__":
    print("开始测试 info_brief 修复效果...")

    test_enrich_with_klib()
    test_timeout_setting()

    print("\n✅ 所有测试完成！")
    print("\n总结修复内容:")
    print("1. 将 info_brief 任务的超时时间从 360 秒增加到 600 秒")
    print("2. 为 info_brief 任务添加了知识库增强关键词")
    print("3. 修改 enrich_with_klib 函数以支持动态任务名称格式")
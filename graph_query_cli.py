#!/usr/bin/env python3
import sys
import asyncio
import argparse
import os
from pathlib import Path

# 确保能找到 graph_pipeline
sys.path.append(str(Path(__file__).parent))

from graph_pipeline import query_graph

async def run_query():
    parser = argparse.ArgumentParser(description="GraphRAG CLI")
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("--mode", type=str, default="hybrid")
    parser.add_argument("--user_prompt", type=str, default=None)
    args = parser.parse_args()

    # 抑制其他库的日志，只输出结果
    import logging
    logging.getLogger("lightrag").setLevel(logging.ERROR)
    logging.getLogger("nano-vectordb").setLevel(logging.ERROR)

    try:
        # aquery 是异步的
        result = await query_graph(args.query, mode=args.mode, user_prompt=args.user_prompt)
        # 只打印结果到 stdout，方便父进程解析
        print(result)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    if "DASHSCOPE_API_KEY" not in os.environ:
        # 尝试从本地环境文件或默认加载（如果有的话）
        pass
    asyncio.run(run_query())

#!/usr/bin/env python3
import asyncio
import logging
import sys
from graph_pipeline import index_knowledge_sources

async def run_index():
    # 抑制无用日志
    logging.getLogger("lightrag").setLevel(logging.WARNING)
    logging.getLogger("nano-vectordb").setLevel(logging.WARNING)
    
    try:
        hotspot_count = await index_knowledge_sources()
        print(f"SUCCESS:{hotspot_count}")
    except Exception as e:
        print(f"ERROR:{e}")
        sys.exit(1)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    asyncio.run(run_index())

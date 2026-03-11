#!/usr/bin/env python3
"""
知识图谱构建管线 (Phase 4c - T-406)
1. 使用 LightRAG 框架提取实体与逻辑关系
2. 索引 Obsidian 笔记与 PDF 研报
"""

import os
import asyncio
import logging
from pathlib import Path
from lightrag import LightRAG, QueryParam
from lightrag.llm.openai import openai_complete_if_cache, openai_embed
from lightrag.llm.openai import wrap_embedding_func_with_attrs

# 环境变量配置 (优先从统一来源 zhiwei-bot/.env 加载)
def _load_env_secrets():
    """
    优先级 (v39.1 修正):
    1. ~/zhiwei-bot/.env (统一来源，override=True 覆盖环境变量)
    2. 进程已有的环境变量
    3. ~/.secrets/zhiwei.env (兜底快照)
    """
    try:
        from dotenv import load_dotenv
        # 优先从 zhiwei-bot/.env 加载，覆盖现有环境变量
        bot_env = Path("/Users/liufang/zhiwei-bot/.env")
        if bot_env.exists():
            load_dotenv(bot_env, override=True)
            key = os.getenv("DASHSCOPE_API_KEY")
            if key and key.startswith("sk-"):
                return key
    except ImportError:
        pass
    
    # 降级：直接读取文件
    key = os.getenv("DASHSCOPE_API_KEY")
    if key and key.startswith("sk-"):
        return key
    
    # 兜底：扫描其他 .env 路径
    search_paths = [
        Path(__file__).parent / ".env",
        Path.home() / ".secrets" / "zhiwei.env"
    ]

    for env_path in search_paths:
        if env_path.exists():
            try:
                with open(env_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("DASHSCOPE_API_KEY="):
                            key = line.split("=", 1)[1].strip()
                            key = key.strip("'").strip('"')
                            if key.startswith("sk-"):
                                os.environ["DASHSCOPE_API_KEY"] = key
                                return key
            except Exception:
                pass
    return key

DASHSCOPE_API_KEY = _load_env_secrets()
DASHSCOPE_API_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"

logger = logging.getLogger(__name__)

# 配置图数据库存储路径
GRAPH_WORKING_DIR = Path("/Users/liufang/zhiwei-scheduler/graph_db")
GRAPH_WORKING_DIR.mkdir(parents=True, exist_ok=True)

# 笔记及研报路径
VAULT_PATH = "/Users/liufang/Documents/ZhiweiVault"
REPORTS_DIR = Path.home() / "knowledge-inbox" / "reports"

async def llm_model_if_cache(
    prompt, system_prompt=None, history=[], gen_conf={"max_tokens": 1024, "temperature": 0}, **kwargs
) -> str:
    """基于百炼的 OpenAI 兼容接口封装"""
    # 再次确保 API KEY 可用
    api_key = DASHSCOPE_API_KEY or os.getenv("DASHSCOPE_API_KEY")
    # 彻底移除不需要且会引发冲突的 history 参数
    clean_kwargs = {k: v for k, v in kwargs.items() if k != "history"}
    return await openai_complete_if_cache(
        "qwen-plus", # 使用通义千问 Plus 作为逻辑提取模型
        prompt,
        system_prompt=system_prompt,
        base_url=DASHSCOPE_API_URL,
        api_key=api_key,
        **gen_conf,
        **clean_kwargs,
    )

async def embedding_func(texts: list[str]) -> list[list[float]]:
    """基于百炼的 Embedding 接口封装"""
    api_key = DASHSCOPE_API_KEY or os.getenv("DASHSCOPE_API_KEY")
    # 使用 .func 绕过 openai_embed 默认自带的 1536 维度校验装饰器
    return await openai_embed.func(
        texts,
        model="text-embedding-v3",
        base_url=DASHSCOPE_API_URL,
        api_key=api_key,
    )

_rag_instance = None
_rag_lock = asyncio.Lock()

async def get_rag():
    """单例获取并初始化 LightRAG 实例"""
    global _rag_instance
    async with _rag_lock:
        if _rag_instance is None:
            logger.info(f"初始化且激活 LightRAG 实例 (工作目录: {GRAPH_WORKING_DIR})")
            _rag_instance = LightRAG(
                working_dir=str(GRAPH_WORKING_DIR),
                llm_model_func=llm_model_if_cache,
                embedding_func=wrap_embedding_func_with_attrs(
                    embedding_dim=1024,
                    max_token_size=2048,
                )(embedding_func)
            )
            # 必须调用初始化存储
            await _rag_instance.initialize_storages()
        return _rag_instance

async def index_knowledge_sources():
    """扫描并索引所有知识源 (Phase 5b: 增加热点检测)"""
    rag = await get_rag()
    logger.info("=== 启动知识图谱增量索引 ===")
    
    from datetime import datetime, timedelta
    cutoff = datetime.now() - timedelta(hours=24)
    hotspots = []

    # 1. 索引 Obsidian 笔记
    notes = list(Path(VAULT_PATH).rglob("*.md"))
    logger.info(f"发现 {len(notes)} 份笔记，开始扫描...")
    for note in notes:
        try:
            mtime = datetime.fromtimestamp(note.stat().st_mtime)
            if mtime > cutoff:
                hotspots.append(f"Obsidian: {note.name}")
            
            content = note.read_text(encoding="utf-8")
            if content.strip():
                # 显式使用 ainsert 异步版本
                await rag.ainsert(content)
        except Exception as e:
            logger.error(f"索引笔记失败 {note.name}: {e}")

    # 2. 索引 PDF 研报的 MD 输出 (如果已解析)
    report_mds = list(REPORTS_DIR.rglob("*.md"))
    logger.info(f"发现 {len(report_mds)} 份研报 Markdown，开始扫描...")
    for r_md in report_mds:
        try:
            mtime = datetime.fromtimestamp(r_md.stat().st_mtime)
            if mtime > cutoff:
                hotspots.append(f"Report: {r_md.name}")

            content = r_md.read_text(encoding="utf-8")
            if content.strip():
                await rag.ainsert(content)
        except Exception as e:
            logger.error(f"索引研报失败 {r_md.name}: {e}")

    if hotspots:
        logger.info(f"🔥 本次索引发现 {len(hotspots)} 个热点更新: {', '.join(hotspots[:5])}...")
    
    logger.info("=== 知识图谱索引构建完成 ===")
    return len(hotspots)

async def query_graph(query: str, mode: str = "hybrid", user_prompt: str = None) -> str:
    """
    提供给调度器的查询接口 (T-407)
    mode: 'naive', 'local', 'global', 'hybrid'
    """
    rag = await get_rag()
    # rag.query 是一个协程
    return await rag.aquery(query, param=QueryParam(mode=mode, user_prompt=user_prompt))

def sync_query_graph(query: str, mode: str = "hybrid") -> str:
    """
    为多线程环境（如 scheduler.py）提供的同步包装器。
    在独立线程中显式启动新的事件循环以彻底避免循环冲突。
    """
    import asyncio
    import threading
    from concurrent.futures import Future

    def _thread_target(future: Future, q: str, m: str):
        try:
            # 在新线程中创建并设置新的事件循环
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(query_graph(q, mode=m))
            future.set_result(result)
            loop.close()
        except Exception as e:
            future.set_exception(e)

    future = Future()
    thread = threading.Thread(target=_thread_target, args=(future, query, mode))
    thread.start()
    
    try:
        # 等待结果，超时 45s
        return future.result(timeout=45)
    except Exception as e:
        return f"GraphRAG Sync Context Exception: {e}"

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    if not DASHSCOPE_API_KEY:
        print("错误: 请先设置 DASHSCOPE_API_KEY 环境变量")
    else:
        # 默认运行索引任务
        asyncio.run(index_knowledge_sources())

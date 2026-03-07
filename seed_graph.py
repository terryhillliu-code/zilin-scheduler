import asyncio
from graph_pipeline import get_rag

async def seed_data():
    rag = await get_rag()
    text = """
    半导体行业最新趋势：
    1. 2nm 节点竞争激烈，台积电与三星均在加速研发。
    2. 先进封装技术如 CoWoS 产能持续紧缺，成为 AI 芯片出货的瓶颈。
    3. 推理加速领域，NVIDIA 的 TensorRT-LLM 提供了显著的性能提升。
    4. HBM3e 内存已成为高端 GPU 的标配。
    """
    print("正在向图谱插入种子数据...")
    # insert 是异步的
    await rag.ainsert(text)
    print("种子数据插入完成！")

if __name__ == "__main__":
    asyncio.run(seed_data())

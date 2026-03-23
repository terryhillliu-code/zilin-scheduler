#!/usr/bin/env python3
import sys
import subprocess
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("graph-maintenance")

def main():
    logger.info("🕸️ 开始执行知识图谱自动化维护 (GraphRAG Indexing)...")
    
    # 获取当前脚本所在目录
    base_dir = Path(__file__).parent
    cli_script = base_dir / "graph_index_cli.py"
    
    if not cli_script.exists():
        logger.error(f"❌ 核心脚本不存在: {cli_script}")
        sys.exit(1)
        
    try:
        # 执行图谱索引任务 (设置为 3600s 超时)
        logger.info(f"🚀 调用 {cli_script.name}，超时设置为 3600s")
        result = subprocess.run(
            [sys.executable, str(cli_script)],
            capture_output=True,
            text=True,
            timeout=3600
        )
        
        if result.returncode == 0:
            logger.info("✅ 图谱维护任务执行成功")
            print(result.stdout)
        else:
            logger.error(f"❌ 图谱维护任务失败 (Exit Code {result.returncode})")
            logger.error(result.stderr)
            sys.exit(result.returncode)
            
    except subprocess.TimeoutExpired:
        logger.error("❌ 任务执行超时 (Force killing after 3600s)")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 运行异常: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

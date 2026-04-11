#!/usr/bin/env python3
import sys
import subprocess
import signal
import logging
import os
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
        # 执行图谱索引任务 (设置为 3600s 超时，使用进程组确保子进程被杀死)
        logger.info(f"🚀 调用 {cli_script.name}，超时设置为 3600s")

        # 使用 Popen 创建新进程组 (start_new_session=True)
        process = subprocess.Popen(
            [sys.executable, str(cli_script)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True  # 创建新进程组，便于杀死整个组
        )

        try:
            stdout, stderr = process.communicate(timeout=3600)

            if process.returncode == 0:
                logger.info("✅ 图谱维护任务执行成功")
                print(stdout)
            else:
                logger.error(f"❌ 图谱维护任务失败 (Exit Code {process.returncode})")
                logger.error(stderr)
                sys.exit(process.returncode)

        except subprocess.TimeoutExpired:
            # 超时后杀死整个进程组
            logger.error("❌ 任务执行超时，正在终止进程组...")
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                logger.info("进程组已终止")
            except ProcessLookupError:
                process.kill()  # 进程组不存在时，单独杀死进程

            # 收集剩余输出
            stdout, stderr = process.communicate()
            sys.exit(1)

    except Exception as e:
        logger.error(f"❌ 运行异常: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

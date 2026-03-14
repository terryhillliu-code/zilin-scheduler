#!/usr/bin/env python3
"""
进程锁管理器 (PID Lock)
防止调度任务并发执行，确保同一时间同一任务只有一个实例

使用示例：
    # 方式1: 上下文管理器（推荐）
    from lock_manager import acquire_lock

    with acquire_lock("researcher") as locked:
        if locked:
            print("获取锁成功，执行任务")
        else:
            print("任务已在执行中，跳过")

    # 方式2: 简单调用（用于非上下文场景）
    from lock_manager import try_acquire_lock, release_lock

    if not try_acquire_lock("my_task"):
        print("任务已在执行中")
        return
    # ... 执行任务 ...
    release_lock("my_task")
"""

import os
import fcntl
import time
from pathlib import Path
from contextlib import contextmanager
from typing import Optional
import logging

logger = logging.getLogger("zhiwei-scheduler.lock")

# 锁文件目录
LOCK_DIR = Path("/tmp/zhiwei-scheduler-locks")

# Agent 锁配置
AGENT_LOCKS = {
    "researcher": {"max_hold_time": 600, "name": "探微信息采集"},
    "operator": {"max_hold_time": 300, "name": "通微系统巡检"},
    "main": {"max_hold_time": 300, "name": "知微对话"},
    "builder": {"max_hold_time": 900, "name": "筑微代码生成"},
    "reviewer": {"max_hold_time": 600, "name": "审微代码审查"},
}


def try_acquire_lock(lock_name: str) -> bool:
    """
    尝试获取锁（非阻塞）

    Args:
        lock_name: 锁名称

    Returns:
        True 表示获取成功，False 表示锁被占用
    """
    LOCK_DIR.mkdir(exist_ok=True)
    lock_file = LOCK_DIR / f"{lock_name}.lock"

    try:
        # 检查是否有 stale 锁
        if lock_file.exists():
            if is_stale_lock(lock_file):
                logger.warning(f"🧹 清理 stale 锁: {lock_name}")
                force_unlock(lock_file)
            else:
                logger.debug(f"🔒 锁被占用: {lock_name}")
                return False

        # 创建锁文件
        lock_fd = open(lock_file, 'w')
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            lock_fd.write(str(os.getpid()))
            lock_fd.flush()
            os.fsync(lock_fd.fileno())
            lock_fd.close()
            logger.debug(f"🔓 获取锁: {lock_name} (PID: {os.getpid()})")
            return True
        except BlockingIOError:
            lock_fd.close()
            return False

    except Exception as e:
        logger.error(f"❌ 获取锁失败 [{lock_name}]: {e}")
        return False


def release_lock(lock_name: str) -> bool:
    """
    释放锁

    Args:
        lock_name: 锁名称

    Returns:
        是否成功释放
    """
    lock_file = LOCK_DIR / f"{lock_name}.lock"

    try:
        if lock_file.exists():
            os.unlink(lock_file)
            logger.debug(f"🔓 释放锁: {lock_name}")
            return True
    except Exception as e:
        logger.error(f"❌ 释放锁失败 [{lock_name}]: {e}")
        return False

    return False


@contextmanager
def acquire_lock(lock_name: str, timeout: int = 0):
    """
    进程锁上下文管理器

    参数:
        lock_name: 锁名称（如 "researcher", "operator"）
        timeout: 等待锁超时时间（秒），0 表示立即返回

    用法:
        with acquire_lock("researcher") as locked:
            if locked:
                # 执行任务
                pass
            else:
                # 跳过或重试
                pass
    """
    LOCK_DIR.mkdir(exist_ok=True)
    lock_file = LOCK_DIR / f"{lock_name}.lock"
    lock_fd = None

    try:
        # 创建锁文件
        lock_fd = open(lock_file, 'w')

        if timeout > 0:
            # 阻塞等待锁
            try:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
            except BlockingIOError:
                logger.warning(f"⏳ 等待锁超时: {lock_name}")
                lock_fd.close()
                yield False
                return
        else:
            # 非阻塞模式
            try:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                # 锁被占用，检查是否 stale
                if is_stale_lock(lock_file):
                    logger.warning(f"🧹 清理 stale 锁: {lock_name}")
                    force_unlock(lock_file)
                    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
                else:
                    logger.info(f"🔒 锁被占用，跳过: {lock_name}")
                    lock_fd.close()
                    yield False
                    return

        # 获取锁成功，写入 PID
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
        os.fsync(lock_fd.fileno())

        logger.debug(f"🔓 获取锁: {lock_name} (PID: {os.getpid()})")
        yield True

    except Exception as e:
        logger.error(f"❌ 锁错误 [{lock_name}]: {e}")
        yield False

    finally:
        if lock_fd and not lock_fd.closed:
            try:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
                lock_fd.close()
            except:
                pass

        # 如果是强制释放，不删除锁文件（让它被下次获取时清理）
        if lock_file.exists():
            try:
                os.unlink(lock_file)
                logger.debug(f"🔓 释放锁: {lock_name}")
            except:
                pass


def is_stale_lock(lock_file: Path, max_age: int = 3600) -> bool:
    """检查锁是否 stale（进程已死）"""
    if not lock_file.exists():
        return False

    try:
        # 读取锁文件中的 PID
        pid = int(lock_file.read_text().strip())

        # 检查进程是否存在
        try:
            os.kill(pid, 0)  # 信号 0 只是检查进程是否存在
            return False  # 进程存在，锁有效
        except OSError:
            # 进程不存在，锁 stale
            return True
    except:
        return False


def force_unlock(lock_file: Path):
    """强制解锁（清理 stale 锁）"""
    try:
        if lock_file.exists():
            os.unlink(lock_file)
            logger.info(f"🧹 强制解锁: {lock_file.name}")
    except Exception as e:
        logger.error(f"❌ 强制解锁失败: {e}")


def get_lock_info(lock_name: str) -> Optional[dict]:
    """获取锁信息"""
    lock_file = LOCK_DIR / f"{lock_name}.lock"

    if not lock_file.exists():
        return None

    try:
        pid = int(lock_file.read_text().strip())
        stat = os.stat(lock_file)
        return {
            "pid": pid,
            "age_seconds": time.time() - stat.st_mtime,
            "is_stale": is_stale_lock(lock_file)
        }
    except:
        return None


def cleanup_all_locks():
    """清理所有锁"""
    if not LOCK_DIR.exists():
        return

    for lock_file in LOCK_DIR.glob("*.lock"):
        if is_stale_lock(lock_file):
            force_unlock(lock_file)


# CLI 接口
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("用法: python3 lock_manager.py <命令>")
        print("命令:")
        print("  list              - 列出所有锁")
        print("  cleanup           - 清理 stale 锁")
        print("  test <name>       - 测试锁")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "list":
        print("📋 当前锁状态:")
        for lock_name in AGENT_LOCKS:
            info = get_lock_info(lock_name)
            if info:
                status = "🔴 STALE" if info["is_stale"] else "🔒 占用"
                print(f"  {lock_name}: {status} (PID: {info['pid']}, {info['age_seconds']:.0f}s)")
            else:
                print(f"  {lock_name}: 🟢 可用")

    elif cmd == "cleanup":
        cleanup_all_locks()
        print("✅ 锁清理完成")

    elif cmd == "test":
        lock_name = sys.argv[2] if len(sys.argv) > 2 else "researcher"
        print(f"🧪 测试锁: {lock_name}")
        with acquire_lock(lock_name) as locked:
            if locked:
                print(f"✅ 获取锁成功，5秒后释放...")
                time.sleep(5)
            else:
                print(f"❌ 获取锁失败")
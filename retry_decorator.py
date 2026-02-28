#!/usr/bin/env python3
"""
重试机制模块 (Retry Mechanism)
为调度器提供重试能力

使用示例：
    from retry_decorator import retry_on_failure

    @retry_on_failure(max_attempts=3, delay_seconds=600)
    def my_task():
        # 任务逻辑
        pass
"""

import time
import functools
import logging
from datetime import datetime, timedelta
from typing import Callable, Any

logger = logging.getLogger("zhiwei-scheduler.retry")


def retry_on_failure(
    max_attempts: int = 3,
    delay_seconds: int = 600,
    exponential_backoff: bool = False,
    on_retry: Callable[[Exception, int], None] = None
):
    """
    重试装饰器

    参数:
        max_attempts: 最大尝试次数
        delay_seconds: 重试延迟（秒）
        exponential_backoff: 是否使用指数退避
        on_retry: 重试时的回调函数 (exception, attempt_number)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    if attempt < max_attempts:
                        # 计算延迟
                        if exponential_backoff:
                            delay = delay_seconds * (2 ** (attempt - 1))
                        else:
                            delay = delay_seconds

                        # 计算下次执行时间
                        next_run = datetime.now() + timedelta(seconds=delay)

                        logger.warning(
                            f"🔄 任务失败 [{func.__name__}] "
                            f"尝试 {attempt}/{max_attempts}，"
                            f"{delay}秒后重试 (计划: {next_run.strftime('%H:%M:%S')})"
                        )

                        # 调用回调
                        if on_retry:
                            on_retry(e, attempt)

                        # 等待
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"❌ 任务彻底失败 [{func.__name__}] "
                            f"已达最大重试次数 {max_attempts}"
                        )

            # 所有尝试都失败
            raise last_exception

        return wrapper
    return decorator


class RetryContext:
    """
    重试上下文管理器
    用于在代码块中手动控制重试
    """

    def __init__(self, name: str, max_attempts: int = 3, delay_seconds: int = 600):
        self.name = name
        self.max_attempts = max_attempts
        self.delay_seconds = delay_seconds
        self.attempt = 0
        self.last_error = None

    def __enter__(self):
        self.attempt += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.last_error = exc_val

            if self.attempt < self.max_attempts:
                logger.warning(
                    f"🔄 [{self.name}] 失败 {self.attempt}/{self.max_attempts}，"
                    f"{self.delay_seconds}秒后重试"
                )
                time.sleep(self.delay_seconds)
                return True  # 抑制异常，继续重试
            else:
                logger.error(f"❌ [{self.name}] 彻底失败")
                return False  # 重新抛出异常
        return True


class RetryScheduler:
    """
    调度器专用重试管理
    与 APScheduler 集成
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.retry_jobs = {}  # job_id -> retry_count

    def schedule_retry(
        self,
        original_job_id: str,
        run_time: datetime = None,
        max_retries: int = 3
    ):
        """安排重试任务"""
        retry_count = self.retry_jobs.get(original_job_id, 0) + 1

        if retry_count < max_retries:
            self.retry_jobs[original_job_id] = retry_count

            if run_time is None:
                run_time = datetime.now() + timedelta(seconds=600)  # 默认10分钟

            retry_job_id = f"{original_job_id}_retry_{retry_count}"

            # 添加重试任务
            self.scheduler.add_job(
                lambda: self.scheduler.print_jobs(original_job_id),
                trigger="date",
                run_date=run_time,
                id=retry_job_id,
                name=f"{original_job_id} 重试 {retry_count}/{max_retries}"
            )

            logger.info(
                f"📅 已安排重试 [{original_job_id}] "
                f"{retry_count}/{max_retries} @ {run_time.strftime('%H:%M:%S')}"
            )
        else:
            logger.error(f"❌ [{original_job_id}] 达到最大重试次数，放弃")
            self.retry_jobs.pop(original_job_id, None)

    def clear_retry(self, job_id: str):
        """清除重试记录"""
        original_job_id = job_id.rsplit("_retry_", 1)[0]
        self.retry_jobs.pop(original_job_id, None)

    def get_retry_count(self, job_id: str) -> int:
        """获取重试次数"""
        return self.retry_jobs.get(job_id, 0)
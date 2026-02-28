#!/usr/bin/env python3
"""
重试失败的推送任务
由 crontab 每小时调用一次
"""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))

from scheduler import load_config, setup_logging
from pusher import PushManager
from scheduler_queue import retry_failed, get_queue_stats, check_and_alert

def main():
    config = load_config()
    logger = setup_logging(
        log_dir=config.get("system", {}).get("log_dir", "logs"),
        retention_days=config.get("system", {}).get("log_retention_days", 30)
    )
    push_manager = PushManager(config)
    
    # 队列状态
    before_stats = get_queue_stats()
    logger.info(f"重试任务开始，队列状态: {before_stats}")
    
    # 检查并发送告警（重试前检查，告警用户）
    check_and_alert(push_manager, alert_threshold=3, logger=logger)
    
    # 执行重试
    result = retry_failed(push_manager, max_retries=3, logger=logger)
    
    # 后续状态
    after_stats = get_queue_stats()
    logger.info(f"重试完成: 重新入队 {result['retried']} 个, 成功推送 {result['success']} 个")
    logger.info(f"当前队列状态: {after_stats}")

if __name__ == "__main__":
    main()

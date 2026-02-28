import sys
from pathlib import Path

# 确保能加载当前目录下的模块
sys.path.insert(0, str(Path.cwd()))

try:
    from scheduler import load_config, setup_logging, job_morning_brief
    import scheduler
    from pusher import PushManager

    # 初始化
    scheduler.config = load_config()
    scheduler.logger = setup_logging()
    scheduler.push_manager = PushManager(scheduler.config)

    print('🚀 开始执行任务: job_morning_brief...')
    job_morning_brief()
    print('✅ 执行完成，请检查飞书/钉钉。')
except Exception as e:
    print(f'❌ 执行失败: {e}')
    import traceback
    traceback.print_exc()

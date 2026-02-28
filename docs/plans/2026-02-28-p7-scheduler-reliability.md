# P7 调度器高可靠性重构实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**目标**：为 zhiwei-scheduler 增加指数退避重试、文件触发器、幂等性检查和并发保护机制

**架构**：
- 指数退避重试：10min → 20min → 40min 延迟重试
- 触发器监听：轮询 ~/zhiwei-scheduler/triggers/ 目录
- 幂等性：通过 Hash 对比价格变化，变化才推送
- 并发保护：PID Lock 机制防止同一任务重复执行

**技术栈**：Python 3.14, APScheduler, fcntl (文件锁)

---

## Task 1: 创建 PID Lock 机制

### 修改文件
`/Users/liufang/zhiwei-scheduler/lock_manager.py` (新建)

### 代码修改
```python
import fcntl
import os
from pathlib import Path
from contextlib import contextmanager

LOCK_DIR = Path("/tmp/zhiwei-scheduler-locks")

@contextmanager
def acquire_lock(lock_name: str):
    """进程锁上下文管理器"""
    LOCK_DIR.mkdir(exist_ok=True)
    lock_file = LOCK_DIR / f"{lock_name}.lock"

    with open(lock_file, 'w') as f:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            f.write(str(os.getpid()))
            yield True
        except BlockingIOError:
            yield False
```

---

## Task 2: 实现指数退避重试

### 修改文件
`/Users/liufang/zhiwei-scheduler/scheduler.py`

### 代码修改
在调度器初始化部分添加：
```python
# 指数退避重试配置
RETRY_DELAYS = [600, 1200, 2400]  # 10min, 20min, 40min

def get_retry_delay(attempt: int) -> int:
    """获取指数退避延迟"""
    if attempt <= len(RETRY_DELAYS):
        return RETRY_DELAYS[attempt - 1]
    return RETRY_DELAYS[-1]  # 最大40分钟
```

---

## Task 3: 创建触发器监听器

### 修改文件
`/Users/liufang/zhiwei-scheduler/trigger_listener.py` (新建)

### 代码修改
```python
import threading
import time
from pathlib import Path
from datetime import datetime

TRIGGER_DIR = Path("/Users/liufang/zhiwei-scheduler/triggers")

class TriggerListener:
    def __init__(self, scheduler, check_interval=5):
        self.scheduler = scheduler
        self.check_interval = check_interval
        self.running = False

    def start(self):
        self.running = True
        thread = threading.Thread(target=self._watch_loop, daemon=True)
        thread.start()

    def _watch_loop(self):
        while self.running:
            for trigger_file in TRIGGER_DIR.glob("*.run"):
                self._handle_trigger(trigger_file)
            time.sleep(self.check_interval)

    def _handle_trigger(self, path):
        # 解析触发器，调度任务
        # 执行后删除文件
        pass
```

---

## Task 4: 实现幂等性检查 (价格 Hash)

### 修改文件
`/Users/liufang/zhiwei-scheduler/price_cache.py` (新建)

### 代码修改
```python
import json
import hashlib
from pathlib import Path

CACHE_DIR = Path("/tmp/zhiwei-scheduler-cache")

def compute_price_hash(data: dict) -> str:
    """计算价格数据的 Hash"""
    normalized = json.dumps(data, sort_keys=True)
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]

def has_price_changed(part_number: str, new_data: dict) -> bool:
    """检查价格是否变化"""
    cache_file = CACHE_DIR / f"price_{part_number}.json"

    if not cache_file.exists():
        return True  # 首次，无缓存

    old_hash = json.loads(cache_file.read_text())["hash"]
    new_hash = compute_price_hash(new_data)

    return old_hash != new_hash
```

---

## Task 5: 集成所有组件

### 修改文件
`/Users/liufang/zhiwei-scheduler/scheduler.py`

### 修改清单
| 位置 | 修改内容 |
|------|----------|
| import | 添加 lock_manager, price_cache, trigger_listener |
| main() | 初始化 TriggerListener 并启动 |
| job_xxx() | 包裹 acquire_lock 上下文管理器 |
| 重试逻辑 | 使用指数退避 RETRY_DELAYS |

---

## Task 6: 测试验证

### 测试用例
1. **重试测试**：模拟任务失败，验证 10min → 20min → 40min 延迟
2. **触发器测试**：`touch triggers/tanwei.run`，验证任务执行
3. **幂等性测试**：相同价格不重复推送
4. **并发测试**：同时触发多个任务，验证 Lock 生效

### 验证命令
```bash
# 触发器测试
touch ~/zhiwei-scheduler/triggers/tanwei.run

# 查看日志
tail -f ~/zhiwei-scheduler/logs/scheduler.log
```

---

## 执行顺序
1. Task 1: PID Lock
2. Task 2: 指数退避
3. Task 3: 触发器监听
4. Task 4: 幂等性检查
5. Task 5: 集成
6. Task 6: 测试验证
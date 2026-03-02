"""
开发任务状态管理
- 任务锁（防并发）
- 共享状态（执微间记忆）
- Git 自动提交
"""
import os
import json
import fcntl
import subprocess
from datetime import datetime
from pathlib import Path

STATE_FILE = os.path.expanduser("~/zhiwei-scheduler/dev_state.json")
LOCK_FILE = os.path.expanduser("~/zhiwei-scheduler/.dev_lock")
WORK_DIRS = [
    os.path.expanduser("~/zhiwei-bot"),
    os.path.expanduser("~/zhiwei-scheduler"),
]


class DevLock:
    """文件锁，确保同时只有一个 /dev 任务执行"""

    def __init__(self):
        self.lock_file = None
        self.locked = False

    def acquire(self, timeout=0) -> tuple:
        """
        获取锁
        返回: (成功, 消息)
        """
        try:
            self.lock_file = open(LOCK_FILE, 'w')
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.locked = True
            # 写入当前任务信息
            self.lock_file.write(json.dumps({
                "pid": os.getpid(),
                "time": datetime.now().isoformat()
            }))
            self.lock_file.flush()
            return True, "锁定成功"
        except BlockingIOError:
            # 已有任务在执行，读取信息
            try:
                with open(LOCK_FILE, 'r') as f:
                    info = json.load(f)
                    return False, f"有任务正在执行 (PID: {info.get('pid')}, 开始于: {info.get('time')})"
            except:
                return False, "有其他任务正在执行"

    def release(self):
        """释放锁"""
        if self.lock_file and self.locked:
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
            self.lock_file.close()
            self.locked = False
            try:
                os.remove(LOCK_FILE)
            except:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.release()


class DevState:
    """共享状态管理"""

    def __init__(self):
        self.state = self._load()

    def _load(self) -> dict:
        """加载状态文件"""
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {
            "current_task": None,
            "recent_tasks": [],
            "modified_files": [],
            "last_update": None
        }

    def _save(self):
        """保存状态文件"""
        self.state["last_update"] = datetime.now().isoformat()
        with open(STATE_FILE, 'w') as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)

    def start_task(self, task_id: str, description: str):
        """记录任务开始"""
        self.state["current_task"] = {
            "id": task_id,
            "description": description,
            "start_time": datetime.now().isoformat()
        }
        self._save()

    def end_task(self, task_id: str, success: bool, modified_files: list = None):
        """记录任务结束"""
        task = self.state["current_task"]
        if task and task["id"] == task_id:
            task["success"] = success
            task["end_time"] = datetime.now().isoformat()

            # 保留最近10个任务
            self.state["recent_tasks"].insert(0, task)
            self.state["recent_tasks"] = self.state["recent_tasks"][:10]

            # 更新最近修改的文件
            if modified_files:
                for f in modified_files:
                    if f not in self.state["modified_files"]:
                        self.state["modified_files"].insert(0, f)
                self.state["modified_files"] = self.state["modified_files"][:20]

            self.state["current_task"] = None
            self._save()

    def get_context_for_prompt(self) -> str:
        """生成注入到 prompt 的上下文"""
        lines = ["## 开发状态上下文\n"]

        if self.state["recent_tasks"]:
            lines.append("### 最近完成的任务")
            for t in self.state["recent_tasks"][:3]:
                status = "✅" if t.get("success") else "❌"
                lines.append(f"- {status} {t['description'][:50]}...")
            lines.append("")

        if self.state["modified_files"]:
            lines.append("### 最近修改的文件")
            for f in self.state["modified_files"][:5]:
                lines.append(f"- {f}")
            lines.append("")

        return "\n".join(lines)


def git_auto_commit(message: str, work_dir: str = None) -> tuple:
    """
    自动 git commit
    返回: (成功, 输出信息)
    """
    dirs = [work_dir] if work_dir else WORK_DIRS
    results = []

    for d in dirs:
        if not os.path.exists(os.path.join(d, ".git")):
            continue

        try:
            # git add -A
            subprocess.run(
                ["git", "add", "-A"],
                cwd=d, capture_output=True, timeout=10
            )

            # 检查是否有变更
            status = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=d, capture_output=True, text=True, timeout=10
            )

            if not status.stdout.strip():
                results.append(f"{d}: 无变更")
                continue

            # git commit
            commit = subprocess.run(
                ["git", "commit", "-m", message],
                cwd=d, capture_output=True, text=True, timeout=30
            )

            if commit.returncode == 0:
                # 获取 commit hash
                hash_result = subprocess.run(
                    ["git", "rev-parse", "--short", "HEAD"],
                    cwd=d, capture_output=True, text=True, timeout=10
                )
                commit_hash = hash_result.stdout.strip()
                results.append(f"{d}: committed {commit_hash}")
            else:
                results.append(f"{d}: commit failed - {commit.stderr[:100]}")

        except Exception as e:
            results.append(f"{d}: error - {str(e)[:50]}")

    return True, "; ".join(results)


def git_revert_last(work_dir: str) -> tuple:
    """回滚最后一次 commit"""
    try:
        result = subprocess.run(
            ["git", "revert", "--no-commit", "HEAD"],
            cwd=work_dir, capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            return True, "已回滚最后一次提交"
        else:
            return False, result.stderr
    except Exception as e:
        return False, str(e)

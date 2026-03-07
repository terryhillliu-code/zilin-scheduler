#!/usr/bin/env python3
"""
scheduler.jsonl 日志分析工具
功能：统计任务耗时、Token 消耗、推送成功率 + 系统资源监控
"""
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

LOG_FILE = Path.home() / "logs" / "scheduler.jsonl"
DB_FILE = Path.home() / "Documents" / "Library" / "klib.db"

# 告警阈值
DISK_CRITICAL_GB = 10


def load_logs(hours: int = 24) -> list[dict]:
    """加载日志文件"""
    if not LOG_FILE.exists():
        print(f"❌ 日志文件不存在: {LOG_FILE}")
        return []

    logs = []
    cutoff = datetime.now() - timedelta(hours=hours) if hours > 0 else None

    with open(LOG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                entry_time = datetime.fromisoformat(entry.get("start_time", ""))

                if cutoff and entry_time < cutoff:
                    continue

                logs.append(entry)
            except json.JSONDecodeError:
                continue

    return logs


def analyze_logs(logs: list[dict]) -> dict:
    """分析日志数据"""
    if not logs:
        return {}

    # 按 task_name 分组
    tasks = {}
    for entry in logs:
        name = entry.get("task_name", "unknown")
        if name not in tasks:
            tasks[name] = {
                "count": 0,
                "latencies": [],
                "successes": 0,
                "failures": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
            }

        tasks[name]["count"] += 1
        tasks[name]["latencies"].append(entry.get("latency_seconds", 0))

        # 推送状态
        push_status = entry.get("push_status", {})
        if any(push_status.values()):
            tasks[name]["successes"] += 1
        else:
            tasks[name]["failures"] += 1

        # Token 消耗
        token_usage = entry.get("token_usage", {})
        tasks[name]["prompt_tokens"] += token_usage.get("prompt_tokens", 0)
        tasks[name]["completion_tokens"] += token_usage.get("completion_tokens", 0)

    return tasks


def get_disk_usage() -> dict:
    """获取磁盘使用情况"""
    try:
        usage = shutil.disk_usage("/")
        free_gb = usage.free / (1024 ** 3)
        total_gb = usage.total / (1024 ** 3)
        return {
            "free_gb": round(free_gb, 1),
            "total_gb": round(total_gb, 1),
            "status": "[CRITICAL]" if free_gb < DISK_CRITICAL_GB else "安全"
        }
    except Exception as e:
        return {"free_gb": 0, "total_gb": 0, "status": f"Error: {e}"}


def get_db_size() -> dict:
    """获取数据库文件大小"""
    try:
        if DB_FILE.exists():
            size_bytes = os.path.getsize(DB_FILE)
            size_mb = size_bytes / (1024 * 1024)
            return {"size_mb": round(size_mb, 1), "exists": True}
        else:
            return {"size_mb": 0, "exists": False}
    except Exception as e:
        return {"size_mb": 0, "exists": False, "error": str(e)}


def get_docker_stats() -> dict:
    """获取容器资源使用"""
    try:
        result = subprocess.run(
            ["docker", "stats", "clawdbot", "--no-stream", "--format", "{{.CPUPerc}}|{{.MemUsage}}"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0 and result.stdout.strip():
            output = result.stdout.strip()
            # 格式: "1.23%|156MiB / 8GiB"
            cpu_raw, mem_raw = output.split("|")
            cpu = cpu_raw.replace("%", "")
            mem = mem_raw.strip()
            return {"cpu": cpu, "mem": mem, "status": "ok"}
        else:
            return {"cpu": "N/A", "mem": "N/A", "status": "error"}
    except subprocess.TimeoutExpired:
        return {"cpu": "N/A", "mem": "N/A", "status": "timeout"}
    except FileNotFoundError:
        return {"cpu": "N/A", "mem": "N/A", "status": "docker_not_found"}
    except Exception as e:
        return {"cpu": "N/A", "mem": "N/A", "status": str(e)}


def print_system_dashboard():
    """打印系统资源看板"""
    disk = get_disk_usage()
    db = get_db_size()
    docker = get_docker_stats()

    print("--- 系统资源看板 ---")
    print(f"[磁盘空间]  剩余: {disk['free_gb']} GB / {disk['total_gb']} GB ({disk['status']})")

    if db.get("exists"):
        print(f"[知识库DB]  体积: {db['size_mb']} MB")
    else:
        print(f"[知识库DB]  文件不存在: {DB_FILE}")

    if docker.get("status") == "ok":
        print(f"[容器负载]  CPU: {docker['cpu']}% | MEM: {docker['mem']}")
    else:
        print(f"[容器负载]  获取失败: {docker.get('status', 'unknown')}")
    print("")


def print_report(tasks: dict, hours: int):
    """打印分析报告"""
    if not tasks:
        print("📊 无日志数据")
        return

    # 表头
    time_range = f"最近 {hours} 小时" if hours > 0 else "全部记录"
    print(f"\n{'='*90}")
    print(f"📊 知微定时任务运行指标分析 ({time_range})")
    print(f"{'='*90}")

    # 列标题
    print(f"{'任务名称':<20} {'执行次数':>8} {'平均耗时':>10} {'成功率':>10} {'Prompt Tokens':>15} {'Completion Tokens':>18}")
    print("-" * 90)

    # 数据行
    for name, stats in sorted(tasks.items()):
        count = stats["count"]
        avg_latency = sum(stats["latencies"]) / count if count > 0 else 0
        success_rate = (stats["successes"] / count * 100) if count > 0 else 0
        prompt = stats["prompt_tokens"]
        completion = stats["completion_tokens"]

        print(f"{name:<20} {count:>8} {avg_latency:>9.1f}s {success_rate:>9.1f}% {prompt:>15,} {completion:>18,}")

    # 汇总
    total_count = sum(s["count"] for s in tasks.values())
    total_latency = sum(sum(s["latencies"]) for s in tasks.values())
    total_success = sum(s["successes"] for s in tasks.values())
    total_prompt = sum(s["prompt_tokens"] for s in tasks.values())
    total_completion = sum(s["completion_tokens"] for s in tasks.values())

    avg_latency = total_latency / total_count if total_count > 0 else 0
    overall_success = (total_success / total_count * 100) if total_count > 0 else 0

    print("-" * 90)
    print(f"{'总计':<20} {total_count:>8} {avg_latency:>9.1f}s {overall_success:>9.1f}% {total_prompt:>15,} {total_completion:>18,}")
    print(f"{'='*90}\n")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="分析 scheduler 日志")
    parser.add_argument("--hours", "-H", type=int, default=0, help="分析最近 N 小时 (0=全部)")
    parser.add_argument("--all", "-a", action="store_true", help="分析所有记录 (等同于 --hours 0)")
    args = parser.parse_args()

    hours = 0 if args.all else args.hours

    logs = load_logs(hours)
    tasks = analyze_logs(logs)
    print_report(tasks, hours)
    print_system_dashboard()

    return 0


if __name__ == "__main__":
    sys.exit(main())

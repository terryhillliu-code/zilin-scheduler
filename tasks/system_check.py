"""
系统巡检模块
"""

import subprocess
import logging
import platform
from datetime import datetime

logger = logging.getLogger("zhiwei-scheduler")


def check_disk_usage() -> dict:
    """检查磁盘使用率"""
    try:
        result = subprocess.run(
            ["df", "-h", "/"],
            capture_output=True, text=True
        )
        lines = result.stdout.strip().split("\n")
        if len(lines) >= 2:
            parts = lines[1].split()
            usage_pct = int(parts[4].replace("%", ""))
            return {
                "name": "磁盘",
                "total": parts[1],
                "used": parts[2],
                "available": parts[3],
                "usage_pct": usage_pct,
                "status": "critical" if usage_pct > 90 else "warning" if usage_pct > 80 else "ok"
            }
    except Exception as e:
        return {"name": "磁盘", "error": str(e), "status": "error"}


def check_memory_usage() -> dict:
    """检查内存使用率"""
    try:
        if platform.system() == "Darwin":
            # macOS
            result = subprocess.run(
                ["vm_stat"],
                capture_output=True, text=True
            )
            lines = result.stdout.strip().split("\n")
            stats = {}
            for line in lines[1:]:
                if ":" in line:
                    key, val = line.split(":")
                    stats[key.strip()] = int(val.strip().rstrip("."))

            page_size = 16384  # M1 Mac
            total_pages = sum(stats.values())
            free_pages = stats.get("Pages free", 0) + stats.get("Pages inactive", 0)
            used_pct = int((1 - free_pages / max(total_pages, 1)) * 100)
        else:
            # Linux
            result = subprocess.run(
                ["free", "-m"],
                capture_output=True, text=True
            )
            lines = result.stdout.strip().split("\n")
            parts = lines[1].split()
            total = int(parts[1])
            used = int(parts[2])
            used_pct = int(used / max(total, 1) * 100)

        return {
            "name": "内存",
            "usage_pct": used_pct,
            "status": "critical" if used_pct > 90 else "warning" if used_pct > 85 else "ok"
        }
    except Exception as e:
        return {"name": "内存", "error": str(e), "status": "error"}


def check_docker_containers() -> list:
    """检查 Docker 容器状态"""
    containers = []
    try:
        result = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.Names}}\t{{.Status}}\t{{.State}}"],
            capture_output=True, text=True
        )
        for line in result.stdout.strip().split("\n"):
            if line:
                parts = line.split("\t")
                if len(parts) >= 3:
                    containers.append({
                        "name": parts[0],
                        "status": parts[1],
                        "state": parts[2],
                        "healthy": "healthy" in parts[1] or parts[2] == "running"
                    })
    except Exception as e:
        logger.error(f"Docker 检查失败: {e}")
    return containers


def check_services() -> list:
    """检查关键服务"""
    services = []

    # 检查知微机器人
    try:
        result = subprocess.run(
            ["launchctl", "list"],
            capture_output=True, text=True
        )
        if "zhiwei-bot" in result.stdout:
            for line in result.stdout.split("\n"):
                if "zhiwei-bot" in line:
                    parts = line.split()
                    pid = parts[0]
                    services.append({
                        "name": "知微机器人",
                        "status": "running" if pid != "-" else "stopped",
                        "pid": pid
                    })
    except Exception:
        pass

    return services


def run_system_check(thresholds: dict = None) -> dict:
    """
    执行全面系统巡检
    返回巡检报告
    """
    if thresholds is None:
        thresholds = {"disk_warn": 80, "disk_critical": 90, "memory_warn": 85}

    report = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "hostname": platform.node(),
        "system": platform.system(),
        "checks": [],
        "alerts": [],
        "overall": "ok"
    }

    # 磁盘检查
    disk = check_disk_usage()
    report["checks"].append(disk)
    if disk.get("status") in ("warning", "critical"):
        report["alerts"].append(f"磁盘使用率 {disk.get('usage_pct', '?')}%")

    # 内存检查
    memory = check_memory_usage()
    report["checks"].append(memory)
    if memory.get("status") in ("warning", "critical"):
        report["alerts"].append(f"内存使用率 {memory.get('usage_pct', '?')}%")

    # Docker 容器
    containers = check_docker_containers()
    for c in containers:
        report["checks"].append({
            "name": f"容器:{c['name']}",
            "status": "ok" if c["healthy"] else "warning",
            "detail": c["status"]
        })
        if not c["healthy"]:
            report["alerts"].append(f"容器 {c['name']} 异常: {c['status']}")

    # 关键服务
    services = check_services()
    for s in services:
        report["checks"].append({
            "name": f"服务:{s['name']}",
            "status": "ok" if s["status"] == "running" else "warning",
            "detail": f"PID={s.get('pid', '?')}"
        })
        if s["status"] != "running":
            report["alerts"].append(f"服务 {s['name']} 未运行")

    # 总体状态
    if any(c.get("status") == "critical" for c in report["checks"]):
        report["overall"] = "critical"
    elif any(c.get("status") == "warning" for c in report["checks"]):
        report["overall"] = "warning"

    return report


def format_system_markdown(report: dict) -> str:
    """格式化系统巡检报告为 Markdown"""
    status_emoji = {"ok": "✅", "warning": "⚠️", "critical": "🚨", "error": "❌"}
    overall = status_emoji.get(report["overall"], "❓")

    lines = [
        f"### 🔧 系统巡检报告 {overall}",
        f"*{report['time']} | {report['hostname']}*\n"
    ]

    for check in report["checks"]:
        emoji = status_emoji.get(check.get("status", "error"), "❓")
        detail = check.get("detail", "")
        usage = f" ({check['usage_pct']}%)" if "usage_pct" in check else ""
        if detail:
            lines.append(f"- {emoji} **{check['name']}**{usage}: {detail}")
        else:
            lines.append(f"- {emoji} **{check['name']}**{usage}")

    if report["alerts"]:
        lines.append("\n**告警项**")
        for alert in report["alerts"]:
            lines.append(f"- 🚨 {alert}")

    return "\n".join(lines)

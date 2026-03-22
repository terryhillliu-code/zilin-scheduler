#!/usr/bin/env python3
"""
Docker 状态缓存器

解决 Docker Daemon API 间歇性超时问题：
- 定期检查 Docker 容器状态（由 Cron 调用）
- 缓存结果到 JSON 文件
- 其他组件读取缓存而非直接调用 Docker API

用法:
    # Cron 调用（每分钟）
    python docker_cache.py --update

    # 读取缓存
    python docker_cache.py --status clawdbot

缓存文件: ~/.cache/docker_status.json
"""

import json
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

# 缓存文件路径
CACHE_DIR = Path.home() / ".cache"
CACHE_FILE = CACHE_DIR / "docker_status.json"
CACHE_TTL = 60  # 缓存有效期（秒）

# 关注的容器列表
CONTAINERS = ["clawdbot"]


def run_docker_command(cmd: list, timeout: int = 5) -> tuple[bool, str]:
    """
    执行 Docker 命令（带超时）

    Returns:
        (success, output)
    """
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        return result.returncode == 0, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT"
    except Exception as e:
        return False, str(e)


def get_container_status(container: str) -> Dict[str, Any]:
    """
    获取单个容器状态

    Returns:
        {
            "name": str,
            "status": str,  # running, exited, timeout, error
            "health": str,  # healthy, unhealthy, N/A
            "uptime": str,  # 运行时长
            "checked_at": str
        }
    """
    now = datetime.now().isoformat()

    # 检查容器是否存在并运行
    success, output = run_docker_command(
        ["docker", "inspect", "-f",
         "{{.State.Status}}|{{.State.Health.Status}}|{{.State.StartedAt}}",
         container]
    )

    if not success:
        return {
            "name": container,
            "status": "timeout" if output == "TIMEOUT" else "error",
            "health": "N/A",
            "uptime": "N/A",
            "error": output,
            "checked_at": now
        }

    try:
        parts = output.split("|")
        status = parts[0] if len(parts) > 0 else "unknown"
        health = parts[1] if len(parts) > 1 else "N/A"
        started_at = parts[2] if len(parts) > 2 else ""

        # 计算运行时长
        uptime = "N/A"
        if status == "running" and started_at:
            try:
                # Docker 时间格式: 2024-01-01T00:00:00.000000000Z
                start_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                uptime_sec = (datetime.now(start_dt.tzinfo) - start_dt).total_seconds()
                hours = int(uptime_sec // 3600)
                minutes = int((uptime_sec % 3600) // 60)
                uptime = f"{hours}h{minutes}m"
            except:
                pass

        return {
            "name": container,
            "status": status,
            "health": health if health else "N/A",
            "uptime": uptime,
            "checked_at": now
        }
    except Exception as e:
        return {
            "name": container,
            "status": "error",
            "health": "N/A",
            "uptime": "N/A",
            "error": str(e),
            "checked_at": now
        }


def update_cache() -> Dict[str, Any]:
    """
    更新所有容器状态缓存

    Returns:
        完整缓存数据
    """
    # 确保缓存目录存在
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    cache_data = {
        "updated_at": datetime.now().isoformat(),
        "containers": {}
    }

    for container in CONTAINERS:
        cache_data["containers"][container] = get_container_status(container)
        print(f"[DockerCache] {container}: {cache_data['containers'][container]['status']}")

    # 写入缓存
    with open(CACHE_FILE, "w") as f:
        json.dump(cache_data, f, indent=2)

    return cache_data


def read_cache() -> Optional[Dict[str, Any]]:
    """
    读取缓存数据

    Returns:
        缓存数据，如果不存在或过期返回 None
    """
    if not CACHE_FILE.exists():
        return None

    try:
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)

        # 检查缓存是否过期
        updated_at = datetime.fromisoformat(data["updated_at"])
        age = (datetime.now() - updated_at).total_seconds()

        if age > CACHE_TTL * 3:  # 允许 3 倍 TTL 的过期时间
            return None

        return data
    except Exception as e:
        print(f"[DockerCache] 读取缓存失败: {e}", file=sys.stderr)
        return None


def get_status(container: str) -> Dict[str, Any]:
    """
    获取容器状态（优先读缓存）

    Returns:
        容器状态字典
    """
    cache = read_cache()

    if cache and container in cache.get("containers", {}):
        return cache["containers"][container]

    # 缓存不存在，直接查询
    return get_container_status(container)


def is_healthy(container: str) -> bool:
    """
    检查容器是否健康

    Returns:
        True 如果容器运行且健康
    """
    status = get_status(container)
    return status.get("status") == "running" and status.get("health") == "healthy"


def main():
    if len(sys.argv) < 2:
        print("用法: docker_cache.py --update | --status <container> | --check <container>")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "--update":
        # 更新缓存
        data = update_cache()
        print(f"[DockerCache] 缓存已更新: {CACHE_FILE}")

    elif cmd == "--status":
        # 查询状态
        container = sys.argv[2] if len(sys.argv) > 2 else CONTAINERS[0]
        status = get_status(container)
        print(json.dumps(status, indent=2))

    elif cmd == "--check":
        # 健康检查
        container = sys.argv[2] if len(sys.argv) > 2 else CONTAINERS[0]
        if is_healthy(container):
            print(f"✅ {container} 健康")
            sys.exit(0)
        else:
            status = get_status(container)
            print(f"❌ {container} 不健康: {status.get('status')}/{status.get('health')}")
            sys.exit(1)

    else:
        print(f"未知命令: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
手动测试 - 直接调用容器内 Skills
用法: python3 test.py <task>
"""

import sys
import subprocess
import json

CONTAINER = "clawdbot"


def run(cmd: str, timeout: int = 60) -> str:
    """容器内执行命令"""
    full_cmd = ["docker", "exec", CONTAINER] + cmd.split()
    result = subprocess.run(full_cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        print(f"❌ 错误: {result.stderr[:300]}")
    return result.stdout.strip()


def test_weather():
    print("🌤 测试天气...\n")
    print(run("python3 /root/workspace/skills/daily-brief/brief.py weather"))


def test_news():
    print("📰 测试新闻...\n")
    print(run("python3 /root/workspace/skills/situation-report/sitrep.py news --topic china --limit 5"))


def test_crypto():
    print("🪙 测试加密货币...\n")
    print(run("python3 /root/workspace/skills/situation-report/sitrep.py crypto"))


def test_market():
    print("📊 测试美股...\n")
    print(run("python3 /root/workspace/skills/situation-report/sitrep.py markets"))


def test_arxiv():
    print("📄 测试 arXiv...\n")
    output = run("python3 /root/workspace/skills/arxiv-tracker/arxiv.py daily --categories 'cs.AI,cs.LG' --min-score 2 --limit 5", timeout=120)
    try:
        data = json.loads(output)
        papers = data.get("papers", [])
        for i, p in enumerate(papers, 1):
            print(f"{i}. {p['title']}")
            print(f"   🔗 {p.get('url', 'N/A')}")
            print(f"   📅 {p.get('published', '')} | ⭐ {p.get('relevance_score', p.get('score', 'N/A'))}")
            print()
    except json.JSONDecodeError:
        print(output)


def test_system():
    print("🔧 测试系统巡检...\n")
    print(run("docker ps --format '{{.Names}}\t{{.Status}}'"))
    print()
    import subprocess as sp
    result = sp.run(["df", "-h", "/"], capture_output=True, text=True)
    print(result.stdout)
    result = sp.run(["launchctl", "list"], capture_output=True, text=True)
    for line in result.stdout.split("\n"):
        if "zhiwei" in line:
            print(f"知微机器人: {line}")


def test_push():
    print("📤 测试钉钉推送...\n")
    output = run('python3 /root/workspace/skills/dingtalk-push/dingtalk.py markdown "🧪 调度器测试" "知微定时任务系统 v2 测试推送\n\n**系统正常运行** ✅"')
    print(output)


def test_all():
    tests = [
        ("天气", test_weather),
        ("新闻", test_news),
        ("加密货币", test_crypto),
        ("美股", test_market),
        ("arXiv", test_arxiv),
        ("系统巡检", test_system),
    ]
    for name, func in tests:
        print(f"\n{'='*50}")
        print(f"测试: {name}")
        print("=" * 50)
        try:
            func()
            print(f"✅ {name} 通过")
        except Exception as e:
            print(f"❌ {name} 失败: {e}")


tasks = {
    "weather": test_weather,
    "news": test_news,
    "crypto": test_crypto,
    "market": test_market,
    "arxiv": test_arxiv,
    "system": test_system,
    "push": test_push,
    "all": test_all,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in tasks:
        print("用法: python3 test.py <task>")
        print(f"可选: {', '.join(tasks.keys())}")
        sys.exit(1)
    tasks[sys.argv[1]]()

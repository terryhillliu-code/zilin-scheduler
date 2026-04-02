#!/usr/bin/env python3
"""
LLM API 健康检查脚本 v1.1

定期检查三层 API 的可用性：
- Coding Plan
- DashScope
- OpenRouter

发现问题自动告警，并记录统计信息。

v1.1 新增:
- 响应时间阈值告警
- 连续失败升级告警
"""
import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# 添加路径
sys.path.insert(0, str(Path.home() / "zhiwei-common"))

# 加载密钥
from zhiwei_common.secrets import load_secrets
load_secrets(silent=True)

from zhiwei_common.llm import llm_client

# ⭐ v1.1: 响应时间阈值（毫秒）
LATENCY_THRESHOLDS = {
    "coding_plan": 10000,   # 10秒（百炼 API 正常延迟 ~8s）
    "dashscope": 3000,      # 3秒
    "openrouter": 20000,    # 20秒（免费模型较慢）
}

# 连续失败阈值
CONSECUTIVE_FAIL_THRESHOLD = 3


def check_all_apis() -> Dict[str, Any]:
    """检查所有 API 可用性"""
    result = {
        "timestamp": datetime.now().isoformat(),
        "status": "healthy",
        "apis": {},
        "stats": llm_client.get_stats(),
        "issues": [],
    }

    # 1. 检查 Coding Plan
    coding_plan_ok, coding_plan_latency = check_coding_plan_with_latency()
    result["apis"]["coding_plan"] = {
        "available": coding_plan_ok,
        "model": "glm-5",
        "latency_ms": coding_plan_latency,
        "slow": coding_plan_latency > LATENCY_THRESHOLDS["coding_plan"] if coding_plan_ok else False,
    }

    # 2. 检查 DashScope
    dashscope_ok, dashscope_latency = check_dashscope()
    result["apis"]["dashscope"] = {
        "available": dashscope_ok,
        "model": "qwen-turbo",
        "latency_ms": dashscope_latency,
        "slow": dashscope_latency > LATENCY_THRESHOLDS["dashscope"] if dashscope_ok else False,
    }

    # 3. 检查 OpenRouter
    openrouter_ok, openrouter_latency = check_openrouter()
    result["apis"]["openrouter"] = {
        "available": openrouter_ok,
        "model": "openrouter/free",
        "latency_ms": openrouter_latency,
        "slow": openrouter_latency > LATENCY_THRESHOLDS["openrouter"] if openrouter_ok else False,
    }

    # ⭐ v1.1: 检查响应时间阈值
    for api_name, api_info in result["apis"].items():
        if api_info.get("slow"):
            result["issues"].append(f"{api_name} 响应过慢: {api_info['latency_ms']}ms > {LATENCY_THRESHOLDS[api_name]}ms")
            if result["status"] == "healthy":
                result["status"] = "warning"

    # ⭐ v1.1: 检查连续失败
    consecutive_fails = llm_client.get_consecutive_fails()
    for api_name, count in consecutive_fails.items():
        if count >= CONSECUTIVE_FAIL_THRESHOLD:
            result["issues"].append(f"⚠️ {api_name} 连续失败 {count} 次，需要关注")
            if result["status"] == "healthy":
                result["status"] = "warning"

    # 判断整体状态
    available_count = sum([
        coding_plan_ok,
        dashscope_ok,
        openrouter_ok
    ])

    if available_count == 0:
        result["status"] = "critical"
        result["issues"].append("所有 LLM API 不可用")
    elif available_count == 1:
        result["status"] = "warning"
        result["issues"].append("只有 1 个 API 可用，建议检查")
    elif not coding_plan_ok:
        result["status"] = "warning"
        result["issues"].append("Coding Plan 不可用")

    return result


def check_coding_plan() -> bool:
    """检查 Coding Plan API"""
    try:
        success, _ = llm_client._call_via_bailian("glm-5", "", "hi", 15)
        return success
    except Exception as e:
        return False


def check_coding_plan_with_latency() -> tuple:
    """检查 Coding Plan API，返回 (是否可用, 延迟ms) ⭐ v1.1"""
    try:
        start = time.time()
        success, _ = llm_client._call_via_bailian("glm-5", "", "hi", 15)
        latency = int((time.time() - start) * 1000)
        return success, latency
    except Exception as e:
        return False, 0


def check_dashscope() -> tuple:
    """检查 DashScope API，返回 (是否可用, 延迟ms)"""
    try:
        start = time.time()
        success, _ = llm_client._call_via_dashscope("qwen-turbo", "", "hi", 15)
        latency = int((time.time() - start) * 1000)
        return success, latency
    except Exception as e:
        return False, 0


def check_openrouter() -> tuple:
    """检查 OpenRouter API，返回 (是否可用, 延迟ms)"""
    try:
        start = time.time()
        success, _ = llm_client._call_via_openrouter("openrouter/free", "", "hi", 15)
        latency = int((time.time() - start) * 1000)
        return success, latency
    except Exception as e:
        return False, 0


def generate_report(result: Dict[str, Any]) -> str:
    """生成 Markdown 报告"""
    lines = [
        "## 🤖 LLM API 健康检查报告",
        f"",
        f"**时间**: {result['timestamp']}",
        f"**状态**: {get_status_emoji(result['status'])} {result['status'].upper()}",
        f"",
        "### API 状态",
        f"",
        "| API | 状态 | 模型 | 延迟 |",
        "|-----|------|------|------|",
    ]

    for api_name, api_info in result["apis"].items():
        status = "✅" if api_info["available"] else "❌"
        latency = f"{api_info['latency_ms']}ms" if api_info.get('latency_ms') else "-"
        lines.append(f"| {api_name} | {status} | {api_info['model']} | {latency} |")

    if result["issues"]:
        lines.extend([
            "",
            "### ⚠️ 发现的问题",
            "",
        ])
        for issue in result["issues"]:
            lines.append(f"- {issue}")

    # 统计信息
    stats = result["stats"]
    lines.extend([
        "",
        "### 📊 调用统计",
        "",
        "| API | 成功 | 失败 |",
        "|-----|------|------|",
    ])
    for api_name, api_stats in stats.items():
        lines.append(f"| {api_name} | {api_stats['success']} | {api_stats['fail']} |")

    return "\n".join(lines)


def get_status_emoji(status: str) -> str:
    """获取状态图标"""
    if status == "healthy":
        return "✅"
    elif status == "warning":
        return "⚠️"
    elif status == "critical":
        return "🔴"
    return "❓"


def save_report(result: Dict[str, Any]):
    """保存报告到文件"""
    report_dir = Path.home() / "zhiwei-docs" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    report_file = report_dir / f"llm_health_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    return report_file


def send_alert(result: Dict[str, Any]):
    """发送告警（如果状态不是 healthy）"""
    if result["status"] == "healthy":
        return

    try:
        # 使用钉钉 webhook 发送告警
        webhook_url = os.environ.get("DINGTALK_WEBHOOK")
        if not webhook_url:
            print("未配置钉钉 Webhook，跳过告警")
            return

        import urllib.request
        import ssl

        title = "🔴 LLM API 告警" if result["status"] == "critical" else "⚠️ LLM API 警告"

        content_lines = [f"- **{api}**: {'✅' if info['available'] else '❌'}"
                        for api, info in result["apis"].items()]

        message = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": f"## {title}\n\n" + "\n".join(content_lines) +
                       f"\n\n**时间**: {result['timestamp']}"
            }
        }

        payload = json.dumps(message).encode('utf-8')
        context = ssl.create_default_context()
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method='POST'
        )

        with urllib.request.urlopen(req, timeout=10, context=context) as resp:
            resp.read()

        print(f"已发送 {result['status']} 告警到钉钉")

    except Exception as e:
        print(f"发送告警失败: {e}")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="LLM API 健康检查")
    parser.add_argument("--json", action="store_true", help="输出 JSON 格式")
    parser.add_argument("--alert", action="store_true", help="发送告警")
    parser.add_argument("--save", action="store_true", help="保存报告到文件")
    args = parser.parse_args()

    # 执行检查
    result = check_all_apis()

    # 输出结果
    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(generate_report(result))

    # 保存报告
    if args.save:
        report_file = save_report(result)
        print(f"\n报告已保存: {report_file}")

    # 发送告警
    if args.alert or result["status"] != "healthy":
        send_alert(result)

    # 返回状态码
    if result["status"] == "critical":
        sys.exit(2)
    elif result["status"] == "warning":
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
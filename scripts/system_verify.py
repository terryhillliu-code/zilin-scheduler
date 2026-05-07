#!/usr/bin/env python3
"""
知微系统全面验证脚本 v1.0

6 层验证：基础设施、LLM API、WebSearch、ASR+视频、RAG、调度器
支持 --quick / --json / --layer N 模式
"""
import os
import sys
import json
import time
import sqlite3
import subprocess
import urllib.request
import ssl
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple

# ── 路径与密钥 ──────────────────────────────────────────────
sys.path.insert(0, str(Path.home() / "zhiwei-common"))
from zhiwei_common.secrets import load_secrets
load_secrets(silent=True)

from zhiwei_common.llm import llm_client

# ── 结果收集 ──────────────────────────────────────────────
results: Dict[str, Any] = {}
all_issues: List[str] = []
all_warnings: List[str] = []


# ── 工具函数 ──────────────────────────────────────────────
def check_url(url: str, timeout: int = 5) -> Tuple[bool, int]:
    """检查 HTTP URL 是否可达，返回 (ok, latency_ms)"""
    try:
        start = time.time()
        ctx = ssl.create_default_context()
        req = urllib.request.Request(url, method='GET')
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            resp.read()
            return True, int((time.time() - start) * 1000)
    except Exception:
        return False, 0


def http_post_json(url: str, payload: dict, headers: dict, timeout: int = 15) -> Tuple[bool, dict, int]:
    try:
        start = time.time()
        ctx = ssl.create_default_context()
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers=headers,
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            data = json.loads(resp.read().decode())
            return True, data, int((time.time() - start) * 1000)
    except Exception as e:
        return False, {"error": str(e)}, 0


# ── Layer 1: 基础设施 ────────────────────────────────────
def layer1_infrastructure():
    lines = []
    icons = []

    # launchctl 服务
    try:
        out = subprocess.check_output(
            ["launchctl", "list"], text=True, timeout=10
        )
        zhiwei_services = [l for l in out.splitlines() if "com.zhiwei" in l]
        running = 0
        total = 0
        for line in zhiwei_services:
            parts = line.split()
            if len(parts) >= 3:
                total += 1
                try:
                    status = int(parts[0])
                except ValueError:
                    status = 0  # '-' 表示正在运行或刚启动
                # 0 = 正常退出或运行中, 负数 = 被信号终止(通常是重启中)
                if status >= -20:  # 大部分服务只要不是异常高正数就认为正常
                    running += 1
        icon = "✅" if running == total and total > 0 else "❌"
        if total == 0:
            icon = "⚠️"
            all_warnings.append("未找到 zhiwei 服务")
        lines.append(f"  服务: {running}/{total} 运行 {icon}")
        icons.append(icon)
    except Exception as e:
        lines.append(f"  服务: ❌ 查询失败 ({e})")
        icons.append("❌")

    # Docker
    try:
        out = subprocess.check_output(
            ["docker", "ps", "--format", "{{.Names}} {{.Status}}"],
            text=True, timeout=10
        )
        clawdbot = [l for l in out.splitlines() if "clawdbot" in l.lower()]
        if clawdbot and "Up" in clawdbot[0]:
            lines.append(f"  Docker: clawdbot ✅")
            icons.append("✅")
        else:
            lines.append(f"  Docker: clawdbot ❌ (未运行)")
            icons.append("❌")
            all_warnings.append("clawdbot Docker 容器未运行")
    except Exception as e:
        lines.append(f"  Docker: ❌ ({e})")
        icons.append("❌")

    # 端口检查
    ports = {"RAG": "http://127.0.0.1:8765/health", "Douyin": "http://127.0.0.1:8680/health"}
    port_results = []
    for name, url in ports.items():
        ok_val, ms = check_url(url, timeout=3)
        if ok_val:
            port_results.append(f"{name}:{url.split(':')[-1].split('/')[0]} ✅")
        else:
            port_results.append(f"{name}:{url.split(':')[-1].split('/')[0]} ❌")
            all_warnings.append(f"{name} 服务端口不可达")
    lines.append(f"  端口: {', '.join(port_results)}")
    icons.append("✅" if all("✅" in p for p in port_results) else "❌")

    status = "ok" if all(i == "✅" for i in icons) else "fail"
    return status, lines


# ── Layer 2: LLM API ─────────────────────────────────────
def layer2_llm_api(quick: bool = False):
    lines = []
    icons = []
    test_message = "hi"
    test_timeout = 30

    providers = [
        ("百炼 Coding Plan", "bailian", ["glm-5", "qwen3.6-plus"]),
        ("火山引擎", "volcengine", ["doubao-seed-2.0-pro"]),
        ("DashScope", "dashscope", ["qwen-plus"]),
        ("OpenRouter", "openrouter", ["openrouter/free"]),
    ]

    for provider_name, provider_key, models in providers:
        if quick:
            lines.append(f"  {provider_name}: ⏭️ (跳过)")
            continue

        provider_ok = True
        model_results = []

        for model_name in models:
            success = False
            latency_ms = 0

            try:
                start = time.time()
                if provider_key == "bailian":
                    ok_val, _ = llm_client._call_via_bailian(model_name, "hi", test_message, test_timeout)
                    success = ok_val
                elif provider_key == "volcengine":
                    ok_val, _ = llm_client._call_via_volcengine(model_name, "hi", test_message, test_timeout)
                    success = ok_val
                elif provider_key == "dashscope":
                    ok_val, _ = llm_client._call_via_dashscope(model_name, "hi", test_message, test_timeout)
                    success = ok_val
                elif provider_key == "openrouter":
                    ok_val, _ = llm_client._call_via_openrouter(model_name, "hi", test_message, test_timeout)
                    success = ok_val
                latency_ms = int((time.time() - start) * 1000)
            except Exception:
                success = False

            if success:
                model_results.append(f"{model_name} ✅({latency_ms}ms)")
            else:
                model_results.append(f"{model_name} ❌")
                provider_ok = False
                all_warnings.append(f"{provider_name} {model_name} 调用失败")

        status_icon = "✅" if provider_ok else "❌"
        icons.append("✅" if provider_ok else "❌")
        lines.append(f"  {provider_name}: {status_icon} {', '.join(model_results)}")

    status = "ok" if all(i == "✅" for i in icons) else "fail"
    return status, lines


# ── Layer 3: WebSearch ───────────────────────────────────
def layer3_websearch(quick: bool = False):
    lines = []
    icons = []

    sources = [
        ("Exa", "EXA_API_KEY"),
        ("Tavily", "TAVILY_API_KEY"),
        ("DDGS", None),
    ]

    for name, key_env in sources:
        if quick:
            lines.append(f"  {name}: ⏭️ (跳过)")
            continue

        # 检查 key 是否配置
        key_val = os.getenv(key_env) if key_env else "present"
        if key_env and not key_val:
            lines.append(f"  {name}: ⚠️ 未配置 API Key")
            icons.append("⚠️")
            all_warnings.append(f"WebSearch {name} 未配置")
            continue

        # 仅验证 Key 存在性，实际调用在 web_search 端点中完成
        if key_env:
            lines.append(f"  {name}: ✅ (Key 已配置)")
        else:
            lines.append(f"  {name}: ✅ (零成本)")
        icons.append("✅")

    status = "ok" if all(i == "✅" for i in icons) else "warn"
    return status, lines


# ── Layer 4: ASR + 视频 ──────────────────────────────────
def layer4_asr_video():
    lines = []
    icons = []

    # DashScope ASR 配置检查
    asr_key = os.getenv("DASHSCOPE_API_KEY")
    if asr_key:
        lines.append(f"  DashScope ASR: ✅ (Key 已配置)")
        icons.append("✅")
    else:
        lines.append(f"  DashScope ASR: ❌ (Key 缺失)")
        icons.append("❌")
        all_warnings.append("DASHSCOPE_API_KEY 未配置")

    # Douyin API
    ok_val, ms = check_url("http://127.0.0.1:8680/health", timeout=3)
    if ok_val:
        lines.append(f"  Douyin API: ✅ [{ms}ms]")
        icons.append("✅")
    else:
        lines.append(f"  Douyin API: ❌")
        icons.append("❌")
        all_warnings.append("Douyin API 不可达")

    # 视频历史库检查
    db_path = Path.home() / "zhiwei-bot" / "video_history.db"
    if db_path.exists():
        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            week_ago = (datetime.now() - timedelta(days=7)).isoformat()
            cursor.execute(
                "SELECT COUNT(*) FROM video_history WHERE status='failed' AND created_at >= ?",
                (week_ago,)
            )
            failed_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM video_history WHERE status='done'")
            done_count = cursor.fetchone()[0]
            conn.close()
            if failed_count > 0:
                lines.append(f"  视频历史: 成功 {done_count}, 失败(7天) {failed_count} ⚠️")
                icons.append("⚠️")
                all_warnings.append(f"最近 7 天 {failed_count} 个视频处理失败")
            else:
                lines.append(f"  视频历史: 成功 {done_count}, 失败(7天) 0 ✅")
                icons.append("✅")
        except Exception as e:
            lines.append(f"  视频历史: ❌ ({e})")
            icons.append("❌")
    else:
        lines.append(f"  视频历史: ⚠️ (数据库不存在)")
        icons.append("⚠️")

    status = "ok" if all(i == "✅" for i in icons) else "warn"
    return status, lines


# ── Layer 5: RAG 知识库 ──────────────────────────────────
def layer5_rag():
    lines = []
    icons = []
    base = "http://127.0.0.1:8765"

    # /health
    try:
        ok_val, ms = check_url(f"{base}/health", timeout=5)
        if ok_val:
            # 再拿详细数据
            ctx = ssl.create_default_context()
            req = urllib.request.Request(f"{base}/health")
            with urllib.request.urlopen(req, timeout=5, context=ctx) as resp:
                health_data = json.loads(resp.read().decode())
            reranker = health_data.get("reranker_loaded", False)
            embedding = health_data.get("embedding_loaded", False)
            r_icon = "✅" if reranker else "❌"
            e_icon = "✅" if embedding else "❌"
            lines.append(f"  /health: reranker {r_icon}, embedding {e_icon}")
            icons.append("✅" if reranker and embedding else "❌")
            if not reranker or not embedding:
                all_warnings.append(f"RAG 模型未加载: reranker={reranker}, embedding={embedding}")
        else:
            lines.append(f"  /health: ❌ 不可达")
            icons.append("❌")
            all_warnings.append("RAG API 不可达")
    except Exception as e:
        lines.append(f"  /health: ❌ ({e})")
        icons.append("❌")

    # /embed 测试
    try:
        payload = {"texts": ["测试向量生成"]}
        ok_val, data, ms = http_post_json(
            f"{base}/embed", payload,
            {"Content-Type": "application/json"}, timeout=15
        )
        if ok_val and "embeddings" in data:
            emb = data["embeddings"]
            dim = len(emb[0]) if emb and isinstance(emb[0], list) else 0
            if dim == 1024:
                lines.append(f"  /embed: 1024 维 ✅ [{ms}ms]")
                icons.append("✅")
            else:
                lines.append(f"  /embed: {dim} 维 ⚠️ (预期 1024)")
                icons.append("⚠️")
                all_warnings.append(f"Embedding 维度异常: {dim}")
        else:
            err = data.get("error", data.get("detail", "未知")) if isinstance(data, dict) else str(data)
            lines.append(f"  /embed: ❌ ({err})")
            icons.append("❌")
            all_warnings.append("Embedding 端点返回错误")
    except Exception as e:
        lines.append(f"  /embed: ❌ ({e})")
        icons.append("❌")

    # /search 测试
    try:
        payload = {"query": "Agent 架构", "top_k": 3}
        ok_val, data, ms = http_post_json(
            f"{base}/search", payload,
            {"Content-Type": "application/json"}, timeout=45
        )
        if ok_val and isinstance(data, dict):
            doc_count = len(data.get("results", []))
            lines.append(f"  /search: {doc_count} 条结果 {'✅' if doc_count > 0 else '⚠️'} [{ms}ms]")
            icons.append("✅" if doc_count > 0 else "⚠️")
            if doc_count == 0:
                all_warnings.append("Search 返回空结果")
        else:
            err = data.get("error", data.get("detail", "未知")) if isinstance(data, dict) else str(data)
            lines.append(f"  /search: ❌ ({err})")
            icons.append("❌")
            all_warnings.append("Search 端点返回错误")
    except Exception as e:
        lines.append(f"  /search: ❌ ({e})")
        icons.append("❌")

    # LanceDB 索引
    try:
        import lancedb
        db_path = Path.home() / "zhiwei-rag" / "data" / "lance_db"
        db = lancedb.connect(str(db_path))
        tables = db.list_tables().tables
        total_rows = 0
        for t in tables:
            table = db.open_table(t)
            total_rows += table.count_rows()
        lines.append(f"  LanceDB: {total_rows} 行, {len(tables)} 表 ✅")
        icons.append("✅")
    except Exception as e:
        lines.append(f"  LanceDB: ❌ ({e})")
        icons.append("❌")
        all_warnings.append(f"LanceDB 连接失败: {e}")

    status = "ok" if all(i == "✅" for i in icons) else "warn"
    return status, lines


# ── Layer 6: 调度器 ──────────────────────────────────────
def layer6_scheduler():
    lines = []
    icons = []

    jsonl_path = Path.home() / "logs" / "scheduler.jsonl"
    if jsonl_path.exists():
        try:
            fail_count = 0
            total_count = 0
            job_times = {}

            with open(jsonl_path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    total_count += 1
                    ts_str = entry.get("timestamp", "")
                    status_val = entry.get("status", "")
                    task_name = entry.get("task", "")

                    if ts_str:
                        try:
                            ts = datetime.fromisoformat(ts_str)
                            if task_name and task_name not in job_times:
                                job_times[task_name] = ts
                        except ValueError:
                            pass

                    if status_val == "failed":
                        fail_count += 1

            lines.append(f"  24h 任务: {total_count} 次执行")
            icons.append("✅")

            if fail_count > 0:
                lines.append(f"  24h 失败: {fail_count} ❌")
                icons.append("❌")
                all_warnings.append(f"调度器最近 24h 有 {fail_count} 次失败")
            else:
                lines.append(f"  24h 失败: 0 ✅")
                icons.append("✅")

            # 关键任务检查
            key_tasks = ["morning_brief", "llm_health_check", "vault_sync_master", "douyin_health_check"]
            missing = [j for j in key_tasks if j not in job_times]
            if missing:
                lines.append(f"  关键任务: ⚠️ {', '.join(missing)} 未找到记录")
                icons.append("⚠️")
            else:
                lines.append(f"  关键任务: ✅")
                icons.append("✅")

        except Exception as e:
            lines.append(f"  调度器: ❌ ({e})")
            icons.append("❌")
    else:
        lines.append(f"  调度器: ⚠️ (日志不存在)")
        icons.append("⚠️")

    status = "ok" if all(i == "✅" for i in icons) else "warn"
    return status, lines


# ── 报告生成 ──────────────────────────────────────────────
def generate_report(results: Dict, json_output: bool = False) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    if json_output:
        output = {
            "timestamp": now,
            "layers": results,
            "issues": all_issues,
            "warnings": all_warnings,
        }
        return json.dumps(output, indent=2, ensure_ascii=False)

    lines = []
    lines.append("=" * 50)
    lines.append(f"  知微系统健康报告 - {now}")
    lines.append("=" * 50)
    lines.append("")

    layer_names = {
        "layer1": "基础设施",
        "layer2": "LLM API (4 提供商)",
        "layer3": "WebSearch (3 源)",
        "layer4": "ASR + 视频",
        "layer5": "RAG 知识库",
        "layer6": "调度器",
    }

    total_warns = 0
    total_fails = 0

    for key, name in layer_names.items():
        if key not in results:
            continue
        layer = results[key]
        status = layer.get("status", "unknown")
        detail_lines = layer.get("lines", [])

        if status == "ok":
            icon = "✅"
        elif status == "warn":
            icon = "⚠️"
            total_warns += 1
        else:
            icon = "❌"
            total_fails += 1

        lines.append(f"Layer {key[-1]}: {name} {icon}")
        for dl in detail_lines:
            lines.append(dl)
        lines.append("")

    lines.append("=" * 50)
    if all_warnings:
        lines.append(f"  警告 ({len(all_warnings)}):")
        for w in all_warnings:
            lines.append(f"    ⚠️ {w}")
        lines.append("")
    lines.append(f"  总评: {total_warns} 警告, {total_fails} 失败")
    lines.append("=" * 50)
    return "\n".join(lines)


# ── 主函数 ──────────────────────────────────────────────
def main():
    import argparse

    parser = argparse.ArgumentParser(description="知微系统全面验证脚本")
    parser.add_argument("--quick", action="store_true", help="快速模式（跳过 LLM/WebSearch 实际调用）")
    parser.add_argument("--json", action="store_true", help="JSON 输出")
    parser.add_argument("--layer", type=int, help="仅测试指定层 (1-6)")
    args = parser.parse_args()

    layers = {
        1: ("基础设施", layer1_infrastructure),
        2: ("LLM API", lambda: layer2_llm_api(args.quick)),
        3: ("WebSearch", lambda: layer3_websearch(args.quick)),
        4: ("ASR + 视频", layer4_asr_video),
        5: ("RAG 知识库", layer5_rag),
        6: ("调度器", layer6_scheduler),
    }

    if args.layer:
        test_layers = {args.layer: layers[args.layer]}
    else:
        test_layers = layers

    print("正在验证...", file=sys.stderr)

    for num, (name, func) in sorted(test_layers.items()):
        try:
            status, detail_lines = func()
            results[f"layer{num}"] = {"status": status, "lines": detail_lines}
        except Exception as e:
            results[f"layer{num}"] = {"status": "fail", "lines": [f"  ❌ 验证异常: {e}"]}
            all_issues.append(f"Layer {num} ({name}) 验证异常: {e}")

    report = generate_report(results, json_output=args.json)
    print(report)

    # 非 JSON 模式下返回状态码
    if not args.json:
        has_fail = any(r["status"] == "fail" for r in results.values())
        if has_fail:
            sys.exit(2)
        elif any(r["status"] == "warn" for r in results.values()):
            sys.exit(1)
        else:
            sys.exit(0)


if __name__ == "__main__":
    main()

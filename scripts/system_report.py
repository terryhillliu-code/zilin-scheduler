#!/usr/bin/env python3
"""
系统任务执行报告生成器
读取最近 24 小时的日志，统计每个任务的执行情况并生成 Markdown 格式报告
"""

import json
import datetime
from collections import defaultdict
from pathlib import Path


def generate_report():
    """
    生成系统任务执行报告（最近 24 小时）
    """
    # 计算 24 小时前的时间点
    now = datetime.datetime.now()
    time_threshold = now - datetime.timedelta(hours=24)

    # 统计数据结构
    stats = defaultdict(lambda: {
        'count': 0,
        'success_count': 0,
        'total_latency': 0.0,
        'errors': []
    })

    # 日志文件路径
    log_file = Path.home() / "logs" / "scheduler.jsonl"

    # 读取日志文件
    if not log_file.exists():
        print(f"日志文件不存在: {log_file}")
        return

    # 解析日志行
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                log_entry = json.loads(line)

                # 解析时间戳
                start_time_str = log_entry.get('start_time')
                if not start_time_str:
                    continue

                # 将时间字符串转换为 datetime 对象
                start_time = datetime.datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))

                # 检查是否在最近 24 小时内
                if start_time < time_threshold:
                    continue

                # 获取任务名称
                task_name = log_entry.get('task_name', 'unknown')

                # 更新统计数据
                stats[task_name]['count'] += 1
                if log_entry.get('success', False):
                    stats[task_name]['success_count'] += 1

                latency = log_entry.get('latency_seconds', 0)
                stats[task_name]['total_latency'] += latency

                # 记录错误信息
                error_msg = log_entry.get('error_msg')
                if error_msg:
                    stats[task_name]['errors'].append(error_msg)

            except json.JSONDecodeError:
                continue
            except ValueError:
                continue

    # 生成 Markdown 格式报告
    print("### 📊 任务执行报告（近 24 小时）")
    print()

    if not stats:
        print("在最近 24 小时内没有找到任何任务执行记录。")
        return

    # 计算总体统计
    total_executions = sum(task['count'] for task in stats.values())
    total_success = sum(task['success_count'] for task in stats.values())
    overall_success_rate = (total_success / total_executions * 100) if total_executions > 0 else 0

    print(f"**统计时间范围**：{time_threshold.strftime('%Y-%m-%d %H:%M:%S')} ~ {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"**总执行次数**：{total_executions} 次")
    print(f"**总体成功率**：{overall_success_rate:.2f}%")
    print()

    # 表格标题
    print("| 任务名 | 执行次数 | 成功次数 | 成功率 | 平均耗时 | 错误数 |")
    print("|--------|----------|----------|--------|----------|--------|")

    # 按执行次数排序输出
    sorted_tasks = sorted(stats.items(), key=lambda x: x[1]['count'], reverse=True)

    for task_name, task_stats in sorted_tasks:
        count = task_stats['count']
        success_count = task_stats['success_count']
        success_rate = (success_count / count * 100) if count > 0 else 0
        avg_latency = task_stats['total_latency'] / count if count > 0 else 0
        error_count = len(task_stats['errors'])

        print(f"| {task_name} | {count} | {success_count} | {success_rate:.2f}% | {avg_latency:.2f}s | {error_count} |")

    print()
    print("#### 🔍 详细错误信息")
    print()

    # 输出错误详情
    has_errors = False
    for task_name, task_stats in sorted_tasks:
        if task_stats['errors']:
            has_errors = True
            print(f"**{task_name}**:")
            for i, error in enumerate(set(task_stats['errors']), 1):  # 使用 set 去重
                if i > 5:  # 只显示前5种不同的错误
                    remaining = len(task_stats['errors']) - 5
                    if remaining > 0:
                        print(f"  ... 还有 {remaining} 个类似错误")
                    break
                print(f"  - {error}")
            print()

    if not has_errors:
        print("✅ 无错误记录")


if __name__ == "__main__":
    generate_report()
#!/bin/bash
# 日志滚动脚本
# 策略：超过 10MB 自动切分，保留最近 5 个归档

LOG_DIR="$HOME/logs"
MAX_SIZE_MB=10
MAX_BACKUPS=5

echo "=== $(date '+%Y-%m-%d %H:%M:%S') 日志滚动检查 ==="

for logfile in "$LOG_DIR"/*.log "$LOG_DIR"/*.jsonl; do
    [ -f "$logfile" ] || continue

    # 获取文件大小 (MB)
    size_mb=$(du -m "$logfile" 2>/dev/null | cut -f1)

    if [ "$size_mb" -ge "$MAX_SIZE_MB" ]; then
        echo "📦 $logfile (${size_mb}MB) 超过阈值，执行滚动..."

        # 滚动备份：删除最旧的
        if [ -f "${logfile}.${MAX_BACKUPS}" ]; then
            rm -f "${logfile}.${MAX_BACKUPS}"
            echo "  🗑 删除旧归档: ${logfile}.${MAX_BACKUPS}"
        fi

        # 依次移动文件
        for i in $(seq $((MAX_BACKUPS - 1)) -1 1); do
            if [ -f "${logfile}.${i}" ]; then
                mv "${logfile}.${i}" "${logfile}.$((i + 1))"
            fi
        done

        # 移动当前文件为 .1
        mv "$logfile" "${logfile}.1"

        # 创建新的空文件
        touch "$logfile"

        echo "  ✅ 滚动完成: $logfile → $logfile.1"
    else
        echo "  ✓ $logfile (${size_mb}MB) 无需滚动"
    fi
done

echo "=== 日志滚动完成 ==="

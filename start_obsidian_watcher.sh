#!/bin/bash
# 启动 Obsidian 文件监听器
# 作为后台服务运行

cd ~/zhiwei-scheduler

# 检查进程是否已在运行
if pgrep -f "obsidian_watcher.py" > /dev/null; then
    echo "❌ Obsidian 监听器已在运行"
    exit 1
fi

echo "🚀 启动 Obsidian 文件监听器..."

# 后台运行监听器
nohup python3 obsidian_watcher.py > /tmp/obsidian_watcher.log 2>&1 &

# 获取进程ID
PID=$!
echo "✅ Obsidian 监听器已启动 (PID: $PID)"

# 等待几秒以确保进程启动
sleep 2

# 检查进程是否仍在运行
if ps -p $PID > /dev/null; then
    echo "✅ 监听器正在后台运行"
    echo "日志文件: /tmp/obsidian_watcher.log"
else
    echo "❌ 监听器启动失败，请检查错误"
    cat /tmp/obsidian_watcher.log
fi
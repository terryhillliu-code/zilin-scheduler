"""
文件队列模块 - 原子落盘 + 并发安全推送
"""

import json
import os
from pathlib import Path
from datetime import datetime, timezone

# 队列目录
QUEUE_BASE = Path(__file__).parent / "outputs" / "artifacts"
PENDING = QUEUE_BASE / "pending"
PROCESSING = QUEUE_BASE / "processing"
SENT = QUEUE_BASE / "sent"
FAILED = QUEUE_BASE / "failed"

# 确保目录存在
for d in [PENDING, PROCESSING, SENT, FAILED]:
    d.mkdir(parents=True, exist_ok=True)


def atomic_write_json(path: Path, data: dict):
    """原子写入 JSON 文件"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def make_job_id(task: str, day: str = None) -> str:
    """生成幂等的 job_id"""
    if day is None:
        day = datetime.now().strftime("%Y-%m-%d")
    return f"{task}:{day}"


def save_result(task: str, content: str, targets: list, 
                metadata: dict = None, day: str = None) -> Path:
    """
    保存生成结果到 pending 队列（原子操作）
    """
    job_id = make_job_id(task, day)
    
    payload = {
        "job_id": job_id,
        "task": task,
        "content": content,
        "push_targets": targets,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "retries": 0,
        "last_error": None,
        "metadata": metadata or {}
    }
    
    # 文件名用 job_id，自动实现幂等
    safe_name = job_id.replace(":", "_")
    final = PENDING / f"{safe_name}.json"
    
    atomic_write_json(final, payload)
    return final


def claim_file(source: Path, dest_dir: Path):
    """
    原子领取文件（并发安全）
    成功返回新路径，失败返回 None
    """
    dest = dest_dir / source.name
    try:
        os.replace(source, dest)
        return dest
    except FileNotFoundError:
        return None


def try_push(file_path: Path, push_manager, logger=None) -> bool:
    """
    尝试推送单个 artifact（并发安全）
    """
    # Step 1: 原子领取到 processing
    processing_path = claim_file(file_path, PROCESSING)
    if processing_path is None:
        return False
    
    try:
        # Step 2: 读取并推送
        data = json.loads(processing_path.read_text())
        content = data["content"]
        
        # 提取标题（取第一行或前20字符）
        title = content.split('\n')[0][:50] if content else data["task"]
        title = title.lstrip('#').strip()
        
        errors = {}
        for target in data["push_targets"]:
            try:
                if target == "dingtalk":
                    result = push_manager.pushers["dingtalk"].send_markdown(title, content)
                    if result.get("errcode") != 0:
                        errors[target] = str(result)
                elif target == "feishu":
                    result = push_manager.pushers["feishu"].send_markdown(title, content)
                    if result.get("code") != 0:
                        errors[target] = str(result)
            except Exception as e:
                errors[target] = str(e)
        
        # Step 3: 根据结果移动文件
        if not errors:
            processing_path.rename(SENT / processing_path.name)
            if logger:
                logger.info(f"✅ 推送成功: {data['job_id']}")
            return True
        else:
            data["retries"] = data.get("retries", 0) + 1
            data["last_error"] = errors
            atomic_write_json(FAILED / processing_path.name, data)
            processing_path.unlink(missing_ok=True)
            if logger:
                logger.warning(f"❌ 推送失败: {data['job_id']}, 错误: {errors}")
            return False
            
    except Exception as e:
        # 异常处理
        try:
            processing_path.rename(FAILED / processing_path.name)
        except:
            pass
        if logger:
            logger.error(f"❌ 推送异常: {e}")
        return False


def retry_failed(push_manager, max_retries: int = 3, logger=None):
    """
    重试 failed 队列中的任务
    """
    retried = 0
    
    for f in sorted(FAILED.glob("*.json")):
        try:
            data = json.loads(f.read_text())
        except:
            continue
        
        if data.get("retries", 0) >= max_retries:
            continue
        
        # 移回 pending
        try:
            f.rename(PENDING / f.name)
            retried += 1
        except:
            continue
    
    # 处理 pending
    success = 0
    for f in sorted(PENDING.glob("*.json")):
        if try_push(f, push_manager, logger):
            success += 1
    
    return {"retried": retried, "success": success}


def get_queue_stats() -> dict:
    """获取队列状态"""
    today = datetime.now().strftime("%Y-%m-%d")
    return {
        "pending": len(list(PENDING.glob("*.json"))),
        "processing": len(list(PROCESSING.glob("*.json"))),
        "sent_today": len([f for f in SENT.glob("*.json") if today in f.name]),
        "sent_total": len(list(SENT.glob("*.json"))),
        "failed": len(list(FAILED.glob("*.json"))),
    }


def cleanup_old_files(days: int = 7):
    """清理旧的 sent 文件"""
    import time
    cutoff = time.time() - (days * 86400)
    cleaned = 0
    for f in SENT.glob("*.json"):
        if f.stat().st_mtime < cutoff:
            f.unlink()
            cleaned += 1
    return cleaned


def check_and_alert(push_manager, alert_threshold: int = 3, logger=None):
    """
    检查 failed 队列，超过阈值时发送飞书告警
    由 retry_failed.py 调用
    """
    failed_files = list(FAILED.glob("*.json"))
    processing_files = list(PROCESSING.glob("*.json"))
    
    alerts = []
    
    # 检查积压
    if len(failed_files) >= alert_threshold:
        alerts.append(f"🚨 **推送失败积压**: {len(failed_files)} 个任务失败待重试")
        for f in failed_files[:5]:  # 最多列出5个
            try:
                data = json.loads(f.read_text())
                retries = data.get("retries", 0)
                error = str(data.get("last_error", ""))[:60]
                alerts.append(f"  - `{data.get('job_id')}` (重试{retries}次): {error}")
            except:
                alerts.append(f"  - {f.name}")
    
    # 检查 processing 卡住（超过30分钟的文件）
    import time
    now = time.time()
    stuck = []
    for f in processing_files:
        if now - f.stat().st_mtime > 1800:  # 30分钟
            stuck.append(f.name)
    
    if stuck:
        alerts.append(f"🚨 **任务卡住**: {len(stuck)} 个任务在 processing 超过30分钟")
        for name in stuck[:3]:
            alerts.append(f"  - `{name}`")
    
    if not alerts:
        return False
    
    # 发送告警
    alert_content = "# 🚨 知微调度器告警\n\n"
    alert_content += "\n".join(alerts)
    alert_content += f"\n\n**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    alert_content += "\n\n> 请检查 `~/logs/retry.log` 或运行 `queue list failed`"
    
    try:
        if "feishu" in push_manager.pushers:
            push_manager.pushers["feishu"].send_markdown("🚨 知微告警", alert_content)
            if logger:
                logger.warning(f"已发送飞书告警: {len(failed_files)} 个失败任务")
        return True
    except Exception as e:
        if logger:
            logger.error(f"发送告警失败: {e}")
        return False


def is_already_sent(job_id: str) -> bool:
    """检查某个 job_id 今天是否已经成功推送过"""
    safe_name = job_id.replace(":", "_")
    sent_file = SENT / f"{safe_name}.json"
    return sent_file.exists()


def save_result_safe(task: str, content: str, targets: list,
                     metadata: dict = None, day: str = None,
                     force: bool = False) -> tuple:
    """
    安全落盘：如果今天已经推送过，跳过（除非 force=True）
    返回 (file_path, skipped)
    """
    job_id = make_job_id(task, day)
    
    if not force and is_already_sent(job_id):
        safe_name = job_id.replace(":", "_")
        existing = SENT / f"{safe_name}.json"
        return existing, True
    
    file_path = save_result(task, content, targets, metadata, day)
    return file_path, False

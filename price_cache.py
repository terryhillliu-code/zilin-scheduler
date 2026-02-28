#!/usr/bin/env python3
"""
价格缓存与幂等性检查
确保只有当价格发生变化时才触发推送

使用示例：
    from price_cache import has_price_changed, update_price_cache

    # 检查价格是否变化
    if has_price_changed("STM32F103C8T6", new_price_data):
        # 价格变了，执行推送
        push_to_feishu(...)
        update_price_cache("STM32F103C8T6", new_price_data)
    else:
        print("价格未变，跳过推送")
"""

import json
import hashlib
import time
import logging
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger("zhiwei-scheduler.cache")

# 缓存目录
CACHE_DIR = Path("/tmp/zhiwei-scheduler-cache")

# 价格数据缓存文件
PRICE_CACHE_FILE = CACHE_DIR / "price_cache.json"

# 缓存 TTL（秒）- 超过这个时间强制刷新
CACHE_TTL = 3600  # 1小时


def ensure_cache_dir():
    """确保缓存目录存在"""
    CACHE_DIR.mkdir(exist_ok=True)


def compute_data_hash(data: Dict[str, Any]) -> str:
    """
    计算数据的 Hash 值
    使用 SHA256，只取前16位
    """
    # 标准化 JSON（key 排序）
    normalized = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:16]


def load_price_cache() -> Dict[str, Any]:
    """加载价格缓存"""
    ensure_cache_dir()

    if not PRICE_CACHE_FILE.exists():
        return {}

    try:
        content = PRICE_CACHE_FILE.read_text(encoding='utf-8')
        return json.loads(content)
    except Exception as e:
        logger.warning(f"⚠️ 加载缓存失败: {e}")
        return {}


def save_price_cache(cache: Dict[str, Any]):
    """保存价格缓存"""
    ensure_cache_dir()

    try:
        content = json.dumps(cache, ensure_ascii=False, indent=2)
        PRICE_CACHE_FILE.write_text(content, encoding='utf-8')
    except Exception as e:
        logger.error(f"❌ 保存缓存失败: {e}")


def has_price_changed(
    part_number: str,
    new_data: Dict[str, Any],
    force_refresh: bool = False
) -> bool:
    """
    检查价格是否发生变化

    参数:
        part_number: 元器件型号
        new_data: 新价格数据（包含 price1, price2 等字段）
        force_refresh: 是否强制刷新（忽略缓存）

    返回:
        True 如果价格变化或缓存不存在，False 如果价格未变
    """
    cache = load_price_cache()

    # 如果缓存不存在，返回 True（需要推送）
    if part_number not in cache:
        logger.info(f"📦 首次价格 [{part_number}]，需要推送")
        return True

    cached_entry = cache[part_number]
    cached_hash = cached_entry.get("hash", "")
    cached_time = cached_entry.get("timestamp", 0)

    # 检查是否过期
    if not force_refresh and (time.time() - cached_time) > CACHE_TTL:
        logger.info(f"⏰ 缓存已过期 [{part_number}]，强制刷新")
        return True

    # 计算新数据的 hash
    new_hash = compute_data_hash(new_data)

    # 比较 hash
    if new_hash != cached_hash:
        logger.info(f"📈 价格变化 [{part_number}]: {cached_hash[:8]} -> {new_hash[:8]}")
        return True

    logger.debug(f"✅ 价格未变 [{part_number}]，跳过推送")
    return False


def update_price_cache(part_number: str, data: Dict[str, Any]):
    """
    更新价格缓存
    在推送成功后调用
    """
    cache = load_price_cache()

    cache[part_number] = {
        "hash": compute_data_hash(data),
        "timestamp": time.time(),
        "data": data  # 保留完整数据用于对比
    }

    save_price_cache(cache)
    logger.debug(f"💾 缓存已更新 [{part_number}]")


def get_cached_price(part_number: str) -> Optional[Dict[str, Any]]:
    """获取缓存的价格数据"""
    cache = load_price_cache()
    entry = cache.get(part_number, {})
    return entry.get("data")


def clear_cache(part_number: str = None):
    """清除缓存"""
    cache = load_price_cache()

    if part_number:
        if part_number in cache:
            del cache[part_number]
            logger.info(f"🗑 缓存已清除 [{part_number}]")
    else:
        cache = {}
        logger.info("🗑 所有缓存已清除")

    save_price_cache(cache)


def get_cache_status() -> Dict[str, Any]:
    """获取缓存状态"""
    cache = load_price_cache()
    now = time.time()

    status = {
        "total_items": len(cache),
        "items": []
    }

    for part_number, entry in cache.items():
        age = now - entry.get("timestamp", 0)
        status["items"].append({
            "part_number": part_number,
            "hash": entry.get("hash", "")[:8],
            "age_seconds": age,
            "age_formatted": f"{age/60:.1f}min" if age < 3600 else f"{age/3600:.1f}h"
        })

    return status


# CLI 接口
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("用法: python3 price_cache.py <命令> [参数]")
        print("命令:")
        print("  status               - 查看缓存状态")
        print("  check <型号> <价格>  - 检查价格是否变化")
        print("  update <型号> <价格> - 更新缓存")
        print("  clear [型号]        - 清除缓存")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "status":
        status = get_cache_status()
        print(f"📊 缓存状态: {status['total_items']} 个型号")
        for item in status['items']:
            print(f"  {item['part_number']}: {item['hash']} ({item['age_formatted']})")

    elif cmd == "check":
        if len(sys.argv) < 4:
            print("用法: check <型号> <价格>")
            sys.exit(1)
        part_number = sys.argv[2]
        price = float(sys.argv[3])
        data = {"price1": price}
        changed = has_price_changed(part_number, data)
        print(f"{'📈 价格已变化' if changed else '✅ 价格未变'}: {part_number}")

    elif cmd == "update":
        if len(sys.argv) < 4:
            print("用法: update <型号> <价格>")
            sys.exit(1)
        part_number = sys.argv[2]
        price = float(sys.argv[3])
        data = {"price1": price}
        update_price_cache(part_number, data)
        print(f"💾 缓存已更新: {part_number} = ¥{price}")

    elif cmd == "clear":
        part_number = sys.argv[2] if len(sys.argv) > 2 else None
        clear_cache(part_number)
        print("🗑 缓存已清除")
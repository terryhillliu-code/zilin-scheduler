#!/usr/bin/env python3
"""
知识管线 - 文件分类器
扫描 unsorted/ 目录，按规则分类到对应目录
"""

import os
import json
import shutil
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

# 机构关键词名单（用于研报二级分类）
INSTITUTIONS = [
    "中金", "中信", "海通", "国泰君安", "华泰", "广发", "招商", "申万", "光大",
    "兴业", "天风", "方正", "国信", "中银", "麦肯锡", "贝恩", "波士顿", "高盛",
    "摩根", "瑞银", "花旗", "东吴", "银河", "国盛", "民生", "德勤", "普华永道", "安永", "毕马威"
]

def extract_institution(filename: str) -> Optional[str]:
    """提取文件名中的机构名称"""
    for inst in INSTITUTIONS:
        if inst in filename:
            return inst
    return None

# 配置
INBOX_DIR = Path.home() / "knowledge-inbox"
UNSORTED_DIR = INBOX_DIR / "unsorted"
RULES_FILE = INBOX_DIR / "classify_rules.json"
STATE_FILE = INBOX_DIR / "processing.json"

# 日志
LOG_DIR = Path.home() / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [knowledge_pipeline] %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "knowledge_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_rules() -> dict:
    """加载分类规则"""
    if not RULES_FILE.exists():
        logger.warning(f"规则文件不存在: {RULES_FILE}")
        return {"rules": [], "default_target": "articles"}
    
    with open(RULES_FILE) as f:
        return json.load(f)


def load_state() -> dict:
    """加载处理状态"""
    if not STATE_FILE.exists():
        return {
            "version": "1.0",
            "last_scan": None,
            "stats": {},
            "pending": []
        }
    
    with open(STATE_FILE) as f:
        return json.load(f)


def save_state(state: dict):
    """保存处理状态"""
    state["last_scan"] = datetime.now().isoformat()
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_file_size_mb(filepath: Path) -> float:
    """获取文件大小（MB）"""
    return filepath.stat().st_size / (1024 * 1024)


def match_rule(filepath: Path, rule: dict) -> bool:
    """检查文件是否匹配规则"""
    conditions = rule.get("conditions", {})
    filename = filepath.name.lower()
    ext = filepath.suffix.lower()
    
    # 检查扩展名
    allowed_exts = conditions.get("extension", [])
    if allowed_exts and ext not in [e.lower() for e in allowed_exts]:
        return False
    
    # 检查文件名关键词
    keywords = conditions.get("filename_contains", [])
    if keywords:
        if not any(kw.lower() in filename for kw in keywords):
            # 特殊规则：PDF 大小判断（电子书）
            if ext == ".pdf" and "or_pdf_size_gt_mb" in conditions:
                if get_file_size_mb(filepath) > conditions["or_pdf_size_gt_mb"]:
                    return True
            return False
    
    # 检查最小文件大小
    min_size = conditions.get("min_size_mb", 0)
    if min_size > 0 and get_file_size_mb(filepath) < min_size:
        return False
    
    return True


def classify_file(filepath: Path, rules_config: dict) -> str:
    """
    根据规则分类文件，返回目标目录名
    """
    rules = rules_config.get("rules", [])
    default_target = rules_config.get("default_target", "articles")
    
    for rule in rules:
        if match_rule(filepath, rule):
            target = rule.get("target", default_target)
            logger.info(f"文件 {filepath.name} 匹配规则 '{rule.get('name')}' → {target}")
            return target
    
    logger.info(f"文件 {filepath.name} 无匹配规则 → {default_target}")
    return default_target


def process_unsorted() -> dict:
    """
    处理 unsorted 目录中的所有文件
    返回处理结果统计
    """
    results = {
        "processed": 0,
        "skipped": 0,
        "errors": 0,
        "by_category": {}
    }
    
    if not UNSORTED_DIR.exists():
        logger.warning(f"unsorted 目录不存在: {UNSORTED_DIR}")
        return results
    
    rules_config = load_rules()
    state = load_state()
    
    # 扫描 unsorted 目录
    files = [f for f in UNSORTED_DIR.iterdir() if f.is_file()]
    
    if not files:
        logger.info("unsorted 目录为空，无需处理")
        save_state(state)
        return results
    
    logger.info(f"发现 {len(files)} 个待分类文件")
    
    for filepath in files:
        try:
            # 跳过隐藏文件和临时文件
            if filepath.name.startswith(".") or filepath.name.endswith(".tmp"):
                results["skipped"] += 1
                continue
            
            # 分类
            target_dir_name = classify_file(filepath, rules_config)
            
            # 二级分类：如果目标是研报，尝试提取投研机构
            if target_dir_name == "reports":
                inst = extract_institution(filepath.name)
                if inst:
                    target_dir_name = f"reports/{inst}"
            
            target_dir = INBOX_DIR / target_dir_name
            
            # 确保目标目录存在
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # 处理重名
            dest_path = target_dir / filepath.name
            if dest_path.exists():
                stem = filepath.stem
                suffix = filepath.suffix
                counter = 1
                while dest_path.exists():
                    dest_path = target_dir / f"{stem}_{counter}{suffix}"
                    counter += 1
            
            # 移动文件
            shutil.move(str(filepath), str(dest_path))
            logger.info(f"移动: {filepath.name} → {target_dir_name}/")
            
            # 更新统计
            results["processed"] += 1
            results["by_category"][target_dir_name] = results["by_category"].get(target_dir_name, 0) + 1
            
            # 更新状态
            state["stats"]["total_processed"] = state["stats"].get("total_processed", 0) + 1
            state["stats"][target_dir_name] = state["stats"].get(target_dir_name, 0) + 1
            
        except Exception as e:
            logger.error(f"处理文件 {filepath.name} 失败: {e}")
            results["errors"] += 1
    
    save_state(state)
    return results


def scan_and_classify():
    """主入口：扫描并分类"""
    logger.info("=" * 50)
    logger.info("开始扫描 knowledge-inbox/unsorted/")
    
    results = process_unsorted()
    
    logger.info(f"处理完成: {results['processed']} 分类, {results['skipped']} 跳过, {results['errors']} 错误")
    if results["by_category"]:
        for cat, count in results["by_category"].items():
            logger.info(f"  → {cat}: {count} 个")
    
    return results


if __name__ == "__main__":
    scan_and_classify()

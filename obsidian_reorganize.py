#!/usr/bin/env python3
"""
OBS-002: Obsidian 目录重组脚本
- 重命名目录（移除【核心】【重要】【参考】前缀）
- 为所有 .md 文件添加/更新 frontmatter
"""

import os
import re
import shutil
from pathlib import Path
from datetime import datetime

VAULT_PATH = Path.home() / "Documents" / "ZhiweiVault"
KB_PATH = VAULT_PATH / "10_Knowledge_Base" / "Reports"

# 重命名映射：(原目录名, 新目录名, 优先级, 目标父目录)
# 目标父目录为 None 表示在原地重命名
RENAMES = [
    # 【核心】目录 -> 30_Knowledge_Base/ 下
    ("【核心】服务器架构", "Server-Architecture", "core", None),
    ("【核心】高性能计算", "HPC", "core", None),
    ("【核心】网络与互联", "Networking", "core", None),
    ("【核心】AI硬件", "AI-Hardware", "core", None),

    # 【重要】目录 -> 30_Knowledge_Base/ 下
    ("【重要】中心设施", "Infra-Core", "important", None),
    ("【重要】云与数据中心", "Cloud-Datacenter", "important", None),
    ("【重要】AI系统", "AI-Systems", "important", None),

    # 【参考】目录 -> 部分移到 20_Notes/
    ("【参考】个人成长", "Personal", "reference", "20_Notes"),
    ("【参考】前沿技术", "Frontier-Tech", "reference", "20_Notes"),
    ("【参考】编程技术", "Tech", "reference", "20_Notes"),
    ("【参考】商业管理", "Business", "reference", "20_Notes"),
    ("【参考】行业报告", "Industry-Reports", "reference", None),

    # 其他目录
    ("行业报告", "Industry-Reports-Merged", "reference", None),  # 稍后合并
    ("工作文档", "Work-Docs", "reference", None),
    ("产品-设计", "Product-Design", "reference", None),
    ("AI-机器学习", "AI-ML", "reference", None),
]

# 分类关键词（用于推断 category）
CATEGORY_KEYWORDS = {
    "Server-Architecture": ["服务器", "架构", "数据中心", "机房", "机架"],
    "HPC": ["高性能", "HPC", "超算", "并行计算", "GPU集群"],
    "Networking": ["网络", "互联", "交换机", "路由", "CXL", "RDMA"],
    "AI-Hardware": ["AI芯片", "GPU", "TPU", "NPU", "推理卡", "训练卡"],
    "Infra-Core": ["中心设施", "供电", "制冷", "UPS", "PUE"],
    "Cloud-Datacenter": ["云计算", "数据中心", "AWS", "Azure", "GCP"],
    "AI-Systems": ["AI系统", "深度学习", "机器学习", "PyTorch", "TensorFlow"],
    "Industry-Reports": ["行业", "市场", "报告", "分析"],
    "Work-Docs": ["工作", "项目", "会议"],
    "Product-Design": ["产品", "设计", "用户体验"],
    "AI-ML": ["AI", "机器学习", "算法", "模型"],
}


def parse_frontmatter(content: str) -> tuple[dict, str]:
    """
    解析 Markdown 文件的 frontmatter

    Returns:
        tuple: (frontmatter_dict, body_content)
    """
    if not content.startswith("---"):
        return {}, content

    # 查找第二个 ---
    match = re.match(r'^---\s*\n(.*?)\n---\s*\n(.*)$', content, re.DOTALL)
    if not match:
        return {}, content

    fm_str, body = match.groups()
    fm = {}

    for line in fm_str.strip().split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            fm[key.strip()] = value.strip().strip('"').strip("'")

    return fm, body


def create_frontmatter(
    title: str,
    priority: str,
    category: str,
    tags: list = None,
    existing_fm: dict = None
) -> str:
    """创建 frontmatter"""
    fm = existing_fm or {}
    fm.setdefault("title", title)
    fm["priority"] = priority
    fm["category"] = category
    fm.setdefault("source", "obsidian")
    fm.setdefault("created", datetime.now().strftime("%Y-%m-%d"))

    if tags:
        fm.setdefault("tags", tags)

    lines = ["---"]
    for key, value in fm.items():
        if isinstance(value, list):
            lines.append(f"{key}:")
            for v in value:
                lines.append(f"  - {v}")
        else:
            lines.append(f'{key}: "{value}"')
    lines.append("---")
    lines.append("")

    return "\n".join(lines)


def update_note_frontmatter(note_path: Path, priority: str, category: str):
    """
    更新笔记的 frontmatter
    """
    try:
        content = note_path.read_text(encoding='utf-8', errors='ignore')
        existing_fm, body = parse_frontmatter(content)

        # 如果已有完整 frontmatter 且无需更新，跳过
        if existing_fm.get("priority") == priority and existing_fm.get("category") == category:
            return False

        # 创建新的 frontmatter
        title = note_path.stem
        new_fm = create_frontmatter(
            title=title,
            priority=priority,
            category=category,
            existing_fm=existing_fm
        )

        # 写入文件
        new_content = new_fm + body
        note_path.write_text(new_content, encoding='utf-8')

        return True

    except Exception as e:
        print(f"  ⚠️ 更新 frontmatter 失败 {note_path.name}: {e}")
        return False


def reorganize_directories():
    """执行目录重组"""
    print("🔄 开始目录重组...")

    # 1. 创建新目录结构
    new_dirs = [
        VAULT_PATH / "00_Inbox",
        VAULT_PATH / "10_Daily",
        VAULT_PATH / "20_Notes",
        VAULT_PATH / "20_Notes" / "Personal",
        VAULT_PATH / "20_Notes" / "Frontier-Tech",
        VAULT_PATH / "20_Notes" / "Tech",
        VAULT_PATH / "20_Notes" / "Business",
        VAULT_PATH / "30_Knowledge_Base",
        VAULT_PATH / "50_Archive",
        VAULT_PATH / "50_Archive" / "Video-Summaries",
    ]

    for d in new_dirs:
        d.mkdir(parents=True, exist_ok=True)
        print(f"  📁 创建目录: {d.relative_to(VAULT_PATH)}")

    # 2. 移动 Video_Summaries 到 Archive
    old_video = VAULT_PATH / "10_Knowledge_Base" / "Video_Summaries"
    new_video = VAULT_PATH / "50_Archive" / "Video-Summaries"
    if old_video.exists():
        if new_video.exists():
            # 合并
            for f in old_video.iterdir():
                shutil.move(str(f), str(new_video / f.name))
            old_video.rmdir()
        else:
            shutil.move(str(old_video), str(new_video))
        print(f"  📦 移动: Video_Summaries → 50_Archive/Video-Summaries")

    # 3. 重命名 Reports 目录下的子目录
    stats = {"renamed": 0, "files_updated": 0, "skipped": 0}

    for old_name, new_name, priority, target_parent in RENAMES:
        old_path = KB_PATH / old_name

        if not old_path.exists():
            print(f"  ⏭️ 跳过不存在的目录: {old_name}")
            stats["skipped"] += 1
            continue

        # 确定目标路径
        if target_parent:
            parent = VAULT_PATH / target_parent
            parent.mkdir(parents=True, exist_ok=True)
        else:
            parent = VAULT_PATH / "30_Knowledge_Base"
            parent.mkdir(parents=True, exist_ok=True)

        new_path = parent / new_name

        # 处理合并情况（行业报告）
        if new_name == "Industry-Reports-Merged":
            target = VAULT_PATH / "30_Knowledge_Base" / "Industry-Reports"
            if target.exists():
                # 合并到已有目录
                for f in old_path.iterdir():
                    shutil.move(str(f), str(target / f.name))
                old_path.rmdir()
                print(f"  🔀 合并: {old_name} → Industry-Reports")
                stats["renamed"] += 1
            else:
                shutil.move(str(old_path), str(target))
                print(f"  📦 重命名: {old_name} → Industry-Reports")
                stats["renamed"] += 1
            continue

        # 重命名/移动目录
        if new_path.exists():
            # 目标已存在，合并内容
            for f in old_path.iterdir():
                shutil.move(str(f), str(new_path / f.name))
            old_path.rmdir()
            print(f"  🔀 合并到已存在目录: {old_name} → {new_name}")
        else:
            shutil.move(str(old_path), str(new_path))
            print(f"  📦 移动: {old_name} → {new_path.relative_to(VAULT_PATH)}")
            stats["renamed"] += 1

        # 更新目录下所有 .md 文件的 frontmatter
        category = new_name
        for md_file in new_path.rglob("*.md"):
            if update_note_frontmatter(md_file, priority, category):
                stats["files_updated"] += 1

    # 4. 清理空目录
    # 移动 AI-Briefs 到 40_AI_Briefs
    old_briefs = VAULT_PATH / "AI-Briefs"
    new_briefs = VAULT_PATH / "40_AI_Briefs"
    if old_briefs.exists() and not new_briefs.exists():
        shutil.move(str(old_briefs), str(new_briefs))
        print(f"  📦 重命名: AI-Briefs → 40_AI_Briefs")

    # 清理旧目录
    old_kb = VAULT_PATH / "10_Knowledge_Base"
    if old_kb.exists():
        # 检查是否还有内容
        remaining = list(old_kb.iterdir())
        if not remaining or all(f.name.startswith('.') for f in remaining):
            shutil.rmtree(old_kb)
            print(f"  🧹 清理空目录: 10_Knowledge_Base")

    # 清理其他空目录
    for empty_dir in ["Articles", "Daily", "Inbox", "References"]:
        p = VAULT_PATH / empty_dir
        if p.exists() and not any(p.iterdir()):
            p.rmdir()
            print(f"  🧹 清理空目录: {empty_dir}")

    print(f"\n✅ 目录重组完成!")
    print(f"  - 重命名目录: {stats['renamed']}")
    print(f"  - 更新文件 frontmatter: {stats['files_updated']}")
    print(f"  - 跳过: {stats['skipped']}")

    return stats


def main():
    print("=" * 60)
    print("OBS-002: Obsidian 目录重组")
    print("=" * 60)

    # 确认备份存在
    backup_path = Path.home() / f"ZhiweiVault.bak.{datetime.now().strftime('%Y%m%d')}"
    if not backup_path.exists():
        print("⚠️ 未找到今日备份，请先运行备份!")
        return

    print(f"✅ 备份已确认: {backup_path}")
    print(f"📂 Vault 路径: {VAULT_PATH}")
    print()

    stats = reorganize_directories()

    # 统计最终文件数
    final_count = len(list(VAULT_PATH.rglob("*.md")))
    print(f"\n📊 最终统计:")
    print(f"  - Markdown 文件总数: {final_count}")


if __name__ == "__main__":
    main()
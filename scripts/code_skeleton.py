#!/usr/bin/env python3
"""
代码骨架生成器（极简版）
用 AST 解析生成项目结构摘要，只保留最重要的公共接口
输出极紧凑格式（< 5K tokens）
"""

import ast
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional


def get_simple_signature(node) -> str:
    """
    从 AST 节点提取简化函数签名（只保留参数名）

    Args:
        node: AST 函数定义节点

    Returns:
        简化的函数签名字符串
    """
    args = []

    # 处理普通参数，只保留参数名
    for arg in node.args.args:
        if arg.arg != 'self':  # 过滤 self 参数
            args.append(arg.arg)

    # 处理 *args
    if node.args.vararg:
        args.append(f"*{node.args.vararg.arg}")

    # 处理关键字参数
    for arg in node.args.kwonlyargs:
        args.append(arg.arg)

    # 处理 **kwargs
    if node.args.kwarg:
        args.append(f"**{node.args.kwarg.arg}")

    # 格式化参数
    args_str = ', '.join(args)

    return f"({args_str})"


def get_docstring_first_line(node) -> str:
    """
    提取 AST 节点的文档字符串第一行

    Args:
        node: AST 节点

    Returns:
        文档字符串第一行
    """
    # 为了减小文件大小，直接返回空字符串，不包含任何 docstring
    return ""


def is_skip_dunder_method(name: str) -> bool:
    """
    判断是否应该跳过的 dunder 方法（保留 __init__）

    Args:
        name: 方法名

    Returns:
        是否应该跳过
    """
    if name == '__init__':
        return False  # 保留 __init__
    # 跳过其他 dunder 方法
    return name.startswith('__') and name.endswith('__')


def should_include_method(name: str) -> bool:
    """
    判断是否应该包含该方法（只包含公共方法）

    Args:
        name: 方法名

    Returns:
        是否应该包含
    """
    if is_skip_dunder_method(name):
        return False  # 跳过不需要的 dunder 方法
    # 只包含公共方法（不以 _ 开头）
    return not name.startswith('_')


def should_include_function(name: str) -> bool:
    """
    判断是否应该包含该函数（只包含公共函数）

    Args:
        name: 函数名

    Returns:
        是否应该包含
    """
    if is_skip_dunder_method(name):
        return False  # 跳过不需要的 dunder 方法
    # 只包含公共函数（不以 _ 开头）
    return not name.startswith('_')


def parse_file(filepath: str) -> Optional[Dict]:
    """
    用 ast 模块解析单个文件，提取类/函数签名

    Args:
        filepath: Python 文件路径

    Returns:
        包含文件信息的字典
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        tree = ast.parse(content)

        # 统计行数
        line_count = len(content.splitlines())

        classes = []
        functions = []

        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                # 提取类信息
                bases = [ast.unparse(base) for base in node.bases]
                class_info = {
                    'name': node.name,
                    'bases': bases,
                    'methods': []
                }

                # 提取方法
                for item in node.body:
                    if isinstance(item, ast.FunctionDef) or isinstance(item, ast.AsyncFunctionDef):
                        method_name = item.name

                        # 只包含公共方法
                        if should_include_method(method_name):
                            signature = get_simple_signature(item)
                            docstring = get_docstring_first_line(item)

                            method_info = {
                                'name': method_name,
                                'signature': signature,
                                'docstring': docstring
                            }
                            class_info['methods'].append(method_info)

                # 只添加有方法的类
                if class_info['methods']:
                    classes.append(class_info)

            elif isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
                # 提取顶层函数
                func_name = node.name

                # 只包含公共函数
                if should_include_function(func_name):
                    signature = get_simple_signature(node)
                    docstring = get_docstring_first_line(node)

                    func_info = {
                        'name': func_name,
                        'signature': signature,
                        'docstring': docstring
                    }
                    functions.append(func_info)

        return {
            'filepath': filepath,
            'relative_path': os.path.relpath(filepath, Path.home()),
            'classes': classes,
            'functions': functions,
            'line_count': line_count
        }

    except Exception as e:
        print(f"❌ 解析文件失败 {filepath}: {e}", file=sys.stderr)
        return None


def scan_directories(dirs: List[str]) -> List[Dict]:
    """
    递归扫描目录，过滤无关文件

    Args:
        dirs: 目录列表

    Returns:
        解析后的文件信息列表
    """
    all_files = []

    for dir_path in dirs:
        path_obj = Path(dir_path)
        if not path_obj.exists():
            print(f"⚠️ 目录不存在: {dir_path}", file=sys.stderr)
            continue

        # 递归遍历目录中的所有 .py 文件
        for py_file in path_obj.rglob("*.py"):
            # 跳过特定文件和目录
            skip_conditions = [
                '__pycache__' in str(py_file),
                '.pyc' in str(py_file.suffix),
                '/venv/' in str(py_file.as_posix()),
                '/node_modules/' in str(py_file.as_posix()),
                '/.git/' in str(py_file.as_posix()),
                'test_' in py_file.name,
                '_test' in py_file.name,
                py_file.name.startswith('.'),
                py_file.name == '__init__.py'  # 过滤 __init__.py 文件
            ]

            if any(skip_conditions):
                continue

            parsed_file = parse_file(str(py_file))
            if parsed_file:
                all_files.append(parsed_file)

    return all_files


def generate_skeleton(files_info: List[Dict]) -> str:
    """
    生成极简骨架

    Args:
        files_info: 解析后的文件信息列表

    Returns:
        完整的骨架字符串
    """
    output_lines = []

    # 添加头部信息（更简洁）
    total_files = len(files_info)
    total_lines = sum(f['line_count'] for f in files_info)
    from datetime import datetime
    date_str = datetime.now().strftime("%Y-%m-%d")

    output_lines.append(f"# 知微 {date_str}")
    output_lines.append(f"> {total_files}F {total_lines}L")
    output_lines.append("")  # 空行

    # 按文件路径排序
    sorted_files = sorted(files_info, key=lambda x: x['relative_path'])

    for file_info in sorted_files:
        rel_path = file_info['relative_path']
        line_count = file_info['line_count']

        output_lines.append(f"## {rel_path} ({line_count}L)")

        # 输出类信息
        for class_info in file_info['classes']:
            bases_str = f"({', '.join(class_info['bases'])})" if class_info['bases'] else ""
            output_lines.append(f"- class {class_info['name']} {bases_str}")

            # 输出公共方法
            for method in class_info['methods']:
                docstring = f" #{method['docstring']}" if method['docstring'] else ""
                output_lines.append(f"  - {method['name']}{method['signature']}{docstring}")

        # 输出顶层公共函数
        for func in file_info['functions']:
            docstring = f" #{func['docstring']}" if func['docstring'] else ""
            output_lines.append(f"- {func['name']}{func['signature']}{docstring}")

    return "\n".join(output_lines)


def main():
    """
    CLI 入口，支持 python3 code_skeleton.py > skeleton.md
    """
    # 定义要扫描的目录
    scan_dirs = [
        os.path.expanduser("~/zhiwei-bot"),
        os.path.expanduser("~/zhiwei-scheduler"),
        os.path.expanduser("~/Documents/Library")
    ]

    print("🔍 开始解析代码库...", file=sys.stderr)

    # 解析所有目录
    files_info = scan_directories(scan_dirs)

    print(f"✅ 解析完成，共 {len(files_info)} 个文件", file=sys.stderr)

    # 生成骨架
    skeleton = generate_skeleton(files_info)

    # 输出结果
    print(skeleton)


if __name__ == "__main__":
    main()
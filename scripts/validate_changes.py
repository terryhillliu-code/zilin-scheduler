#!/usr/bin/env python3
"""
知微系统变更验证工具
三层验证：语法→导入→测试
"""

import ast
import importlib.util
import json
import subprocess
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional


def validate_syntax(filepath: str) -> Dict[str, Any]:
    """
    L1: 检查Python语法

    Args:
        filepath: 文件路径

    Returns:
        {"passed": bool, "error": str or None}
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # 使用ast.parse检查语法
        ast.parse(content, filename=filepath)
        return {"passed": True, "error": None}
    except SyntaxError as e:
        error_msg = f"SyntaxError at line {e.lineno}: {e.msg}"
        if e.text:
            error_msg += f"\n{e.text.strip()}"
            if e.offset:
                error_msg += f"\n{' ' * (e.offset - 1)}^"
        return {"passed": False, "error": error_msg}
    except Exception as e:
        return {"passed": False, "error": str(e)}


def validate_import(filepath: str) -> Dict[str, Any]:
    """
    L2: 尝试导入模块

    Args:
        filepath: 文件路径

    Returns:
        {"passed": bool, "error": str or None}
    """
    try:
        # 获取模块名（去掉.py后缀）
        module_name = Path(filepath).stem
        spec = importlib.util.spec_from_file_location(module_name, filepath)
        if spec is None or spec.loader is None:
            return {"passed": False, "error": f"Could not create module spec for {filepath}"}

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return {"passed": True, "error": None}
    except ImportError as e:
        return {"passed": False, "error": f"ImportError: {str(e)}"}
    except Exception as e:
        return {"passed": False, "error": f"Runtime Error during import: {str(e)}"}


def validate_test(test_cmd: str = None) -> Dict[str, Any]:
    """
    L3: 运行测试命令

    Args:
        test_cmd: 测试命令，默认运行 ~/scripts/pre_check_v2.sh

    Returns:
        {"passed": bool, "output": str}
    """
    if test_cmd is None:
        test_cmd = str(Path.home() / "scripts" / "pre_check_v2.sh")

    try:
        result = subprocess.run(
            test_cmd.split(),
            capture_output=True,
            text=True,
            timeout=300  # 5分钟超时
        )

        output = result.stdout
        if result.stderr:
            output += f"\nSTDERR:\n{result.stderr}"

        return {
            "passed": result.returncode == 0,
            "output": output
        }
    except subprocess.TimeoutExpired:
        return {
            "passed": False,
            "output": "Test command timed out after 300 seconds"
        }
    except FileNotFoundError:
        return {
            "passed": False,
            "output": f"Test command not found: {test_cmd}"
        }
    except Exception as e:
        return {
            "passed": False,
            "output": f"Error running test: {str(e)}"
        }


def validate_files(filepaths: List[str], run_test: bool = True) -> Dict[str, Any]:
    """
    验证多个文件

    Args:
        filepaths: 文件路径列表
        run_test: 是否运行测试

    Returns:
        {
          "all_passed": bool,
          "results": [
            {"file": "xxx.py", "syntax": {...}, "import": {...}}
          ],
          "test": {...}
        }
    """
    results = []

    for filepath in filepaths:
        path_obj = Path(filepath)
        if not path_obj.exists():
            results.append({
                "file": filepath,
                "syntax": {"passed": False, "error": f"File does not exist"},
                "import": {"passed": False, "error": "File does not exist"}
            })
            continue

        if not filepath.endswith('.py'):
            # 非Python文件跳过语法和导入检查
            results.append({
                "file": filepath,
                "syntax": {"passed": True, "error": "Skipped (non-Python file)"},
                "import": {"passed": True, "error": "Skipped (non-Python file)"}
            })
            continue

        # 运行语法检查
        syntax_result = validate_syntax(filepath)

        # 如果语法通过才进行导入检查
        import_result = {"passed": False, "error": "Skipped due to syntax error"}
        if syntax_result["passed"]:
            import_result = validate_import(filepath)

        results.append({
            "file": filepath,
            "syntax": syntax_result,
            "import": import_result
        })

    # 检查是否有任何验证失败
    all_passed = all(
        item["syntax"]["passed"] and item["import"]["passed"]
        for item in results
        if item["file"].endswith('.py')  # 只检查Python文件
    )

    # 运行测试
    test_result = {"passed": True, "output": "Skipped"}
    if run_test and all_passed:
        test_result = validate_test()
        if not test_result["passed"]:
            all_passed = False

    return {
        "all_passed": all_passed,
        "results": results,
        "test": test_result
    }


def main():
    parser = argparse.ArgumentParser(description="知微系统变更验证工具")
    parser.add_argument("files", nargs="+", help="要验证的文件路径")
    parser.add_argument("--no-test", action="store_true", help="跳过运行测试")
    parser.add_argument("--json", action="store_true", help="输出JSON格式")

    args = parser.parse_args()

    result = validate_files(args.files, run_test=not args.no_test)

    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        # 输出人类可读格式
        print("验证结果:")
        print(f"总览: {'通过' if result['all_passed'] else '失败'}")
        print()

        for item in result['results']:
            print(f"文件: {item['file']}")
            print(f"  语法检查: {'通过' if item['syntax']['passed'] else '失败'}")
            if item['syntax']['error']:
                print(f"    错误: {item['syntax']['error']}")

            print(f"  导入检查: {'通过' if item['import']['passed'] else '失败'}")
            if item['import']['error']:
                print(f"    错误: {item['import']['error']}")
            print()

        print("系统测试:")
        print(f"  测试运行: {'通过' if result['test']['passed'] else '失败'}")
        if not args.no_test:
            print(f"  输出摘要: {result['test']['output'][:200]}..." if len(result['test']['output']) > 200 else result['test']['output'])


if __name__ == "__main__":
    main()
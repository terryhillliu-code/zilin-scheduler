"""
风险检查器
在执行前检查操作风险
"""

import re
from pathlib import Path


class RiskChecker:
    """风险检查器"""

    CRITICAL_FILES = {
        "ws_client.py", "task_executor.py", "pre_check_v2.sh",
        "docker-compose.yml", "openclaw.json", "CLAUDE.md",
        "backup.sh", "scheduler.py"
    }

    DANGEROUS_PATTERNS = [
        r"rm\s+-rf\s+[~/]",
        r"rm\s+-rf\s+\*",
        r">\s*/etc/",
        r"chmod\s+777",
        r"curl.*\|\s*sh",
        r"wget.*\|\s*sh",
    ]

    def check(self, operation: str) -> dict:
        """
        检查操作风险

        Returns:
            {
                "level": "low" | "medium" | "high" | "critical",
                "reasons": [str],
                "allow": bool
            }
        """
        reasons = []
        level = "low"

        # 检查危险命令
        for pattern in self.DANGEROUS_PATTERNS:
            if re.search(pattern, operation, re.IGNORECASE):
                reasons.append(f"危险命令模式: {pattern}")
                level = "critical"

        # 检查关键文件
        for fname in self.CRITICAL_FILES:
            if fname in operation:
                if any(word in operation.lower() for word in ["delete", "rm ", "remove"]):
                    reasons.append(f"删除关键文件: {fname}")
                    level = "critical"
                elif any(word in operation.lower() for word in ["修改", "重写", "overwrite"]):
                    reasons.append(f"修改关键文件: {fname}")
                    if level not in ["critical"]:
                        level = "high"

        # 检查大范围操作
        if re.search(r"所有|全部|批量|all files", operation, re.IGNORECASE):
            reasons.append("大范围操作")
            if level == "low":
                level = "medium"

        return {
            "level": level,
            "reasons": reasons,
            "allow": level in ["low", "medium"]  # low/medium 自动允许
        }


if __name__ == "__main__":
    checker = RiskChecker()

    # 测试
    tests = [
        "修改 README.md",
        "重构 ws_client.py",
        "rm -rf ~/",
        "删除所有日志文件",
    ]

    for t in tests:
        result = checker.check(t)
        print(f"{t[:30]:30} => {result['level']:8} allow={result['allow']}")

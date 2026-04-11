#!/usr/bin/env python3
"""
GitHub 活跃度分析器
- 查询 GitHub 用户公开数据
- 计算活跃度评分
- 生成报告

使用方式:
    python github_analyzer.py <github_username>
    python github_analyzer.py --file data/twitter_accounts.txt
"""

import json
import urllib.request
import urllib.error
import ssl
import time
from dataclasses import dataclass
from typing import Optional, List
from pathlib import Path
from datetime import datetime


@dataclass
class GitHubProfile:
    """GitHub 用户画像"""
    username: str
    exists: bool
    public_repos: int = 0
    followers: int = 0
    following: int = 0
    created_at: str = ""
    bio: str = ""
    company: str = ""
    blog: str = ""
    location: str = ""
    recent_repos: Optional[List[dict]] = None
    activity_score: float = 0.0
    activity_level: str = "Unknown"


class GitHubAnalyzer:
    """GitHub 活跃度分析器"""

    # 活跃度等级阈值
    LEVELS = {
        (80, 100): "🔥 高度活跃 Builder",
        (60, 80): "✅ 活跃 Builder",
        (40, 60): "📊 中等活跃",
        (20, 40): "💤 低活跃",
        (0, 20): "❓ 不活跃/非开发者",
    }

    def __init__(self, token: Optional[str] = None):
        """
        初始化分析器

        Args:
            token: GitHub Personal Access Token（可选，有 token 可提高限额）
        """
        self.token = token
        self.base_url = "https://api.github.com"
        self.ssl_context = ssl.create_default_context()

    def _request(self, endpoint: str) -> Optional[dict]:
        """发送 API 请求"""
        url = f"{self.base_url}/{endpoint}"
        headers = {
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "Zhiwei-GitHub-Analyzer/1.0"
        }
        if self.token:
            headers["Authorization"] = f"token {self.token}"

        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30, context=self.ssl_context) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code == 403:
                print(f"⚠️ API 限额已用尽，请稍后再试或配置 GitHub Token")
            return None
        except Exception as e:
            print(f"⚠️ 请求失败: {e}")
            return None

    def get_user(self, username: str) -> Optional[dict]:
        """获取用户基本信息"""
        return self._request(f"users/{username}")

    def get_repos(self, username: str, limit: int = 10) -> List[dict]:
        """获取用户仓库列表（按更新时间排序）"""
        data = self._request(f"users/{username}/repos?sort=updated&per_page={limit}")
        return data if isinstance(data, list) else []

    def analyze(self, username: str) -> GitHubProfile:
        """
        分析 GitHub 用户

        Args:
            username: GitHub 用户名

        Returns:
            GitHubProfile 对象
        """
        user_data = self.get_user(username)

        if not user_data:
            return GitHubProfile(username=username, exists=False)

        # 获取最近仓库
        repos = self.get_repos(username, limit=10)

        # 计算活跃度评分
        score = self._calculate_score(user_data, repos)

        # 确定活跃等级
        level = self._get_level(score)

        return GitHubProfile(
            username=username,
            exists=True,
            public_repos=user_data.get("public_repos", 0),
            followers=user_data.get("followers", 0),
            following=user_data.get("following", 0),
            created_at=user_data.get("created_at", ""),
            bio=user_data.get("bio", "") or "",
            company=user_data.get("company", "") or "",
            blog=user_data.get("blog", "") or "",
            location=user_data.get("location", "") or "",
            recent_repos=repos,
            activity_score=score,
            activity_level=level
        )

    def _calculate_score(self, user: dict, repos: List[dict]) -> float:
        """
        计算活跃度评分 (0-100)

        评分维度:
        - 公开仓库数量 (30%): 最多 30 分，每个仓库 1 分
        - 关注者数量 (20%): 最多 20 分，每 100 关注者 1 分
        - 最近仓库活动 (30%): 最近更新的仓库情况
        - Bio/Company 完整度 (10%): 是否填写资料
        - 仓库 Stars (10%): 仓库受欢迎程度
        """
        score = 0.0

        # 1. 公开仓库 (30%)
        public_repos = user.get("public_repos", 0)
        score += min(30, public_repos)

        # 2. 关注者 (20%)
        followers = user.get("followers", 0)
        score += min(20, followers / 100 * 20)

        # 3. 最近仓库活动 (30%)
        if repos:
            recent_updates = 0
            now = datetime.now()
            for repo in repos[:5]:  # 看最近 5 个仓库
                if repo.get("pushed_at"):
                    try:
                        pushed = datetime.strptime(
                            repo["pushed_at"], "%Y-%m-%dT%H:%M:%SZ"
                        )
                        days_ago = (now - pushed).days
                        if days_ago < 30:
                            recent_updates += 6
                        elif days_ago < 90:
                            recent_updates += 4
                        elif days_ago < 180:
                            recent_updates += 2
                    except Exception:
                        pass
            score += min(30, recent_updates)

        # 4. 资料完整度 (10%)
        if user.get("bio"):
            score += 5
        if user.get("company") or user.get("blog"):
            score += 5

        # 5. 仓库 Stars (10%)
        if repos:
            total_stars = sum(r.get("stargazers_count", 0) for r in repos[:5])
            score += min(10, total_stars / 100 * 10)

        return round(score, 1)

    def _get_level(self, score: float) -> str:
        """根据评分确定活跃等级"""
        for (low, high), level in self.LEVELS.items():
            if low <= score < high:
                return level
        return "❓ 未知"

    def format_report(self, profile: GitHubProfile) -> str:
        """格式化报告"""
        if not profile.exists:
            return f"❌ GitHub 用户 `{profile.username}` 不存在"

        lines = [
            f"## GitHub 分析: {profile.username}",
            "",
            f"**活跃度**: {profile.activity_level} (评分: {profile.activity_score})",
            f"**仓库**: {profile.public_repos} | **关注者**: {profile.followers}",
        ]

        if profile.bio:
            lines.append(f"**简介**: {profile.bio}")
        if profile.company:
            lines.append(f"**公司**: {profile.company}")

        if profile.recent_repos:
            lines.append("")
            lines.append("**最近仓库**:")
            for repo in profile.recent_repos[:5]:
                name = repo.get("name", "unknown")
                stars = repo.get("stargazers_count", 0)
                lang = repo.get("language", "") or "Unknown"
                desc = (repo.get("description") or "")[:50]
                lines.append(f"- `{name}` ⭐{stars} ({lang}) - {desc}")

        return "\n".join(lines)


def analyze_accounts_from_file(filepath: str, output_path: Optional[str] = None) -> str:
    """
    从文件批量分析账号

    文件格式: twitter_handle | 领域 | 备注
    假设 GitHub 用户名与 Twitter 用户名相同或可推断
    """
    analyzer = GitHubAnalyzer()
    results = []

    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split("|")
        twitter_handle = parts[0].strip()

        # 跳过已有状态标记的行
        if "[已加入列表]" in line or "[已跳过]" in line:
            continue

        print(f"分析: {twitter_handle}...")

        # 假设 GitHub 用户名相同
        profile = analyzer.analyze(twitter_handle)
        results.append((twitter_handle, profile))

        # 避免 API 限流
        time.sleep(1)

    # 生成报告
    report_lines = [
        "# GitHub 活跃度分析报告",
        f"\n> 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"> 分析账号: {len(results)} 个\n",
        "",
        "| Twitter | GitHub 评分 | 活跃等级 | 仓库数 | 关注者 |",
        "|---------|------------|----------|--------|--------|",
    ]

    for handle, profile in results:
        if profile.exists:
            report_lines.append(
                f"| @{handle} | {profile.activity_score} | {profile.activity_level[:6]} | "
                f"{profile.public_repos} | {profile.followers} |"
            )
        else:
            report_lines.append(f"| @{handle} | N/A | 未找到 | - | - |")

    report = "\n".join(report_lines)

    # 保存报告
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"\n✅ 报告已保存: {output_path}")

    return report


if __name__ == "__main__":
    import sys

    analyzer = GitHubAnalyzer()

    if len(sys.argv) < 2:
        print("用法:")
        print("  python github_analyzer.py <github_username>")
        print("  python github_analyzer.py --file data/twitter_accounts.txt")
        sys.exit(1)

    if sys.argv[1] == "--file" and len(sys.argv) >= 3:
        # 批量分析
        report = analyze_accounts_from_file(
            sys.argv[2],
            output_path="outputs/github_report.md"
        )
        print(report)
    else:
        # 单个分析
        username = sys.argv[1]
        print(f"分析 GitHub 用户: {username}\n")
        profile = analyzer.analyze(username)
        print(analyzer.format_report(profile))
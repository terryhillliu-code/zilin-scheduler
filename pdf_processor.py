#!/usr/bin/env python3
"""
PDF 研报处理模块 (Phase 4a)
1. 封装 marker-pdf 能力将 PDF 转为 Markdown
2. 结合 market-research 准则提取深度分析结论
"""

import os
import json
import subprocess
import logging
from pathlib import Path
from tempfile import TemporaryDirectory

# 尝试引入原有大模型调用能力
try:
    import sys
    sys.path.append(str(Path(__file__).parent))
    from scheduler import call_llm_direct
except ImportError:
    call_llm_direct = None

logger = logging.getLogger(__name__)

MARKET_RESEARCH_PROMPT = """
你是一个顶尖的投研分析师。
请基于这篇PDF研报的内容，依据严格的投研逻辑提取以下结构化的关键洞察：
1. 核心观点 (执行摘要)
2. 市场/行业核心数据指标 (Market Sizing/Metrics，若无请留空)
3. 关键结论推演事实 (区分推论与事实，仅提取1-3条最重要的事实)
4. 风险与相反观点 (Downside cases/Risks，若无请留空)

研报内容（前置截取）：
{content}

请以严格的 JSON 格式返回，务必直接输出合法的 JSON 字符串（无需任何 markdown 包裹，如```json）：
{{
  "summary": "执行摘要字符串",
  "key_metrics": ["数据指标1", "数据指标2"],
  "key_insights": ["事实1", "事实2"],
  "risks": ["风险1", "风险2"]
}}
"""

def parse_pdf_to_md(pdf_path: Path, output_dir: Path) -> str:
    """使用 marker 将 PDF 转换为 Markdown"""
    # 使用宿主机 venv 中的 marker-pdf (针对 1.0+ 版本)
    marker_bin = str(Path.home() / "zhiwei-scheduler" / "venv_marker" / "bin" / "marker_single")
    cmd = [marker_bin, str(pdf_path), "--output_dir", str(output_dir)]
    logger.info(f"运行 marker 转换: {' '.join(cmd)}")
    
    # 此处可能因为首次运行需要下载模型文件而很慢
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Marker 执行失败: {result.stderr}")
    
    stem = pdf_path.stem
    # marker_single 默认会在 output_dir 下创建一个同名带后缀的子目录，并把结果放入其中
    # 也有可能直接生成在 output_dir 下
    md_file = output_dir / stem / f"{stem}.md"
    
    if not md_file.exists():
        md_file = output_dir / f"{stem}.md"
        if not md_file.exists():
            # 搜索 output_dir 下唯一的一个 .md
            md_files = list(output_dir.rglob("*.md"))
            if md_files:
                md_file = md_files[0]
            else:
                raise FileNotFoundError(f"未能找到生成的 Markdown: {output_dir}\nMarker 输出: {result.stdout}")
             
    with open(md_file, "r", encoding="utf-8") as f:
        return f.read()

def process_research_report(pdf_path: Path) -> dict:
    """处理单篇 PDF，返回提取的混合知识结构体"""
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF 文件不存在: {pdf_path}")
        
    logger.info(f"开始处理研报: {pdf_path.name}")
    
    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        # 1. 解析 Markdown
        md_content = parse_pdf_to_md(pdf_path, tmp_path)
        
        # 2. 大模型提炼深度分析结果 (截取前 25000 字符用于洞察，防止 Token 超限)
        prompt = MARKET_RESEARCH_PROMPT.format(content=md_content[:25000])
        
        metadata = {
            "summary": "",
            "key_metrics": [],
            "key_insights": [],
            "risks": []
        }
        
        if call_llm_direct:
            try:
                logger.info("调用大模型提取核心市场研究洞察...")
                response = call_llm_direct(prompt)
                
                cleaned_response = response.strip()
                if cleaned_response.startswith("```json"):
                    cleaned_response = cleaned_response[7:]
                if cleaned_response.endswith("```"):
                    cleaned_response = cleaned_response[:-3]
                    
                meta_json = json.loads(cleaned_response.strip())
                metadata.update(meta_json)
                logger.info("洞察提取成功。")
            except Exception as e:
                logger.error(f"模型提取洞察失败: {e}\nResponse: {response}")
        else:
            logger.warning("未找到 scheduler.call_llm_direct，跳过大模型提炼")
            
        return {
            "filename": pdf_path.name,
            "markdown": md_content,
            "metadata": metadata
        }

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import sys
    if len(sys.argv) > 1:
        # 测试入口
        test_path = Path(sys.argv[1])
        res = process_research_report(test_path)
        print("\n=== 处理完毕 ===")
        print(f"标题: {res['filename']}")
        print(f"摘要: {res['metadata'].get('summary')}")
        print(f"数据指标: {res['metadata'].get('key_metrics')}")
        print(f"正文长度: {len(res['markdown'])} 字符")

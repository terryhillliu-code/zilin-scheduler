#!/usr/bin/env python3
"""
Agent 协调器 — 实现多 Agent 协作
T-080: Phase 3 Agent 协同
"""

import json
import logging
from typing import Optional, Dict, Any
from pathlib import Path

from event_bus import get_event_bus, AgentMessage, EventType

# 日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s [coordinator] %(message)s')
logger = logging.getLogger(__name__)


class AgentCoordinator:
    """
    Agent 协调器
    - 知微：任务拆解、路由决策
    - 探微：信息采集、深度分析
    - 通微：格式化、推送
    - 执微：代码执行
    """
    
    AGENTS = {
        "知微": {"model": "qwen3.5-plus", "skills": ["routing", "planning", "qa"]},
        "探微": {"model": "kimi-k2.5", "skills": ["research", "analysis", "summarize"]},
        "通微": {"model": "qwen3.5-plus", "skills": ["format", "push", "monitor"]},
        "执微": {"model": "qwen3-coder-plus", "skills": ["code", "execute", "develop"]}
    }
    
    def __init__(self):
        self.bus = get_event_bus()
    
    def analyze_task(self, task: str, context: dict = None) -> Dict[str, Any]:
        """
        分析任务，决定需要哪些 Agent 协作
        返回执行计划
        """
        task_lower = task.lower()
        
        plan = {
            "task": task,
            "steps": [],
            "agents_involved": []
        }
        
        # 规则匹配（后续可改为 LLM 判断）
        if any(kw in task_lower for kw in ["分析", "研究", "对比", "调研", "论文"]):
            plan["steps"].append({
                "agent": "探微",
                "action": "research",
                "description": "信息采集和深度分析"
            })
            plan["agents_involved"].append("探微")
        
        if any(kw in task_lower for kw in ["代码", "开发", "修改", "实现", "创建"]):
            plan["steps"].append({
                "agent": "执微",
                "action": "develop",
                "description": "代码开发或修改"
            })
            plan["agents_involved"].append("执微")
        
        if any(kw in task_lower for kw in ["推送", "通知", "发送", "报告"]):
            plan["steps"].append({
                "agent": "通微",
                "action": "push",
                "description": "格式化并推送结果"
            })
            plan["agents_involved"].append("通微")
        
        # 知微始终作为协调者
        if "知微" not in plan["agents_involved"]:
            plan["agents_involved"].insert(0, "知微")
        
        plan["steps"].insert(0, {
            "agent": "知微",
            "action": "coordinate",
            "description": "任务协调和结果整合"
        })
        
        return plan
    
    def delegate_to_agent(self, 
                          from_agent: str,
                          to_agent: str, 
                          task_id: str,
                          action: str,
                          payload: dict,
                          context: dict = None,
                          timeout: float = 120) -> Optional[Dict]:
        """
        委派任务给其他 Agent
        """
        logger.info(f"委派任务: {from_agent} → {to_agent} ({action})")
        
        response = self.bus.request(
            from_agent=from_agent,
            to_agent=to_agent,
            task_id=task_id,
            payload={"action": action, **payload},
            context=context or {},
            timeout=timeout
        )
        
        if response:
            logger.info(f"收到响应: {to_agent} → {from_agent}")
            return response.payload
        else:
            logger.warning(f"委派超时: {to_agent}")
            return None
    
    def execute_plan(self, plan: Dict, task_id: str, context: dict = None) -> Dict:
        """
        执行协作计划
        """
        results = {
            "task_id": task_id,
            "plan": plan,
            "step_results": [],
            "success": True
        }
        
        for i, step in enumerate(plan["steps"]):
            agent = step["agent"]
            action = step["action"]
            
            logger.info(f"执行步骤 {i+1}: {agent} - {action}")
            
            if action == "coordinate":
                # 知微协调，不需要实际执行
                results["step_results"].append({
                    "step": i+1,
                    "agent": agent,
                    "status": "coordinating"
                })
                continue
            
            # 委派给对应 Agent
            result = self.delegate_to_agent(
                from_agent="知微",
                to_agent=agent,
                task_id=task_id,
                action=action,
                payload={"task": plan["task"], "step": step},
                context=context
            )
            
            results["step_results"].append({
                "step": i+1,
                "agent": agent,
                "result": result,
                "status": "completed" if result else "timeout"
            })
            
            if not result:
                results["success"] = False
        
        return results


def demo_collaboration():
    """演示多 Agent 协作"""
    coordinator = AgentCoordinator()
    
    # 示例任务
    tasks = [
        "帮我分析 RISC-V 和 ARM 在 AI 芯片上的对比",
        "创建一个新的定时任务脚本",
        "推送今日系统状态报告"
    ]
    
    for task in tasks:
        print(f"\n{'='*50}")
        print(f"任务: {task}")
        plan = coordinator.analyze_task(task)
        print(f"执行计划:")
        print(f"  涉及 Agent: {plan['agents_involved']}")
        for step in plan["steps"]:
            print(f"  - {step['agent']}: {step['description']}")


if __name__ == "__main__":
    print("=== Agent 协调器测试 ===")
    demo_collaboration()

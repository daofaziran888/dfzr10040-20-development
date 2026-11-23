"""空窗确认领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ReadProgressService(BaseModuleService):
    MODULE = "read_progress"
    ENTITY = "空窗确认"
    OPERATION_TERM = "确认"
    LIST_FIELDS = [
        "确认编号", "数据源", "窗口起始", "窗口截止",
        "回执行数", "结束回执", "是否空窗", "补采要求",
    ]
    REQUIRED_FIELDS = ["确认编号", "数据源", "窗口起始", "窗口截止", "结束回执"]
    STATUS_ORDER = ["待核对", "已确认", "读取中断", "已归档"]
    ACTION_RULES = {
        "核对回执": "已确认",
        "标记中断": "读取中断",
        "归档记录": "已归档",
    }
    NEGATIVE_ACTIONS = ["标记中断"]

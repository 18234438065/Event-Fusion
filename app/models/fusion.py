"""融合规则数据模型

定义事件融合相关的规则和策略：
- 融合规则基类
- 抑制规则
- 相邻融合规则
- 时间窗口规则
"""

from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Set
from enum import Enum
from abc import ABC, abstractmethod

from pydantic import BaseModel, Field, validator
from app.models.event import EventData, EventType, EventKey


class RuleType(str, Enum):
    """规则类型枚举"""
    SUPPRESSION = "suppression"          # 抑制规则
    ADJACENT_FUSION = "adjacent_fusion"  # 相邻融合规则
    TIME_WINDOW = "time_window"          # 时间窗口规则
    PRIORITY = "priority"                # 优先级规则


class RuleAction(str, Enum):
    """规则动作枚举"""
    ALLOW = "allow"          # 允许
    SUPPRESS = "suppress"    # 抑制
    FUSE = "fuse"           # 融合
    DELAY = "delay"         # 延迟
    PRIORITY = "priority"    # 优先处理


class FusionRule(BaseModel, ABC):
    """融合规则基类"""
    rule_id: str = Field(..., description="规则ID")
    rule_type: RuleType = Field(..., description="规则类型")
    name: str = Field(..., description="规则名称")
    description: str = Field(..., description="规则描述")
    enabled: bool = Field(default=True, description="是否启用")
    priority: int = Field(default=5, ge=1, le=10, description="优先级(1-10，10最高)")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    @abstractmethod
    def evaluate(self, event: EventData, context: Dict[str, Any]) -> RuleAction:
        """评估规则"""
        pass

    @abstractmethod
    def get_rule_config(self) -> Dict[str, Any]:
        """获取规则配置"""
        pass

    def is_applicable(self, event: EventData) -> bool:
        """判断规则是否适用于事件"""
        return self.enabled


class SuppressionRule(FusionRule):
    """抑制规则"""
    rule_type: RuleType = Field(default=RuleType.SUPPRESSION, const=True)
    trigger_event_types: List[EventType] = Field(..., description="触发抑制的事件类型")
    suppressed_event_types: List[EventType] = Field(..., description="被抑制的事件类型")
    suppression_duration: int = Field(..., ge=0, description="抑制持续时间(秒)")
    scope: str = Field(default="global", description="抑制范围: global, stake, adjacent")
    conditions: Dict[str, Any] = Field(default_factory=dict, description="额外条件")
    
    class Config:
        use_enum_values = True

    def evaluate(self, event: EventData, context: Dict[str, Any]) -> RuleAction:
        """评估抑制规则"""
        if not self.is_applicable(event):
            return RuleAction.ALLOW
        
        # 检查是否为触发事件
        if event.eventType in self.trigger_event_types:
            # 检查额外条件
            if self._check_conditions(event, context):
                return RuleAction.SUPPRESS
        
        # 检查是否为被抑制事件
        if event.eventType in self.suppressed_event_types:
            if self._is_currently_suppressed(event, context):
                return RuleAction.SUPPRESS
        
        return RuleAction.ALLOW

    def _check_conditions(self, event: EventData, context: Dict[str, Any]) -> bool:
        """检查额外条件"""
        for condition, value in self.conditions.items():
            if condition == "event_level":
                if hasattr(event, 'eventLevel') and event.eventLevel != value:
                    return False
            elif condition == "direction":
                if event.direction != value:
                    return False
            elif condition == "stake_pattern":
                import re
                if not re.match(value, event.stakeNum):
                    return False
        return True

    def _is_currently_suppressed(self, event: EventData, context: Dict[str, Any]) -> bool:
        """检查当前是否被抑制"""
        suppression_records = context.get('suppression_records', {})
        current_time = datetime.now(timezone.utc)
        
        # 根据抑制范围检查
        if self.scope == "global":
            key = f"global_{self.rule_id}"
        elif self.scope == "stake":
            key = f"stake_{event.stakeNum}_{self.rule_id}"
        elif self.scope == "adjacent":
            # 检查相邻桩号的抑制记录
            stakes = context.get('stakes', [])
            try:
                stake_index = stakes.index(event.stakeNum)
                adjacent_stakes = []
                if stake_index > 0:
                    adjacent_stakes.append(stakes[stake_index - 1])
                if stake_index < len(stakes) - 1:
                    adjacent_stakes.append(stakes[stake_index + 1])
                
                for stake in [event.stakeNum] + adjacent_stakes:
                    key = f"stake_{stake}_{self.rule_id}"
                    if key in suppression_records:
                        suppression_end = suppression_records[key]
                        if current_time < suppression_end:
                            return True
                return False
            except ValueError:
                key = f"stake_{event.stakeNum}_{self.rule_id}"
        else:
            key = f"stake_{event.stakeNum}_{self.rule_id}"
        
        if key in suppression_records:
            suppression_end = suppression_records[key]
            return current_time < suppression_end
        
        return False

    def get_rule_config(self) -> Dict[str, Any]:
        """获取规则配置"""
        return {
            "trigger_event_types": [et.value for et in self.trigger_event_types],
            "suppressed_event_types": [et.value for et in self.suppressed_event_types],
            "suppression_duration": self.suppression_duration,
            "scope": self.scope,
            "conditions": self.conditions
        }

    @classmethod
    def create_severe_jam_rule(cls) -> 'SuppressionRule':
        """创建严重拥堵抑制规则"""
        return cls(
            rule_id="severe_jam_suppression",
            name="严重拥堵抑制规则",
            description="严重拥堵时抑制异常停车、行人相关事件",
            trigger_event_types=[EventType.TRAFFIC_JAM],
            suppressed_event_types=[
                EventType.ABNORMAL_PARKING,
                EventType.PEDESTRIAN_WALKING,
                EventType.PEDESTRIAN_INTRUSION
            ],
            suppression_duration=180,  # 3分钟
            scope="adjacent",
            conditions={"event_level": "5"}  # 严重拥堵
        )

    @classmethod
    def create_construction_rule(cls) -> 'SuppressionRule':
        """创建施工占道抑制规则"""
        return cls(
            rule_id="construction_suppression",
            name="施工占道抑制规则",
            description="施工占道时抑制其他所有事件",
            trigger_event_types=[EventType.CONSTRUCTION],
            suppressed_event_types=[
                EventType.TRAFFIC_JAM, EventType.TRAFFIC_FLOW,
                EventType.SPEED_DETECTION, EventType.DEBRIS,
                EventType.WRONG_WAY, EventType.VEHICLE_VIOLATION,
                EventType.ABNORMAL_PARKING, EventType.PEDESTRIAN_WALKING,
                EventType.PEDESTRIAN_INTRUSION, EventType.NON_MOTOR_INTRUSION,
                EventType.FIRE_SMOKE, EventType.LANE_DETECTION,
                EventType.FACILITY_DETECTION, EventType.TRAFFIC_ACCIDENT
            ],
            suppression_duration=300,  # 5分钟
            scope="stake"
        )


class AdjacentFusionRule(FusionRule):
    """相邻融合规则"""
    rule_type: RuleType = Field(default=RuleType.ADJACENT_FUSION, const=True)
    fusion_event_types: List[EventType] = Field(..., description="支持融合的事件类型")
    max_distance: int = Field(default=1, ge=1, description="最大融合距离(桩号间隔)")
    fusion_window: int = Field(default=300, ge=0, description="融合时间窗口(秒)")
    max_fusion_count: int = Field(default=5, ge=1, description="最大融合事件数")
    
    class Config:
        use_enum_values = True

    def evaluate(self, event: EventData, context: Dict[str, Any]) -> RuleAction:
        """评估相邻融合规则"""
        if not self.is_applicable(event):
            return RuleAction.ALLOW
        
        if event.eventType not in self.fusion_event_types:
            return RuleAction.ALLOW
        
        # 检查是否有相邻的同类型事件
        adjacent_events = self._find_adjacent_events(event, context)
        if adjacent_events:
            return RuleAction.FUSE
        
        return RuleAction.ALLOW

    def _find_adjacent_events(self, event: EventData, context: Dict[str, Any]) -> List[EventKey]:
        """查找相邻的同类型事件"""
        stakes = context.get('stakes', [])
        active_events = context.get('active_events', {})
        current_time = datetime.now(timezone.utc)
        
        try:
            current_index = stakes.index(event.stakeNum)
        except ValueError:
            return []
        
        adjacent_events = []
        
        # 检查前后相邻的桩号
        for offset in range(-self.max_distance, self.max_distance + 1):
            if offset == 0:  # 跳过当前桩号
                continue
            
            adjacent_index = current_index + offset
            if 0 <= adjacent_index < len(stakes):
                adjacent_stake = stakes[adjacent_index]
                
                # 创建相邻事件键
                adjacent_key = EventKey(
                    eventType=event.eventType,
                    direction=event.direction,
                    stakeNum=adjacent_stake
                )
                
                # 检查是否存在活跃的相邻事件
                if adjacent_key in active_events:
                    fused_event = active_events[adjacent_key]
                    # 检查时间窗口
                    time_diff = (current_time - fused_event.last_update_time).total_seconds()
                    if time_diff <= self.fusion_window:
                        adjacent_events.append(adjacent_key)
        
        return adjacent_events

    def get_rule_config(self) -> Dict[str, Any]:
        """获取规则配置"""
        return {
            "fusion_event_types": [et.value for et in self.fusion_event_types],
            "max_distance": self.max_distance,
            "fusion_window": self.fusion_window,
            "max_fusion_count": self.max_fusion_count
        }

    @classmethod
    def create_default_rule(cls) -> 'AdjacentFusionRule':
        """创建默认相邻融合规则"""
        return cls(
            rule_id="default_adjacent_fusion",
            name="默认相邻融合规则",
            description="支持相邻桩号的同类型事件融合",
            fusion_event_types=[
                EventType.TRAFFIC_JAM, EventType.DEBRIS,
                EventType.WRONG_WAY, EventType.VEHICLE_VIOLATION,
                EventType.ABNORMAL_PARKING, EventType.PEDESTRIAN_WALKING,
                EventType.PEDESTRIAN_INTRUSION, EventType.TRAFFIC_ACCIDENT
            ],
            max_distance=1,
            fusion_window=300,
            max_fusion_count=5
        )


class TimeWindowRule(FusionRule):
    """时间窗口规则"""
    rule_type: RuleType = Field(default=RuleType.TIME_WINDOW, const=True)
    silence_window: int = Field(default=60, ge=0, description="静默窗口(秒)")
    new_event_threshold: int = Field(default=120, ge=0, description="新事件阈值(秒)")
    max_report_count: int = Field(default=10, ge=1, description="最大上报次数")
    event_expiry: int = Field(default=300, ge=0, description="事件过期时间(秒)")
    
    def evaluate(self, event: EventData, context: Dict[str, Any]) -> RuleAction:
        """评估时间窗口规则"""
        if not self.is_applicable(event):
            return RuleAction.ALLOW
        
        event_key = EventKey.from_event(event)
        active_events = context.get('active_events', {})
        current_time = datetime.now(timezone.utc)
        
        if event_key in active_events:
            fused_event = active_events[event_key]
            
            # 检查是否超过新事件阈值
            time_since_last = (current_time - fused_event.last_report_time).total_seconds()
            if time_since_last > self.new_event_threshold:
                return RuleAction.ALLOW  # 作为新事件处理
            
            # 检查静默窗口
            if time_since_last < self.silence_window:
                return RuleAction.SUPPRESS  # 在静默窗口内，抑制
            
            # 检查最大上报次数
            if fused_event.total_reports >= self.max_report_count:
                return RuleAction.SUPPRESS
            
            return RuleAction.FUSE  # 融合到现有事件
        
        return RuleAction.ALLOW  # 新事件

    def get_rule_config(self) -> Dict[str, Any]:
        """获取规则配置"""
        return {
            "silence_window": self.silence_window,
            "new_event_threshold": self.new_event_threshold,
            "max_report_count": self.max_report_count,
            "event_expiry": self.event_expiry
        }

    @classmethod
    def create_default_rule(cls) -> 'TimeWindowRule':
        """创建默认时间窗口规则"""
        return cls(
            rule_id="default_time_window",
            name="默认时间窗口规则",
            description="默认的事件时间窗口处理规则",
            silence_window=60,
            new_event_threshold=120,
            max_report_count=10,
            event_expiry=300
        )


class PriorityRule(FusionRule):
    """优先级规则"""
    rule_type: RuleType = Field(default=RuleType.PRIORITY, const=True)
    event_priorities: Dict[EventType, int] = Field(..., description="事件类型优先级映射")
    high_priority_threshold: int = Field(default=8, ge=1, le=10, description="高优先级阈值")
    
    class Config:
        use_enum_values = True

    def evaluate(self, event: EventData, context: Dict[str, Any]) -> RuleAction:
        """评估优先级规则"""
        if not self.is_applicable(event):
            return RuleAction.ALLOW
        
        priority = self.event_priorities.get(event.eventType, 5)
        
        if priority >= self.high_priority_threshold:
            return RuleAction.PRIORITY
        
        return RuleAction.ALLOW

    def get_event_priority(self, event_type: EventType) -> int:
        """获取事件类型的优先级"""
        return self.event_priorities.get(event_type, 5)

    def get_rule_config(self) -> Dict[str, Any]:
        """获取规则配置"""
        return {
            "event_priorities": {et.value: p for et, p in self.event_priorities.items()},
            "high_priority_threshold": self.high_priority_threshold
        }

    @classmethod
    def create_default_rule(cls) -> 'PriorityRule':
        """创建默认优先级规则"""
        return cls(
            rule_id="default_priority",
            name="默认优先级规则",
            description="默认的事件优先级处理规则",
            event_priorities={
                EventType.TRAFFIC_ACCIDENT: 10,    # 交通事故最高优先级
                EventType.FIRE_SMOKE: 9,           # 火灾烟雾高优先级
                EventType.CONSTRUCTION: 8,         # 施工占道高优先级
                EventType.WRONG_WAY: 8,            # 逆行高优先级
                EventType.PEDESTRIAN_INTRUSION: 7, # 行人闯入较高优先级
                EventType.TRAFFIC_JAM: 6,          # 交通拥堵中等优先级
                EventType.ABNORMAL_PARKING: 5,     # 异常停车中等优先级
                EventType.DEBRIS: 5,               # 抛洒物中等优先级
                EventType.VEHICLE_VIOLATION: 4,    # 违法事件较低优先级
                EventType.PEDESTRIAN_WALKING: 3,   # 行人行走较低优先级
                EventType.NON_MOTOR_INTRUSION: 3,  # 非机动车闯入较低优先级
                EventType.TRAFFIC_FLOW: 2,         # 车流量检测低优先级
                EventType.SPEED_DETECTION: 2,      # 车速检测低优先级
                EventType.LANE_DETECTION: 1,       # 车道检测最低优先级
                EventType.FACILITY_DETECTION: 1    # 设施检测最低优先级
            },
            high_priority_threshold=8
        )


class RuleEngine(BaseModel):
    """规则引擎"""
    rules: List[FusionRule] = Field(default_factory=list, description="规则列表")
    
    def add_rule(self, rule: FusionRule):
        """添加规则"""
        # 按优先级排序插入
        inserted = False
        for i, existing_rule in enumerate(self.rules):
            if rule.priority > existing_rule.priority:
                self.rules.insert(i, rule)
                inserted = True
                break
        
        if not inserted:
            self.rules.append(rule)
    
    def remove_rule(self, rule_id: str):
        """移除规则"""
        self.rules = [rule for rule in self.rules if rule.rule_id != rule_id]
    
    def get_rule(self, rule_id: str) -> Optional[FusionRule]:
        """获取规则"""
        for rule in self.rules:
            if rule.rule_id == rule_id:
                return rule
        return None
    
    def evaluate_event(self, event: EventData, context: Dict[str, Any]) -> List[RuleAction]:
        """评估事件，返回所有适用规则的动作"""
        actions = []
        for rule in self.rules:
            if rule.enabled and rule.is_applicable(event):
                action = rule.evaluate(event, context)
                actions.append(action)
        return actions
    
    def get_final_action(self, event: EventData, context: Dict[str, Any]) -> RuleAction:
        """获取最终动作（基于优先级）"""
        actions = self.evaluate_event(event, context)
        
        # 动作优先级：SUPPRESS > PRIORITY > FUSE > DELAY > ALLOW
        if RuleAction.SUPPRESS in actions:
            return RuleAction.SUPPRESS
        elif RuleAction.PRIORITY in actions:
            return RuleAction.PRIORITY
        elif RuleAction.FUSE in actions:
            return RuleAction.FUSE
        elif RuleAction.DELAY in actions:
            return RuleAction.DELAY
        else:
            return RuleAction.ALLOW
    
    @classmethod
    def create_default_engine(cls) -> 'RuleEngine':
        """创建默认规则引擎"""
        engine = cls()
        
        # 添加默认规则
        engine.add_rule(SuppressionRule.create_severe_jam_rule())
        engine.add_rule(SuppressionRule.create_construction_rule())
        engine.add_rule(AdjacentFusionRule.create_default_rule())
        engine.add_rule(TimeWindowRule.create_default_rule())
        engine.add_rule(PriorityRule.create_default_rule())
        
        return engine
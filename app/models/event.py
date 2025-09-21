"""事件数据模型

定义了完整的事件数据结构，包括：
- 事件基础模型
- 事件类型枚举
- 融合事件模型
- 事件统计模型
"""

import re
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, Field, validator, root_validator


class Direction(int, Enum):
    """方向枚举"""
    IN = 0      # 进
    OUT = 1     # 出
    BOTH = 2    # 双向

    @classmethod
    def from_string(cls, value: str) -> 'Direction':
        """从字符串创建方向"""
        mapping = {
            '进': cls.IN,
            '出': cls.OUT,
            '双向': cls.BOTH,
            'in': cls.IN,
            'out': cls.OUT,
            'both': cls.BOTH
        }
        return mapping.get(value.lower(), cls.BOTH)


class EventType(str, Enum):
    """事件类型枚举"""
    TRAFFIC_JAM = "01"              # 交通拥堵
    TRAFFIC_FLOW = "02"             # 车流量检测
    SPEED_DETECTION = "03"          # 车速检测
    DEBRIS = "04"                   # 抛洒物事件检测
    WRONG_WAY = "05"               # 逆行事件检测
    VEHICLE_VIOLATION = "06"        # 机动车违法事件
    ABNORMAL_PARKING = "07"         # 异常停车事件
    PEDESTRIAN_WALKING = "08"       # 行人行走驻留事件
    PEDESTRIAN_INTRUSION = "09"     # 行人闯入事件检测
    NON_MOTOR_INTRUSION = "10"      # 非机动车闯入事件检测
    CONSTRUCTION = "11"             # 施工占道事件检测
    FIRE_SMOKE = "12"              # 起火/烟雾事件检测
    LANE_DETECTION = "13"          # 车道路面检测
    FACILITY_DETECTION = "14"       # 基础及周边设施检测
    TRAFFIC_ACCIDENT = "15"         # 交通事故

    @classmethod
    def get_description(cls, event_type: str) -> str:
        """获取事件类型描述"""
        descriptions = {
            cls.TRAFFIC_JAM: "交通拥堵",
            cls.TRAFFIC_FLOW: "车流量检测",
            cls.SPEED_DETECTION: "车速检测",
            cls.DEBRIS: "抛洒物事件检测",
            cls.WRONG_WAY: "逆行事件检测",
            cls.VEHICLE_VIOLATION: "机动车违法事件",
            cls.ABNORMAL_PARKING: "异常停车事件",
            cls.PEDESTRIAN_WALKING: "行人行走驻留事件",
            cls.PEDESTRIAN_INTRUSION: "行人闯入事件检测",
            cls.NON_MOTOR_INTRUSION: "非机动车闯入事件检测",
            cls.CONSTRUCTION: "施工占道事件检测",
            cls.FIRE_SMOKE: "起火/烟雾事件检测",
            cls.LANE_DETECTION: "车道路面检测",
            cls.FACILITY_DETECTION: "基础及周边设施检测",
            cls.TRAFFIC_ACCIDENT: "交通事故"
        }
        return descriptions.get(event_type, "未知事件类型")

    @classmethod
    def is_adjacent_fusion_type(cls, event_type: str) -> bool:
        """判断是否为相邻融合事件类型"""
        adjacent_types = {
            cls.TRAFFIC_JAM, cls.DEBRIS, cls.WRONG_WAY,
            cls.VEHICLE_VIOLATION, cls.ABNORMAL_PARKING,
            cls.PEDESTRIAN_WALKING, cls.PEDESTRIAN_INTRUSION,
            cls.TRAFFIC_ACCIDENT
        }
        return event_type in adjacent_types

    @classmethod
    def is_suppressed_by_severe_jam(cls, event_type: str) -> bool:
        """判断是否被严重拥堵抑制"""
        suppressed_types = {
            cls.ABNORMAL_PARKING,
            cls.PEDESTRIAN_WALKING,
            cls.PEDESTRIAN_INTRUSION
        }
        return event_type in suppressed_types

    @classmethod
    def is_suppressed_by_construction(cls, event_type: str) -> bool:
        """判断是否被施工占道抑制"""
        # 施工占道时，除了施工本身，其他所有事件都被抑制
        return event_type != cls.CONSTRUCTION


class EventLevel(str, Enum):
    """拥堵等级枚举"""
    SMOOTH = "1"        # 畅通
    SLOW = "2"          # 缓行
    LIGHT = "3"         # 轻度
    MODERATE = "4"      # 中度
    SEVERE = "5"        # 严重

    @classmethod
    def get_description(cls, level: str) -> str:
        """获取拥堵等级描述"""
        descriptions = {
            cls.SMOOTH: "畅通",
            cls.SLOW: "缓行",
            cls.LIGHT: "轻度拥堵",
            cls.MODERATE: "中度拥堵",
            cls.SEVERE: "严重拥堵"
        }
        return descriptions.get(level, "未知等级")

    def is_severe(self) -> bool:
        """是否为严重拥堵"""
        return self == self.SEVERE


class EventData(BaseModel):
    """事件数据模型"""
    alarmID: str = Field(..., description="事件唯一ID")
    stakeNum: str = Field(..., description="摄像头桩号")
    reportCount: int = Field(default=1, ge=1, description="报警次数")
    eventTime: str = Field(..., description="事件发生时间")
    eventType: EventType = Field(..., description="事件类型")
    direction: Direction = Field(..., description="方向")
    eventLevel: Optional[EventLevel] = Field(default=None, description="拥堵等级")
    photoUrl: str = Field(..., description="事件图片URL")
    videoUrl: Optional[str] = Field(default=None, description="事件视频URL")
    remark1: Optional[str] = Field(default=None, description="备用字段1")
    remark2: Optional[str] = Field(default=None, description="备用字段2")
    remark3: Optional[str] = Field(default=None, description="备用字段3")

    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    @validator('alarmID')
    def validate_alarm_id(cls, v):
        """验证事件ID格式"""
        if not v or len(v.strip()) == 0:
            raise ValueError('alarmID不能为空')
        # 移除前后空格
        return v.strip()

    @validator('stakeNum')
    def validate_stake_num(cls, v):
        """验证桩号格式"""
        if not v or len(v.strip()) == 0:
            raise ValueError('stakeNum不能为空')
        
        # 桩号格式验证：K\d+\+\d+
        stake_pattern = r'^K\d+\+\d+$'
        if not re.match(stake_pattern, v.strip()):
            raise ValueError('桩号格式不正确，应为K数字+数字格式，如K1+000')
        
        return v.strip()

    @validator('eventTime')
    def validate_event_time(cls, v):
        """验证事件时间格式"""
        if not v:
            raise ValueError('eventTime不能为空')
        
        # 支持多种时间格式
        time_formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%fZ'
        ]
        
        for fmt in time_formats:
            try:
                datetime.strptime(v, fmt)
                return v
            except ValueError:
                continue
        
        raise ValueError('eventTime格式不正确，支持格式：YYYY-MM-DD HH:MM:SS')

    @validator('photoUrl')
    def validate_photo_url(cls, v):
        """验证图片URL"""
        if not v or len(v.strip()) == 0:
            raise ValueError('photoUrl不能为空')
        
        # 简单的URL格式验证
        url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        if not re.match(url_pattern, v.strip()):
            raise ValueError('photoUrl格式不正确')
        
        return v.strip()

    @validator('videoUrl')
    def validate_video_url(cls, v):
        """验证视频URL"""
        if v is None:
            return v
        
        if len(v.strip()) == 0:
            return None
        
        # 简单的URL格式验证
        url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
        if not re.match(url_pattern, v.strip()):
            raise ValueError('videoUrl格式不正确')
        
        return v.strip()

    @root_validator
    def validate_event_level_for_traffic_jam(cls, values):
        """验证交通拥堵事件必须有拥堵等级"""
        event_type = values.get('eventType')
        event_level = values.get('eventLevel')
        
        if event_type == EventType.TRAFFIC_JAM and event_level is None:
            raise ValueError('交通拥堵事件必须指定拥堵等级')
        
        return values

    def get_event_time_datetime(self) -> datetime:
        """获取事件时间的datetime对象"""
        time_formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%fZ'
        ]
        
        for fmt in time_formats:
            try:
                dt = datetime.strptime(self.eventTime, fmt)
                # 如果没有时区信息，假设为UTC
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        
        raise ValueError(f"无法解析时间格式: {self.eventTime}")

    def is_severe_traffic_jam(self) -> bool:
        """是否为严重交通拥堵"""
        return (self.eventType == EventType.TRAFFIC_JAM and 
                self.eventLevel == EventLevel.SEVERE)

    def is_construction_event(self) -> bool:
        """是否为施工占道事件"""
        return self.eventType == EventType.CONSTRUCTION

    def should_suppress_event(self, other_event_type: EventType) -> bool:
        """判断是否应该抑制其他事件"""
        if self.is_severe_traffic_jam():
            return EventType.is_suppressed_by_severe_jam(other_event_type)
        
        if self.is_construction_event():
            return EventType.is_suppressed_by_construction(other_event_type)
        
        return False

    def generate_new_alarm_id(self, prefix: str = None) -> str:
        """生成新的事件ID"""
        if prefix is None:
            # 从桩号提取前缀
            stake_match = re.match(r'(K\d+)', self.stakeNum)
            prefix = stake_match.group(1) if stake_match else "K01"
        
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid4())[:8].upper()
        return f"{prefix}001{timestamp}{unique_id}"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return self.dict(by_alias=True, exclude_none=False)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EventData':
        """从字典创建事件"""
        return cls(**data)


class EventKey(BaseModel):
    """事件键，用于事件融合判断"""
    eventType: EventType
    direction: Direction
    stakeNum: str
    
    class Config:
        frozen = True  # 使对象不可变，可以作为字典键
    
    def __hash__(self):
        return hash((self.eventType, self.direction, self.stakeNum))
    
    def __eq__(self, other):
        if not isinstance(other, EventKey):
            return False
        return (self.eventType == other.eventType and 
                self.direction == other.direction and 
                self.stakeNum == other.stakeNum)

    def is_adjacent_to(self, other: 'EventKey', stakes: List[str]) -> bool:
        """判断是否与另一个事件键相邻"""
        if (self.eventType != other.eventType or 
            self.direction != other.direction):
            return False
        
        try:
            self_index = stakes.index(self.stakeNum)
            other_index = stakes.index(other.stakeNum)
            return abs(self_index - other_index) == 1
        except ValueError:
            return False

    @classmethod
    def from_event(cls, event: EventData) -> 'EventKey':
        """从事件数据创建事件键"""
        return cls(
            eventType=event.eventType,
            direction=event.direction,
            stakeNum=event.stakeNum
        )


class FusedEvent(BaseModel):
    """融合后的事件"""
    key: EventKey
    data: EventData
    first_report_time: datetime
    last_report_time: datetime
    last_update_time: datetime
    suppressed_until: Optional[datetime] = None
    adjacent_events: List[EventKey] = Field(default_factory=list)
    total_reports: int = Field(default=1)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    def is_suppressed(self, current_time: datetime = None) -> bool:
        """判断事件是否被抑制"""
        if self.suppressed_until is None:
            return False
        
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        return current_time < self.suppressed_until

    def should_report(self, current_time: datetime = None, 
                     silence_window: int = 60) -> bool:
        """判断是否应该上报事件"""
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        # 如果被抑制，不上报
        if self.is_suppressed(current_time):
            return False
        
        # 检查静默窗口
        silence_end = self.last_report_time + timedelta(seconds=silence_window)
        return current_time >= silence_end

    def update_with_new_event(self, new_event: EventData, 
                            current_time: datetime = None):
        """使用新事件更新融合事件"""
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        # 更新数据
        self.data = new_event
        self.data.reportCount = self.total_reports + 1
        self.total_reports += 1
        self.last_update_time = current_time
        
        # 如果需要上报，更新上报时间
        if self.should_report(current_time):
            self.last_report_time = current_time

    def add_adjacent_event(self, adjacent_key: EventKey):
        """添加相邻事件"""
        if adjacent_key not in self.adjacent_events:
            self.adjacent_events.append(adjacent_key)

    def suppress_until(self, until_time: datetime):
        """抑制事件直到指定时间"""
        self.suppressed_until = until_time


class EventResponse(BaseModel):
    """事件响应模型"""
    success: bool = Field(description="处理是否成功")
    message: str = Field(description="响应消息")
    event_id: Optional[str] = Field(default=None, description="事件ID")
    action: str = Field(description="执行的动作")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EventStats(BaseModel):
    """事件统计模型"""
    total_events: int = Field(default=0, description="总事件数")
    processed_events: int = Field(default=0, description="已处理事件数")
    suppressed_events: int = Field(default=0, description="被抑制事件数")
    fused_events: int = Field(default=0, description="融合事件数")
    video_generated: int = Field(default=0, description="生成视频数")
    events_by_type: Dict[str, int] = Field(default_factory=dict, description="按类型统计")
    events_by_stake: Dict[str, int] = Field(default_factory=dict, description="按桩号统计")
    avg_processing_time: float = Field(default=0.0, description="平均处理时间")
    last_update: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    def update_stats(self, event: EventData, processing_time: float, 
                    action: str):
        """更新统计信息"""
        self.total_events += 1
        
        if action == "processed":
            self.processed_events += 1
        elif action == "suppressed":
            self.suppressed_events += 1
        elif action == "fused":
            self.fused_events += 1
        
        # 按类型统计
        event_type = event.eventType.value
        self.events_by_type[event_type] = self.events_by_type.get(event_type, 0) + 1
        
        # 按桩号统计
        stake_num = event.stakeNum
        self.events_by_stake[stake_num] = self.events_by_stake.get(stake_num, 0) + 1
        
        # 更新平均处理时间
        total_time = self.avg_processing_time * (self.total_events - 1) + processing_time
        self.avg_processing_time = total_time / self.total_events
        
        self.last_update = datetime.now(timezone.utc)


# 导入datetime用于时间计算
from datetime import timedelta
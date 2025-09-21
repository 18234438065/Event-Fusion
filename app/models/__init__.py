"""数据模型包

包含所有的数据模型定义：
- 事件模型
- 视频模型
- 融合规则模型
- 响应模型
"""

from app.models.event import (
    EventData,
    EventType,
    Direction,
    EventLevel,
    EventKey,
    FusedEvent,
    EventResponse,
    EventStats
)
from app.models.video import (
    VideoRequest,
    VideoResponse,
    VideoStatus,
    VideoMetadata
)
from app.models.fusion import (
    FusionRule,
    SuppressionRule,
    AdjacentFusionRule
)

__all__ = [
    # Event models
    "EventData",
    "EventType",
    "Direction",
    "EventLevel",
    "EventKey",
    "FusedEvent",
    "EventResponse",
    "EventStats",
    
    # Video models
    "VideoRequest",
    "VideoResponse",
    "VideoStatus",
    "VideoMetadata",
    
    # Fusion models
    "FusionRule",
    "SuppressionRule",
    "AdjacentFusionRule"
]
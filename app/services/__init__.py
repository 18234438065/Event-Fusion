"""服务模块

包含所有业务服务：
- 事件融合服务
- 视频处理服务
- 通知服务
- 监控服务
"""

from app.services.fusion.event_fusion_service import EventFusionService
from app.services.video.video_processor import VideoProcessor
from app.services.notification.event_sender import EventSender
from app.services.monitoring.metrics_collector import MetricsCollector

__all__ = [
    "EventFusionService",
    "VideoProcessor",
    "EventSender",
    "MetricsCollector"
]
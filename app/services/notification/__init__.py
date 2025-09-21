"""通知服务模块

提供事件通知和发送功能：
- 事件发送服务
- 通知管理
- 回调处理
- 批量发送
"""

from app.services.notification.event_sender import (
    EventSender,
    SendResult,
    BatchSendResult,
    EventSenderStats,
    event_sender,
    send_fused_event,
    send_fused_event_async,
    send_batch_events
)

__all__ = [
    "EventSender",
    "SendResult",
    "BatchSendResult",
    "EventSenderStats",
    "event_sender",
    "send_fused_event",
    "send_fused_event_async",
    "send_batch_events"
]
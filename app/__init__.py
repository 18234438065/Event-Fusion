"""事件融合服务主包

企业级高速公路事件融合服务，支持高并发、消息不丢失、实时处理。

主要功能：
- 事件去重和聚合
- 视频片段生成
- 高并发处理
- 消息队列集成
- 分布式缓存
- 事件存储和查询
"""

__version__ = "1.0.0"
__author__ = "Event Fusion Team"
__email__ = "team@eventfusion.com"

# 导出主要组件
from app.core.config import settings
from app.core.logging import logger

__all__ = [
    "settings",
    "logger",
]
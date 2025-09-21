"""视频处理服务模块

提供完整的视频处理功能：
- 视频处理器
- RTSP流管理
- 视频存储管理
- 异步任务队列
"""

from app.services.video.video_processor import VideoProcessor
from app.services.video.rtsp_manager import (
    RTSPManager, RTSPConnection, RTSPStreamInfo, RTSPStatus,
    rtsp_manager, get_rtsp_frame, add_rtsp_stream
)
from app.services.video.storage_manager import (
    VideoStorageManager, StorageConfig, StorageStats, StorageType,
    video_storage_manager, save_video_file, delete_video_file, get_storage_stats
)
from app.services.video.task_queue import (
    VideoTaskQueue, VideoTask, TaskStatus, TaskPriority,
    video_task_queue, submit_video_task, get_video_task_status, cancel_video_task
)

__all__ = [
    "VideoProcessor",
    "RTSPManager",
    "RTSPConnection",
    "RTSPStreamInfo",
    "RTSPStatus",
    "rtsp_manager",
    "get_rtsp_frame",
    "add_rtsp_stream",
    "VideoStorageManager",
    "StorageConfig",
    "StorageStats",
    "StorageType",
    "video_storage_manager",
    "save_video_file",
    "delete_video_file",
    "get_storage_stats",
    "VideoTaskQueue",
    "VideoTask",
    "TaskStatus",
    "TaskPriority",
    "video_task_queue",
    "submit_video_task",
    "get_video_task_status",
    "cancel_video_task"
]
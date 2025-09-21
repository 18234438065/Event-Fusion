"""视频处理数据模型

定义视频处理相关的数据结构：
- 视频请求模型
- 视频响应模型
- 视频状态枚举
- 视频元数据模型
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, validator


class VideoStatus(str, Enum):
    """视频状态枚举"""
    PENDING = "pending"          # 等待处理
    PROCESSING = "processing"    # 处理中
    COMPLETED = "completed"      # 处理完成
    FAILED = "failed"           # 处理失败
    EXPIRED = "expired"         # 已过期

    @classmethod
    def get_description(cls, status: str) -> str:
        """获取状态描述"""
        descriptions = {
            cls.PENDING: "等待处理",
            cls.PROCESSING: "处理中",
            cls.COMPLETED: "处理完成",
            cls.FAILED: "处理失败",
            cls.EXPIRED: "已过期"
        }
        return descriptions.get(status, "未知状态")


class VideoQuality(str, Enum):
    """视频质量枚举"""
    LOW = "480p"        # 低质量
    MEDIUM = "720p"     # 中等质量
    HIGH = "1080p"      # 高质量
    ULTRA = "4K"        # 超高质量


class VideoCodec(str, Enum):
    """视频编码格式枚举"""
    H264 = "h264"       # H.264编码
    H265 = "h265"       # H.265编码
    VP9 = "vp9"         # VP9编码
    AV1 = "av1"         # AV1编码


class VideoRequest(BaseModel):
    """视频请求模型"""
    request_id: str = Field(..., description="请求ID")
    event_id: str = Field(..., description="关联的事件ID")
    stake_num: str = Field(..., description="桩号")
    rtsp_url: str = Field(..., description="RTSP流地址")
    event_time: datetime = Field(..., description="事件发生时间")
    before_seconds: int = Field(default=15, ge=0, le=60, description="事件前秒数")
    after_seconds: int = Field(default=15, ge=0, le=60, description="事件后秒数")
    quality: VideoQuality = Field(default=VideoQuality.MEDIUM, description="视频质量")
    codec: VideoCodec = Field(default=VideoCodec.H264, description="编码格式")
    fps: int = Field(default=25, ge=1, le=60, description="帧率")
    max_size_mb: int = Field(default=100, ge=1, le=1000, description="最大文件大小MB")
    priority: int = Field(default=5, ge=1, le=10, description="优先级(1-10，10最高)")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    @validator('rtsp_url')
    def validate_rtsp_url(cls, v):
        """验证RTSP URL格式"""
        if not v.startswith('rtsp://'):
            raise ValueError('RTSP URL必须以rtsp://开头')
        return v

    @validator('event_time')
    def validate_event_time(cls, v):
        """验证事件时间"""
        if v.tzinfo is None:
            # 如果没有时区信息，假设为UTC
            v = v.replace(tzinfo=timezone.utc)
        return v

    def get_total_duration(self) -> int:
        """获取总视频时长"""
        return self.before_seconds + self.after_seconds

    def get_start_time(self) -> datetime:
        """获取视频开始时间"""
        from datetime import timedelta
        return self.event_time - timedelta(seconds=self.before_seconds)

    def get_end_time(self) -> datetime:
        """获取视频结束时间"""
        from datetime import timedelta
        return self.event_time + timedelta(seconds=self.after_seconds)

    def generate_filename(self) -> str:
        """生成视频文件名"""
        timestamp = self.event_time.strftime("%Y%m%d_%H%M%S")
        return f"{self.stake_num}_{self.event_id}_{timestamp}.mp4"


class VideoResponse(BaseModel):
    """视频响应模型"""
    request_id: str = Field(..., description="请求ID")
    event_id: str = Field(..., description="事件ID")
    status: VideoStatus = Field(..., description="处理状态")
    video_url: Optional[str] = Field(default=None, description="视频URL")
    file_path: Optional[str] = Field(default=None, description="文件路径")
    file_size: Optional[int] = Field(default=None, description="文件大小(字节)")
    duration: Optional[float] = Field(default=None, description="视频时长(秒)")
    error_message: Optional[str] = Field(default=None, description="错误信息")
    processing_time: Optional[float] = Field(default=None, description="处理耗时(秒)")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = Field(default=None, description="完成时间")
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    def is_success(self) -> bool:
        """是否处理成功"""
        return self.status == VideoStatus.COMPLETED and self.video_url is not None

    def is_failed(self) -> bool:
        """是否处理失败"""
        return self.status == VideoStatus.FAILED

    def is_processing(self) -> bool:
        """是否正在处理"""
        return self.status == VideoStatus.PROCESSING

    def mark_completed(self, video_url: str, file_path: str, 
                      file_size: int, duration: float, processing_time: float):
        """标记为完成"""
        self.status = VideoStatus.COMPLETED
        self.video_url = video_url
        self.file_path = file_path
        self.file_size = file_size
        self.duration = duration
        self.processing_time = processing_time
        self.completed_at = datetime.now(timezone.utc)

    def mark_failed(self, error_message: str, processing_time: float = None):
        """标记为失败"""
        self.status = VideoStatus.FAILED
        self.error_message = error_message
        if processing_time:
            self.processing_time = processing_time
        self.completed_at = datetime.now(timezone.utc)

    def mark_processing(self):
        """标记为处理中"""
        self.status = VideoStatus.PROCESSING


class VideoMetadata(BaseModel):
    """视频元数据模型"""
    file_path: str = Field(..., description="文件路径")
    file_name: str = Field(..., description="文件名")
    file_size: int = Field(..., description="文件大小(字节)")
    duration: float = Field(..., description="视频时长(秒)")
    width: int = Field(..., description="视频宽度")
    height: int = Field(..., description="视频高度")
    fps: float = Field(..., description="帧率")
    codec: str = Field(..., description="编码格式")
    bitrate: Optional[int] = Field(default=None, description="比特率")
    format: str = Field(..., description="文件格式")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # 关联信息
    event_id: str = Field(..., description="关联事件ID")
    stake_num: str = Field(..., description="桩号")
    event_time: datetime = Field(..., description="事件时间")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    @validator('file_path')
    def validate_file_path(cls, v):
        """验证文件路径"""
        path = Path(v)
        if not path.suffix.lower() in ['.mp4', '.avi', '.mov', '.mkv']:
            raise ValueError('不支持的视频文件格式')
        return str(path)

    def get_file_size_mb(self) -> float:
        """获取文件大小(MB)"""
        return self.file_size / (1024 * 1024)

    def get_resolution(self) -> str:
        """获取分辨率字符串"""
        return f"{self.width}x{self.height}"

    def get_quality_level(self) -> VideoQuality:
        """根据分辨率判断质量等级"""
        if self.height >= 2160:
            return VideoQuality.ULTRA
        elif self.height >= 1080:
            return VideoQuality.HIGH
        elif self.height >= 720:
            return VideoQuality.MEDIUM
        else:
            return VideoQuality.LOW

    def is_valid_duration(self, expected_duration: float, tolerance: float = 2.0) -> bool:
        """检查视频时长是否符合预期"""
        return abs(self.duration - expected_duration) <= tolerance

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return self.dict(by_alias=True)

    @classmethod
    def from_file(cls, file_path: str, event_id: str, stake_num: str, 
                 event_time: datetime) -> 'VideoMetadata':
        """从文件创建元数据"""
        import cv2
        import os
        
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"视频文件不存在: {file_path}")
        
        # 获取文件信息
        file_size = path.stat().st_size
        file_name = path.name
        
        # 使用OpenCV获取视频信息
        cap = cv2.VideoCapture(str(path))
        try:
            width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            fps = cap.get(cv2.CAP_PROP_FPS)
            frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            duration = frame_count / fps if fps > 0 else 0
            
            # 获取编码格式
            fourcc = int(cap.get(cv2.CAP_PROP_FOURCC))
            codec = "".join([chr((fourcc >> 8 * i) & 0xFF) for i in range(4)])
            
        finally:
            cap.release()
        
        return cls(
            file_path=str(path),
            file_name=file_name,
            file_size=file_size,
            duration=duration,
            width=width,
            height=height,
            fps=fps,
            codec=codec,
            format=path.suffix.lower()[1:],  # 移除点号
            event_id=event_id,
            stake_num=stake_num,
            event_time=event_time
        )


class VideoProcessingStats(BaseModel):
    """视频处理统计模型"""
    total_requests: int = Field(default=0, description="总请求数")
    completed_requests: int = Field(default=0, description="完成请求数")
    failed_requests: int = Field(default=0, description="失败请求数")
    processing_requests: int = Field(default=0, description="处理中请求数")
    pending_requests: int = Field(default=0, description="等待请求数")
    
    total_duration: float = Field(default=0.0, description="总视频时长(秒)")
    total_file_size: int = Field(default=0, description="总文件大小(字节)")
    avg_processing_time: float = Field(default=0.0, description="平均处理时间(秒)")
    avg_file_size: float = Field(default=0.0, description="平均文件大小(MB)")
    
    success_rate: float = Field(default=0.0, description="成功率")
    last_update: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

    def update_stats(self, response: VideoResponse):
        """更新统计信息"""
        if response.status == VideoStatus.COMPLETED:
            self.completed_requests += 1
            if response.file_size:
                self.total_file_size += response.file_size
            if response.duration:
                self.total_duration += response.duration
        elif response.status == VideoStatus.FAILED:
            self.failed_requests += 1
        elif response.status == VideoStatus.PROCESSING:
            self.processing_requests += 1
        elif response.status == VideoStatus.PENDING:
            self.pending_requests += 1
        
        # 更新总请求数
        self.total_requests = (self.completed_requests + self.failed_requests + 
                              self.processing_requests + self.pending_requests)
        
        # 计算成功率
        if self.total_requests > 0:
            self.success_rate = self.completed_requests / self.total_requests
        
        # 计算平均文件大小
        if self.completed_requests > 0:
            self.avg_file_size = (self.total_file_size / self.completed_requests) / (1024 * 1024)
        
        # 更新平均处理时间
        if response.processing_time and response.status in [VideoStatus.COMPLETED, VideoStatus.FAILED]:
            total_time = self.avg_processing_time * (self.completed_requests + self.failed_requests - 1)
            total_time += response.processing_time
            self.avg_processing_time = total_time / (self.completed_requests + self.failed_requests)
        
        self.last_update = datetime.now(timezone.utc)

    def get_queue_size(self) -> int:
        """获取队列大小"""
        return self.pending_requests + self.processing_requests
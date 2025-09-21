"""视频处理器

实现完整的视频处理功能：
- RTSP流录制
- 视频片段生成
- 异步处理队列
- 存储管理
- 错误处理和重试
"""

import os
import asyncio
import tempfile
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path
from uuid import uuid4

import cv2
import numpy as np
from loguru import logger

from app.core.config import settings
from app.models.video import (
    VideoRequest, VideoResponse, VideoStatus, 
    VideoMetadata, VideoProcessingStats
)
from app.models.event import EventData
from app.decorators.performance import async_performance_monitor
from app.decorators.retry import async_retry, circuit_breaker
from infrastructure.kafka.kafka_manager import kafka_manager


class RTSPStreamCapture:
    """RTSP流捕获器"""
    
    def __init__(self, rtsp_url: str, timeout: int = 10):
        self.rtsp_url = rtsp_url
        self.timeout = timeout
        self.cap: Optional[cv2.VideoCapture] = None
        self.is_connected = False
        self.frame_buffer = []
        self.max_buffer_size = 1000  # 最大缓冲帧数
        self.fps = 25
        self.frame_width = 1920
        self.frame_height = 1080
    
    async def connect(self) -> bool:
        """连接RTSP流"""
        try:
            # 在线程池中执行阻塞操作
            loop = asyncio.get_event_loop()
            self.cap = await loop.run_in_executor(
                None, self._create_capture
            )
            
            if self.cap and self.cap.isOpened():
                # 获取流信息
                self.fps = self.cap.get(cv2.CAP_PROP_FPS) or 25
                self.frame_width = int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 1920)
                self.frame_height = int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 1080)
                
                self.is_connected = True
                logger.info(f"RTSP stream connected: {self.rtsp_url}",
                           fps=self.fps,
                           resolution=f"{self.frame_width}x{self.frame_height}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to connect RTSP stream {self.rtsp_url}: {e}")
            return False
    
    def _create_capture(self) -> cv2.VideoCapture:
        """创建视频捕获对象"""
        cap = cv2.VideoCapture(self.rtsp_url)
        
        # 设置缓冲区大小
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        
        # 设置超时
        cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, self.timeout * 1000)
        cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 5000)
        
        return cap
    
    async def capture_frames(self, duration: float, start_offset: float = 0) -> List[np.ndarray]:
        """捕获指定时长的帧"""
        if not self.is_connected or not self.cap:
            raise RuntimeError("RTSP stream not connected")
        
        frames = []
        target_frame_count = int(duration * self.fps)
        skip_frames = int(start_offset * self.fps)
        
        try:
            # 跳过开始的帧
            for _ in range(skip_frames):
                ret, _ = self.cap.read()
                if not ret:
                    break
            
            # 捕获目标帧
            for _ in range(target_frame_count):
                ret, frame = self.cap.read()
                if not ret:
                    logger.warning("Failed to read frame from RTSP stream")
                    break
                
                frames.append(frame.copy())
                
                # 控制帧率
                await asyncio.sleep(1.0 / self.fps)
            
            logger.info(f"Captured {len(frames)} frames from RTSP stream")
            return frames
            
        except Exception as e:
            logger.error(f"Error capturing frames: {e}")
            raise
    
    async def disconnect(self):
        """断开连接"""
        if self.cap:
            self.cap.release()
            self.cap = None
        
        self.is_connected = False
        self.frame_buffer.clear()
        logger.info(f"RTSP stream disconnected: {self.rtsp_url}")


class VideoEncoder:
    """视频编码器"""
    
    def __init__(self):
        self.supported_codecs = ['h264', 'h265', 'vp9']
        self.quality_settings = {
            '480p': (854, 480),
            '720p': (1280, 720),
            '1080p': (1920, 1080),
            '4K': (3840, 2160)
        }
    
    async def encode_video(self, 
                          frames: List[np.ndarray],
                          output_path: str,
                          fps: float = 25,
                          quality: str = '720p',
                          codec: str = 'h264') -> VideoMetadata:
        """编码视频"""
        if not frames:
            raise ValueError("No frames to encode")
        
        # 获取输出尺寸
        target_width, target_height = self.quality_settings.get(quality, (1280, 720))
        
        # 设置编码器
        fourcc = self._get_fourcc(codec)
        
        try:
            # 在线程池中执行编码
            loop = asyncio.get_event_loop()
            metadata = await loop.run_in_executor(
                None, self._encode_frames, 
                frames, output_path, fps, target_width, target_height, fourcc
            )
            
            return metadata
            
        except Exception as e:
            logger.error(f"Video encoding failed: {e}")
            raise
    
    def _get_fourcc(self, codec: str) -> int:
        """获取编码器FourCC"""
        codec_map = {
            'h264': cv2.VideoWriter_fourcc(*'H264'),
            'h265': cv2.VideoWriter_fourcc(*'H265'),
            'vp9': cv2.VideoWriter_fourcc(*'VP90'),
            'mp4v': cv2.VideoWriter_fourcc(*'mp4v')
        }
        
        return codec_map.get(codec.lower(), cv2.VideoWriter_fourcc(*'H264'))
    
    def _encode_frames(self, frames: List[np.ndarray], output_path: str,
                      fps: float, width: int, height: int, fourcc: int) -> VideoMetadata:
        """编码帧序列"""
        # 创建输出目录
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # 创建视频写入器
        writer = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
        
        if not writer.isOpened():
            raise RuntimeError(f"Failed to open video writer for {output_path}")
        
        try:
            for frame in frames:
                # 调整帧大小
                if frame.shape[:2] != (height, width):
                    frame = cv2.resize(frame, (width, height))
                
                writer.write(frame)
            
            writer.release()
            
            # 获取文件信息
            file_size = os.path.getsize(output_path)
            duration = len(frames) / fps
            
            # 创建元数据
            metadata = VideoMetadata(
                file_path=output_path,
                file_name=os.path.basename(output_path),
                file_size=file_size,
                duration=duration,
                width=width,
                height=height,
                fps=fps,
                codec=fourcc,
                format='mp4',
                event_id='',  # 将在调用处设置
                stake_num='',  # 将在调用处设置
                event_time=datetime.now(timezone.utc)
            )
            
            logger.info(f"Video encoded successfully: {output_path}",
                       duration=duration,
                       file_size_mb=file_size / 1024 / 1024,
                       resolution=f"{width}x{height}")
            
            return metadata
            
        except Exception as e:
            writer.release()
            # 清理失败的文件
            if os.path.exists(output_path):
                os.remove(output_path)
            raise


class VideoProcessor:
    """视频处理器"""
    
    def __init__(self):
        self.encoder = VideoEncoder()
        self.processing_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.active_captures: Dict[str, RTSPStreamCapture] = {}
        self.processing_stats = VideoProcessingStats()
        self.is_running = False
        self.worker_tasks: List[asyncio.Task] = []
        
        # 配置
        self.max_workers = settings.performance.task_worker_count
        self.storage_path = Path(settings.video.storage_path)
        self.temp_path = Path(settings.video.temp_path)
        
        # 创建目录
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.temp_path.mkdir(parents=True, exist_ok=True)
        
        logger.info("VideoProcessor initialized",
                   max_workers=self.max_workers,
                   storage_path=str(self.storage_path),
                   temp_path=str(self.temp_path))
    
    async def start(self):
        """启动视频处理器"""
        if not self.is_running:
            self.is_running = True
            
            # 启动工作线程
            for i in range(self.max_workers):
                task = asyncio.create_task(self._worker(f"worker-{i}"))
                self.worker_tasks.append(task)
            
            # 启动Kafka消费者
            await self._setup_kafka_consumer()
            
            logger.info(f"VideoProcessor started with {self.max_workers} workers")
    
    async def stop(self):
        """停止视频处理器"""
        self.is_running = False
        
        # 停止所有工作任务
        for task in self.worker_tasks:
            task.cancel()
        
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        self.worker_tasks.clear()
        
        # 断开所有RTSP连接
        for capture in self.active_captures.values():
            await capture.disconnect()
        self.active_captures.clear()
        
        logger.info("VideoProcessor stopped")
    
    async def _setup_kafka_consumer(self):
        """设置Kafka消费者"""
        await kafka_manager.create_consumer(
            topics=[settings.kafka.video_topic],
            consumer_name="video_processor",
            message_handler=self._handle_video_request
        )
    
    async def _handle_video_request(self, message):
        """处理视频请求消息"""
        try:
            # 解析视频请求
            if isinstance(message.value, dict) and 'data' in message.value:
                request_data = message.value['data']
            else:
                request_data = message.value
            
            video_request = VideoRequest(**request_data)
            
            # 添加到处理队列
            await self.processing_queue.put(video_request)
            
            logger.info(f"Video request queued: {video_request.request_id}")
            
        except Exception as e:
            logger.error(f"Error handling video request message: {e}")
    
    @async_performance_monitor("video.process_request")
    async def process_video_request(self, request: VideoRequest) -> VideoResponse:
        """处理视频请求"""
        response = VideoResponse(
            request_id=request.request_id,
            event_id=request.event_id,
            status=VideoStatus.PROCESSING
        )
        
        start_time = datetime.now(timezone.utc)
        
        try:
            response.mark_processing()
            
            # 生成输出文件路径
            output_filename = self._generate_filename(request)
            output_path = self.storage_path / output_filename
            
            # 处理视频
            await self._process_video(request, str(output_path))
            
            # 生成视频URL
            video_url = f"{settings.video.base_url}/{output_filename}"
            
            # 获取文件信息
            file_size = output_path.stat().st_size
            
            # 获取视频时长
            duration = request.get_total_duration()
            
            processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            response.mark_completed(
                video_url=video_url,
                file_path=str(output_path),
                file_size=file_size,
                duration=duration,
                processing_time=processing_time
            )
            
            # 更新统计
            self.processing_stats.update_stats(response)
            
            logger.info(f"Video processing completed: {request.request_id}",
                       processing_time=processing_time,
                       file_size_mb=file_size / 1024 / 1024)
            
            return response
            
        except Exception as e:
            processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            response.mark_failed(str(e), processing_time)
            
            # 更新统计
            self.processing_stats.update_stats(response)
            
            logger.error(f"Video processing failed: {request.request_id}: {e}")
            return response
    
    @async_retry(max_attempts=3, base_delay=2.0)
    @circuit_breaker(failure_threshold=5, recovery_timeout=60)
    async def _process_video(self, request: VideoRequest, output_path: str):
        """处理视频的核心逻辑"""
        # 获取或创建RTSP捕获器
        capture = await self._get_rtsp_capture(request.rtsp_url)
        
        try:
            # 计算捕获参数
            start_offset = 0  # 从事件时间开始
            duration = request.get_total_duration()
            
            # 捕获帧
            frames = await capture.capture_frames(duration, start_offset)
            
            if not frames:
                raise RuntimeError("No frames captured from RTSP stream")
            
            # 编码视频
            metadata = await self.encoder.encode_video(
                frames=frames,
                output_path=output_path,
                fps=request.fps,
                quality=request.quality.value,
                codec=request.codec.value
            )
            
            # 设置元数据
            metadata.event_id = request.event_id
            metadata.stake_num = request.stake_num
            metadata.event_time = request.event_time
            
            logger.info(f"Video processed successfully: {output_path}")
            
        except Exception as e:
            logger.error(f"Error processing video for request {request.request_id}: {e}")
            raise
    
    async def _get_rtsp_capture(self, rtsp_url: str) -> RTSPStreamCapture:
        """获取或创建RTSP捕获器"""
        if rtsp_url not in self.active_captures:
            capture = RTSPStreamCapture(rtsp_url, settings.video.rtsp_timeout)
            
            if await capture.connect():
                self.active_captures[rtsp_url] = capture
            else:
                raise RuntimeError(f"Failed to connect to RTSP stream: {rtsp_url}")
        
        return self.active_captures[rtsp_url]
    
    def _generate_filename(self, request: VideoRequest) -> str:
        """生成视频文件名"""
        timestamp = request.event_time.strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid4())[:8]
        return f"{request.stake_num}_{request.event_id}_{timestamp}_{unique_id}.mp4"
    
    async def _worker(self, worker_name: str):
        """工作线程"""
        logger.info(f"Video worker started: {worker_name}")
        
        while self.is_running:
            try:
                # 获取处理请求
                request = await asyncio.wait_for(
                    self.processing_queue.get(),
                    timeout=5.0
                )
                
                # 处理请求
                response = await self.process_video_request(request)
                
                # 发送处理结果到Kafka
                await self._send_processing_result(response)
                
                # 标记任务完成
                self.processing_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in video worker {worker_name}: {e}")
                await asyncio.sleep(1)
        
        logger.info(f"Video worker stopped: {worker_name}")
    
    async def _send_processing_result(self, response: VideoResponse):
        """发送处理结果"""
        try:
            result_data = {
                'request_id': response.request_id,
                'event_id': response.event_id,
                'status': response.status.value,
                'video_url': response.video_url,
                'file_path': response.file_path,
                'file_size': response.file_size,
                'duration': response.duration,
                'processing_time': response.processing_time,
                'error_message': response.error_message,
                'completed_at': response.completed_at.isoformat() if response.completed_at else None
            }
            
            await kafka_manager.send_message(
                topic="video-results",
                message=result_data,
                key=response.event_id
            )
            
        except Exception as e:
            logger.error(f"Failed to send video processing result: {e}")
    
    async def create_video_from_event(self, event: EventData) -> VideoRequest:
        """从事件创建视频请求"""
        # 生成RTSP URL（这里需要根据实际情况映射桩号到RTSP URL）
        rtsp_url = self._get_rtsp_url_for_stake(event.stakeNum)
        
        request = VideoRequest(
            request_id=str(uuid4()),
            event_id=event.alarmID,
            stake_num=event.stakeNum,
            rtsp_url=rtsp_url,
            event_time=event.get_event_time_datetime(),
            before_seconds=settings.video.video_before_seconds,
            after_seconds=settings.video.video_after_seconds
        )
        
        # 添加到处理队列
        await self.processing_queue.put(request)
        
        logger.info(f"Video request created for event: {event.alarmID}")
        return request
    
    def _get_rtsp_url_for_stake(self, stake_num: str) -> str:
        """根据桩号获取RTSP URL"""
        # 这里应该根据实际的摄像头配置映射桩号到RTSP URL
        # 示例映射
        stake_rtsp_mapping = {
            'K0+900': 'rtsp://admin:password@192.168.1.100:554/stream1',
            'K0+800': 'rtsp://admin:password@192.168.1.101:554/stream1',
            'K1+000': 'rtsp://admin:password@192.168.1.102:554/stream1',
            'K1+100': 'rtsp://admin:password@192.168.1.103:554/stream1',
            'K1+200': 'rtsp://admin:password@192.168.1.104:554/stream1'
        }
        
        rtsp_url = stake_rtsp_mapping.get(stake_num)
        if not rtsp_url:
            # 默认RTSP URL或抛出异常
            logger.warning(f"No RTSP URL configured for stake: {stake_num}")
            rtsp_url = f"rtsp://admin:password@192.168.1.100:554/stream1"  # 默认URL
        
        return rtsp_url
    
    async def get_processing_stats(self) -> Dict[str, Any]:
        """获取处理统计"""
        stats = self.processing_stats.dict()
        
        # 添加队列信息
        stats['queue_size'] = self.processing_queue.qsize()
        stats['active_captures'] = len(self.active_captures)
        stats['worker_count'] = len(self.worker_tasks)
        stats['is_running'] = self.is_running
        
        return stats
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        return {
            'service': 'VideoProcessor',
            'status': 'healthy' if self.is_running else 'stopped',
            'queue_size': self.processing_queue.qsize(),
            'active_captures': len(self.active_captures),
            'worker_count': len(self.worker_tasks),
            'processing_stats': self.processing_stats.dict()
        }
    
    async def cleanup_old_videos(self, max_age_days: int = 7):
        """清理旧视频文件"""
        try:
            cutoff_time = datetime.now() - timedelta(days=max_age_days)
            deleted_count = 0
            total_size = 0
            
            for video_file in self.storage_path.glob("*.mp4"):
                file_stat = video_file.stat()
                file_time = datetime.fromtimestamp(file_stat.st_mtime)
                
                if file_time < cutoff_time:
                    file_size = file_stat.st_size
                    video_file.unlink()
                    deleted_count += 1
                    total_size += file_size
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old video files",
                           total_size_mb=total_size / 1024 / 1024)
            
            return {
                'deleted_count': deleted_count,
                'total_size_mb': total_size / 1024 / 1024
            }
            
        except Exception as e:
            logger.error(f"Error cleaning up old videos: {e}")
            return {'error': str(e)}
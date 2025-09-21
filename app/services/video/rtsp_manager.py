"""RTSP流管理器

管理RTSP视频流的连接和录制：
- RTSP连接管理
- 流状态监控
- 自动重连机制
- 多路流并发处理
- 连接池管理
"""

import asyncio
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass
from enum import Enum
from urllib.parse import urlparse
from threading import Lock

import cv2
import numpy as np
from loguru import logger

from app.core.config import settings
from app.decorators.performance import async_performance_monitor
from app.decorators.retry import async_retry, circuit_breaker


class RTSPStatus(Enum):
    """RTSP连接状态"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECORDING = "recording"
    ERROR = "error"
    RECONNECTING = "reconnecting"


@dataclass
class RTSPStreamInfo:
    """RTSP流信息"""
    url: str
    stake_num: str
    description: str = ""
    backup_url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    timeout: int = 10
    retry_attempts: int = 3
    quality: str = "720p"
    fps: int = 25
    
    def get_authenticated_url(self) -> str:
        """获取带认证的URL"""
        if self.username and self.password:
            parsed = urlparse(self.url)
            return f"{parsed.scheme}://{self.username}:{self.password}@{parsed.netloc}{parsed.path}"
        return self.url


@dataclass
class RTSPConnectionStats:
    """RTSP连接统计"""
    total_connections: int = 0
    successful_connections: int = 0
    failed_connections: int = 0
    reconnections: int = 0
    total_frames_captured: int = 0
    total_bytes_received: int = 0
    avg_fps: float = 0.0
    last_frame_time: Optional[datetime] = None
    connection_uptime: float = 0.0
    error_count: int = 0
    last_error: Optional[str] = None
    last_error_time: Optional[datetime] = None


class RTSPConnection:
    """RTSP连接管理器"""
    
    def __init__(self, stream_info: RTSPStreamInfo):
        self.stream_info = stream_info
        self.status = RTSPStatus.DISCONNECTED
        self.capture: Optional[cv2.VideoCapture] = None
        self.stats = RTSPConnectionStats()
        
        # 连接管理
        self.connection_time: Optional[datetime] = None
        self.last_frame_time: Optional[datetime] = None
        self.frame_count = 0
        self.error_count = 0
        
        # 重连配置
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5.0
        self.current_reconnect_attempt = 0
        
        # 帧率监控
        self.fps_window = 30  # 计算FPS的窗口大小
        self.frame_times = []
        
        # 锁
        self._lock = Lock()
    
    async def connect(self) -> bool:
        """连接RTSP流"""
        if self.status in [RTSPStatus.CONNECTED, RTSPStatus.CONNECTING]:
            return True
        
        self.status = RTSPStatus.CONNECTING
        self.stats.total_connections += 1
        
        try:
            # 尝试主URL
            if await self._try_connect(self.stream_info.get_authenticated_url()):
                return True
            
            # 尝试备用URL
            if self.stream_info.backup_url:
                backup_info = RTSPStreamInfo(
                    url=self.stream_info.backup_url,
                    stake_num=self.stream_info.stake_num,
                    username=self.stream_info.username,
                    password=self.stream_info.password
                )
                if await self._try_connect(backup_info.get_authenticated_url()):
                    logger.warning(f"Using backup URL for {self.stream_info.stake_num}")
                    return True
            
            self.status = RTSPStatus.ERROR
            self.stats.failed_connections += 1
            return False
            
        except Exception as e:
            logger.error(f"Error connecting to RTSP stream {self.stream_info.stake_num}: {e}")
            self.status = RTSPStatus.ERROR
            self.stats.failed_connections += 1
            self._record_error(str(e))
            return False
    
    async def _try_connect(self, url: str) -> bool:
        """尝试连接指定URL"""
        try:
            # 在线程池中执行OpenCV操作
            loop = asyncio.get_event_loop()
            capture = await loop.run_in_executor(
                None, self._create_capture, url
            )
            
            if capture and capture.isOpened():
                self.capture = capture
                self.status = RTSPStatus.CONNECTED
                self.connection_time = datetime.now(timezone.utc)
                self.stats.successful_connections += 1
                self.current_reconnect_attempt = 0
                
                logger.info(f"RTSP connected: {self.stream_info.stake_num}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to connect to {url}: {e}")
            return False
    
    def _create_capture(self, url: str) -> Optional[cv2.VideoCapture]:
        """创建VideoCapture对象"""
        try:
            capture = cv2.VideoCapture(url)
            
            # 设置缓冲区大小
            capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            
            # 设置超时
            capture.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, self.stream_info.timeout * 1000)
            capture.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, self.stream_info.timeout * 1000)
            
            # 测试读取一帧
            ret, frame = capture.read()
            if ret and frame is not None:
                return capture
            else:
                capture.release()
                return None
                
        except Exception as e:
            logger.error(f"Error creating capture: {e}")
            return None
    
    async def disconnect(self):
        """断开连接"""
        with self._lock:
            if self.capture:
                try:
                    self.capture.release()
                except Exception as e:
                    logger.error(f"Error releasing capture: {e}")
                finally:
                    self.capture = None
            
            self.status = RTSPStatus.DISCONNECTED
            
            # 更新统计
            if self.connection_time:
                uptime = (datetime.now(timezone.utc) - self.connection_time).total_seconds()
                self.stats.connection_uptime += uptime
                self.connection_time = None
    
    async def read_frame(self) -> Tuple[bool, Optional[np.ndarray]]:
        """读取帧"""
        if not self.is_connected():
            return False, None
        
        try:
            # 在线程池中执行读取操作
            loop = asyncio.get_event_loop()
            ret, frame = await loop.run_in_executor(
                None, self._read_frame_sync
            )
            
            if ret and frame is not None:
                self._update_frame_stats()
                return True, frame
            else:
                # 读取失败，可能需要重连
                await self._handle_read_error()
                return False, None
                
        except Exception as e:
            logger.error(f"Error reading frame from {self.stream_info.stake_num}: {e}")
            await self._handle_read_error()
            return False, None
    
    def _read_frame_sync(self) -> Tuple[bool, Optional[np.ndarray]]:
        """同步读取帧"""
        if not self.capture or not self.capture.isOpened():
            return False, None
        
        return self.capture.read()
    
    def _update_frame_stats(self):
        """更新帧统计"""
        current_time = datetime.now(timezone.utc)
        
        with self._lock:
            self.frame_count += 1
            self.stats.total_frames_captured += 1
            self.last_frame_time = current_time
            self.stats.last_frame_time = current_time
            
            # 更新FPS计算
            self.frame_times.append(current_time)
            
            # 保持窗口大小
            if len(self.frame_times) > self.fps_window:
                self.frame_times.pop(0)
            
            # 计算FPS
            if len(self.frame_times) >= 2:
                time_span = (self.frame_times[-1] - self.frame_times[0]).total_seconds()
                if time_span > 0:
                    self.stats.avg_fps = (len(self.frame_times) - 1) / time_span
    
    async def _handle_read_error(self):
        """处理读取错误"""
        self.error_count += 1
        self.stats.error_count += 1
        
        if self.error_count >= 3:  # 连续3次错误后重连
            logger.warning(f"Multiple read errors for {self.stream_info.stake_num}, reconnecting")
            await self.reconnect()
    
    async def reconnect(self) -> bool:
        """重新连接"""
        if self.current_reconnect_attempt >= self.max_reconnect_attempts:
            logger.error(f"Max reconnect attempts reached for {self.stream_info.stake_num}")
            self.status = RTSPStatus.ERROR
            return False
        
        self.current_reconnect_attempt += 1
        self.stats.reconnections += 1
        self.status = RTSPStatus.RECONNECTING
        
        logger.info(f"Reconnecting to {self.stream_info.stake_num} (attempt {self.current_reconnect_attempt})")
        
        # 断开当前连接
        await self.disconnect()
        
        # 等待重连延迟
        await asyncio.sleep(self.reconnect_delay)
        
        # 尝试重新连接
        return await self.connect()
    
    def _record_error(self, error_message: str):
        """记录错误"""
        self.stats.last_error = error_message
        self.stats.last_error_time = datetime.now(timezone.utc)
        self.stats.error_count += 1
    
    def is_connected(self) -> bool:
        """检查是否已连接"""
        return (self.status == RTSPStatus.CONNECTED and 
                self.capture is not None and 
                self.capture.isOpened())
    
    def is_healthy(self) -> bool:
        """检查连接是否健康"""
        if not self.is_connected():
            return False
        
        # 检查最近是否有帧
        if self.last_frame_time:
            time_since_last_frame = (datetime.now(timezone.utc) - self.last_frame_time).total_seconds()
            return time_since_last_frame < 30  # 30秒内有帧认为健康
        
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """获取连接统计"""
        with self._lock:
            stats_dict = {
                "stake_num": self.stream_info.stake_num,
                "status": self.status.value,
                "is_connected": self.is_connected(),
                "is_healthy": self.is_healthy(),
                "frame_count": self.frame_count,
                "error_count": self.error_count,
                "reconnect_attempt": self.current_reconnect_attempt,
                "connection_time": self.connection_time.isoformat() if self.connection_time else None,
                "last_frame_time": self.last_frame_time.isoformat() if self.last_frame_time else None,
                "stats": {
                    "total_connections": self.stats.total_connections,
                    "successful_connections": self.stats.successful_connections,
                    "failed_connections": self.stats.failed_connections,
                    "reconnections": self.stats.reconnections,
                    "total_frames_captured": self.stats.total_frames_captured,
                    "avg_fps": round(self.stats.avg_fps, 2),
                    "connection_uptime": self.stats.connection_uptime,
                    "error_count": self.stats.error_count,
                    "last_error": self.stats.last_error,
                    "last_error_time": self.stats.last_error_time.isoformat() if self.stats.last_error_time else None
                }
            }
            
            return stats_dict


class RTSPManager:
    """RTSP流管理器"""
    
    def __init__(self, max_connections: int = 96):
        self.max_connections = max_connections
        self.connections: Dict[str, RTSPConnection] = {}
        self.stream_configs: Dict[str, RTSPStreamInfo] = {}
        
        # 管理任务
        self.health_check_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        # 回调函数
        self.connection_callbacks: List[Callable] = []
        self.error_callbacks: List[Callable] = []
        
        # 统计
        self.global_stats = {
            "total_streams": 0,
            "active_connections": 0,
            "healthy_connections": 0,
            "error_connections": 0,
            "total_frames": 0,
            "avg_fps": 0.0
        }
    
    async def start(self):
        """启动RTSP管理器"""
        if self.is_running:
            return
        
        self.is_running = True
        
        # 启动健康检查任务
        self.health_check_task = asyncio.create_task(self._health_check_loop())
        
        logger.info(f"RTSPManager started with max {self.max_connections} connections")
    
    async def stop(self):
        """停止RTSP管理器"""
        if not self.is_running:
            return
        
        self.is_running = False
        
        # 停止健康检查任务
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
        
        # 断开所有连接
        for connection in self.connections.values():
            await connection.disconnect()
        
        self.connections.clear()
        
        logger.info("RTSPManager stopped")
    
    def add_stream(self, stream_info: RTSPStreamInfo):
        """添加RTSP流配置"""
        if len(self.stream_configs) >= self.max_connections:
            raise ValueError(f"Maximum connections ({self.max_connections}) reached")
        
        self.stream_configs[stream_info.stake_num] = stream_info
        self.global_stats["total_streams"] = len(self.stream_configs)
        
        logger.info(f"Added RTSP stream: {stream_info.stake_num}")
    
    def remove_stream(self, stake_num: str):
        """移除RTSP流配置"""
        if stake_num in self.stream_configs:
            del self.stream_configs[stake_num]
            
            # 断开连接
            if stake_num in self.connections:
                asyncio.create_task(self.connections[stake_num].disconnect())
                del self.connections[stake_num]
            
            self.global_stats["total_streams"] = len(self.stream_configs)
            logger.info(f"Removed RTSP stream: {stake_num}")
    
    async def get_connection(self, stake_num: str) -> Optional[RTSPConnection]:
        """获取RTSP连接"""
        if stake_num not in self.stream_configs:
            logger.error(f"No stream config found for {stake_num}")
            return None
        
        # 如果连接不存在，创建新连接
        if stake_num not in self.connections:
            stream_info = self.stream_configs[stake_num]
            connection = RTSPConnection(stream_info)
            self.connections[stake_num] = connection
        
        connection = self.connections[stake_num]
        
        # 如果连接未建立，尝试连接
        if not connection.is_connected():
            success = await connection.connect()
            if not success:
                logger.error(f"Failed to connect to {stake_num}")
                return None
        
        return connection
    
    async def read_frame(self, stake_num: str) -> Tuple[bool, Optional[np.ndarray]]:
        """从指定流读取帧"""
        connection = await self.get_connection(stake_num)
        if not connection:
            return False, None
        
        return await connection.read_frame()
    
    async def _health_check_loop(self):
        """健康检查循环"""
        while self.is_running:
            try:
                await self._perform_health_check()
                await asyncio.sleep(30)  # 每30秒检查一次
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(30)
    
    async def _perform_health_check(self):
        """执行健康检查"""
        active_count = 0
        healthy_count = 0
        error_count = 0
        total_frames = 0
        total_fps = 0.0
        
        for stake_num, connection in self.connections.items():
            if connection.is_connected():
                active_count += 1
                
                if connection.is_healthy():
                    healthy_count += 1
                else:
                    # 尝试重连不健康的连接
                    logger.warning(f"Connection {stake_num} is unhealthy, attempting reconnect")
                    asyncio.create_task(connection.reconnect())
            else:
                error_count += 1
            
            # 收集统计信息
            total_frames += connection.stats.total_frames_captured
            total_fps += connection.stats.avg_fps
        
        # 更新全局统计
        self.global_stats.update({
            "active_connections": active_count,
            "healthy_connections": healthy_count,
            "error_connections": error_count,
            "total_frames": total_frames,
            "avg_fps": total_fps / max(active_count, 1)
        })
        
        logger.debug(f"Health check: {healthy_count}/{active_count} healthy connections")
    
    def get_all_stats(self) -> Dict[str, Any]:
        """获取所有统计信息"""
        connection_stats = {}
        for stake_num, connection in self.connections.items():
            connection_stats[stake_num] = connection.get_stats()
        
        return {
            "global_stats": self.global_stats,
            "connections": connection_stats,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    def get_connection_status(self, stake_num: str) -> Optional[Dict[str, Any]]:
        """获取指定连接的状态"""
        if stake_num in self.connections:
            return self.connections[stake_num].get_stats()
        return None
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        return {
            "status": "healthy" if self.is_running else "stopped",
            "is_running": self.is_running,
            "total_streams": len(self.stream_configs),
            "active_connections": len(self.connections),
            "global_stats": self.global_stats
        }
    
    def add_connection_callback(self, callback: Callable):
        """添加连接状态回调"""
        self.connection_callbacks.append(callback)
    
    def add_error_callback(self, callback: Callable):
        """添加错误回调"""
        self.error_callbacks.append(callback)


# 全局实例
rtsp_manager = RTSPManager()


# 便捷函数
async def get_rtsp_frame(stake_num: str) -> Tuple[bool, Optional[np.ndarray]]:
    """获取RTSP帧的便捷函数"""
    return await rtsp_manager.read_frame(stake_num)


def add_rtsp_stream(stake_num: str, url: str, backup_url: str = None, 
                   username: str = None, password: str = None):
    """添加RTSP流的便捷函数"""
    stream_info = RTSPStreamInfo(
        url=url,
        stake_num=stake_num,
        backup_url=backup_url,
        username=username,
        password=password
    )
    rtsp_manager.add_stream(stream_info)
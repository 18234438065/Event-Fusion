"""视频存储管理器

管理视频文件的存储和访问：
- 文件存储管理
- URL生成
- 清理策略
- 存储统计
- 云存储支持
"""

import os
import shutil
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
from urllib.parse import urljoin

from loguru import logger

from app.core.config import settings
from app.models.video import VideoRequest, VideoResponse, VideoStatus
from app.decorators.performance import async_performance_monitor


class StorageType(Enum):
    """存储类型"""
    LOCAL = "local"
    S3 = "s3"
    OSS = "oss"
    COS = "cos"
    NFS = "nfs"


@dataclass
class StorageConfig:
    """存储配置"""
    storage_type: StorageType
    base_path: str
    base_url: str
    max_size_gb: float = 100.0
    retention_days: int = 30
    cleanup_interval_hours: int = 6
    
    # 云存储配置
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    bucket: Optional[str] = None
    region: Optional[str] = None
    endpoint: Optional[str] = None


@dataclass
class StorageStats:
    """存储统计"""
    total_files: int = 0
    total_size_bytes: int = 0
    total_size_gb: float = 0.0
    available_space_gb: float = 0.0
    usage_percent: float = 0.0
    oldest_file_date: Optional[datetime] = None
    newest_file_date: Optional[datetime] = None
    files_by_date: Dict[str, int] = None
    
    def __post_init__(self):
        if self.files_by_date is None:
            self.files_by_date = {}
        
        # 计算GB
        self.total_size_gb = self.total_size_bytes / (1024**3)
        
        # 计算使用率
        if self.available_space_gb > 0:
            total_space = self.total_size_gb + self.available_space_gb
            self.usage_percent = (self.total_size_gb / total_space) * 100


class VideoStorageManager:
    """视频存储管理器"""
    
    def __init__(self, config: StorageConfig = None):
        self.config = config or self._get_default_config()
        self.stats = StorageStats()
        
        # 确保存储目录存在
        self._ensure_storage_directory()
        
        # 清理任务
        self.cleanup_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        # 文件锁
        self._file_locks: Dict[str, asyncio.Lock] = {}
        
        logger.info(f"VideoStorageManager initialized", 
                   storage_type=self.config.storage_type.value,
                   base_path=self.config.base_path)
    
    def _get_default_config(self) -> StorageConfig:
        """获取默认配置"""
        return StorageConfig(
            storage_type=StorageType.LOCAL,
            base_path=str(settings.video_processing.storage_path),
            base_url=settings.video_processing.base_url,
            max_size_gb=settings.video_processing.max_storage_gb,
            retention_days=settings.video_processing.retention_days
        )
    
    def _ensure_storage_directory(self):
        """确保存储目录存在"""
        if self.config.storage_type == StorageType.LOCAL:
            Path(self.config.base_path).mkdir(parents=True, exist_ok=True)
            
            # 创建子目录结构
            subdirs = ['videos', 'temp', 'thumbnails', 'metadata']
            for subdir in subdirs:
                Path(self.config.base_path, subdir).mkdir(exist_ok=True)
    
    async def start(self):
        """启动存储管理器"""
        if self.is_running:
            return
        
        self.is_running = True
        
        # 启动清理任务
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        # 初始统计
        await self.update_stats()
        
        logger.info("VideoStorageManager started")
    
    async def stop(self):
        """停止存储管理器"""
        if not self.is_running:
            return
        
        self.is_running = False
        
        # 停止清理任务
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        
        logger.info("VideoStorageManager stopped")
    
    @async_performance_monitor("storage.save_video")
    async def save_video(self, video_data: bytes, request: VideoRequest) -> Tuple[str, str]:
        """保存视频文件
        
        Returns:
            Tuple[str, str]: (file_path, video_url)
        """
        # 生成文件路径
        file_path = self._generate_file_path(request)
        
        # 获取文件锁
        lock = self._get_file_lock(file_path)
        
        async with lock:
            try:
                # 确保目录存在
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                # 写入文件
                await self._write_file(file_path, video_data)
                
                # 生成URL
                video_url = self._generate_video_url(file_path)
                
                # 保存元数据
                await self._save_metadata(file_path, request, len(video_data))
                
                logger.info(f"Video saved: {file_path}", 
                           size_mb=len(video_data) / (1024*1024),
                           request_id=request.request_id)
                
                return file_path, video_url
                
            except Exception as e:
                logger.error(f"Error saving video: {e}")
                # 清理可能的部分文件
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass
                raise
    
    async def delete_video(self, file_path: str) -> bool:
        """删除视频文件"""
        lock = self._get_file_lock(file_path)
        
        async with lock:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    
                    # 删除元数据
                    metadata_path = self._get_metadata_path(file_path)
                    if os.path.exists(metadata_path):
                        os.remove(metadata_path)
                    
                    logger.info(f"Video deleted: {file_path}")
                    return True
                
                return False
                
            except Exception as e:
                logger.error(f"Error deleting video {file_path}: {e}")
                return False
    
    async def get_video_info(self, file_path: str) -> Optional[Dict[str, Any]]:
        """获取视频信息"""
        try:
            if not os.path.exists(file_path):
                return None
            
            # 获取文件统计
            stat = os.stat(file_path)
            
            # 读取元数据
            metadata = await self._load_metadata(file_path)
            
            return {
                "file_path": file_path,
                "file_size": stat.st_size,
                "created_time": datetime.fromtimestamp(stat.st_ctime, timezone.utc),
                "modified_time": datetime.fromtimestamp(stat.st_mtime, timezone.utc),
                "metadata": metadata
            }
            
        except Exception as e:
            logger.error(f"Error getting video info for {file_path}: {e}")
            return None
    
    async def list_videos(self, 
                         start_date: datetime = None,
                         end_date: datetime = None,
                         stake_num: str = None,
                         limit: int = 100) -> List[Dict[str, Any]]:
        """列出视频文件"""
        try:
            videos = []
            video_dir = Path(self.config.base_path) / "videos"
            
            if not video_dir.exists():
                return videos
            
            # 遍历视频文件
            for file_path in video_dir.rglob("*.mp4"):
                try:
                    stat = file_path.stat()
                    created_time = datetime.fromtimestamp(stat.st_ctime, timezone.utc)
                    
                    # 时间过滤
                    if start_date and created_time < start_date:
                        continue
                    if end_date and created_time > end_date:
                        continue
                    
                    # 桩号过滤
                    if stake_num and stake_num not in str(file_path):
                        continue
                    
                    # 加载元数据
                    metadata = await self._load_metadata(str(file_path))
                    
                    video_info = {
                        "file_path": str(file_path),
                        "file_name": file_path.name,
                        "file_size": stat.st_size,
                        "created_time": created_time,
                        "video_url": self._generate_video_url(str(file_path)),
                        "metadata": metadata
                    }
                    
                    videos.append(video_info)
                    
                    # 限制数量
                    if len(videos) >= limit:
                        break
                        
                except Exception as e:
                    logger.error(f"Error processing video file {file_path}: {e}")
                    continue
            
            # 按创建时间排序
            videos.sort(key=lambda x: x['created_time'], reverse=True)
            
            return videos
            
        except Exception as e:
            logger.error(f"Error listing videos: {e}")
            return []
    
    @async_performance_monitor("storage.cleanup")
    async def cleanup_old_files(self) -> Dict[str, Any]:
        """清理过期文件"""
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.config.retention_days)
            
            deleted_count = 0
            deleted_size = 0
            errors = []
            
            video_dir = Path(self.config.base_path) / "videos"
            
            if video_dir.exists():
                for file_path in video_dir.rglob("*.mp4"):
                    try:
                        stat = file_path.stat()
                        created_time = datetime.fromtimestamp(stat.st_ctime, timezone.utc)
                        
                        if created_time < cutoff_date:
                            file_size = stat.st_size
                            
                            # 删除文件
                            if await self.delete_video(str(file_path)):
                                deleted_count += 1
                                deleted_size += file_size
                            else:
                                errors.append(f"Failed to delete {file_path}")
                                
                    except Exception as e:
                        errors.append(f"Error processing {file_path}: {e}")
            
            result = {
                "deleted_count": deleted_count,
                "deleted_size_mb": deleted_size / (1024*1024),
                "cutoff_date": cutoff_date.isoformat(),
                "errors": errors
            }
            
            logger.info(f"Cleanup completed", **result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            return {"error": str(e)}
    
    async def update_stats(self) -> StorageStats:
        """更新存储统计"""
        try:
            total_files = 0
            total_size = 0
            oldest_date = None
            newest_date = None
            files_by_date = {}
            
            video_dir = Path(self.config.base_path) / "videos"
            
            if video_dir.exists():
                for file_path in video_dir.rglob("*.mp4"):
                    try:
                        stat = file_path.stat()
                        created_time = datetime.fromtimestamp(stat.st_ctime, timezone.utc)
                        date_key = created_time.strftime("%Y-%m-%d")
                        
                        total_files += 1
                        total_size += stat.st_size
                        
                        # 更新日期范围
                        if oldest_date is None or created_time < oldest_date:
                            oldest_date = created_time
                        if newest_date is None or created_time > newest_date:
                            newest_date = created_time
                        
                        # 按日期统计
                        files_by_date[date_key] = files_by_date.get(date_key, 0) + 1
                        
                    except Exception as e:
                        logger.error(f"Error processing file {file_path}: {e}")
            
            # 获取可用空间
            available_space = 0
            if self.config.storage_type == StorageType.LOCAL:
                try:
                    statvfs = os.statvfs(self.config.base_path)
                    available_space = statvfs.f_bavail * statvfs.f_frsize
                except Exception as e:
                    logger.error(f"Error getting disk space: {e}")
            
            # 更新统计
            self.stats = StorageStats(
                total_files=total_files,
                total_size_bytes=total_size,
                available_space_gb=available_space / (1024**3),
                oldest_file_date=oldest_date,
                newest_file_date=newest_date,
                files_by_date=files_by_date
            )
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Error updating storage stats: {e}")
            return self.stats
    
    def _generate_file_path(self, request: VideoRequest) -> str:
        """生成文件路径"""
        # 按日期组织目录结构
        date_str = request.event_time.strftime("%Y/%m/%d")
        
        # 文件名格式: {stake_num}_{event_id}_{timestamp}.mp4
        timestamp = request.event_time.strftime("%H%M%S")
        filename = f"{request.stake_num}_{request.event_id}_{timestamp}.mp4"
        
        return os.path.join(
            self.config.base_path,
            "videos",
            date_str,
            filename
        )
    
    def _generate_video_url(self, file_path: str) -> str:
        """生成视频URL"""
        # 计算相对路径
        rel_path = os.path.relpath(file_path, self.config.base_path)
        
        # 生成URL
        return urljoin(self.config.base_url, rel_path.replace(os.sep, '/'))
    
    def _get_metadata_path(self, file_path: str) -> str:
        """获取元数据文件路径"""
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        metadata_dir = os.path.join(self.config.base_path, "metadata")
        return os.path.join(metadata_dir, f"{base_name}.json")
    
    async def _save_metadata(self, file_path: str, request: VideoRequest, file_size: int):
        """保存元数据"""
        try:
            import json
            
            metadata = {
                "request_id": request.request_id,
                "event_id": request.event_id,
                "stake_num": request.stake_num,
                "rtsp_url": request.rtsp_url,
                "event_time": request.event_time.isoformat(),
                "before_seconds": request.before_seconds,
                "after_seconds": request.after_seconds,
                "quality": request.quality.value,
                "codec": request.codec.value,
                "fps": request.fps,
                "file_size": file_size,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            
            metadata_path = self._get_metadata_path(file_path)
            os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
                
        except Exception as e:
            logger.error(f"Error saving metadata: {e}")
    
    async def _load_metadata(self, file_path: str) -> Optional[Dict[str, Any]]:
        """加载元数据"""
        try:
            import json
            
            metadata_path = self._get_metadata_path(file_path)
            
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    return json.load(f)
            
            return None
            
        except Exception as e:
            logger.error(f"Error loading metadata: {e}")
            return None
    
    async def _write_file(self, file_path: str, data: bytes):
        """写入文件"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._write_file_sync, file_path, data)
    
    def _write_file_sync(self, file_path: str, data: bytes):
        """同步写入文件"""
        with open(file_path, 'wb') as f:
            f.write(data)
    
    def _get_file_lock(self, file_path: str) -> asyncio.Lock:
        """获取文件锁"""
        if file_path not in self._file_locks:
            self._file_locks[file_path] = asyncio.Lock()
        return self._file_locks[file_path]
    
    async def _cleanup_loop(self):
        """清理循环"""
        while self.is_running:
            try:
                # 执行清理
                await self.cleanup_old_files()
                
                # 更新统计
                await self.update_stats()
                
                # 等待下次清理
                await asyncio.sleep(self.config.cleanup_interval_hours * 3600)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(3600)  # 出错后等待1小时
    
    def get_stats(self) -> Dict[str, Any]:
        """获取存储统计"""
        return {
            "config": {
                "storage_type": self.config.storage_type.value,
                "base_path": self.config.base_path,
                "max_size_gb": self.config.max_size_gb,
                "retention_days": self.config.retention_days
            },
            "stats": {
                "total_files": self.stats.total_files,
                "total_size_gb": round(self.stats.total_size_gb, 2),
                "available_space_gb": round(self.stats.available_space_gb, 2),
                "usage_percent": round(self.stats.usage_percent, 2),
                "oldest_file_date": self.stats.oldest_file_date.isoformat() if self.stats.oldest_file_date else None,
                "newest_file_date": self.stats.newest_file_date.isoformat() if self.stats.newest_file_date else None,
                "files_by_date": self.stats.files_by_date
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            # 检查存储空间
            await self.update_stats()
            
            is_healthy = (
                self.is_running and
                self.stats.usage_percent < 90 and  # 使用率不超过90%
                self.stats.available_space_gb > 1   # 至少1GB可用空间
            )
            
            return {
                "status": "healthy" if is_healthy else "unhealthy",
                "is_running": self.is_running,
                "storage_usage_percent": round(self.stats.usage_percent, 2),
                "available_space_gb": round(self.stats.available_space_gb, 2),
                "total_files": self.stats.total_files
            }
            
        except Exception as e:
            logger.error(f"Error in storage health check: {e}")
            return {
                "status": "error",
                "error": str(e)
            }


# 全局实例
video_storage_manager = VideoStorageManager()


# 便捷函数
async def save_video_file(video_data: bytes, request: VideoRequest) -> Tuple[str, str]:
    """保存视频文件的便捷函数"""
    return await video_storage_manager.save_video(video_data, request)


async def delete_video_file(file_path: str) -> bool:
    """删除视频文件的便捷函数"""
    return await video_storage_manager.delete_video(file_path)


async def get_storage_stats() -> Dict[str, Any]:
    """获取存储统计的便捷函数"""
    return video_storage_manager.get_stats()
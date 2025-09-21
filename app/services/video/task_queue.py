"""视频任务队列管理器

管理视频处理任务的队列和调度：
- 优先级队列
- 任务调度
- 并发控制
- 任务状态跟踪
- 失败重试
"""

import asyncio
import heapq
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from uuid import uuid4

from loguru import logger

from app.core.config import settings
from app.models.video import VideoRequest, VideoResponse, VideoStatus
from app.decorators.performance import async_performance_monitor


class TaskStatus(Enum):
    """任务状态"""
    PENDING = "pending"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class TaskPriority(Enum):
    """任务优先级"""
    LOW = 1
    NORMAL = 5
    HIGH = 8
    URGENT = 10


@dataclass
class VideoTask:
    """视频处理任务"""
    task_id: str
    request: VideoRequest
    priority: int
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3
    error_message: Optional[str] = None
    result: Optional[VideoResponse] = None
    
    # 用于优先级队列排序
    def __lt__(self, other):
        # 优先级高的先处理（数字大的优先级高）
        if self.priority != other.priority:
            return self.priority > other.priority
        # 优先级相同时，先创建的先处理
        return self.created_at < other.created_at
    
    def can_retry(self) -> bool:
        """检查是否可以重试"""
        return self.retry_count < self.max_retries
    
    def mark_processing(self):
        """标记为处理中"""
        self.status = TaskStatus.PROCESSING
        self.started_at = datetime.now(timezone.utc)
    
    def mark_completed(self, result: VideoResponse):
        """标记为完成"""
        self.status = TaskStatus.COMPLETED
        self.completed_at = datetime.now(timezone.utc)
        self.result = result
    
    def mark_failed(self, error_message: str):
        """标记为失败"""
        self.status = TaskStatus.FAILED
        self.completed_at = datetime.now(timezone.utc)
        self.error_message = error_message
    
    def mark_retry(self):
        """标记为重试"""
        self.retry_count += 1
        self.status = TaskStatus.RETRYING
    
    def get_processing_time(self) -> Optional[float]:
        """获取处理时间（秒）"""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
    
    def get_wait_time(self) -> float:
        """获取等待时间（秒）"""
        start_time = self.started_at or datetime.now(timezone.utc)
        return (start_time - self.created_at).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "task_id": self.task_id,
            "request_id": self.request.request_id,
            "event_id": self.request.event_id,
            "stake_num": self.request.stake_num,
            "priority": self.priority,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "error_message": self.error_message,
            "processing_time": self.get_processing_time(),
            "wait_time": self.get_wait_time()
        }


@dataclass
class QueueStats:
    """队列统计"""
    total_tasks: int = 0
    pending_tasks: int = 0
    processing_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    cancelled_tasks: int = 0
    
    avg_wait_time: float = 0.0
    avg_processing_time: float = 0.0
    success_rate: float = 0.0
    
    tasks_per_minute: float = 0.0
    peak_queue_size: int = 0
    current_queue_size: int = 0
    
    last_update: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def update_success_rate(self):
        """更新成功率"""
        total_finished = self.completed_tasks + self.failed_tasks
        if total_finished > 0:
            self.success_rate = (self.completed_tasks / total_finished) * 100


class VideoTaskQueue:
    """视频任务队列管理器"""
    
    def __init__(self, 
                 max_workers: int = 4,
                 max_queue_size: int = 1000,
                 task_timeout: int = 300):
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size
        self.task_timeout = task_timeout
        
        # 队列和任务管理
        self.priority_queue: List[VideoTask] = []
        self.tasks: Dict[str, VideoTask] = {}
        self.processing_tasks: Dict[str, VideoTask] = {}
        
        # 工作器管理
        self.workers: List[asyncio.Task] = []
        self.worker_semaphore = asyncio.Semaphore(max_workers)
        
        # 统计信息
        self.stats = QueueStats()
        
        # 回调函数
        self.task_callbacks: Dict[TaskStatus, List[Callable]] = {
            TaskStatus.COMPLETED: [],
            TaskStatus.FAILED: [],
            TaskStatus.CANCELLED: []
        }
        
        # 运行状态
        self.is_running = False
        self.stats_task: Optional[asyncio.Task] = None
        
        # 锁
        self._queue_lock = asyncio.Lock()
        
        logger.info(f"VideoTaskQueue initialized", 
                   max_workers=max_workers,
                   max_queue_size=max_queue_size)
    
    async def start(self):
        """启动任务队列"""
        if self.is_running:
            return
        
        self.is_running = True
        
        # 启动工作器
        self.workers = [
            asyncio.create_task(self._worker(f"worker-{i}"))
            for i in range(self.max_workers)
        ]
        
        # 启动统计任务
        self.stats_task = asyncio.create_task(self._stats_updater())
        
        logger.info(f"VideoTaskQueue started with {self.max_workers} workers")
    
    async def stop(self):
        """停止任务队列"""
        if not self.is_running:
            return
        
        self.is_running = False
        
        # 取消所有工作器
        for worker in self.workers:
            worker.cancel()
        
        # 等待工作器完成
        await asyncio.gather(*self.workers, return_exceptions=True)
        
        # 停止统计任务
        if self.stats_task:
            self.stats_task.cancel()
            try:
                await self.stats_task
            except asyncio.CancelledError:
                pass
        
        # 取消所有待处理任务
        async with self._queue_lock:
            for task in self.priority_queue:
                if task.status in [TaskStatus.PENDING, TaskStatus.QUEUED]:
                    task.status = TaskStatus.CANCELLED
        
        logger.info("VideoTaskQueue stopped")
    
    @async_performance_monitor("task_queue.submit_task")
    async def submit_task(self, request: VideoRequest, 
                         priority: int = TaskPriority.NORMAL.value) -> str:
        """提交任务"""
        if not self.is_running:
            await self.start()
        
        # 检查队列大小
        if len(self.priority_queue) >= self.max_queue_size:
            raise ValueError(f"Queue is full (max size: {self.max_queue_size})")
        
        # 创建任务
        task = VideoTask(
            task_id=f"task_{uuid4().hex[:8]}",
            request=request,
            priority=priority,
            status=TaskStatus.QUEUED
        )
        
        async with self._queue_lock:
            # 添加到队列
            heapq.heappush(self.priority_queue, task)
            self.tasks[task.task_id] = task
            
            # 更新统计
            self.stats.total_tasks += 1
            self.stats.pending_tasks += 1
            self.stats.current_queue_size = len(self.priority_queue)
            
            if self.stats.current_queue_size > self.stats.peak_queue_size:
                self.stats.peak_queue_size = self.stats.current_queue_size
        
        logger.info(f"Task submitted: {task.task_id}", 
                   request_id=request.request_id,
                   priority=priority,
                   queue_size=len(self.priority_queue))
        
        return task.task_id
    
    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        task = self.tasks.get(task_id)
        if task:
            return task.to_dict()
        return None
    
    async def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        task = self.tasks.get(task_id)
        if not task:
            return False
        
        if task.status in [TaskStatus.PENDING, TaskStatus.QUEUED]:
            task.status = TaskStatus.CANCELLED
            
            # 从队列中移除
            async with self._queue_lock:
                self.priority_queue = [t for t in self.priority_queue if t.task_id != task_id]
                heapq.heapify(self.priority_queue)
                
                self.stats.cancelled_tasks += 1
                self.stats.pending_tasks -= 1
                self.stats.current_queue_size = len(self.priority_queue)
            
            # 执行回调
            await self._execute_callbacks(TaskStatus.CANCELLED, task)
            
            logger.info(f"Task cancelled: {task_id}")
            return True
        
        return False
    
    async def retry_task(self, task_id: str) -> bool:
        """重试任务"""
        task = self.tasks.get(task_id)
        if not task or not task.can_retry():
            return False
        
        if task.status == TaskStatus.FAILED:
            task.mark_retry()
            task.status = TaskStatus.QUEUED
            
            # 重新加入队列
            async with self._queue_lock:
                heapq.heappush(self.priority_queue, task)
                self.stats.pending_tasks += 1
                self.stats.failed_tasks -= 1
                self.stats.current_queue_size = len(self.priority_queue)
            
            logger.info(f"Task retrying: {task_id}", retry_count=task.retry_count)
            return True
        
        return False
    
    async def _worker(self, worker_name: str):
        """工作器"""
        logger.info(f"Worker {worker_name} started")
        
        while self.is_running:
            try:
                # 获取任务
                task = await self._get_next_task()
                if not task:
                    await asyncio.sleep(0.1)
                    continue
                
                # 处理任务
                await self._process_task(task, worker_name)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in worker {worker_name}: {e}")
                await asyncio.sleep(1)
        
        logger.info(f"Worker {worker_name} stopped")
    
    async def _get_next_task(self) -> Optional[VideoTask]:
        """获取下一个任务"""
        async with self._queue_lock:
            if self.priority_queue:
                task = heapq.heappop(self.priority_queue)
                self.processing_tasks[task.task_id] = task
                
                # 更新统计
                self.stats.pending_tasks -= 1
                self.stats.processing_tasks += 1
                self.stats.current_queue_size = len(self.priority_queue)
                
                return task
        
        return None
    
    async def _process_task(self, task: VideoTask, worker_name: str):
        """处理任务"""
        async with self.worker_semaphore:
            try:
                task.mark_processing()
                
                logger.info(f"Processing task: {task.task_id}", 
                           worker=worker_name,
                           request_id=task.request.request_id)
                
                # 设置超时
                try:
                    # 这里应该调用实际的视频处理逻辑
                    result = await asyncio.wait_for(
                        self._execute_video_processing(task),
                        timeout=self.task_timeout
                    )
                    
                    # 任务完成
                    task.mark_completed(result)
                    
                    # 更新统计
                    async with self._queue_lock:
                        self.stats.processing_tasks -= 1
                        self.stats.completed_tasks += 1
                        del self.processing_tasks[task.task_id]
                    
                    # 执行回调
                    await self._execute_callbacks(TaskStatus.COMPLETED, task)
                    
                    logger.info(f"Task completed: {task.task_id}", 
                               processing_time=task.get_processing_time())
                    
                except asyncio.TimeoutError:
                    raise Exception(f"Task timeout after {self.task_timeout} seconds")
                
            except Exception as e:
                error_message = str(e)
                task.mark_failed(error_message)
                
                # 更新统计
                async with self._queue_lock:
                    self.stats.processing_tasks -= 1
                    self.stats.failed_tasks += 1
                    del self.processing_tasks[task.task_id]
                
                logger.error(f"Task failed: {task.task_id}", 
                           error=error_message,
                           retry_count=task.retry_count)
                
                # 检查是否可以重试
                if task.can_retry():
                    await asyncio.sleep(2 ** task.retry_count)  # 指数退避
                    await self.retry_task(task.task_id)
                else:
                    # 执行失败回调
                    await self._execute_callbacks(TaskStatus.FAILED, task)
    
    async def _execute_video_processing(self, task: VideoTask) -> VideoResponse:
        """执行视频处理（这里应该调用实际的视频处理器）"""
        # 这是一个占位符实现
        # 实际应该调用 VideoProcessor.process_video_request()
        
        # 模拟处理时间
        await asyncio.sleep(1)
        
        # 返回模拟结果
        return VideoResponse(
            request_id=task.request.request_id,
            event_id=task.request.event_id,
            status=VideoStatus.COMPLETED,
            video_url=f"http://example.com/videos/{task.request.event_id}.mp4",
            file_path=f"/app/data/videos/{task.request.event_id}.mp4",
            file_size=1024*1024,  # 1MB
            duration=30.0,
            processing_time=1.0
        )
    
    async def _execute_callbacks(self, status: TaskStatus, task: VideoTask):
        """执行回调函数"""
        try:
            for callback in self.task_callbacks.get(status, []):
                if asyncio.iscoroutinefunction(callback):
                    await callback(task)
                else:
                    callback(task)
        except Exception as e:
            logger.error(f"Error executing callback for {status}: {e}")
    
    async def _stats_updater(self):
        """统计信息更新器"""
        last_completed = 0
        last_update = datetime.now(timezone.utc)
        
        while self.is_running:
            try:
                await asyncio.sleep(60)  # 每分钟更新一次
                
                current_time = datetime.now(timezone.utc)
                time_diff = (current_time - last_update).total_seconds() / 60  # 分钟
                
                # 计算每分钟任务数
                completed_diff = self.stats.completed_tasks - last_completed
                if time_diff > 0:
                    self.stats.tasks_per_minute = completed_diff / time_diff
                
                # 计算平均等待时间和处理时间
                self._calculate_average_times()
                
                # 更新成功率
                self.stats.update_success_rate()
                
                # 更新时间戳
                self.stats.last_update = current_time
                last_completed = self.stats.completed_tasks
                last_update = current_time
                
                logger.debug(f"Queue stats updated", 
                           pending=self.stats.pending_tasks,
                           processing=self.stats.processing_tasks,
                           completed=self.stats.completed_tasks,
                           success_rate=f"{self.stats.success_rate:.1f}%")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error updating stats: {e}")
    
    def _calculate_average_times(self):
        """计算平均时间"""
        completed_tasks = [
            task for task in self.tasks.values() 
            if task.status == TaskStatus.COMPLETED
        ]
        
        if completed_tasks:
            # 计算平均等待时间
            wait_times = [task.get_wait_time() for task in completed_tasks]
            self.stats.avg_wait_time = sum(wait_times) / len(wait_times)
            
            # 计算平均处理时间
            processing_times = [
                task.get_processing_time() for task in completed_tasks
                if task.get_processing_time() is not None
            ]
            if processing_times:
                self.stats.avg_processing_time = sum(processing_times) / len(processing_times)
    
    def add_callback(self, status: TaskStatus, callback: Callable):
        """添加回调函数"""
        self.task_callbacks[status].append(callback)
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """获取队列统计"""
        return {
            "total_tasks": self.stats.total_tasks,
            "pending_tasks": self.stats.pending_tasks,
            "processing_tasks": self.stats.processing_tasks,
            "completed_tasks": self.stats.completed_tasks,
            "failed_tasks": self.stats.failed_tasks,
            "cancelled_tasks": self.stats.cancelled_tasks,
            "success_rate": round(self.stats.success_rate, 2),
            "avg_wait_time": round(self.stats.avg_wait_time, 2),
            "avg_processing_time": round(self.stats.avg_processing_time, 2),
            "tasks_per_minute": round(self.stats.tasks_per_minute, 2),
            "current_queue_size": self.stats.current_queue_size,
            "peak_queue_size": self.stats.peak_queue_size,
            "max_workers": self.max_workers,
            "active_workers": len([w for w in self.workers if not w.done()]),
            "last_update": self.stats.last_update.isoformat()
        }
    
    def get_task_list(self, 
                     status: TaskStatus = None,
                     limit: int = 100) -> List[Dict[str, Any]]:
        """获取任务列表"""
        tasks = list(self.tasks.values())
        
        # 状态过滤
        if status:
            tasks = [task for task in tasks if task.status == status]
        
        # 按创建时间排序
        tasks.sort(key=lambda x: x.created_at, reverse=True)
        
        # 限制数量
        tasks = tasks[:limit]
        
        return [task.to_dict() for task in tasks]
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        is_healthy = (
            self.is_running and
            len(self.workers) == self.max_workers and
            all(not w.done() for w in self.workers) and
            self.stats.current_queue_size < self.max_queue_size * 0.9  # 队列使用率不超过90%
        )
        
        return {
            "status": "healthy" if is_healthy else "unhealthy",
            "is_running": self.is_running,
            "active_workers": len([w for w in self.workers if not w.done()]),
            "max_workers": self.max_workers,
            "queue_usage_percent": (self.stats.current_queue_size / self.max_queue_size) * 100,
            "stats": self.get_queue_stats()
        }


# 全局实例
video_task_queue = VideoTaskQueue()


# 便捷函数
async def submit_video_task(request: VideoRequest, priority: int = TaskPriority.NORMAL.value) -> str:
    """提交视频任务的便捷函数"""
    return await video_task_queue.submit_task(request, priority)


async def get_video_task_status(task_id: str) -> Optional[Dict[str, Any]]:
    """获取视频任务状态的便捷函数"""
    return await video_task_queue.get_task_status(task_id)


async def cancel_video_task(task_id: str) -> bool:
    """取消视频任务的便捷函数"""
    return await video_task_queue.cancel_task(task_id)
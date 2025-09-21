"""事件发送服务

负责将融合后的事件发送到目标接收服务：
- HTTP POST发送
- 重试机制
- 失败处理
- 批量发送
- 监控和统计
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
from uuid import uuid4
from urllib.parse import urljoin

import aiohttp
from loguru import logger
from pydantic import BaseModel, Field

from app.core.config import settings
from app.models.event import EventData, FusedEvent, EventResponse
from app.decorators.performance import async_performance_monitor
from app.decorators.retry import async_retry, circuit_breaker
from infrastructure.kafka.kafka_manager import kafka_manager


class SendResult(BaseModel):
    """发送结果模型"""
    event_id: str = Field(..., description="事件ID")
    success: bool = Field(..., description="是否成功")
    status_code: Optional[int] = Field(default=None, description="HTTP状态码")
    response_data: Optional[Dict[str, Any]] = Field(default=None, description="响应数据")
    error_message: Optional[str] = Field(default=None, description="错误信息")
    retry_count: int = Field(default=0, description="重试次数")
    send_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    processing_time: Optional[float] = Field(default=None, description="处理时间(秒)")


class BatchSendResult(BaseModel):
    """批量发送结果模型"""
    batch_id: str = Field(..., description="批次ID")
    total_count: int = Field(..., description="总数量")
    success_count: int = Field(default=0, description="成功数量")
    failed_count: int = Field(default=0, description="失败数量")
    results: List[SendResult] = Field(default_factory=list, description="详细结果")
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = Field(default=None, description="结束时间")
    total_time: Optional[float] = Field(default=None, description="总耗时(秒)")


class EventSenderStats(BaseModel):
    """事件发送统计模型"""
    total_sent: int = Field(default=0, description="总发送数")
    total_success: int = Field(default=0, description="总成功数")
    total_failed: int = Field(default=0, description="总失败数")
    total_retries: int = Field(default=0, description="总重试数")
    avg_response_time: float = Field(default=0.0, description="平均响应时间(秒)")
    success_rate: float = Field(default=0.0, description="成功率")
    last_send_time: Optional[datetime] = Field(default=None, description="最后发送时间")
    last_success_time: Optional[datetime] = Field(default=None, description="最后成功时间")
    last_failure_time: Optional[datetime] = Field(default=None, description="最后失败时间")
    
    def update_stats(self, result: SendResult):
        """更新统计信息"""
        self.total_sent += 1
        self.last_send_time = result.send_time
        
        if result.success:
            self.total_success += 1
            self.last_success_time = result.send_time
        else:
            self.total_failed += 1
            self.last_failure_time = result.send_time
        
        self.total_retries += result.retry_count
        
        # 更新成功率
        if self.total_sent > 0:
            self.success_rate = self.total_success / self.total_sent * 100
        
        # 更新平均响应时间
        if result.processing_time and self.total_success > 0:
            self.avg_response_time = (
                (self.avg_response_time * (self.total_success - 1) + result.processing_time) 
                / self.total_success
            )


class EventSender:
    """事件发送服务"""
    
    def __init__(self, 
                 receiver_url: str = None,
                 max_concurrent: int = 50,
                 timeout: int = 30,
                 max_retries: int = 3):
        self.receiver_url = receiver_url or settings.external_services.receiver_url
        self.max_concurrent = max_concurrent
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.max_retries = max_retries
        
        # 统计信息
        self.stats = EventSenderStats()
        
        # 发送队列
        self.send_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self.batch_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        
        # 会话管理
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # 回调函数
        self.success_callbacks: List[Callable[[SendResult], None]] = []
        self.failure_callbacks: List[Callable[[SendResult], None]] = []
        
        # 运行状态
        self.is_running = False
        self._tasks: List[asyncio.Task] = []
    
    async def start(self):
        """启动发送服务"""
        if self.is_running:
            return
        
        self.is_running = True
        
        # 创建HTTP会话
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent * 2,
            limit_per_host=self.max_concurrent,
            keepalive_timeout=60,
            enable_cleanup_closed=True
        )
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout,
            headers={
                'User-Agent': 'EventFusionService/1.0',
                'Content-Type': 'application/json'
            }
        )
        
        # 启动处理任务
        self._tasks = [
            asyncio.create_task(self._process_send_queue()),
            asyncio.create_task(self._process_batch_queue()),
            asyncio.create_task(self._stats_reporter())
        ]
        
        logger.info("EventSender started", 
                   receiver_url=self.receiver_url,
                   max_concurrent=self.max_concurrent)
    
    async def stop(self):
        """停止发送服务"""
        if not self.is_running:
            return
        
        self.is_running = False
        
        # 取消所有任务
        for task in self._tasks:
            task.cancel()
        
        # 等待任务完成
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # 关闭HTTP会话
        if self.session:
            await self.session.close()
            self.session = None
        
        logger.info("EventSender stopped")
    
    @async_performance_monitor("event_sender.send_event")
    async def send_event(self, event: FusedEvent, priority: int = 5) -> SendResult:
        """发送单个事件"""
        if not self.is_running:
            await self.start()
        
        # 转换为发送格式
        event_data = self._prepare_event_data(event)
        
        # 执行发送
        result = await self._send_single_event(event_data)
        
        # 更新统计
        self.stats.update_stats(result)
        
        # 执行回调
        await self._execute_callbacks(result)
        
        # 发送到Kafka（用于审计和监控）
        await self._send_to_kafka(event_data, result)
        
        return result
    
    async def send_event_async(self, event: FusedEvent, priority: int = 5):
        """异步发送事件（加入队列）"""
        if not self.is_running:
            await self.start()
        
        try:
            await self.send_queue.put((event, priority), timeout=1.0)
            logger.debug(f"Event {event.data.alarmID} queued for sending")
        except asyncio.TimeoutError:
            logger.error(f"Send queue full, dropping event {event.data.alarmID}")
    
    async def send_batch(self, events: List[FusedEvent], 
                        batch_id: str = None) -> BatchSendResult:
        """批量发送事件"""
        if not batch_id:
            batch_id = f"batch_{uuid4().hex[:8]}"
        
        start_time = datetime.now(timezone.utc)
        
        result = BatchSendResult(
            batch_id=batch_id,
            total_count=len(events),
            start_time=start_time
        )
        
        # 并发发送
        tasks = []
        for event in events:
            task = asyncio.create_task(self.send_event(event))
            tasks.append(task)
        
        # 等待所有任务完成
        send_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        for send_result in send_results:
            if isinstance(send_result, Exception):
                # 处理异常
                error_result = SendResult(
                    event_id="unknown",
                    success=False,
                    error_message=str(send_result)
                )
                result.results.append(error_result)
                result.failed_count += 1
            else:
                result.results.append(send_result)
                if send_result.success:
                    result.success_count += 1
                else:
                    result.failed_count += 1
        
        # 计算总时间
        result.end_time = datetime.now(timezone.utc)
        result.total_time = (result.end_time - result.start_time).total_seconds()
        
        logger.info(f"Batch {batch_id} completed",
                   total=result.total_count,
                   success=result.success_count,
                   failed=result.failed_count,
                   time=result.total_time)
        
        return result
    
    @async_retry(max_attempts=3, base_delay=1.0, exponential_base=2.0)
    @circuit_breaker(failure_threshold=10, recovery_timeout=60)
    async def _send_single_event(self, event_data: Dict[str, Any]) -> SendResult:
        """发送单个事件的核心逻辑"""
        start_time = datetime.now(timezone.utc)
        event_id = event_data.get('alarmID', 'unknown')
        
        async with self.semaphore:
            try:
                # 构建请求URL
                url = urljoin(self.receiver_url, '/receive')
                
                # 发送HTTP请求
                async with self.session.post(url, json=event_data) as response:
                    processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                    
                    # 读取响应
                    response_data = None
                    try:
                        response_data = await response.json()
                    except Exception:
                        response_data = {"text": await response.text()}
                    
                    # 判断是否成功
                    success = 200 <= response.status < 300
                    
                    result = SendResult(
                        event_id=event_id,
                        success=success,
                        status_code=response.status,
                        response_data=response_data,
                        processing_time=processing_time
                    )
                    
                    if success:
                        logger.debug(f"Event {event_id} sent successfully",
                                   status_code=response.status,
                                   processing_time=processing_time)
                    else:
                        logger.warning(f"Event {event_id} send failed",
                                     status_code=response.status,
                                     response=response_data)
                    
                    return result
                    
            except asyncio.TimeoutError:
                processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                logger.error(f"Event {event_id} send timeout")
                return SendResult(
                    event_id=event_id,
                    success=False,
                    error_message="Request timeout",
                    processing_time=processing_time
                )
            
            except Exception as e:
                processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                logger.error(f"Event {event_id} send error: {e}")
                return SendResult(
                    event_id=event_id,
                    success=False,
                    error_message=str(e),
                    processing_time=processing_time
                )
    
    def _prepare_event_data(self, event: FusedEvent) -> Dict[str, Any]:
        """准备发送的事件数据"""
        # 转换为标准格式
        event_data = {
            "alarmID": event.data.alarmID,
            "stakeNum": event.data.stakeNum,
            "reportCount": event.total_reports,
            "eventTime": event.last_update_time.strftime("%Y-%m-%d %H:%M:%S"),
            "eventType": event.data.eventType,
            "direction": event.data.direction,
            "eventLevel": event.data.eventLevel,
            "photoUrl": event.data.photoUrl,
            "videoUrl": event.data.videoUrl,
            "remark1": event.data.remark1,
            "remark2": event.data.remark2,
            "remark3": event.data.remark3
        }
        
        # 添加融合信息
        event_data["fusionInfo"] = {
            "firstReportTime": event.first_report_time.isoformat(),
            "lastReportTime": event.last_report_time.isoformat(),
            "lastUpdateTime": event.last_update_time.isoformat(),
            "totalReports": event.total_reports,
            "isSuppressed": event.is_suppressed,
            "fusionKey": str(event.key)
        }
        
        return event_data
    
    async def _process_send_queue(self):
        """处理发送队列"""
        while self.is_running:
            try:
                # 获取队列中的事件
                event, priority = await asyncio.wait_for(
                    self.send_queue.get(), timeout=1.0
                )
                
                # 发送事件
                await self.send_event(event, priority)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing send queue: {e}")
    
    async def _process_batch_queue(self):
        """处理批量发送队列"""
        while self.is_running:
            try:
                # 获取批量事件
                events, batch_id = await asyncio.wait_for(
                    self.batch_queue.get(), timeout=1.0
                )
                
                # 批量发送
                await self.send_batch(events, batch_id)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing batch queue: {e}")
    
    async def _stats_reporter(self):
        """统计信息报告器"""
        while self.is_running:
            try:
                await asyncio.sleep(60)  # 每分钟报告一次
                
                logger.info("EventSender stats",
                           total_sent=self.stats.total_sent,
                           success_rate=f"{self.stats.success_rate:.2f}%",
                           avg_response_time=f"{self.stats.avg_response_time:.3f}s",
                           queue_size=self.send_queue.qsize())
                
            except Exception as e:
                logger.error(f"Error in stats reporter: {e}")
    
    async def _execute_callbacks(self, result: SendResult):
        """执行回调函数"""
        try:
            if result.success:
                for callback in self.success_callbacks:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(result)
                    else:
                        callback(result)
            else:
                for callback in self.failure_callbacks:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(result)
                    else:
                        callback(result)
        except Exception as e:
            logger.error(f"Error executing callbacks: {e}")
    
    async def _send_to_kafka(self, event_data: Dict[str, Any], result: SendResult):
        """发送到Kafka用于审计"""
        try:
            audit_data = {
                "event_data": event_data,
                "send_result": result.dict(),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            await kafka_manager.send_message(
                topic="event-send-audit",
                message=audit_data
            )
        except Exception as e:
            logger.error(f"Error sending to Kafka: {e}")
    
    def add_success_callback(self, callback: Callable[[SendResult], None]):
        """添加成功回调"""
        self.success_callbacks.append(callback)
    
    def add_failure_callback(self, callback: Callable[[SendResult], None]):
        """添加失败回调"""
        self.failure_callbacks.append(callback)
    
    def get_stats(self) -> EventSenderStats:
        """获取统计信息"""
        return self.stats
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            # 检查接收服务是否可达
            url = urljoin(self.receiver_url, '/health')
            async with self.session.get(url) as response:
                receiver_healthy = 200 <= response.status < 300
        except Exception:
            receiver_healthy = False
        
        return {
            "status": "healthy" if self.is_running and receiver_healthy else "unhealthy",
            "is_running": self.is_running,
            "receiver_healthy": receiver_healthy,
            "receiver_url": self.receiver_url,
            "queue_size": self.send_queue.qsize(),
            "batch_queue_size": self.batch_queue.qsize(),
            "stats": self.stats.dict()
        }


# 全局实例
event_sender = EventSender()


# 便捷函数
async def send_fused_event(event: FusedEvent) -> SendResult:
    """发送融合事件的便捷函数"""
    return await event_sender.send_event(event)


async def send_fused_event_async(event: FusedEvent):
    """异步发送融合事件的便捷函数"""
    await event_sender.send_event_async(event)


async def send_batch_events(events: List[FusedEvent], batch_id: str = None) -> BatchSendResult:
    """批量发送事件的便捷函数"""
    return await event_sender.send_batch(events, batch_id)
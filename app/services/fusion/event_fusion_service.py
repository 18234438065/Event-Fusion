"""事件融合服务

核心事件融合服务，负责：
- 事件接收和预处理
- 事件融合和去重
- 抑制规则应用
- 事件上报和存储
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Set
from uuid import uuid4

from loguru import logger
from app.core.config import settings
from app.models.event import (
    EventData, EventKey, FusedEvent, EventResponse, 
    EventStats, EventType, EventLevel
)
from app.models.fusion import RuleEngine, RuleAction
from app.decorators.performance import async_performance_monitor
from app.decorators.retry import async_retry, circuit_breaker
from app.services.fusion.fusion_engine import FusionEngine
from app.services.fusion.event_cache import EventCache
from app.services.fusion.suppression_manager import SuppressionManager


class EventFusionService:
    """事件融合服务"""
    
    def __init__(self, 
                 event_cache: EventCache,
                 suppression_manager: SuppressionManager,
                 fusion_engine: FusionEngine):
        self.event_cache = event_cache
        self.suppression_manager = suppression_manager
        self.fusion_engine = fusion_engine
        self.rule_engine = RuleEngine.create_default_engine()
        self.stats = EventStats()
        
        # 配置参数
        self.silence_window = settings.fusion_rules.silence_window_seconds
        self.new_event_threshold = settings.fusion_rules.new_event_threshold_seconds
        self.event_expiry = settings.fusion_rules.event_expiry_seconds
        self.stakes = settings.fusion_rules.stakes
        
        # 处理队列
        self.processing_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self.is_processing = False
        
        logger.info("EventFusionService initialized", 
                   silence_window=self.silence_window,
                   new_event_threshold=self.new_event_threshold,
                   stakes_count=len(self.stakes))
    
    async def start(self):
        """启动服务"""
        if not self.is_processing:
            self.is_processing = True
            # 启动事件处理任务
            asyncio.create_task(self._process_events())
            # 启动清理任务
            asyncio.create_task(self._cleanup_expired_events())
            logger.info("EventFusionService started")
    
    async def stop(self):
        """停止服务"""
        self.is_processing = False
        logger.info("EventFusionService stopped")
    
    @async_performance_monitor("event_fusion.process_event")
    @async_retry(max_attempts=3, base_delay=0.1)
    async def process_event(self, event: EventData) -> EventResponse:
        """处理单个事件"""
        start_time = datetime.now(timezone.utc)
        
        try:
            # 验证事件数据
            await self._validate_event(event)
            
            # 添加到处理队列
            await self.processing_queue.put(event)
            
            # 等待处理完成（简化版，实际应该使用回调或其他机制）
            response = await self._process_single_event(event)
            
            # 更新统计信息
            processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.stats.update_stats(event, processing_time, response.action)
            
            return response
            
        except Exception as e:
            logger.error(f"Error processing event {event.alarmID}: {e}", exc_info=True)
            processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            self.stats.update_stats(event, processing_time, "error")
            
            return EventResponse(
                success=False,
                message=f"处理事件失败: {str(e)}",
                event_id=event.alarmID,
                action="error"
            )
    
    async def _process_events(self):
        """事件处理循环"""
        while self.is_processing:
            try:
                # 批量处理事件
                events = []
                try:
                    # 收集一批事件
                    for _ in range(settings.performance.task_batch_size):
                        event = await asyncio.wait_for(
                            self.processing_queue.get(),
                            timeout=settings.performance.task_batch_timeout
                        )
                        events.append(event)
                except asyncio.TimeoutError:
                    pass  # 超时是正常的，处理已收集的事件
                
                if events:
                    await self._process_event_batch(events)
                    
            except Exception as e:
                logger.error(f"Error in event processing loop: {e}", exc_info=True)
                await asyncio.sleep(1)  # 避免快速失败循环
    
    async def _process_event_batch(self, events: List[EventData]):
        """批量处理事件"""
        tasks = []
        for event in events:
            task = asyncio.create_task(self._process_single_event(event))
            tasks.append(task)
        
        # 并发处理所有事件
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _process_single_event(self, event: EventData) -> EventResponse:
        """处理单个事件的核心逻辑"""
        current_time = datetime.now(timezone.utc)
        event_key = EventKey.from_event(event)
        
        logger.info(f"Processing event {event.alarmID}", 
                   event_type=event.eventType,
                   stake_num=event.stakeNum,
                   direction=event.direction)
        
        # 1. 检查抑制规则
        if await self._is_event_suppressed(event, current_time):
            logger.info(f"Event {event.alarmID} suppressed by rules")
            return EventResponse(
                success=True,
                message="事件被抑制规则阻止",
                event_id=event.alarmID,
                action="suppressed"
            )
        
        # 2. 应用融合规则
        context = await self._build_fusion_context(event, current_time)
        action = self.rule_engine.get_final_action(event, context)
        
        if action == RuleAction.SUPPRESS:
            logger.info(f"Event {event.alarmID} suppressed by fusion rules")
            return EventResponse(
                success=True,
                message="事件被融合规则抑制",
                event_id=event.alarmID,
                action="suppressed"
            )
        
        # 3. 处理事件融合
        if action == RuleAction.FUSE:
            response = await self._fuse_event(event, event_key, current_time)
        else:
            response = await self._create_new_event(event, event_key, current_time)
        
        # 4. 应用抑制规则（如果当前事件会触发抑制）
        await self._apply_suppression_rules(event, current_time)
        
        # 5. 处理相邻事件融合
        await self._handle_adjacent_fusion(event, event_key, current_time)
        
        return response
    
    async def _validate_event(self, event: EventData):
        """验证事件数据"""
        # 检查桩号是否有效
        if event.stakeNum not in self.stakes:
            logger.warning(f"Unknown stake number: {event.stakeNum}")
        
        # 检查事件时间是否合理
        event_time = event.get_event_time_datetime()
        current_time = datetime.now(timezone.utc)
        time_diff = abs((current_time - event_time).total_seconds())
        
        if time_diff > 3600:  # 超过1小时
            logger.warning(f"Event time seems unusual: {event.eventTime}", 
                         time_diff_seconds=time_diff)
    
    async def _is_event_suppressed(self, event: EventData, current_time: datetime) -> bool:
        """检查事件是否被抑制"""
        return await self.suppression_manager.is_suppressed(
            event.eventType, event.stakeNum, current_time
        )
    
    async def _build_fusion_context(self, event: EventData, current_time: datetime) -> Dict[str, Any]:
        """构建融合上下文"""
        active_events = await self.event_cache.get_active_events()
        suppression_records = await self.suppression_manager.get_suppression_records()
        
        return {
            'active_events': active_events,
            'suppression_records': suppression_records,
            'stakes': self.stakes,
            'current_time': current_time,
            'silence_window': self.silence_window,
            'new_event_threshold': self.new_event_threshold
        }
    
    async def _fuse_event(self, event: EventData, event_key: EventKey, 
                         current_time: datetime) -> EventResponse:
        """融合事件到现有事件"""
        existing_event = await self.event_cache.get_event(event_key)
        
        if existing_event is None:
            # 如果现有事件不存在，创建新事件
            return await self._create_new_event(event, event_key, current_time)
        
        # 检查是否应该作为新事件处理
        time_since_last = (current_time - existing_event.last_report_time).total_seconds()
        if time_since_last > self.new_event_threshold:
            logger.info(f"Creating new event due to time threshold: {time_since_last}s")
            return await self._create_new_event(event, event_key, current_time)
        
        # 检查静默窗口
        if time_since_last < self.silence_window:
            logger.info(f"Event in silence window: {time_since_last}s")
            # 更新事件但不上报
            existing_event.update_with_new_event(event, current_time)
            await self.event_cache.update_event(event_key, existing_event)
            
            return EventResponse(
                success=True,
                message="事件在静默窗口内，已更新但未上报",
                event_id=event.alarmID,
                action="updated"
            )
        
        # 融合事件并上报
        existing_event.update_with_new_event(event, current_time)
        await self.event_cache.update_event(event_key, existing_event)
        
        # 发送融合后的事件
        await self._send_fused_event(existing_event)
        
        logger.info(f"Event {event.alarmID} fused with existing event", 
                   report_count=existing_event.total_reports)
        
        return EventResponse(
            success=True,
            message=f"事件已融合，累计上报次数: {existing_event.total_reports}",
            event_id=event.alarmID,
            action="fused"
        )
    
    async def _create_new_event(self, event: EventData, event_key: EventKey, 
                               current_time: datetime) -> EventResponse:
        """创建新事件"""
        # 生成新的事件ID
        if not event.alarmID or event.alarmID.startswith("temp_"):
            event.alarmID = event.generate_new_alarm_id()
        
        # 创建融合事件
        fused_event = FusedEvent(
            key=event_key,
            data=event,
            first_report_time=current_time,
            last_report_time=current_time,
            last_update_time=current_time,
            total_reports=1
        )
        
        # 缓存事件
        await self.event_cache.store_event(event_key, fused_event)
        
        # 请求视频生成
        await self._request_video_generation(event)
        
        # 发送事件
        await self._send_new_event(event)
        
        logger.info(f"New event created: {event.alarmID}", 
                   event_type=event.eventType,
                   stake_num=event.stakeNum)
        
        return EventResponse(
            success=True,
            message="新事件已创建并上报",
            event_id=event.alarmID,
            action="created"
        )
    
    async def _apply_suppression_rules(self, event: EventData, current_time: datetime):
        """应用抑制规则"""
        # 严重拥堵抑制规则
        if event.is_severe_traffic_jam():
            suppressed_types = settings.fusion_rules.severe_jam_suppressed_events
            await self.suppression_manager.suppress_events(
                suppressed_types, 
                event.stakeNum, 
                current_time + timedelta(seconds=settings.fusion_rules.suppression_window_seconds),
                scope="adjacent"
            )
            logger.info(f"Applied severe jam suppression for stake {event.stakeNum}")
        
        # 施工占道抑制规则
        if event.is_construction_event():
            suppressed_types = settings.fusion_rules.construction_suppressed_events
            await self.suppression_manager.suppress_events(
                suppressed_types,
                event.stakeNum,
                current_time + timedelta(seconds=settings.fusion_rules.suppression_window_seconds),
                scope="stake"
            )
            logger.info(f"Applied construction suppression for stake {event.stakeNum}")
    
    async def _handle_adjacent_fusion(self, event: EventData, event_key: EventKey, 
                                     current_time: datetime):
        """处理相邻事件融合"""
        if not EventType.is_adjacent_fusion_type(event.eventType):
            return
        
        # 查找相邻的同类型事件
        adjacent_keys = self._find_adjacent_event_keys(event_key)
        
        for adj_key in adjacent_keys:
            adj_event = await self.event_cache.get_event(adj_key)
            if adj_event:
                # 检查时间窗口
                time_diff = (current_time - adj_event.last_update_time).total_seconds()
                if time_diff <= 300:  # 5分钟内
                    # 添加相邻事件关联
                    adj_event.add_adjacent_event(event_key)
                    await self.event_cache.update_event(adj_key, adj_event)
                    
                    # 更新当前事件的相邻关联
                    current_event = await self.event_cache.get_event(event_key)
                    if current_event:
                        current_event.add_adjacent_event(adj_key)
                        await self.event_cache.update_event(event_key, current_event)
                    
                    logger.info(f"Adjacent fusion applied between {event_key.stakeNum} and {adj_key.stakeNum}")
    
    def _find_adjacent_event_keys(self, event_key: EventKey) -> List[EventKey]:
        """查找相邻的事件键"""
        adjacent_keys = []
        
        try:
            current_index = self.stakes.index(event_key.stakeNum)
            
            # 检查前一个桩号
            if current_index > 0:
                prev_stake = self.stakes[current_index - 1]
                adjacent_keys.append(EventKey(
                    eventType=event_key.eventType,
                    direction=event_key.direction,
                    stakeNum=prev_stake
                ))
            
            # 检查后一个桩号
            if current_index < len(self.stakes) - 1:
                next_stake = self.stakes[current_index + 1]
                adjacent_keys.append(EventKey(
                    eventType=event_key.eventType,
                    direction=event_key.direction,
                    stakeNum=next_stake
                ))
                
        except ValueError:
            logger.warning(f"Stake {event_key.stakeNum} not found in configured stakes")
        
        return adjacent_keys
    
    @circuit_breaker(failure_threshold=5, recovery_timeout=30)
    async def _request_video_generation(self, event: EventData):
        """请求视频生成"""
        try:
            # 这里应该发送到视频处理队列
            # 暂时使用日志记录
            logger.info(f"Video generation requested for event {event.alarmID}",
                       stake_num=event.stakeNum,
                       event_time=event.eventTime)
            
            # TODO: 实际实现应该发送到Kafka视频处理主题
            
        except Exception as e:
            logger.error(f"Failed to request video generation: {e}", exc_info=True)
            raise
    
    @circuit_breaker(failure_threshold=3, recovery_timeout=60)
    async def _send_new_event(self, event: EventData):
        """发送新事件"""
        try:
            # 这里应该发送到事件接收服务
            # 暂时使用日志记录
            logger.info(f"Sending new event {event.alarmID} to receiver service",
                       event_type=event.eventType,
                       stake_num=event.stakeNum)
            
            # TODO: 实际实现应该调用事件发送服务
            
        except Exception as e:
            logger.error(f"Failed to send new event: {e}", exc_info=True)
            raise
    
    @circuit_breaker(failure_threshold=3, recovery_timeout=60)
    async def _send_fused_event(self, fused_event: FusedEvent):
        """发送融合事件"""
        try:
            # 这里应该发送到事件接收服务
            # 暂时使用日志记录
            logger.info(f"Sending fused event {fused_event.data.alarmID} to receiver service",
                       event_type=fused_event.data.eventType,
                       stake_num=fused_event.data.stakeNum,
                       report_count=fused_event.total_reports)
            
            # TODO: 实际实现应该调用事件发送服务
            
        except Exception as e:
            logger.error(f"Failed to send fused event: {e}", exc_info=True)
            raise
    
    async def _cleanup_expired_events(self):
        """清理过期事件"""
        while self.is_processing:
            try:
                current_time = datetime.now(timezone.utc)
                expired_count = await self.event_cache.cleanup_expired_events(
                    current_time - timedelta(seconds=self.event_expiry)
                )
                
                if expired_count > 0:
                    logger.info(f"Cleaned up {expired_count} expired events")
                
                # 每分钟清理一次
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def get_stats(self) -> EventStats:
        """获取统计信息"""
        return self.stats
    
    async def get_active_events(self) -> Dict[str, Any]:
        """获取活跃事件"""
        active_events = await self.event_cache.get_active_events()
        return {
            'count': len(active_events),
            'events': {
                f"{key.eventType}_{key.direction}_{key.stakeNum}": {
                    'event_id': event.data.alarmID,
                    'first_report_time': event.first_report_time.isoformat(),
                    'last_report_time': event.last_report_time.isoformat(),
                    'total_reports': event.total_reports,
                    'adjacent_events': len(event.adjacent_events)
                }
                for key, event in active_events.items()
            }
        }
    
    async def get_suppression_status(self) -> Dict[str, Any]:
        """获取抑制状态"""
        return await self.suppression_manager.get_status()
    
    async def reset_stats(self):
        """重置统计信息"""
        self.stats = EventStats()
        logger.info("Event fusion stats reset")
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        return {
            'service': 'EventFusionService',
            'status': 'healthy' if self.is_processing else 'stopped',
            'queue_size': self.processing_queue.qsize(),
            'active_events_count': len(await self.event_cache.get_active_events()),
            'stats': self.stats.dict()
        }
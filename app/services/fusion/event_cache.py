"""事件缓存管理器

负责事件的缓存管理：
- 内存缓存
- Redis分布式缓存
- 事件过期管理
- 高效检索
"""

import json
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Set
from collections import defaultdict

import aioredis
from loguru import logger
from app.core.config import settings
from app.models.event import EventKey, FusedEvent, EventData
from app.decorators.performance import async_performance_monitor
from app.decorators.retry import async_retry


class EventCache:
    """事件缓存管理器"""
    
    def __init__(self, redis_client: Optional[aioredis.Redis] = None):
        self.redis_client = redis_client
        self.local_cache: Dict[EventKey, FusedEvent] = {}
        self.cache_stats = {
            'hits': 0,
            'misses': 0,
            'stores': 0,
            'updates': 0,
            'deletes': 0,
            'expires': 0
        }
        
        # 缓存配置
        self.max_local_cache_size = 10000
        self.local_cache_ttl = 300  # 5分钟
        self.redis_ttl = 3600  # 1小时
        
        # 索引
        self.stake_index: Dict[str, Set[EventKey]] = defaultdict(set)
        self.type_index: Dict[str, Set[EventKey]] = defaultdict(set)
        self.time_index: List[tuple[datetime, EventKey]] = []
        
        logger.info("EventCache initialized", 
                   max_local_size=self.max_local_cache_size,
                   local_ttl=self.local_cache_ttl,
                   redis_ttl=self.redis_ttl)
    
    async def initialize_redis(self):
        """初始化Redis连接"""
        if not self.redis_client:
            try:
                self.redis_client = aioredis.from_url(
                    settings.get_redis_url(),
                    max_connections=settings.redis.max_connections,
                    retry_on_timeout=settings.redis.retry_on_timeout,
                    socket_timeout=settings.redis.socket_timeout,
                    socket_connect_timeout=settings.redis.socket_connect_timeout
                )
                # 测试连接
                await self.redis_client.ping()
                logger.info("Redis connection established")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                self.redis_client = None
    
    @async_performance_monitor("event_cache.get_event")
    async def get_event(self, event_key: EventKey) -> Optional[FusedEvent]:
        """获取事件"""
        # 1. 检查本地缓存
        if event_key in self.local_cache:
            self.cache_stats['hits'] += 1
            event = self.local_cache[event_key]
            
            # 检查是否过期
            if self._is_event_expired(event):
                await self._remove_from_local_cache(event_key)
                self.cache_stats['expires'] += 1
            else:
                return event
        
        # 2. 检查Redis缓存
        if self.redis_client:
            try:
                redis_key = self._get_redis_key(event_key)
                data = await self.redis_client.get(redis_key)
                
                if data:
                    self.cache_stats['hits'] += 1
                    event = self._deserialize_event(data)
                    
                    # 添加到本地缓存
                    await self._add_to_local_cache(event_key, event)
                    return event
            except Exception as e:
                logger.error(f"Redis get error: {e}")
        
        self.cache_stats['misses'] += 1
        return None
    
    @async_performance_monitor("event_cache.store_event")
    @async_retry(max_attempts=3, base_delay=0.1)
    async def store_event(self, event_key: EventKey, event: FusedEvent):
        """存储事件"""
        # 1. 存储到本地缓存
        await self._add_to_local_cache(event_key, event)
        
        # 2. 存储到Redis
        if self.redis_client:
            try:
                redis_key = self._get_redis_key(event_key)
                data = self._serialize_event(event)
                await self.redis_client.setex(redis_key, self.redis_ttl, data)
            except Exception as e:
                logger.error(f"Redis store error: {e}")
        
        self.cache_stats['stores'] += 1
        logger.debug(f"Event stored: {event_key.stakeNum}_{event_key.eventType}")
    
    @async_performance_monitor("event_cache.update_event")
    async def update_event(self, event_key: EventKey, event: FusedEvent):
        """更新事件"""
        # 更新本地缓存
        if event_key in self.local_cache:
            self.local_cache[event_key] = event
            self._update_time_index(event_key, event.last_update_time)
        
        # 更新Redis
        if self.redis_client:
            try:
                redis_key = self._get_redis_key(event_key)
                data = self._serialize_event(event)
                await self.redis_client.setex(redis_key, self.redis_ttl, data)
            except Exception as e:
                logger.error(f"Redis update error: {e}")
        
        self.cache_stats['updates'] += 1
    
    @async_performance_monitor("event_cache.delete_event")
    async def delete_event(self, event_key: EventKey):
        """删除事件"""
        # 从本地缓存删除
        await self._remove_from_local_cache(event_key)
        
        # 从Redis删除
        if self.redis_client:
            try:
                redis_key = self._get_redis_key(event_key)
                await self.redis_client.delete(redis_key)
            except Exception as e:
                logger.error(f"Redis delete error: {e}")
        
        self.cache_stats['deletes'] += 1
    
    async def get_active_events(self) -> Dict[EventKey, FusedEvent]:
        """获取所有活跃事件"""
        current_time = datetime.now(timezone.utc)
        active_events = {}
        
        # 从本地缓存获取
        for key, event in self.local_cache.items():
            if not self._is_event_expired(event, current_time):
                active_events[key] = event
        
        # 如果启用了Redis，还需要从Redis获取其他事件
        if self.redis_client:
            try:
                pattern = self._get_redis_pattern()
                keys = await self.redis_client.keys(pattern)
                
                for redis_key in keys:
                    event_key = self._parse_redis_key(redis_key)
                    if event_key and event_key not in active_events:
                        data = await self.redis_client.get(redis_key)
                        if data:
                            event = self._deserialize_event(data)
                            if not self._is_event_expired(event, current_time):
                                active_events[event_key] = event
            except Exception as e:
                logger.error(f"Error getting active events from Redis: {e}")
        
        return active_events
    
    async def get_events_by_stake(self, stake_num: str) -> List[FusedEvent]:
        """根据桩号获取事件"""
        events = []
        
        # 从索引获取
        if stake_num in self.stake_index:
            for event_key in self.stake_index[stake_num]:
                event = await self.get_event(event_key)
                if event:
                    events.append(event)
        
        return events
    
    async def get_events_by_type(self, event_type: str) -> List[FusedEvent]:
        """根据事件类型获取事件"""
        events = []
        
        # 从索引获取
        if event_type in self.type_index:
            for event_key in self.type_index[event_type]:
                event = await self.get_event(event_key)
                if event:
                    events.append(event)
        
        return events
    
    async def cleanup_expired_events(self, before_time: datetime) -> int:
        """清理过期事件"""
        expired_count = 0
        expired_keys = []
        
        # 检查本地缓存
        for key, event in self.local_cache.items():
            if event.last_update_time < before_time:
                expired_keys.append(key)
        
        # 删除过期事件
        for key in expired_keys:
            await self._remove_from_local_cache(key)
            expired_count += 1
        
        # 清理Redis中的过期事件
        if self.redis_client:
            try:
                pattern = self._get_redis_pattern()
                keys = await self.redis_client.keys(pattern)
                
                for redis_key in keys:
                    data = await self.redis_client.get(redis_key)
                    if data:
                        event = self._deserialize_event(data)
                        if event.last_update_time < before_time:
                            await self.redis_client.delete(redis_key)
                            expired_count += 1
            except Exception as e:
                logger.error(f"Error cleaning up Redis events: {e}")
        
        if expired_count > 0:
            self.cache_stats['expires'] += expired_count
            logger.info(f"Cleaned up {expired_count} expired events")
        
        return expired_count
    
    async def _add_to_local_cache(self, event_key: EventKey, event: FusedEvent):
        """添加到本地缓存"""
        # 检查缓存大小限制
        if len(self.local_cache) >= self.max_local_cache_size:
            await self._evict_oldest_events()
        
        self.local_cache[event_key] = event
        
        # 更新索引
        self.stake_index[event_key.stakeNum].add(event_key)
        self.type_index[event_key.eventType].add(event_key)
        self.time_index.append((event.last_update_time, event_key))
        
        # 保持时间索引排序
        self.time_index.sort(key=lambda x: x[0])
    
    async def _remove_from_local_cache(self, event_key: EventKey):
        """从本地缓存移除"""
        if event_key in self.local_cache:
            del self.local_cache[event_key]
            
            # 更新索引
            self.stake_index[event_key.stakeNum].discard(event_key)
            self.type_index[event_key.eventType].discard(event_key)
            
            # 从时间索引移除
            self.time_index = [(t, k) for t, k in self.time_index if k != event_key]
    
    async def _evict_oldest_events(self, count: int = 100):
        """驱逐最旧的事件"""
        if not self.time_index:
            return
        
        # 按时间排序，移除最旧的事件
        self.time_index.sort(key=lambda x: x[0])
        
        for i in range(min(count, len(self.time_index))):
            _, event_key = self.time_index[i]
            await self._remove_from_local_cache(event_key)
    
    def _update_time_index(self, event_key: EventKey, update_time: datetime):
        """更新时间索引"""
        # 移除旧的时间记录
        self.time_index = [(t, k) for t, k in self.time_index if k != event_key]
        # 添加新的时间记录
        self.time_index.append((update_time, event_key))
        # 保持排序
        self.time_index.sort(key=lambda x: x[0])
    
    def _is_event_expired(self, event: FusedEvent, current_time: datetime = None) -> bool:
        """检查事件是否过期"""
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        time_diff = (current_time - event.last_update_time).total_seconds()
        return time_diff > self.local_cache_ttl
    
    def _get_redis_key(self, event_key: EventKey) -> str:
        """生成Redis键"""
        return f"event:{event_key.eventType}:{event_key.direction}:{event_key.stakeNum}"
    
    def _get_redis_pattern(self) -> str:
        """获取Redis键模式"""
        return "event:*"
    
    def _parse_redis_key(self, redis_key: str) -> Optional[EventKey]:
        """解析Redis键"""
        try:
            if isinstance(redis_key, bytes):
                redis_key = redis_key.decode('utf-8')
            
            parts = redis_key.split(':')
            if len(parts) == 4 and parts[0] == 'event':
                return EventKey(
                    eventType=parts[1],
                    direction=int(parts[2]),
                    stakeNum=parts[3]
                )
        except Exception as e:
            logger.error(f"Error parsing Redis key {redis_key}: {e}")
        
        return None
    
    def _serialize_event(self, event: FusedEvent) -> str:
        """序列化事件"""
        try:
            data = {
                'key': {
                    'eventType': event.key.eventType,
                    'direction': event.key.direction,
                    'stakeNum': event.key.stakeNum
                },
                'data': event.data.dict(),
                'first_report_time': event.first_report_time.isoformat(),
                'last_report_time': event.last_report_time.isoformat(),
                'last_update_time': event.last_update_time.isoformat(),
                'suppressed_until': event.suppressed_until.isoformat() if event.suppressed_until else None,
                'adjacent_events': [
                    {
                        'eventType': key.eventType,
                        'direction': key.direction,
                        'stakeNum': key.stakeNum
                    }
                    for key in event.adjacent_events
                ],
                'total_reports': event.total_reports
            }
            return json.dumps(data, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error serializing event: {e}")
            raise
    
    def _deserialize_event(self, data: str) -> FusedEvent:
        """反序列化事件"""
        try:
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            
            obj = json.loads(data)
            
            # 重建EventKey
            key = EventKey(
                eventType=obj['key']['eventType'],
                direction=obj['key']['direction'],
                stakeNum=obj['key']['stakeNum']
            )
            
            # 重建EventData
            event_data = EventData(**obj['data'])
            
            # 重建相邻事件键
            adjacent_events = [
                EventKey(
                    eventType=adj['eventType'],
                    direction=adj['direction'],
                    stakeNum=adj['stakeNum']
                )
                for adj in obj['adjacent_events']
            ]
            
            # 重建FusedEvent
            return FusedEvent(
                key=key,
                data=event_data,
                first_report_time=datetime.fromisoformat(obj['first_report_time']),
                last_report_time=datetime.fromisoformat(obj['last_report_time']),
                last_update_time=datetime.fromisoformat(obj['last_update_time']),
                suppressed_until=datetime.fromisoformat(obj['suppressed_until']) if obj['suppressed_until'] else None,
                adjacent_events=adjacent_events,
                total_reports=obj['total_reports']
            )
        except Exception as e:
            logger.error(f"Error deserializing event: {e}")
            raise
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        total_requests = self.cache_stats['hits'] + self.cache_stats['misses']
        hit_rate = (self.cache_stats['hits'] / total_requests * 100) if total_requests > 0 else 0
        
        redis_info = {}
        if self.redis_client:
            try:
                redis_info = await self.redis_client.info('memory')
            except Exception as e:
                logger.error(f"Error getting Redis info: {e}")
        
        return {
            'local_cache': {
                'size': len(self.local_cache),
                'max_size': self.max_local_cache_size,
                'usage_percent': (len(self.local_cache) / self.max_local_cache_size * 100) if self.max_local_cache_size > 0 else 0
            },
            'statistics': {
                **self.cache_stats,
                'hit_rate_percent': round(hit_rate, 2),
                'total_requests': total_requests
            },
            'indexes': {
                'stake_index_size': len(self.stake_index),
                'type_index_size': len(self.type_index),
                'time_index_size': len(self.time_index)
            },
            'redis': redis_info
        }
    
    async def clear_cache(self):
        """清空缓存"""
        self.local_cache.clear()
        self.stake_index.clear()
        self.type_index.clear()
        self.time_index.clear()
        
        if self.redis_client:
            try:
                pattern = self._get_redis_pattern()
                keys = await self.redis_client.keys(pattern)
                if keys:
                    await self.redis_client.delete(*keys)
            except Exception as e:
                logger.error(f"Error clearing Redis cache: {e}")
        
        logger.info("Event cache cleared")
    
    async def close(self):
        """关闭缓存"""
        if self.redis_client:
            await self.redis_client.close()
        logger.info("EventCache closed")
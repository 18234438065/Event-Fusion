"""抑制管理器

负责事件抑制规则的管理和执行：
- 抑制规则管理
- 抑制状态跟踪
- 时间窗口管理
- 分布式抑制协调
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Any
from enum import Enum
from collections import defaultdict

import aioredis
from loguru import logger
from app.core.config import settings
from app.models.event import EventType
from app.decorators.performance import async_performance_monitor
from app.decorators.retry import async_retry


class SuppressionScope(str, Enum):
    """抑制范围枚举"""
    GLOBAL = "global"        # 全局抑制
    STAKE = "stake"          # 单个桩号抑制
    ADJACENT = "adjacent"    # 相邻桩号抑制
    REGION = "region"        # 区域抑制


class SuppressionRecord:
    """抑制记录"""
    
    def __init__(self,
                 event_types: List[str],
                 scope: SuppressionScope,
                 target: str,
                 until_time: datetime,
                 triggered_by: str,
                 reason: str):
        self.event_types = event_types
        self.scope = scope
        self.target = target  # 目标桩号或区域
        self.until_time = until_time
        self.triggered_by = triggered_by  # 触发抑制的事件ID
        self.reason = reason
        self.created_at = datetime.now(timezone.utc)
        self.suppressed_count = 0
    
    def is_active(self, current_time: datetime = None) -> bool:
        """检查抑制是否仍然有效"""
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        return current_time < self.until_time
    
    def should_suppress(self, event_type: str, stake_num: str, 
                       stakes: List[str] = None) -> bool:
        """检查是否应该抑制指定事件"""
        if not self.is_active():
            return False
        
        if event_type not in self.event_types:
            return False
        
        if self.scope == SuppressionScope.GLOBAL:
            return True
        elif self.scope == SuppressionScope.STAKE:
            return stake_num == self.target
        elif self.scope == SuppressionScope.ADJACENT:
            if stake_num == self.target:
                return True
            
            # 检查相邻桩号
            if stakes:
                try:
                    target_index = stakes.index(self.target)
                    stake_index = stakes.index(stake_num)
                    return abs(target_index - stake_index) <= 1
                except ValueError:
                    return False
        elif self.scope == SuppressionScope.REGION:
            # 区域抑制逻辑（可以根据需要扩展）
            return stake_num.startswith(self.target)
        
        return False
    
    def increment_suppressed_count(self):
        """增加被抑制事件计数"""
        self.suppressed_count += 1
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'event_types': self.event_types,
            'scope': self.scope.value,
            'target': self.target,
            'until_time': self.until_time.isoformat(),
            'triggered_by': self.triggered_by,
            'reason': self.reason,
            'created_at': self.created_at.isoformat(),
            'suppressed_count': self.suppressed_count,
            'is_active': self.is_active()
        }


class SuppressionManager:
    """抑制管理器"""
    
    def __init__(self, redis_client: Optional[aioredis.Redis] = None):
        self.redis_client = redis_client
        self.local_suppressions: Dict[str, SuppressionRecord] = {}
        self.suppression_stats = {
            'total_suppressions': 0,
            'active_suppressions': 0,
            'suppressed_events': 0,
            'expired_suppressions': 0
        }
        
        # 配置
        self.redis_key_prefix = "suppression:"
        self.cleanup_interval = 60  # 清理间隔（秒）
        
        # 启动清理任务
        self._cleanup_task = None
        
        logger.info("SuppressionManager initialized")
    
    async def initialize_redis(self):
        """初始化Redis连接"""
        if not self.redis_client:
            try:
                self.redis_client = aioredis.from_url(
                    settings.get_redis_url(),
                    max_connections=settings.redis.max_connections
                )
                await self.redis_client.ping()
                logger.info("SuppressionManager Redis connection established")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                self.redis_client = None
    
    async def start(self):
        """启动抑制管理器"""
        if not self._cleanup_task:
            self._cleanup_task = asyncio.create_task(self._cleanup_expired_suppressions())
            logger.info("SuppressionManager started")
    
    async def stop(self):
        """停止抑制管理器"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
        logger.info("SuppressionManager stopped")
    
    @async_performance_monitor("suppression.suppress_events")
    async def suppress_events(self,
                            event_types: List[str],
                            target: str,
                            until_time: datetime,
                            scope: SuppressionScope = SuppressionScope.STAKE,
                            triggered_by: str = "system",
                            reason: str = "automatic suppression"):
        """创建事件抑制"""
        suppression_id = self._generate_suppression_id(scope, target, event_types)
        
        suppression = SuppressionRecord(
            event_types=event_types,
            scope=scope,
            target=target,
            until_time=until_time,
            triggered_by=triggered_by,
            reason=reason
        )
        
        # 存储到本地缓存
        self.local_suppressions[suppression_id] = suppression
        
        # 存储到Redis（如果可用）
        if self.redis_client:
            try:
                await self._store_suppression_to_redis(suppression_id, suppression)
            except Exception as e:
                logger.error(f"Failed to store suppression to Redis: {e}")
        
        self.suppression_stats['total_suppressions'] += 1
        self.suppression_stats['active_suppressions'] += 1
        
        logger.info(f"Suppression created: {suppression_id}",
                   event_types=event_types,
                   scope=scope.value,
                   target=target,
                   until_time=until_time.isoformat(),
                   reason=reason)
    
    @async_performance_monitor("suppression.is_suppressed")
    async def is_suppressed(self,
                          event_type: str,
                          stake_num: str,
                          current_time: datetime = None) -> bool:
        """检查事件是否被抑制"""
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        stakes = settings.fusion_rules.stakes
        
        # 检查本地抑制记录
        for suppression_id, suppression in self.local_suppressions.items():
            if suppression.should_suppress(event_type, stake_num, stakes):
                suppression.increment_suppressed_count()
                self.suppression_stats['suppressed_events'] += 1
                
                logger.debug(f"Event suppressed by local rule: {suppression_id}",
                           event_type=event_type,
                           stake_num=stake_num,
                           reason=suppression.reason)
                return True
        
        # 检查Redis中的抑制记录（如果本地没有找到）
        if self.redis_client:
            try:
                suppressed = await self._check_redis_suppressions(
                    event_type, stake_num, current_time, stakes
                )
                if suppressed:
                    self.suppression_stats['suppressed_events'] += 1
                    return True
            except Exception as e:
                logger.error(f"Error checking Redis suppressions: {e}")
        
        return False
    
    async def remove_suppression(self, suppression_id: str) -> bool:
        """移除抑制规则"""
        removed = False
        
        # 从本地缓存移除
        if suppression_id in self.local_suppressions:
            del self.local_suppressions[suppression_id]
            removed = True
            self.suppression_stats['active_suppressions'] -= 1
        
        # 从Redis移除
        if self.redis_client:
            try:
                redis_key = f"{self.redis_key_prefix}{suppression_id}"
                result = await self.redis_client.delete(redis_key)
                if result > 0:
                    removed = True
            except Exception as e:
                logger.error(f"Error removing suppression from Redis: {e}")
        
        if removed:
            logger.info(f"Suppression removed: {suppression_id}")
        
        return removed
    
    async def get_active_suppressions(self) -> Dict[str, SuppressionRecord]:
        """获取所有活跃的抑制规则"""
        current_time = datetime.now(timezone.utc)
        active_suppressions = {}
        
        # 检查本地抑制
        for suppression_id, suppression in self.local_suppressions.items():
            if suppression.is_active(current_time):
                active_suppressions[suppression_id] = suppression
        
        # 检查Redis抑制（如果需要）
        if self.redis_client:
            try:
                redis_suppressions = await self._get_redis_suppressions()
                for suppression_id, suppression in redis_suppressions.items():
                    if (suppression_id not in active_suppressions and 
                        suppression.is_active(current_time)):
                        active_suppressions[suppression_id] = suppression
            except Exception as e:
                logger.error(f"Error getting Redis suppressions: {e}")
        
        return active_suppressions
    
    async def get_suppression_records(self) -> Dict[str, datetime]:
        """获取抑制记录（用于融合上下文）"""
        records = {}
        active_suppressions = await self.get_active_suppressions()
        
        for suppression_id, suppression in active_suppressions.items():
            records[suppression_id] = suppression.until_time
        
        return records
    
    async def get_status(self) -> Dict[str, Any]:
        """获取抑制管理器状态"""
        active_suppressions = await self.get_active_suppressions()
        
        status = {
            'statistics': self.suppression_stats.copy(),
            'active_suppressions_count': len(active_suppressions),
            'active_suppressions': {
                suppression_id: suppression.to_dict()
                for suppression_id, suppression in active_suppressions.items()
            }
        }
        
        # 更新活跃抑制数量
        status['statistics']['active_suppressions'] = len(active_suppressions)
        
        return status
    
    def _generate_suppression_id(self, scope: SuppressionScope, target: str, 
                                event_types: List[str]) -> str:
        """生成抑制ID"""
        types_str = "_".join(sorted(event_types))
        timestamp = int(datetime.now(timezone.utc).timestamp())
        return f"{scope.value}_{target}_{types_str}_{timestamp}"
    
    async def _store_suppression_to_redis(self, suppression_id: str, 
                                        suppression: SuppressionRecord):
        """存储抑制记录到Redis"""
        redis_key = f"{self.redis_key_prefix}{suppression_id}"
        data = {
            'event_types': suppression.event_types,
            'scope': suppression.scope.value,
            'target': suppression.target,
            'until_time': suppression.until_time.isoformat(),
            'triggered_by': suppression.triggered_by,
            'reason': suppression.reason,
            'created_at': suppression.created_at.isoformat(),
            'suppressed_count': suppression.suppressed_count
        }
        
        # 计算TTL
        ttl = int((suppression.until_time - datetime.now(timezone.utc)).total_seconds())
        if ttl > 0:
            await self.redis_client.setex(redis_key, ttl, json.dumps(data))
    
    async def _check_redis_suppressions(self, event_type: str, stake_num: str,
                                       current_time: datetime, stakes: List[str]) -> bool:
        """检查Redis中的抑制记录"""
        pattern = f"{self.redis_key_prefix}*"
        keys = await self.redis_client.keys(pattern)
        
        for key in keys:
            try:
                data = await self.redis_client.get(key)
                if data:
                    suppression_data = json.loads(data)
                    suppression = self._deserialize_suppression(suppression_data)
                    
                    if suppression.should_suppress(event_type, stake_num, stakes):
                        # 将抑制记录加载到本地缓存
                        suppression_id = key.decode('utf-8').replace(self.redis_key_prefix, '')
                        self.local_suppressions[suppression_id] = suppression
                        return True
            except Exception as e:
                logger.error(f"Error processing Redis suppression key {key}: {e}")
        
        return False
    
    async def _get_redis_suppressions(self) -> Dict[str, SuppressionRecord]:
        """获取Redis中的所有抑制记录"""
        suppressions = {}
        pattern = f"{self.redis_key_prefix}*"
        keys = await self.redis_client.keys(pattern)
        
        for key in keys:
            try:
                data = await self.redis_client.get(key)
                if data:
                    suppression_data = json.loads(data)
                    suppression = self._deserialize_suppression(suppression_data)
                    suppression_id = key.decode('utf-8').replace(self.redis_key_prefix, '')
                    suppressions[suppression_id] = suppression
            except Exception as e:
                logger.error(f"Error deserializing Redis suppression {key}: {e}")
        
        return suppressions
    
    def _deserialize_suppression(self, data: Dict[str, Any]) -> SuppressionRecord:
        """反序列化抑制记录"""
        suppression = SuppressionRecord(
            event_types=data['event_types'],
            scope=SuppressionScope(data['scope']),
            target=data['target'],
            until_time=datetime.fromisoformat(data['until_time']),
            triggered_by=data['triggered_by'],
            reason=data['reason']
        )
        suppression.created_at = datetime.fromisoformat(data['created_at'])
        suppression.suppressed_count = data.get('suppressed_count', 0)
        return suppression
    
    async def _cleanup_expired_suppressions(self):
        """清理过期的抑制记录"""
        while True:
            try:
                current_time = datetime.now(timezone.utc)
                expired_ids = []
                
                # 检查本地抑制记录
                for suppression_id, suppression in self.local_suppressions.items():
                    if not suppression.is_active(current_time):
                        expired_ids.append(suppression_id)
                
                # 移除过期记录
                for suppression_id in expired_ids:
                    del self.local_suppressions[suppression_id]
                    self.suppression_stats['expired_suppressions'] += 1
                    self.suppression_stats['active_suppressions'] -= 1
                
                if expired_ids:
                    logger.info(f"Cleaned up {len(expired_ids)} expired suppressions")
                
                # 等待下次清理
                await asyncio.sleep(self.cleanup_interval)
                
            except Exception as e:
                logger.error(f"Error in suppression cleanup: {e}")
                await asyncio.sleep(self.cleanup_interval)
    
    async def clear_all_suppressions(self):
        """清除所有抑制记录"""
        # 清除本地记录
        count = len(self.local_suppressions)
        self.local_suppressions.clear()
        
        # 清除Redis记录
        if self.redis_client:
            try:
                pattern = f"{self.redis_key_prefix}*"
                keys = await self.redis_client.keys(pattern)
                if keys:
                    await self.redis_client.delete(*keys)
                    count += len(keys)
            except Exception as e:
                logger.error(f"Error clearing Redis suppressions: {e}")
        
        # 重置统计
        self.suppression_stats = {
            'total_suppressions': 0,
            'active_suppressions': 0,
            'suppressed_events': 0,
            'expired_suppressions': 0
        }
        
        logger.info(f"Cleared {count} suppression records")
    
    async def close(self):
        """关闭抑制管理器"""
        await self.stop()
        if self.redis_client:
            await self.redis_client.close()
        logger.info("SuppressionManager closed")


# 导入json用于序列化
import json
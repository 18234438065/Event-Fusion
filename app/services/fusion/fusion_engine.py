"""融合引擎

实现事件融合的核心算法和策略：
- 事件相似度计算
- 融合决策算法
- 智能融合策略
- 性能优化
"""

import math
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass
from enum import Enum

from loguru import logger
from app.core.config import settings
from app.models.event import EventData, EventKey, FusedEvent, EventType, Direction
from app.decorators.performance import performance_monitor


class FusionStrategy(str, Enum):
    """融合策略枚举"""
    STRICT = "strict"              # 严格融合（完全匹配）
    RELAXED = "relaxed"            # 宽松融合（允许部分差异）
    INTELLIGENT = "intelligent"    # 智能融合（基于机器学习）
    ADAPTIVE = "adaptive"          # 自适应融合（动态调整）


class SimilarityMetric(str, Enum):
    """相似度度量枚举"""
    EXACT_MATCH = "exact_match"        # 精确匹配
    TEMPORAL = "temporal"              # 时间相似度
    SPATIAL = "spatial"                # 空间相似度
    SEMANTIC = "semantic"              # 语义相似度
    COMPOSITE = "composite"            # 复合相似度


@dataclass
class FusionScore:
    """融合评分"""
    total_score: float
    temporal_score: float
    spatial_score: float
    semantic_score: float
    confidence: float
    reasons: List[str]
    
    def should_fuse(self, threshold: float = 0.7) -> bool:
        """判断是否应该融合"""
        return self.total_score >= threshold and self.confidence >= 0.6


@dataclass
class FusionCandidate:
    """融合候选"""
    event_key: EventKey
    fused_event: FusedEvent
    score: FusionScore
    distance: float  # 空间距离
    time_diff: float  # 时间差异


class FusionEngine:
    """融合引擎"""
    
    def __init__(self, strategy: FusionStrategy = FusionStrategy.INTELLIGENT):
        self.strategy = strategy
        self.fusion_stats = {
            'total_evaluations': 0,
            'successful_fusions': 0,
            'rejected_fusions': 0,
            'avg_fusion_score': 0.0,
            'strategy_performance': {}
        }
        
        # 融合参数
        self.temporal_weight = 0.3
        self.spatial_weight = 0.4
        self.semantic_weight = 0.3
        self.fusion_threshold = 0.7
        self.confidence_threshold = 0.6
        
        # 缓存
        self.similarity_cache: Dict[Tuple[str, str], float] = {}
        self.distance_cache: Dict[Tuple[str, str], float] = {}
        
        logger.info(f"FusionEngine initialized with strategy: {strategy}")
    
    @performance_monitor("fusion_engine.find_fusion_candidates")
    def find_fusion_candidates(self, 
                             event: EventData,
                             active_events: Dict[EventKey, FusedEvent],
                             max_candidates: int = 10) -> List[FusionCandidate]:
        """查找融合候选事件"""
        candidates = []
        event_key = EventKey.from_event(event)
        
        for existing_key, existing_event in active_events.items():
            # 基本过滤
            if not self._is_potential_candidate(event, existing_event):
                continue
            
            # 计算融合评分
            score = self.calculate_fusion_score(event, existing_event)
            
            if score.total_score > 0.3:  # 最低阈值
                # 计算距离和时间差
                distance = self._calculate_spatial_distance(event_key, existing_key)
                time_diff = self._calculate_time_difference(event, existing_event)
                
                candidate = FusionCandidate(
                    event_key=existing_key,
                    fused_event=existing_event,
                    score=score,
                    distance=distance,
                    time_diff=time_diff
                )
                candidates.append(candidate)
        
        # 按评分排序
        candidates.sort(key=lambda c: c.score.total_score, reverse=True)
        
        # 返回前N个候选
        return candidates[:max_candidates]
    
    @performance_monitor("fusion_engine.calculate_fusion_score")
    def calculate_fusion_score(self, event: EventData, existing_event: FusedEvent) -> FusionScore:
        """计算融合评分"""
        self.fusion_stats['total_evaluations'] += 1
        
        # 计算各维度相似度
        temporal_score = self._calculate_temporal_similarity(event, existing_event)
        spatial_score = self._calculate_spatial_similarity(event, existing_event)
        semantic_score = self._calculate_semantic_similarity(event, existing_event)
        
        # 计算加权总分
        total_score = (
            temporal_score * self.temporal_weight +
            spatial_score * self.spatial_weight +
            semantic_score * self.semantic_weight
        )
        
        # 计算置信度
        confidence = self._calculate_confidence(temporal_score, spatial_score, semantic_score)
        
        # 生成评分原因
        reasons = self._generate_score_reasons(temporal_score, spatial_score, semantic_score)
        
        score = FusionScore(
            total_score=total_score,
            temporal_score=temporal_score,
            spatial_score=spatial_score,
            semantic_score=semantic_score,
            confidence=confidence,
            reasons=reasons
        )
        
        # 更新统计
        self._update_fusion_stats(score)
        
        return score
    
    def should_fuse_events(self, event: EventData, existing_event: FusedEvent) -> bool:
        """判断是否应该融合事件"""
        score = self.calculate_fusion_score(event, existing_event)
        return score.should_fuse(self.fusion_threshold)
    
    def get_best_fusion_candidate(self, 
                                event: EventData,
                                active_events: Dict[EventKey, FusedEvent]) -> Optional[FusionCandidate]:
        """获取最佳融合候选"""
        candidates = self.find_fusion_candidates(event, active_events, max_candidates=5)
        
        if not candidates:
            return None
        
        best_candidate = candidates[0]
        
        # 检查是否满足融合条件
        if best_candidate.score.should_fuse(self.fusion_threshold):
            return best_candidate
        
        return None
    
    def _is_potential_candidate(self, event: EventData, existing_event: FusedEvent) -> bool:
        """检查是否为潜在候选"""
        # 事件类型必须相同
        if event.eventType != existing_event.data.eventType:
            return False
        
        # 方向必须相同
        if event.direction != existing_event.data.direction:
            return False
        
        # 检查时间窗口
        current_time = datetime.now(timezone.utc)
        time_diff = (current_time - existing_event.last_update_time).total_seconds()
        
        if time_diff > settings.fusion_rules.new_event_threshold_seconds:
            return False
        
        return True
    
    def _calculate_temporal_similarity(self, event: EventData, existing_event: FusedEvent) -> float:
        """计算时间相似度"""
        try:
            event_time = event.get_event_time_datetime()
            existing_time = existing_event.data.get_event_time_datetime()
            
            time_diff = abs((event_time - existing_time).total_seconds())
            
            # 时间差越小，相似度越高
            max_time_diff = settings.fusion_rules.new_event_threshold_seconds
            similarity = max(0, 1 - (time_diff / max_time_diff))
            
            return similarity
        except Exception as e:
            logger.error(f"Error calculating temporal similarity: {e}")
            return 0.0
    
    def _calculate_spatial_similarity(self, event: EventData, existing_event: FusedEvent) -> float:
        """计算空间相似度"""
        try:
            event_key = EventKey.from_event(event)
            existing_key = existing_event.key
            
            # 相同桩号，相似度为1
            if event.stakeNum == existing_event.data.stakeNum:
                return 1.0
            
            # 计算桩号距离
            distance = self._calculate_spatial_distance(event_key, existing_key)
            
            # 距离越近，相似度越高
            max_distance = settings.fusion_rules.adjacent_fusion_distance
            similarity = max(0, 1 - (distance / max_distance))
            
            return similarity
        except Exception as e:
            logger.error(f"Error calculating spatial similarity: {e}")
            return 0.0
    
    def _calculate_semantic_similarity(self, event: EventData, existing_event: FusedEvent) -> float:
        """计算语义相似度"""
        try:
            # 事件类型相似度
            type_similarity = 1.0 if event.eventType == existing_event.data.eventType else 0.0
            
            # 方向相似度
            direction_similarity = 1.0 if event.direction == existing_event.data.direction else 0.0
            
            # 事件等级相似度（如果适用）
            level_similarity = 1.0
            if (hasattr(event, 'eventLevel') and hasattr(existing_event.data, 'eventLevel') and
                event.eventLevel and existing_event.data.eventLevel):
                if event.eventLevel == existing_event.data.eventLevel:
                    level_similarity = 1.0
                else:
                    # 计算等级差异
                    level_diff = abs(int(event.eventLevel) - int(existing_event.data.eventLevel))
                    level_similarity = max(0, 1 - (level_diff / 4))  # 最大等级差为4
            
            # 加权平均
            semantic_similarity = (
                type_similarity * 0.5 +
                direction_similarity * 0.3 +
                level_similarity * 0.2
            )
            
            return semantic_similarity
        except Exception as e:
            logger.error(f"Error calculating semantic similarity: {e}")
            return 0.0
    
    def _calculate_spatial_distance(self, key1: EventKey, key2: EventKey) -> float:
        """计算空间距离"""
        cache_key = (key1.stakeNum, key2.stakeNum)
        if cache_key in self.distance_cache:
            return self.distance_cache[cache_key]
        
        try:
            stakes = settings.fusion_rules.stakes
            
            if key1.stakeNum not in stakes or key2.stakeNum not in stakes:
                distance = float('inf')
            else:
                index1 = stakes.index(key1.stakeNum)
                index2 = stakes.index(key2.stakeNum)
                distance = abs(index1 - index2)
            
            # 缓存结果
            self.distance_cache[cache_key] = distance
            
            return distance
        except Exception as e:
            logger.error(f"Error calculating spatial distance: {e}")
            return float('inf')
    
    def _calculate_time_difference(self, event: EventData, existing_event: FusedEvent) -> float:
        """计算时间差异"""
        try:
            event_time = event.get_event_time_datetime()
            existing_time = existing_event.data.get_event_time_datetime()
            return abs((event_time - existing_time).total_seconds())
        except Exception as e:
            logger.error(f"Error calculating time difference: {e}")
            return float('inf')
    
    def _calculate_confidence(self, temporal: float, spatial: float, semantic: float) -> float:
        """计算置信度"""
        # 基于各维度分数的方差计算置信度
        scores = [temporal, spatial, semantic]
        mean_score = sum(scores) / len(scores)
        variance = sum((score - mean_score) ** 2 for score in scores) / len(scores)
        
        # 方差越小，置信度越高
        confidence = max(0, 1 - variance)
        
        # 如果所有分数都很高，增加置信度
        if all(score > 0.8 for score in scores):
            confidence = min(1.0, confidence + 0.2)
        
        return confidence
    
    def _generate_score_reasons(self, temporal: float, spatial: float, semantic: float) -> List[str]:
        """生成评分原因"""
        reasons = []
        
        if temporal > 0.8:
            reasons.append("时间高度相似")
        elif temporal > 0.6:
            reasons.append("时间相似")
        elif temporal < 0.3:
            reasons.append("时间差异较大")
        
        if spatial > 0.8:
            reasons.append("位置高度相似")
        elif spatial > 0.6:
            reasons.append("位置相似")
        elif spatial < 0.3:
            reasons.append("位置差异较大")
        
        if semantic > 0.8:
            reasons.append("语义高度相似")
        elif semantic > 0.6:
            reasons.append("语义相似")
        elif semantic < 0.3:
            reasons.append("语义差异较大")
        
        return reasons
    
    def _update_fusion_stats(self, score: FusionScore):
        """更新融合统计"""
        if score.should_fuse(self.fusion_threshold):
            self.fusion_stats['successful_fusions'] += 1
        else:
            self.fusion_stats['rejected_fusions'] += 1
        
        # 更新平均分数
        total_evals = self.fusion_stats['total_evaluations']
        current_avg = self.fusion_stats['avg_fusion_score']
        new_avg = (current_avg * (total_evals - 1) + score.total_score) / total_evals
        self.fusion_stats['avg_fusion_score'] = new_avg
    
    def optimize_parameters(self, feedback_data: List[Dict[str, Any]]):
        """基于反馈数据优化参数"""
        if not feedback_data:
            return
        
        # 分析成功和失败的融合案例
        successful_cases = [case for case in feedback_data if case.get('success', False)]
        failed_cases = [case for case in feedback_data if not case.get('success', False)]
        
        if successful_cases:
            # 分析成功案例的特征
            avg_temporal = sum(case.get('temporal_score', 0) for case in successful_cases) / len(successful_cases)
            avg_spatial = sum(case.get('spatial_score', 0) for case in successful_cases) / len(successful_cases)
            avg_semantic = sum(case.get('semantic_score', 0) for case in successful_cases) / len(successful_cases)
            
            # 调整权重
            total_weight = avg_temporal + avg_spatial + avg_semantic
            if total_weight > 0:
                self.temporal_weight = avg_temporal / total_weight
                self.spatial_weight = avg_spatial / total_weight
                self.semantic_weight = avg_semantic / total_weight
        
        if failed_cases:
            # 分析失败案例，调整阈值
            avg_failed_score = sum(case.get('total_score', 0) for case in failed_cases) / len(failed_cases)
            if avg_failed_score > self.fusion_threshold:
                self.fusion_threshold = min(0.9, self.fusion_threshold + 0.05)
        
        logger.info("Fusion parameters optimized",
                   temporal_weight=round(self.temporal_weight, 3),
                   spatial_weight=round(self.spatial_weight, 3),
                   semantic_weight=round(self.semantic_weight, 3),
                   fusion_threshold=round(self.fusion_threshold, 3))
    
    def get_fusion_stats(self) -> Dict[str, Any]:
        """获取融合统计信息"""
        total_evals = self.fusion_stats['total_evaluations']
        success_rate = (self.fusion_stats['successful_fusions'] / total_evals * 100) if total_evals > 0 else 0
        
        return {
            **self.fusion_stats,
            'success_rate_percent': round(success_rate, 2),
            'parameters': {
                'temporal_weight': self.temporal_weight,
                'spatial_weight': self.spatial_weight,
                'semantic_weight': self.semantic_weight,
                'fusion_threshold': self.fusion_threshold,
                'confidence_threshold': self.confidence_threshold
            },
            'cache_stats': {
                'similarity_cache_size': len(self.similarity_cache),
                'distance_cache_size': len(self.distance_cache)
            }
        }
    
    def clear_cache(self):
        """清空缓存"""
        self.similarity_cache.clear()
        self.distance_cache.clear()
        logger.info("Fusion engine cache cleared")
    
    def reset_stats(self):
        """重置统计信息"""
        self.fusion_stats = {
            'total_evaluations': 0,
            'successful_fusions': 0,
            'rejected_fusions': 0,
            'avg_fusion_score': 0.0,
            'strategy_performance': {}
        }
        logger.info("Fusion engine stats reset")
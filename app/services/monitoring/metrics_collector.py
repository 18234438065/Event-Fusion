"""指标收集服务

负责收集和管理系统各项指标：
- 业务指标收集
- 性能指标监控
- 系统资源监控
- Prometheus指标导出
- 实时统计
"""

import asyncio
import time
import psutil
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Callable
from collections import defaultdict, deque
from dataclasses import dataclass, field
from threading import Lock

from loguru import logger
from prometheus_client import (
    Counter, Histogram, Gauge, Summary, Info,
    CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
)

from app.core.config import settings
from app.models.event import EventData, FusedEvent, EventStats
from app.decorators.performance import performance_monitor


@dataclass
class MetricPoint:
    """指标数据点"""
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "value": self.value,
            "labels": self.labels
        }


@dataclass
class TimeSeriesMetric:
    """时间序列指标"""
    name: str
    description: str
    unit: str
    points: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    def add_point(self, value: float, labels: Dict[str, str] = None):
        """添加数据点"""
        point = MetricPoint(
            timestamp=datetime.now(timezone.utc),
            value=value,
            labels=labels or {}
        )
        self.points.append(point)
    
    def get_latest(self) -> Optional[MetricPoint]:
        """获取最新数据点"""
        return self.points[-1] if self.points else None
    
    def get_average(self, duration_minutes: int = 5) -> float:
        """获取指定时间内的平均值"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=duration_minutes)
        recent_points = [
            p for p in self.points 
            if p.timestamp >= cutoff_time
        ]
        
        if not recent_points:
            return 0.0
        
        return sum(p.value for p in recent_points) / len(recent_points)
    
    def get_max(self, duration_minutes: int = 5) -> float:
        """获取指定时间内的最大值"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=duration_minutes)
        recent_points = [
            p for p in self.points 
            if p.timestamp >= cutoff_time
        ]
        
        if not recent_points:
            return 0.0
        
        return max(p.value for p in recent_points)


class BusinessMetrics:
    """业务指标"""
    
    def __init__(self):
        # Prometheus指标
        self.events_received = Counter(
            'events_received_total',
            'Total number of events received',
            ['event_type', 'stake_num', 'direction']
        )
        
        self.events_processed = Counter(
            'events_processed_total',
            'Total number of events processed',
            ['action', 'event_type']
        )
        
        self.events_fused = Counter(
            'events_fused_total',
            'Total number of events fused',
            ['event_type', 'stake_num']
        )
        
        self.events_suppressed = Counter(
            'events_suppressed_total',
            'Total number of events suppressed',
            ['event_type', 'suppression_rule']
        )
        
        self.event_processing_time = Histogram(
            'event_processing_duration_seconds',
            'Time spent processing events',
            ['action', 'event_type'],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
        )
        
        self.active_events = Gauge(
            'active_events_count',
            'Number of currently active events',
            ['event_type']
        )
        
        self.fusion_cache_size = Gauge(
            'fusion_cache_size',
            'Size of the fusion cache'
        )
        
        self.video_requests = Counter(
            'video_requests_total',
            'Total number of video processing requests',
            ['status']
        )
        
        self.video_processing_time = Histogram(
            'video_processing_duration_seconds',
            'Time spent processing videos',
            buckets=[1, 5, 10, 30, 60, 120, 300, 600]
        )
        
        self.api_requests = Counter(
            'api_requests_total',
            'Total number of API requests',
            ['method', 'endpoint', 'status_code']
        )
        
        self.api_request_duration = Histogram(
            'api_request_duration_seconds',
            'API request duration',
            ['method', 'endpoint'],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
        )


class SystemMetrics:
    """系统指标"""
    
    def __init__(self):
        # Prometheus指标
        self.cpu_usage = Gauge(
            'system_cpu_usage_percent',
            'System CPU usage percentage'
        )
        
        self.memory_usage = Gauge(
            'system_memory_usage_bytes',
            'System memory usage in bytes'
        )
        
        self.memory_usage_percent = Gauge(
            'system_memory_usage_percent',
            'System memory usage percentage'
        )
        
        self.disk_usage = Gauge(
            'system_disk_usage_bytes',
            'System disk usage in bytes',
            ['device', 'mountpoint']
        )
        
        self.disk_usage_percent = Gauge(
            'system_disk_usage_percent',
            'System disk usage percentage',
            ['device', 'mountpoint']
        )
        
        self.network_bytes_sent = Counter(
            'system_network_bytes_sent_total',
            'Total network bytes sent',
            ['interface']
        )
        
        self.network_bytes_recv = Counter(
            'system_network_bytes_received_total',
            'Total network bytes received',
            ['interface']
        )
        
        self.process_count = Gauge(
            'system_process_count',
            'Number of running processes'
        )
        
        self.load_average = Gauge(
            'system_load_average',
            'System load average',
            ['period']
        )
        
        # 应用特定指标
        self.kafka_lag = Gauge(
            'kafka_consumer_lag',
            'Kafka consumer lag',
            ['topic', 'partition']
        )
        
        self.redis_connections = Gauge(
            'redis_connections_count',
            'Number of Redis connections'
        )
        
        self.elasticsearch_health = Gauge(
            'elasticsearch_health_status',
            'Elasticsearch cluster health status (0=red, 1=yellow, 2=green)'
        )


class MetricsCollector:
    """指标收集器"""
    
    def __init__(self):
        # 指标实例
        self.business_metrics = BusinessMetrics()
        self.system_metrics = SystemMetrics()
        
        # 自定义指标存储
        self.custom_metrics: Dict[str, TimeSeriesMetric] = {}
        self._metrics_lock = Lock()
        
        # 收集器注册表
        self.registry = CollectorRegistry()
        
        # 注册所有指标
        self._register_metrics()
        
        # 收集任务
        self.collection_tasks: List[asyncio.Task] = []
        self.is_collecting = False
        
        # 回调函数
        self.metric_callbacks: Dict[str, List[Callable]] = defaultdict(list)
        
        # 缓存最新的系统信息
        self._last_system_update = 0
        self._system_info_cache = {}
    
    def _register_metrics(self):
        """注册所有Prometheus指标"""
        # 业务指标
        for attr_name in dir(self.business_metrics):
            attr = getattr(self.business_metrics, attr_name)
            if hasattr(attr, '_name'):
                self.registry.register(attr)
        
        # 系统指标
        for attr_name in dir(self.system_metrics):
            attr = getattr(self.system_metrics, attr_name)
            if hasattr(attr, '_name'):
                self.registry.register(attr)
    
    async def start_collection(self):
        """开始指标收集"""
        if self.is_collecting:
            return
        
        self.is_collecting = True
        
        # 启动收集任务
        self.collection_tasks = [
            asyncio.create_task(self._collect_system_metrics()),
            asyncio.create_task(self._collect_application_metrics()),
            asyncio.create_task(self._collect_external_metrics()),
            asyncio.create_task(self._cleanup_old_metrics())
        ]
        
        logger.info("MetricsCollector started")
    
    async def stop_collection(self):
        """停止指标收集"""
        if not self.is_collecting:
            return
        
        self.is_collecting = False
        
        # 取消所有收集任务
        for task in self.collection_tasks:
            task.cancel()
        
        # 等待任务完成
        await asyncio.gather(*self.collection_tasks, return_exceptions=True)
        
        logger.info("MetricsCollector stopped")
    
    @performance_monitor("metrics.collect_system")
    async def _collect_system_metrics(self):
        """收集系统指标"""
        while self.is_collecting:
            try:
                # 限制收集频率
                current_time = time.time()
                if current_time - self._last_system_update < 5:  # 5秒间隔
                    await asyncio.sleep(1)
                    continue
                
                # CPU使用率
                cpu_percent = psutil.cpu_percent(interval=None)
                self.system_metrics.cpu_usage.set(cpu_percent)
                
                # 内存使用
                memory = psutil.virtual_memory()
                self.system_metrics.memory_usage.set(memory.used)
                self.system_metrics.memory_usage_percent.set(memory.percent)
                
                # 磁盘使用
                for partition in psutil.disk_partitions():
                    try:
                        disk_usage = psutil.disk_usage(partition.mountpoint)
                        self.system_metrics.disk_usage.labels(
                            device=partition.device,
                            mountpoint=partition.mountpoint
                        ).set(disk_usage.used)
                        
                        usage_percent = (disk_usage.used / disk_usage.total) * 100
                        self.system_metrics.disk_usage_percent.labels(
                            device=partition.device,
                            mountpoint=partition.mountpoint
                        ).set(usage_percent)
                    except (PermissionError, OSError):
                        continue
                
                # 网络统计
                net_io = psutil.net_io_counters(pernic=True)
                for interface, stats in net_io.items():
                    self.system_metrics.network_bytes_sent.labels(
                        interface=interface
                    )._value._value = stats.bytes_sent
                    
                    self.system_metrics.network_bytes_recv.labels(
                        interface=interface
                    )._value._value = stats.bytes_recv
                
                # 进程数量
                process_count = len(psutil.pids())
                self.system_metrics.process_count.set(process_count)
                
                # 系统负载
                try:
                    load_avg = psutil.getloadavg()
                    self.system_metrics.load_average.labels(period='1m').set(load_avg[0])
                    self.system_metrics.load_average.labels(period='5m').set(load_avg[1])
                    self.system_metrics.load_average.labels(period='15m').set(load_avg[2])
                except (AttributeError, OSError):
                    pass
                
                self._last_system_update = current_time
                
                await asyncio.sleep(5)  # 每5秒收集一次
                
            except Exception as e:
                logger.error(f"Error collecting system metrics: {e}")
                await asyncio.sleep(10)
    
    async def _collect_application_metrics(self):
        """收集应用指标"""
        while self.is_collecting:
            try:
                # 这里可以添加应用特定的指标收集逻辑
                # 例如：数据库连接池状态、缓存命中率等
                
                await asyncio.sleep(10)  # 每10秒收集一次
                
            except Exception as e:
                logger.error(f"Error collecting application metrics: {e}")
                await asyncio.sleep(10)
    
    async def _collect_external_metrics(self):
        """收集外部服务指标"""
        while self.is_collecting:
            try:
                # Kafka指标
                await self._collect_kafka_metrics()
                
                # Redis指标
                await self._collect_redis_metrics()
                
                # Elasticsearch指标
                await self._collect_elasticsearch_metrics()
                
                await asyncio.sleep(30)  # 每30秒收集一次
                
            except Exception as e:
                logger.error(f"Error collecting external metrics: {e}")
                await asyncio.sleep(30)
    
    async def _collect_kafka_metrics(self):
        """收集Kafka指标"""
        try:
            # 这里应该实现Kafka指标收集
            # 例如：消费者延迟、主题分区信息等
            pass
        except Exception as e:
            logger.error(f"Error collecting Kafka metrics: {e}")
    
    async def _collect_redis_metrics(self):
        """收集Redis指标"""
        try:
            # 这里应该实现Redis指标收集
            # 例如：连接数、内存使用、命令统计等
            pass
        except Exception as e:
            logger.error(f"Error collecting Redis metrics: {e}")
    
    async def _collect_elasticsearch_metrics(self):
        """收集Elasticsearch指标"""
        try:
            # 这里应该实现Elasticsearch指标收集
            # 例如：集群健康状态、索引统计等
            pass
        except Exception as e:
            logger.error(f"Error collecting Elasticsearch metrics: {e}")
    
    async def _cleanup_old_metrics(self):
        """清理旧的指标数据"""
        while self.is_collecting:
            try:
                cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
                
                with self._metrics_lock:
                    for metric in self.custom_metrics.values():
                        # 清理24小时前的数据点
                        while (metric.points and 
                               metric.points[0].timestamp < cutoff_time):
                            metric.points.popleft()
                
                await asyncio.sleep(3600)  # 每小时清理一次
                
            except Exception as e:
                logger.error(f"Error cleaning up metrics: {e}")
                await asyncio.sleep(3600)
    
    # 业务指标记录方法
    def record_event_received(self, event: EventData):
        """记录事件接收"""
        self.business_metrics.events_received.labels(
            event_type=event.eventType,
            stake_num=event.stakeNum,
            direction=str(event.direction)
        ).inc()
    
    def record_event_processed(self, action: str, event_type: str, processing_time: float):
        """记录事件处理"""
        self.business_metrics.events_processed.labels(
            action=action,
            event_type=event_type
        ).inc()
        
        self.business_metrics.event_processing_time.labels(
            action=action,
            event_type=event_type
        ).observe(processing_time)
    
    def record_event_fused(self, event_type: str, stake_num: str):
        """记录事件融合"""
        self.business_metrics.events_fused.labels(
            event_type=event_type,
            stake_num=stake_num
        ).inc()
    
    def record_event_suppressed(self, event_type: str, suppression_rule: str):
        """记录事件抑制"""
        self.business_metrics.events_suppressed.labels(
            event_type=event_type,
            suppression_rule=suppression_rule
        ).inc()
    
    def update_active_events(self, event_counts: Dict[str, int]):
        """更新活跃事件数量"""
        for event_type, count in event_counts.items():
            self.business_metrics.active_events.labels(
                event_type=event_type
            ).set(count)
    
    def update_fusion_cache_size(self, size: int):
        """更新融合缓存大小"""
        self.business_metrics.fusion_cache_size.set(size)
    
    def record_video_request(self, status: str, processing_time: float = None):
        """记录视频请求"""
        self.business_metrics.video_requests.labels(status=status).inc()
        
        if processing_time is not None:
            self.business_metrics.video_processing_time.observe(processing_time)
    
    def record_api_request(self, method: str, endpoint: str, 
                          status_code: int, duration: float):
        """记录API请求"""
        self.business_metrics.api_requests.labels(
            method=method,
            endpoint=endpoint,
            status_code=str(status_code)
        ).inc()
        
        self.business_metrics.api_request_duration.labels(
            method=method,
            endpoint=endpoint
        ).observe(duration)
    
    # 自定义指标方法
    def add_custom_metric(self, name: str, description: str, unit: str = ""):
        """添加自定义指标"""
        with self._metrics_lock:
            if name not in self.custom_metrics:
                self.custom_metrics[name] = TimeSeriesMetric(
                    name=name,
                    description=description,
                    unit=unit
                )
    
    def record_custom_metric(self, name: str, value: float, 
                           labels: Dict[str, str] = None):
        """记录自定义指标"""
        with self._metrics_lock:
            if name in self.custom_metrics:
                self.custom_metrics[name].add_point(value, labels)
                
                # 执行回调
                for callback in self.metric_callbacks.get(name, []):
                    try:
                        callback(name, value, labels)
                    except Exception as e:
                        logger.error(f"Error in metric callback: {e}")
    
    def add_metric_callback(self, metric_name: str, callback: Callable):
        """添加指标回调"""
        self.metric_callbacks[metric_name].append(callback)
    
    # 查询方法
    def get_custom_metric(self, name: str) -> Optional[TimeSeriesMetric]:
        """获取自定义指标"""
        with self._metrics_lock:
            return self.custom_metrics.get(name)
    
    def get_all_custom_metrics(self) -> Dict[str, TimeSeriesMetric]:
        """获取所有自定义指标"""
        with self._metrics_lock:
            return self.custom_metrics.copy()
    
    def get_prometheus_metrics(self) -> str:
        """获取Prometheus格式的指标"""
        return generate_latest(self.registry).decode('utf-8')
    
    def get_system_summary(self) -> Dict[str, Any]:
        """获取系统摘要"""
        try:
            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                "cpu_usage_percent": cpu_percent,
                "memory_usage_percent": memory.percent,
                "memory_used_gb": memory.used / (1024**3),
                "memory_total_gb": memory.total / (1024**3),
                "disk_usage_percent": (disk.used / disk.total) * 100,
                "disk_used_gb": disk.used / (1024**3),
                "disk_total_gb": disk.total / (1024**3),
                "process_count": len(psutil.pids()),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting system summary: {e}")
            return {}
    
    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        return {
            "status": "healthy" if self.is_collecting else "stopped",
            "is_collecting": self.is_collecting,
            "active_tasks": len([t for t in self.collection_tasks if not t.done()]),
            "custom_metrics_count": len(self.custom_metrics),
            "system_summary": self.get_system_summary()
        }


# 全局实例
metrics_collector = MetricsCollector()


# 便捷函数
def record_event_metric(event: EventData, action: str, processing_time: float = None):
    """记录事件指标的便捷函数"""
    metrics_collector.record_event_received(event)
    
    if processing_time is not None:
        metrics_collector.record_event_processed(action, event.eventType, processing_time)


def record_fusion_metric(event_type: str, stake_num: str):
    """记录融合指标的便捷函数"""
    metrics_collector.record_event_fused(event_type, stake_num)


def record_suppression_metric(event_type: str, rule: str):
    """记录抑制指标的便捷函数"""
    metrics_collector.record_event_suppressed(event_type, rule)
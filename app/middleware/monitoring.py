"""监控中间件

实现性能监控和指标收集：
- 请求响应时间监控
- 吞吐量统计
- 错误率监控
- 资源使用监控
- 自定义指标收集
"""

import time
import psutil
import asyncio
from typing import Dict, Any, Callable, Optional, List
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass, field

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from loguru import logger
from app.core.config import settings


@dataclass
class RequestMetrics:
    """请求指标"""
    timestamp: datetime
    method: str
    path: str
    status_code: int
    duration: float
    request_size: int
    response_size: int
    client_ip: str
    user_agent: str
    error: Optional[str] = None


@dataclass
class SystemMetrics:
    """系统指标"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    disk_usage_percent: float
    network_io: Dict[str, int] = field(default_factory=dict)
    process_count: int = 0
    open_files: int = 0


class MetricsCollector:
    """指标收集器"""
    
    def __init__(self, max_history: int = 10000):
        self.max_history = max_history
        
        # 请求指标
        self.request_metrics: deque = deque(maxlen=max_history)
        self.request_count = 0
        self.error_count = 0
        
        # 系统指标
        self.system_metrics: deque = deque(maxlen=1000)  # 保留最近1000个系统指标
        
        # 实时统计
        self.current_stats = {
            'requests_per_second': 0.0,
            'avg_response_time': 0.0,
            'error_rate': 0.0,
            'active_requests': 0,
            'total_requests': 0,
            'total_errors': 0
        }
        
        # 按路径统计
        self.path_stats = defaultdict(lambda: {
            'count': 0,
            'total_duration': 0.0,
            'avg_duration': 0.0,
            'min_duration': float('inf'),
            'max_duration': 0.0,
            'error_count': 0,
            'status_codes': defaultdict(int)
        })
        
        # 按状态码统计
        self.status_code_stats = defaultdict(int)
        
        # 时间窗口统计（最近1分钟、5分钟、15分钟）
        self.time_windows = {
            '1m': deque(maxlen=60),
            '5m': deque(maxlen=300),
            '15m': deque(maxlen=900)
        }
        
        # 启动系统监控
        self.system_monitor_task = None
        self.start_system_monitoring()
    
    def start_system_monitoring(self):
        """启动系统监控"""
        if not self.system_monitor_task:
            self.system_monitor_task = asyncio.create_task(self._collect_system_metrics())
    
    async def _collect_system_metrics(self):
        """收集系统指标"""
        while True:
            try:
                # 收集系统指标
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                
                # 网络IO
                network_io = psutil.net_io_counters()
                network_data = {
                    'bytes_sent': network_io.bytes_sent,
                    'bytes_recv': network_io.bytes_recv,
                    'packets_sent': network_io.packets_sent,
                    'packets_recv': network_io.packets_recv
                }
                
                # 进程信息
                process = psutil.Process()
                process_count = len(psutil.pids())
                open_files = len(process.open_files())
                
                metrics = SystemMetrics(
                    timestamp=datetime.now(timezone.utc),
                    cpu_percent=cpu_percent,
                    memory_percent=memory.percent,
                    memory_used_mb=memory.used / 1024 / 1024,
                    disk_usage_percent=disk.percent,
                    network_io=network_data,
                    process_count=process_count,
                    open_files=open_files
                )
                
                self.system_metrics.append(metrics)
                
                # 每30秒收集一次
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Error collecting system metrics: {e}")
                await asyncio.sleep(30)
    
    def record_request(self, metrics: RequestMetrics):
        """记录请求指标"""
        self.request_metrics.append(metrics)
        self.request_count += 1
        
        if metrics.status_code >= 400:
            self.error_count += 1
        
        # 更新路径统计
        path_stat = self.path_stats[metrics.path]
        path_stat['count'] += 1
        path_stat['total_duration'] += metrics.duration
        path_stat['avg_duration'] = path_stat['total_duration'] / path_stat['count']
        path_stat['min_duration'] = min(path_stat['min_duration'], metrics.duration)
        path_stat['max_duration'] = max(path_stat['max_duration'], metrics.duration)
        
        if metrics.status_code >= 400:
            path_stat['error_count'] += 1
        
        path_stat['status_codes'][metrics.status_code] += 1
        
        # 更新状态码统计
        self.status_code_stats[metrics.status_code] += 1
        
        # 更新时间窗口统计
        current_time = time.time()
        for window in self.time_windows.values():
            window.append((current_time, metrics))
        
        # 更新实时统计
        self._update_current_stats()
    
    def _update_current_stats(self):
        """更新实时统计"""
        current_time = time.time()
        
        # 计算最近1分钟的请求数
        recent_requests = [
            m for t, m in self.time_windows['1m']
            if current_time - t <= 60
        ]
        
        self.current_stats['requests_per_second'] = len(recent_requests) / 60.0
        
        if recent_requests:
            total_duration = sum(m.duration for m in recent_requests)
            self.current_stats['avg_response_time'] = total_duration / len(recent_requests)
            
            error_count = sum(1 for m in recent_requests if m.status_code >= 400)
            self.current_stats['error_rate'] = (error_count / len(recent_requests)) * 100
        else:
            self.current_stats['avg_response_time'] = 0.0
            self.current_stats['error_rate'] = 0.0
        
        self.current_stats['total_requests'] = self.request_count
        self.current_stats['total_errors'] = self.error_count
    
    def get_stats(self, time_window: str = '1m') -> Dict[str, Any]:
        """获取统计信息"""
        current_time = time.time()
        window_seconds = {'1m': 60, '5m': 300, '15m': 900}.get(time_window, 60)
        
        # 获取时间窗口内的请求
        window_requests = [
            m for t, m in self.time_windows.get(time_window, self.time_windows['1m'])
            if current_time - t <= window_seconds
        ]
        
        if not window_requests:
            return {
                'time_window': time_window,
                'request_count': 0,
                'error_count': 0,
                'error_rate': 0.0,
                'avg_response_time': 0.0,
                'requests_per_second': 0.0
            }
        
        # 计算统计信息
        total_requests = len(window_requests)
        error_requests = [m for m in window_requests if m.status_code >= 400]
        error_count = len(error_requests)
        
        total_duration = sum(m.duration for m in window_requests)
        avg_response_time = total_duration / total_requests
        
        error_rate = (error_count / total_requests) * 100
        requests_per_second = total_requests / window_seconds
        
        # 响应时间分布
        durations = [m.duration for m in window_requests]
        durations.sort()
        
        percentiles = {}
        if durations:
            percentiles = {
                'p50': durations[int(len(durations) * 0.5)],
                'p90': durations[int(len(durations) * 0.9)],
                'p95': durations[int(len(durations) * 0.95)],
                'p99': durations[int(len(durations) * 0.99)]
            }
        
        # 状态码分布
        status_distribution = defaultdict(int)
        for m in window_requests:
            status_distribution[m.status_code] += 1
        
        return {
            'time_window': time_window,
            'request_count': total_requests,
            'error_count': error_count,
            'error_rate': round(error_rate, 2),
            'avg_response_time': round(avg_response_time * 1000, 2),  # 转换为毫秒
            'requests_per_second': round(requests_per_second, 2),
            'response_time_percentiles': {
                k: round(v * 1000, 2) for k, v in percentiles.items()
            },
            'status_code_distribution': dict(status_distribution),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    def get_path_stats(self) -> Dict[str, Any]:
        """获取路径统计"""
        path_data = {}
        
        for path, stats in self.path_stats.items():
            if stats['count'] > 0:
                path_data[path] = {
                    'request_count': stats['count'],
                    'error_count': stats['error_count'],
                    'error_rate': (stats['error_count'] / stats['count']) * 100,
                    'avg_response_time': round(stats['avg_duration'] * 1000, 2),
                    'min_response_time': round(stats['min_duration'] * 1000, 2),
                    'max_response_time': round(stats['max_duration'] * 1000, 2),
                    'status_codes': dict(stats['status_codes'])
                }
        
        return path_data
    
    def get_system_stats(self) -> Dict[str, Any]:
        """获取系统统计"""
        if not self.system_metrics:
            return {}
        
        latest = self.system_metrics[-1]
        
        # 计算平均值（最近10个数据点）
        recent_metrics = list(self.system_metrics)[-10:]
        
        avg_cpu = sum(m.cpu_percent for m in recent_metrics) / len(recent_metrics)
        avg_memory = sum(m.memory_percent for m in recent_metrics) / len(recent_metrics)
        
        return {
            'current': {
                'cpu_percent': round(latest.cpu_percent, 2),
                'memory_percent': round(latest.memory_percent, 2),
                'memory_used_mb': round(latest.memory_used_mb, 2),
                'disk_usage_percent': round(latest.disk_usage_percent, 2),
                'process_count': latest.process_count,
                'open_files': latest.open_files,
                'timestamp': latest.timestamp.isoformat()
            },
            'average': {
                'cpu_percent': round(avg_cpu, 2),
                'memory_percent': round(avg_memory, 2)
            },
            'network_io': latest.network_io
        }
    
    def get_top_slow_requests(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取最慢的请求"""
        # 获取最近的请求并按响应时间排序
        recent_requests = list(self.request_metrics)[-1000:]  # 最近1000个请求
        slow_requests = sorted(recent_requests, key=lambda x: x.duration, reverse=True)[:limit]
        
        return [
            {
                'timestamp': req.timestamp.isoformat(),
                'method': req.method,
                'path': req.path,
                'status_code': req.status_code,
                'duration_ms': round(req.duration * 1000, 2),
                'client_ip': req.client_ip,
                'error': req.error
            }
            for req in slow_requests
        ]
    
    def get_error_summary(self) -> Dict[str, Any]:
        """获取错误摘要"""
        current_time = time.time()
        
        # 最近1小时的错误
        recent_errors = [
            m for t, m in self.time_windows['15m']
            if current_time - t <= 3600 and m.status_code >= 400
        ]
        
        if not recent_errors:
            return {'total_errors': 0, 'error_types': {}, 'error_paths': {}}
        
        # 按状态码分组
        error_types = defaultdict(int)
        for error in recent_errors:
            error_types[error.status_code] += 1
        
        # 按路径分组
        error_paths = defaultdict(int)
        for error in recent_errors:
            error_paths[error.path] += 1
        
        return {
            'total_errors': len(recent_errors),
            'error_types': dict(error_types),
            'error_paths': dict(sorted(error_paths.items(), key=lambda x: x[1], reverse=True)[:10])
        }
    
    def reset_stats(self):
        """重置统计信息"""
        self.request_metrics.clear()
        self.request_count = 0
        self.error_count = 0
        self.path_stats.clear()
        self.status_code_stats.clear()
        
        for window in self.time_windows.values():
            window.clear()
        
        self.current_stats = {
            'requests_per_second': 0.0,
            'avg_response_time': 0.0,
            'error_rate': 0.0,
            'active_requests': 0,
            'total_requests': 0,
            'total_errors': 0
        }
    
    async def stop(self):
        """停止监控"""
        if self.system_monitor_task:
            self.system_monitor_task.cancel()
            try:
                await self.system_monitor_task
            except asyncio.CancelledError:
                pass


class MonitoringMiddleware(BaseHTTPMiddleware):
    """监控中间件"""
    
    def __init__(self, app, enabled: bool = True):
        super().__init__(app)
        self.enabled = enabled
        self.metrics_collector = MetricsCollector()
        self.active_requests = 0
        
        # 排除的路径
        self.excluded_paths = {
            '/health', '/health/live', '/health/ready',
            '/metrics', '/docs', '/redoc', '/openapi.json'
        }
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """处理请求"""
        if not self.enabled or self._should_exclude_path(request.url.path):
            return await call_next(request)
        
        # 记录请求开始
        start_time = time.time()
        self.active_requests += 1
        
        # 获取请求信息
        request_size = int(request.headers.get('content-length', 0))
        client_ip = self._get_client_ip(request)
        user_agent = request.headers.get('user-agent', 'unknown')
        
        error_message = None
        
        try:
            # 处理请求
            response = await call_next(request)
            
            # 记录请求结束
            end_time = time.time()
            duration = end_time - start_time
            
            # 获取响应大小
            response_size = self._get_response_size(response)
            
            # 记录指标
            metrics = RequestMetrics(
                timestamp=datetime.now(timezone.utc),
                method=request.method,
                path=self._normalize_path(request.url.path),
                status_code=response.status_code,
                duration=duration,
                request_size=request_size,
                response_size=response_size,
                client_ip=client_ip,
                user_agent=user_agent,
                error=error_message
            )
            
            self.metrics_collector.record_request(metrics)
            
            # 添加监控头
            response.headers['X-Response-Time'] = str(round(duration * 1000, 2))
            response.headers['X-Request-ID'] = request.headers.get('X-Request-ID', 'unknown')
            
            return response
            
        except Exception as exc:
            # 记录错误
            end_time = time.time()
            duration = end_time - start_time
            error_message = str(exc)
            
            # 记录错误指标
            metrics = RequestMetrics(
                timestamp=datetime.now(timezone.utc),
                method=request.method,
                path=self._normalize_path(request.url.path),
                status_code=500,
                duration=duration,
                request_size=request_size,
                response_size=0,
                client_ip=client_ip,
                user_agent=user_agent,
                error=error_message
            )
            
            self.metrics_collector.record_request(metrics)
            
            raise
            
        finally:
            self.active_requests -= 1
            self.metrics_collector.current_stats['active_requests'] = self.active_requests
    
    def _should_exclude_path(self, path: str) -> bool:
        """检查是否应该排除路径"""
        return any(excluded in path for excluded in self.excluded_paths)
    
    def _normalize_path(self, path: str) -> str:
        """标准化路径（移除参数）"""
        # 移除查询参数
        if '?' in path:
            path = path.split('?')[0]
        
        # 标准化路径参数（如 /api/v1/events/123 -> /api/v1/events/{id}）
        path_parts = path.split('/')
        normalized_parts = []
        
        for part in path_parts:
            if part.isdigit():
                normalized_parts.append('{id}')
            elif len(part) == 32 and all(c in '0123456789abcdef' for c in part.lower()):
                normalized_parts.append('{uuid}')
            else:
                normalized_parts.append(part)
        
        return '/'.join(normalized_parts)
    
    def _get_client_ip(self, request: Request) -> str:
        """获取客户端IP"""
        forwarded_for = request.headers.get('X-Forwarded-For')
        if forwarded_for:
            return forwarded_for.split(',')[0].strip()
        
        real_ip = request.headers.get('X-Real-IP')
        if real_ip:
            return real_ip
        
        if hasattr(request, 'client') and request.client:
            return request.client.host
        
        return 'unknown'
    
    def _get_response_size(self, response: Response) -> int:
        """获取响应大小"""
        try:
            content_length = response.headers.get('content-length')
            if content_length:
                return int(content_length)
            
            if hasattr(response, 'body') and response.body:
                if isinstance(response.body, bytes):
                    return len(response.body)
                else:
                    return len(str(response.body).encode('utf-8'))
            
            return 0
            
        except Exception:
            return 0
    
    def get_metrics(self, time_window: str = '1m') -> Dict[str, Any]:
        """获取监控指标"""
        return {
            'overview': self.metrics_collector.get_stats(time_window),
            'paths': self.metrics_collector.get_path_stats(),
            'system': self.metrics_collector.get_system_stats(),
            'slow_requests': self.metrics_collector.get_top_slow_requests(),
            'errors': self.metrics_collector.get_error_summary(),
            'current_stats': self.metrics_collector.current_stats
        }
    
    def reset_metrics(self):
        """重置监控指标"""
        self.metrics_collector.reset_stats()
        logger.info("Monitoring metrics reset")
    
    async def stop(self):
        """停止监控"""
        await self.metrics_collector.stop()


# 全局监控实例
monitoring_middleware = None


def get_monitoring_middleware() -> Optional[MonitoringMiddleware]:
    """获取监控中间件实例"""
    return monitoring_middleware


def set_monitoring_middleware(middleware: MonitoringMiddleware):
    """设置监控中间件实例"""
    global monitoring_middleware
    monitoring_middleware = middleware
"""性能监控装饰器

提供各种性能监控和分析功能：
- 执行时间监控
- 内存使用监控
- 异步性能监控
- 性能指标收集
"""

import time
import psutil
import asyncio
import functools
from typing import Any, Callable, Dict, Optional, Union
from datetime import datetime, timezone
from contextlib import contextmanager

from loguru import logger
from app.core.config import settings


class PerformanceMetrics:
    """性能指标收集器"""
    
    def __init__(self):
        self.metrics: Dict[str, Dict[str, Any]] = {}
        self.start_time = time.time()
    
    def record_execution(self, func_name: str, duration: float, 
                        memory_usage: Optional[float] = None,
                        cpu_usage: Optional[float] = None,
                        success: bool = True,
                        error: Optional[str] = None):
        """记录执行指标"""
        if func_name not in self.metrics:
            self.metrics[func_name] = {
                'total_calls': 0,
                'total_duration': 0.0,
                'min_duration': float('inf'),
                'max_duration': 0.0,
                'avg_duration': 0.0,
                'success_count': 0,
                'error_count': 0,
                'last_called': None,
                'memory_usage': [],
                'cpu_usage': [],
                'errors': []
            }
        
        metric = self.metrics[func_name]
        metric['total_calls'] += 1
        metric['total_duration'] += duration
        metric['min_duration'] = min(metric['min_duration'], duration)
        metric['max_duration'] = max(metric['max_duration'], duration)
        metric['avg_duration'] = metric['total_duration'] / metric['total_calls']
        metric['last_called'] = datetime.now(timezone.utc).isoformat()
        
        if success:
            metric['success_count'] += 1
        else:
            metric['error_count'] += 1
            if error:
                metric['errors'].append({
                    'error': error,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
        
        if memory_usage is not None:
            metric['memory_usage'].append(memory_usage)
            # 只保留最近100次的内存使用记录
            if len(metric['memory_usage']) > 100:
                metric['memory_usage'] = metric['memory_usage'][-100:]
        
        if cpu_usage is not None:
            metric['cpu_usage'].append(cpu_usage)
            # 只保留最近100次的CPU使用记录
            if len(metric['cpu_usage']) > 100:
                metric['cpu_usage'] = metric['cpu_usage'][-100:]
    
    def get_metrics(self, func_name: Optional[str] = None) -> Dict[str, Any]:
        """获取性能指标"""
        if func_name:
            return self.metrics.get(func_name, {})
        return self.metrics
    
    def get_summary(self) -> Dict[str, Any]:
        """获取性能摘要"""
        total_calls = sum(m['total_calls'] for m in self.metrics.values())
        total_duration = sum(m['total_duration'] for m in self.metrics.values())
        total_errors = sum(m['error_count'] for m in self.metrics.values())
        
        return {
            'total_functions': len(self.metrics),
            'total_calls': total_calls,
            'total_duration': total_duration,
            'total_errors': total_errors,
            'success_rate': (total_calls - total_errors) / total_calls if total_calls > 0 else 0,
            'avg_duration_per_call': total_duration / total_calls if total_calls > 0 else 0,
            'uptime': time.time() - self.start_time
        }


# 全局性能指标收集器
performance_metrics = PerformanceMetrics()


@contextmanager
def performance_context(operation_name: str):
    """性能监控上下文管理器"""
    start_time = time.time()
    start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
    start_cpu = psutil.cpu_percent()
    
    try:
        yield
        success = True
        error = None
    except Exception as e:
        success = False
        error = str(e)
        raise
    finally:
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        end_cpu = psutil.cpu_percent()
        
        duration = end_time - start_time
        memory_usage = end_memory - start_memory
        cpu_usage = end_cpu - start_cpu
        
        performance_metrics.record_execution(
            operation_name, duration, memory_usage, cpu_usage, success, error
        )
        
        logger.info(
            f"Performance: {operation_name}",
            duration=round(duration * 1000, 2),
            memory_usage=round(memory_usage, 2),
            cpu_usage=round(cpu_usage, 2),
            success=success
        )


def performance_monitor(operation_name: Optional[str] = None, 
                       log_level: str = "INFO",
                       include_memory: bool = True,
                       include_cpu: bool = True):
    """性能监控装饰器"""
    def decorator(func: Callable) -> Callable:
        func_name = operation_name or f"{func.__module__}.{func.__name__}"
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            start_memory = None
            start_cpu = None
            
            if include_memory:
                start_memory = psutil.Process().memory_info().rss / 1024 / 1024
            if include_cpu:
                start_cpu = psutil.cpu_percent()
            
            try:
                result = func(*args, **kwargs)
                success = True
                error = None
            except Exception as e:
                success = False
                error = str(e)
                raise
            finally:
                end_time = time.time()
                duration = end_time - start_time
                
                memory_usage = None
                cpu_usage = None
                
                if include_memory and start_memory is not None:
                    end_memory = psutil.Process().memory_info().rss / 1024 / 1024
                    memory_usage = end_memory - start_memory
                
                if include_cpu and start_cpu is not None:
                    end_cpu = psutil.cpu_percent()
                    cpu_usage = end_cpu - start_cpu
                
                # 记录性能指标
                performance_metrics.record_execution(
                    func_name, duration, memory_usage, cpu_usage, success, error
                )
                
                # 记录日志
                log_data = {
                    "operation": func_name,
                    "duration_ms": round(duration * 1000, 2),
                    "success": success
                }
                
                if memory_usage is not None:
                    log_data["memory_usage_mb"] = round(memory_usage, 2)
                if cpu_usage is not None:
                    log_data["cpu_usage_percent"] = round(cpu_usage, 2)
                if error:
                    log_data["error"] = error
                
                getattr(logger, log_level.lower())(
                    f"Performance: {func_name}", **log_data
                )
            
            return result
        
        return wrapper
    return decorator


def async_performance_monitor(operation_name: Optional[str] = None,
                             log_level: str = "INFO",
                             include_memory: bool = True,
                             include_cpu: bool = True):
    """异步性能监控装饰器"""
    def decorator(func: Callable) -> Callable:
        func_name = operation_name or f"{func.__module__}.{func.__name__}"
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            start_memory = None
            start_cpu = None
            
            if include_memory:
                start_memory = psutil.Process().memory_info().rss / 1024 / 1024
            if include_cpu:
                start_cpu = psutil.cpu_percent()
            
            try:
                result = await func(*args, **kwargs)
                success = True
                error = None
            except Exception as e:
                success = False
                error = str(e)
                raise
            finally:
                end_time = time.time()
                duration = end_time - start_time
                
                memory_usage = None
                cpu_usage = None
                
                if include_memory and start_memory is not None:
                    end_memory = psutil.Process().memory_info().rss / 1024 / 1024
                    memory_usage = end_memory - start_memory
                
                if include_cpu and start_cpu is not None:
                    end_cpu = psutil.cpu_percent()
                    cpu_usage = end_cpu - start_cpu
                
                # 记录性能指标
                performance_metrics.record_execution(
                    func_name, duration, memory_usage, cpu_usage, success, error
                )
                
                # 记录日志
                log_data = {
                    "operation": func_name,
                    "duration_ms": round(duration * 1000, 2),
                    "success": success
                }
                
                if memory_usage is not None:
                    log_data["memory_usage_mb"] = round(memory_usage, 2)
                if cpu_usage is not None:
                    log_data["cpu_usage_percent"] = round(cpu_usage, 2)
                if error:
                    log_data["error"] = error
                
                getattr(logger, log_level.lower())(
                    f"Performance: {func_name}", **log_data
                )
            
            return result
        
        return wrapper
    return decorator


def memory_monitor(threshold_mb: float = 100.0, 
                  alert_on_exceed: bool = True):
    """内存监控装饰器"""
    def decorator(func: Callable) -> Callable:
        func_name = f"{func.__module__}.{func.__name__}"
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            try:
                result = func(*args, **kwargs)
            finally:
                end_memory = psutil.Process().memory_info().rss / 1024 / 1024
                memory_usage = end_memory - start_memory
                
                if alert_on_exceed and memory_usage > threshold_mb:
                    logger.warning(
                        f"High memory usage detected in {func_name}",
                        memory_usage_mb=round(memory_usage, 2),
                        threshold_mb=threshold_mb,
                        start_memory_mb=round(start_memory, 2),
                        end_memory_mb=round(end_memory, 2)
                    )
                else:
                    logger.debug(
                        f"Memory usage for {func_name}",
                        memory_usage_mb=round(memory_usage, 2)
                    )
            
            return result
        
        return wrapper
    return decorator


def execution_time(threshold_seconds: float = 1.0,
                  alert_on_exceed: bool = True,
                  log_all: bool = False):
    """执行时间监控装饰器"""
    def decorator(func: Callable) -> Callable:
        func_name = f"{func.__module__}.{func.__name__}"
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
            finally:
                duration = time.time() - start_time
                
                if alert_on_exceed and duration > threshold_seconds:
                    logger.warning(
                        f"Slow execution detected in {func_name}",
                        duration_seconds=round(duration, 3),
                        threshold_seconds=threshold_seconds
                    )
                elif log_all:
                    logger.debug(
                        f"Execution time for {func_name}",
                        duration_seconds=round(duration, 3)
                    )
            
            return result
        
        return wrapper
    return decorator


class PerformanceProfiler:
    """性能分析器"""
    
    def __init__(self, name: str):
        self.name = name
        self.checkpoints: Dict[str, float] = {}
        self.start_time = time.time()
        self.last_checkpoint = self.start_time
    
    def checkpoint(self, name: str):
        """添加检查点"""
        current_time = time.time()
        self.checkpoints[name] = current_time - self.last_checkpoint
        self.last_checkpoint = current_time
    
    def finish(self) -> Dict[str, Any]:
        """完成分析并返回结果"""
        total_time = time.time() - self.start_time
        
        result = {
            'profiler_name': self.name,
            'total_time': total_time,
            'checkpoints': self.checkpoints,
            'checkpoint_percentages': {}
        }
        
        # 计算每个检查点的时间占比
        for checkpoint, duration in self.checkpoints.items():
            result['checkpoint_percentages'][checkpoint] = (
                (duration / total_time) * 100 if total_time > 0 else 0
            )
        
        logger.info(
            f"Performance Profile: {self.name}",
            total_time=round(total_time, 3),
            checkpoints={k: round(v, 3) for k, v in self.checkpoints.items()}
        )
        
        return result


def profile_performance(profiler_name: Optional[str] = None):
    """性能分析装饰器"""
    def decorator(func: Callable) -> Callable:
        name = profiler_name or f"{func.__module__}.{func.__name__}"
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            profiler = PerformanceProfiler(name)
            
            # 将profiler作为关键字参数传递给函数
            if 'profiler' not in kwargs:
                kwargs['profiler'] = profiler
            
            try:
                result = func(*args, **kwargs)
            finally:
                profiler.finish()
            
            return result
        
        return wrapper
    return decorator


def get_performance_metrics() -> Dict[str, Any]:
    """获取全局性能指标"""
    return performance_metrics.get_metrics()


def get_performance_summary() -> Dict[str, Any]:
    """获取性能摘要"""
    return performance_metrics.get_summary()


def reset_performance_metrics():
    """重置性能指标"""
    global performance_metrics
    performance_metrics = PerformanceMetrics()
"""重试装饰器

提供各种重试策略和熔断器功能：
- 指数退避重试
- 线性重试
- 熔断器模式
- 异步重试
- 条件重试
"""

import time
import random
import asyncio
import functools
from typing import Any, Callable, Optional, Union, Type, Tuple, List
from datetime import datetime, timedelta, timezone
from enum import Enum

from loguru import logger


class RetryStrategy(str, Enum):
    """重试策略枚举"""
    FIXED = "fixed"              # 固定间隔
    LINEAR = "linear"            # 线性增长
    EXPONENTIAL = "exponential"  # 指数退避
    RANDOM = "random"            # 随机间隔


class CircuitState(str, Enum):
    """熔断器状态枚举"""
    CLOSED = "closed"        # 关闭状态（正常）
    OPEN = "open"            # 开启状态（熔断）
    HALF_OPEN = "half_open"  # 半开状态（试探）


class RetryConfig:
    """重试配置"""
    
    def __init__(self,
                 max_attempts: int = 3,
                 strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
                 base_delay: float = 1.0,
                 max_delay: float = 60.0,
                 multiplier: float = 2.0,
                 jitter: bool = True,
                 exceptions: Tuple[Type[Exception], ...] = (Exception,),
                 exclude_exceptions: Tuple[Type[Exception], ...] = (),
                 condition: Optional[Callable[[Exception], bool]] = None):
        self.max_attempts = max_attempts
        self.strategy = strategy
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter = jitter
        self.exceptions = exceptions
        self.exclude_exceptions = exclude_exceptions
        self.condition = condition
    
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        """判断是否应该重试"""
        # 检查是否超过最大尝试次数
        if attempt >= self.max_attempts:
            return False
        
        # 检查是否为排除的异常类型
        if isinstance(exception, self.exclude_exceptions):
            return False
        
        # 检查是否为允许重试的异常类型
        if not isinstance(exception, self.exceptions):
            return False
        
        # 检查自定义条件
        if self.condition and not self.condition(exception):
            return False
        
        return True
    
    def calculate_delay(self, attempt: int) -> float:
        """计算延迟时间"""
        if self.strategy == RetryStrategy.FIXED:
            delay = self.base_delay
        elif self.strategy == RetryStrategy.LINEAR:
            delay = self.base_delay * attempt
        elif self.strategy == RetryStrategy.EXPONENTIAL:
            delay = self.base_delay * (self.multiplier ** (attempt - 1))
        elif self.strategy == RetryStrategy.RANDOM:
            delay = random.uniform(self.base_delay, self.max_delay)
        else:
            delay = self.base_delay
        
        # 应用最大延迟限制
        delay = min(delay, self.max_delay)
        
        # 添加抖动
        if self.jitter and self.strategy != RetryStrategy.RANDOM:
            jitter_range = delay * 0.1  # 10%的抖动
            delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0, delay)


class CircuitBreaker:
    """熔断器"""
    
    def __init__(self,
                 failure_threshold: int = 5,
                 recovery_timeout: float = 60.0,
                 expected_exception: Type[Exception] = Exception,
                 name: Optional[str] = None):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name or "CircuitBreaker"
        
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = CircuitState.CLOSED
        self.success_count = 0
    
    def call(self, func: Callable, *args, **kwargs):
        """调用函数并应用熔断器逻辑"""
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
                logger.info(f"Circuit breaker {self.name} entering HALF_OPEN state")
            else:
                raise Exception(f"Circuit breaker {self.name} is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise
    
    async def async_call(self, func: Callable, *args, **kwargs):
        """异步调用函数并应用熔断器逻辑"""
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
                logger.info(f"Circuit breaker {self.name} entering HALF_OPEN state")
            else:
                raise Exception(f"Circuit breaker {self.name} is OPEN")
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """判断是否应该尝试重置"""
        if self.last_failure_time is None:
            return True
        
        time_since_failure = datetime.now(timezone.utc) - self.last_failure_time
        return time_since_failure.total_seconds() >= self.recovery_timeout
    
    def _on_success(self):
        """成功时的处理"""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= 3:  # 连续3次成功后重置
                self._reset()
        elif self.state == CircuitState.CLOSED:
            self.failure_count = 0
    
    def _on_failure(self):
        """失败时的处理"""
        self.failure_count += 1
        self.last_failure_time = datetime.now(timezone.utc)
        
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker {self.name} opened due to failure in HALF_OPEN state")
        elif self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(
                f"Circuit breaker {self.name} opened due to {self.failure_count} failures"
            )
    
    def _reset(self):
        """重置熔断器"""
        self.failure_count = 0
        self.success_count = 0
        self.state = CircuitState.CLOSED
        self.last_failure_time = None
        logger.info(f"Circuit breaker {self.name} reset to CLOSED state")
    
    def get_state(self) -> dict:
        """获取熔断器状态"""
        return {
            'name': self.name,
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'last_failure_time': self.last_failure_time.isoformat() if self.last_failure_time else None,
            'failure_threshold': self.failure_threshold,
            'recovery_timeout': self.recovery_timeout
        }


def retry(max_attempts: int = 3,
         strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
         base_delay: float = 1.0,
         max_delay: float = 60.0,
         multiplier: float = 2.0,
         jitter: bool = True,
         exceptions: Tuple[Type[Exception], ...] = (Exception,),
         exclude_exceptions: Tuple[Type[Exception], ...] = (),
         condition: Optional[Callable[[Exception], bool]] = None,
         on_retry: Optional[Callable[[int, Exception], None]] = None):
    """重试装饰器"""
    config = RetryConfig(
        max_attempts=max_attempts,
        strategy=strategy,
        base_delay=base_delay,
        max_delay=max_delay,
        multiplier=multiplier,
        jitter=jitter,
        exceptions=exceptions,
        exclude_exceptions=exclude_exceptions,
        condition=condition
    )
    
    def decorator(func: Callable) -> Callable:
        func_name = f"{func.__module__}.{func.__name__}"
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(1, config.max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 1:
                        logger.info(
                            f"Retry succeeded for {func_name}",
                            attempt=attempt,
                            total_attempts=config.max_attempts
                        )
                    return result
                except Exception as e:
                    last_exception = e
                    
                    if not config.should_retry(e, attempt):
                        logger.error(
                            f"Retry failed for {func_name} - not retryable",
                            attempt=attempt,
                            error=str(e),
                            error_type=type(e).__name__
                        )
                        raise
                    
                    if attempt < config.max_attempts:
                        delay = config.calculate_delay(attempt)
                        
                        logger.warning(
                            f"Retry attempt {attempt} failed for {func_name}",
                            attempt=attempt,
                            total_attempts=config.max_attempts,
                            error=str(e),
                            error_type=type(e).__name__,
                            next_delay=round(delay, 2)
                        )
                        
                        if on_retry:
                            on_retry(attempt, e)
                        
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"All retry attempts failed for {func_name}",
                            total_attempts=config.max_attempts,
                            final_error=str(e),
                            error_type=type(e).__name__
                        )
            
            raise last_exception
        
        return wrapper
    return decorator


def async_retry(max_attempts: int = 3,
               strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
               base_delay: float = 1.0,
               max_delay: float = 60.0,
               multiplier: float = 2.0,
               jitter: bool = True,
               exceptions: Tuple[Type[Exception], ...] = (Exception,),
               exclude_exceptions: Tuple[Type[Exception], ...] = (),
               condition: Optional[Callable[[Exception], bool]] = None,
               on_retry: Optional[Callable[[int, Exception], None]] = None):
    """异步重试装饰器"""
    config = RetryConfig(
        max_attempts=max_attempts,
        strategy=strategy,
        base_delay=base_delay,
        max_delay=max_delay,
        multiplier=multiplier,
        jitter=jitter,
        exceptions=exceptions,
        exclude_exceptions=exclude_exceptions,
        condition=condition
    )
    
    def decorator(func: Callable) -> Callable:
        func_name = f"{func.__module__}.{func.__name__}"
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(1, config.max_attempts + 1):
                try:
                    result = await func(*args, **kwargs)
                    if attempt > 1:
                        logger.info(
                            f"Async retry succeeded for {func_name}",
                            attempt=attempt,
                            total_attempts=config.max_attempts
                        )
                    return result
                except Exception as e:
                    last_exception = e
                    
                    if not config.should_retry(e, attempt):
                        logger.error(
                            f"Async retry failed for {func_name} - not retryable",
                            attempt=attempt,
                            error=str(e),
                            error_type=type(e).__name__
                        )
                        raise
                    
                    if attempt < config.max_attempts:
                        delay = config.calculate_delay(attempt)
                        
                        logger.warning(
                            f"Async retry attempt {attempt} failed for {func_name}",
                            attempt=attempt,
                            total_attempts=config.max_attempts,
                            error=str(e),
                            error_type=type(e).__name__,
                            next_delay=round(delay, 2)
                        )
                        
                        if on_retry:
                            on_retry(attempt, e)
                        
                        await asyncio.sleep(delay)
                    else:
                        logger.error(
                            f"All async retry attempts failed for {func_name}",
                            total_attempts=config.max_attempts,
                            final_error=str(e),
                            error_type=type(e).__name__
                        )
            
            raise last_exception
        
        return wrapper
    return decorator


def exponential_backoff(max_attempts: int = 3,
                       base_delay: float = 1.0,
                       max_delay: float = 60.0,
                       multiplier: float = 2.0,
                       jitter: bool = True):
    """指数退避重试装饰器"""
    return retry(
        max_attempts=max_attempts,
        strategy=RetryStrategy.EXPONENTIAL,
        base_delay=base_delay,
        max_delay=max_delay,
        multiplier=multiplier,
        jitter=jitter
    )


def circuit_breaker(failure_threshold: int = 5,
                   recovery_timeout: float = 60.0,
                   expected_exception: Type[Exception] = Exception,
                   name: Optional[str] = None):
    """熔断器装饰器"""
    def decorator(func: Callable) -> Callable:
        breaker_name = name or f"{func.__module__}.{func.__name__}"
        breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=expected_exception,
            name=breaker_name
        )
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return breaker.call(func, *args, **kwargs)
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await breaker.async_call(func, *args, **kwargs)
        
        # 添加熔断器状态查询方法
        wrapper.get_circuit_state = breaker.get_state
        async_wrapper.get_circuit_state = breaker.get_state
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    
    return decorator


def retry_with_circuit_breaker(max_attempts: int = 3,
                              strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
                              base_delay: float = 1.0,
                              max_delay: float = 60.0,
                              multiplier: float = 2.0,
                              failure_threshold: int = 5,
                              recovery_timeout: float = 60.0,
                              name: Optional[str] = None):
    """带熔断器的重试装饰器"""
    def decorator(func: Callable) -> Callable:
        # 先应用重试装饰器
        retry_decorator = retry(
            max_attempts=max_attempts,
            strategy=strategy,
            base_delay=base_delay,
            max_delay=max_delay,
            multiplier=multiplier
        )
        
        # 再应用熔断器装饰器
        circuit_decorator = circuit_breaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            name=name
        )
        
        return circuit_decorator(retry_decorator(func))
    
    return decorator


class RetryManager:
    """重试管理器"""
    
    def __init__(self):
        self.circuit_breakers: dict[str, CircuitBreaker] = {}
        self.retry_stats: dict[str, dict] = {}
    
    def get_circuit_breaker(self, name: str) -> Optional[CircuitBreaker]:
        """获取熔断器"""
        return self.circuit_breakers.get(name)
    
    def get_all_circuit_states(self) -> dict[str, dict]:
        """获取所有熔断器状态"""
        return {name: breaker.get_state() for name, breaker in self.circuit_breakers.items()}
    
    def reset_circuit_breaker(self, name: str) -> bool:
        """重置熔断器"""
        if name in self.circuit_breakers:
            self.circuit_breakers[name]._reset()
            return True
        return False
    
    def get_retry_stats(self) -> dict[str, dict]:
        """获取重试统计"""
        return self.retry_stats


# 全局重试管理器
retry_manager = RetryManager()
"""限流中间件

实现API请求限流功能：
- 基于IP的限流
- 基于用户的限流
- 滑动窗口算法
- 令牌桶算法
- 分布式限流
"""

import time
import asyncio
from typing import Dict, Optional, Callable, Any
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass
from enum import Enum

from fastapi import Request, Response, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from loguru import logger
from app.core.config import settings


class RateLimitAlgorithm(str, Enum):
    """限流算法枚举"""
    FIXED_WINDOW = "fixed_window"        # 固定窗口
    SLIDING_WINDOW = "sliding_window"    # 滑动窗口
    TOKEN_BUCKET = "token_bucket"        # 令牌桶
    LEAKY_BUCKET = "leaky_bucket"        # 漏桶


@dataclass
class RateLimitRule:
    """限流规则"""
    requests_per_minute: int
    requests_per_hour: int
    burst_size: int = 10  # 突发请求大小
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.SLIDING_WINDOW
    enabled: bool = True


class TokenBucket:
    """令牌桶实现"""
    
    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.tokens = capacity
        self.refill_rate = refill_rate  # 每秒添加的令牌数
        self.last_refill = time.time()
        self.lock = asyncio.Lock()
    
    async def consume(self, tokens: int = 1) -> bool:
        """消费令牌"""
        async with self.lock:
            now = time.time()
            
            # 添加令牌
            time_passed = now - self.last_refill
            tokens_to_add = time_passed * self.refill_rate
            self.tokens = min(self.capacity, self.tokens + tokens_to_add)
            self.last_refill = now
            
            # 检查是否有足够的令牌
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """获取状态"""
        return {
            'capacity': self.capacity,
            'current_tokens': self.tokens,
            'refill_rate': self.refill_rate,
            'last_refill': self.last_refill
        }


class SlidingWindow:
    """滑动窗口实现"""
    
    def __init__(self, window_size: int, max_requests: int):
        self.window_size = window_size  # 窗口大小（秒）
        self.max_requests = max_requests
        self.requests = deque()
        self.lock = asyncio.Lock()
    
    async def is_allowed(self) -> bool:
        """检查是否允许请求"""
        async with self.lock:
            now = time.time()
            
            # 移除过期的请求
            while self.requests and self.requests[0] <= now - self.window_size:
                self.requests.popleft()
            
            # 检查是否超过限制
            if len(self.requests) >= self.max_requests:
                return False
            
            # 添加当前请求
            self.requests.append(now)
            return True
    
    def get_status(self) -> Dict[str, Any]:
        """获取状态"""
        now = time.time()
        # 清理过期请求
        while self.requests and self.requests[0] <= now - self.window_size:
            self.requests.popleft()
        
        return {
            'window_size': self.window_size,
            'max_requests': self.max_requests,
            'current_requests': len(self.requests),
            'remaining_requests': max(0, self.max_requests - len(self.requests))
        }


class RateLimiter:
    """限流器"""
    
    def __init__(self):
        self.ip_limiters: Dict[str, Any] = {}
        self.user_limiters: Dict[str, Any] = {}
        self.path_limiters: Dict[str, Any] = {}
        self.global_limiter: Optional[Any] = None
        
        # 默认规则
        self.default_rules = {
            'global': RateLimitRule(
                requests_per_minute=1000,
                requests_per_hour=10000,
                burst_size=50
            ),
            'ip': RateLimitRule(
                requests_per_minute=100,
                requests_per_hour=1000,
                burst_size=10
            ),
            'user': RateLimitRule(
                requests_per_minute=200,
                requests_per_hour=2000,
                burst_size=20
            )
        }
        
        # 路径特定规则
        self.path_rules = {
            '/api/v1/event': RateLimitRule(
                requests_per_minute=500,
                requests_per_hour=5000,
                burst_size=25
            ),
            '/api/v1/events/batch': RateLimitRule(
                requests_per_minute=50,
                requests_per_hour=500,
                burst_size=5
            )
        }
        
        # 清理任务
        self.cleanup_task = None
        self.start_cleanup_task()
    
    def start_cleanup_task(self):
        """启动清理任务"""
        if not self.cleanup_task:
            self.cleanup_task = asyncio.create_task(self._cleanup_expired_limiters())
    
    async def _cleanup_expired_limiters(self):
        """清理过期的限流器"""
        while True:
            try:
                await asyncio.sleep(300)  # 每5分钟清理一次
                
                now = time.time()
                expired_threshold = now - 3600  # 1小时未使用的限流器
                
                # 清理IP限流器
                expired_ips = [
                    ip for ip, limiter in self.ip_limiters.items()
                    if hasattr(limiter, 'last_access') and limiter.last_access < expired_threshold
                ]
                for ip in expired_ips:
                    del self.ip_limiters[ip]
                
                # 清理用户限流器
                expired_users = [
                    user for user, limiter in self.user_limiters.items()
                    if hasattr(limiter, 'last_access') and limiter.last_access < expired_threshold
                ]
                for user in expired_users:
                    del self.user_limiters[user]
                
                if expired_ips or expired_users:
                    logger.info(f"Cleaned up {len(expired_ips)} IP limiters and {len(expired_users)} user limiters")
                    
            except Exception as e:
                logger.error(f"Error in rate limiter cleanup: {e}")
    
    async def is_allowed(self, request: Request) -> tuple[bool, Dict[str, Any]]:
        """检查请求是否被允许"""
        client_ip = self._get_client_ip(request)
        user_id = request.headers.get('X-User-ID')
        path = request.url.path
        
        # 检查全局限流
        global_allowed, global_info = await self._check_global_limit()
        if not global_allowed:
            return False, {
                'type': 'global',
                'limit_info': global_info,
                'retry_after': self._calculate_retry_after(global_info)
            }
        
        # 检查IP限流
        ip_allowed, ip_info = await self._check_ip_limit(client_ip)
        if not ip_allowed:
            return False, {
                'type': 'ip',
                'limit_info': ip_info,
                'retry_after': self._calculate_retry_after(ip_info)
            }
        
        # 检查用户限流（如果有用户ID）
        if user_id:
            user_allowed, user_info = await self._check_user_limit(user_id)
            if not user_allowed:
                return False, {
                    'type': 'user',
                    'limit_info': user_info,
                    'retry_after': self._calculate_retry_after(user_info)
                }
        
        # 检查路径特定限流
        path_allowed, path_info = await self._check_path_limit(path, client_ip)
        if not path_allowed:
            return False, {
                'type': 'path',
                'limit_info': path_info,
                'retry_after': self._calculate_retry_after(path_info)
            }
        
        return True, {
            'global': global_info,
            'ip': ip_info,
            'user': user_info if user_id else None,
            'path': path_info
        }
    
    async def _check_global_limit(self) -> tuple[bool, Dict[str, Any]]:
        """检查全局限流"""
        if not self.global_limiter:
            rule = self.default_rules['global']
            self.global_limiter = SlidingWindow(
                window_size=60,  # 1分钟窗口
                max_requests=rule.requests_per_minute
            )
        
        allowed = await self.global_limiter.is_allowed()
        info = self.global_limiter.get_status()
        
        return allowed, info
    
    async def _check_ip_limit(self, ip: str) -> tuple[bool, Dict[str, Any]]:
        """检查IP限流"""
        if ip not in self.ip_limiters:
            rule = self.default_rules['ip']
            self.ip_limiters[ip] = SlidingWindow(
                window_size=60,  # 1分钟窗口
                max_requests=rule.requests_per_minute
            )
            self.ip_limiters[ip].last_access = time.time()
        
        limiter = self.ip_limiters[ip]
        limiter.last_access = time.time()
        
        allowed = await limiter.is_allowed()
        info = limiter.get_status()
        info['ip'] = ip
        
        return allowed, info
    
    async def _check_user_limit(self, user_id: str) -> tuple[bool, Dict[str, Any]]:
        """检查用户限流"""
        if user_id not in self.user_limiters:
            rule = self.default_rules['user']
            self.user_limiters[user_id] = SlidingWindow(
                window_size=60,  # 1分钟窗口
                max_requests=rule.requests_per_minute
            )
            self.user_limiters[user_id].last_access = time.time()
        
        limiter = self.user_limiters[user_id]
        limiter.last_access = time.time()
        
        allowed = await limiter.is_allowed()
        info = limiter.get_status()
        info['user_id'] = user_id
        
        return allowed, info
    
    async def _check_path_limit(self, path: str, ip: str) -> tuple[bool, Dict[str, Any]]:
        """检查路径特定限流"""
        # 查找匹配的路径规则
        rule = None
        for path_pattern, path_rule in self.path_rules.items():
            if path.startswith(path_pattern):
                rule = path_rule
                break
        
        if not rule:
            return True, {'path': path, 'no_limit': True}
        
        limiter_key = f"{path}:{ip}"
        
        if limiter_key not in self.path_limiters:
            self.path_limiters[limiter_key] = SlidingWindow(
                window_size=60,  # 1分钟窗口
                max_requests=rule.requests_per_minute
            )
            self.path_limiters[limiter_key].last_access = time.time()
        
        limiter = self.path_limiters[limiter_key]
        limiter.last_access = time.time()
        
        allowed = await limiter.is_allowed()
        info = limiter.get_status()
        info['path'] = path
        info['ip'] = ip
        
        return allowed, info
    
    def _get_client_ip(self, request: Request) -> str:
        """获取客户端IP"""
        # 检查代理头
        forwarded_for = request.headers.get('X-Forwarded-For')
        if forwarded_for:
            return forwarded_for.split(',')[0].strip()
        
        real_ip = request.headers.get('X-Real-IP')
        if real_ip:
            return real_ip
        
        # 回退到直接连接IP
        if hasattr(request, 'client') and request.client:
            return request.client.host
        
        return 'unknown'
    
    def _calculate_retry_after(self, limit_info: Dict[str, Any]) -> int:
        """计算重试等待时间"""
        # 基于窗口大小和当前请求数计算
        window_size = limit_info.get('window_size', 60)
        current_requests = limit_info.get('current_requests', 0)
        max_requests = limit_info.get('max_requests', 100)
        
        if current_requests >= max_requests:
            # 如果已达到限制，建议等待窗口大小的一半时间
            return window_size // 2
        
        return 10  # 默认10秒
    
    def get_stats(self) -> Dict[str, Any]:
        """获取限流统计信息"""
        return {
            'ip_limiters_count': len(self.ip_limiters),
            'user_limiters_count': len(self.user_limiters),
            'path_limiters_count': len(self.path_limiters),
            'global_limiter_status': self.global_limiter.get_status() if self.global_limiter else None,
            'rules': {
                'default': {k: v.__dict__ for k, v in self.default_rules.items()},
                'path_specific': {k: v.__dict__ for k, v in self.path_rules.items()}
            }
        }
    
    async def reset_limiter(self, limiter_type: str, identifier: str = None):
        """重置限流器"""
        if limiter_type == 'global' and self.global_limiter:
            self.global_limiter = None
        elif limiter_type == 'ip' and identifier and identifier in self.ip_limiters:
            del self.ip_limiters[identifier]
        elif limiter_type == 'user' and identifier and identifier in self.user_limiters:
            del self.user_limiters[identifier]
        elif limiter_type == 'all':
            self.ip_limiters.clear()
            self.user_limiters.clear()
            self.path_limiters.clear()
            self.global_limiter = None


class RateLimitingMiddleware(BaseHTTPMiddleware):
    """限流中间件"""
    
    def __init__(self, app, enabled: bool = True):
        super().__init__(app)
        self.enabled = enabled
        self.rate_limiter = RateLimiter()
        self.exempt_paths = {
            '/health', '/health/live', '/health/ready', 
            '/docs', '/redoc', '/openapi.json'
        }
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """处理请求"""
        # 检查是否启用限流
        if not self.enabled:
            return await call_next(request)
        
        # 检查是否为免限流路径
        if self._is_exempt_path(request.url.path):
            return await call_next(request)
        
        # 执行限流检查
        try:
            allowed, limit_info = await self.rate_limiter.is_allowed(request)
            
            if not allowed:
                # 记录限流日志
                logger.warning(
                    "Rate limit exceeded",
                    client_ip=self.rate_limiter._get_client_ip(request),
                    path=request.url.path,
                    limit_type=limit_info.get('type'),
                    limit_info=limit_info.get('limit_info')
                )
                
                # 返回限流响应
                return self._create_rate_limit_response(limit_info)
            
            # 继续处理请求
            response = await call_next(request)
            
            # 添加限流信息到响应头
            self._add_rate_limit_headers(response, limit_info)
            
            return response
            
        except Exception as e:
            logger.error(f"Rate limiting error: {e}", exc_info=True)
            # 限流出错时允许请求通过
            return await call_next(request)
    
    def _is_exempt_path(self, path: str) -> bool:
        """检查是否为免限流路径"""
        return any(exempt in path for exempt in self.exempt_paths)
    
    def _create_rate_limit_response(self, limit_info: Dict[str, Any]) -> JSONResponse:
        """创建限流响应"""
        retry_after = limit_info.get('retry_after', 60)
        limit_type = limit_info.get('type', 'unknown')
        
        content = {
            "error": {
                "code": 429,
                "message": f"请求频率过高 ({limit_type} limit exceeded)",
                "type": "rate_limit_exceeded",
                "limit_type": limit_type,
                "retry_after": retry_after
            },
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        headers = {
            "Retry-After": str(retry_after),
            "X-RateLimit-Limit-Type": limit_type
        }
        
        # 添加具体的限流信息
        if 'limit_info' in limit_info:
            info = limit_info['limit_info']
            if 'max_requests' in info:
                headers["X-RateLimit-Limit"] = str(info['max_requests'])
            if 'remaining_requests' in info:
                headers["X-RateLimit-Remaining"] = str(info['remaining_requests'])
            if 'window_size' in info:
                headers["X-RateLimit-Window"] = str(info['window_size'])
        
        return JSONResponse(
            status_code=429,
            content=content,
            headers=headers
        )
    
    def _add_rate_limit_headers(self, response: Response, limit_info: Dict[str, Any]):
        """添加限流信息到响应头"""
        try:
            # 添加IP限流信息
            if 'ip' in limit_info and limit_info['ip']:
                ip_info = limit_info['ip']
                response.headers["X-RateLimit-IP-Limit"] = str(ip_info.get('max_requests', 0))
                response.headers["X-RateLimit-IP-Remaining"] = str(ip_info.get('remaining_requests', 0))
            
            # 添加全局限流信息
            if 'global' in limit_info and limit_info['global']:
                global_info = limit_info['global']
                response.headers["X-RateLimit-Global-Limit"] = str(global_info.get('max_requests', 0))
                response.headers["X-RateLimit-Global-Remaining"] = str(global_info.get('remaining_requests', 0))
            
        except Exception as e:
            logger.warning(f"Failed to add rate limit headers: {e}")
    
    async def get_stats(self) -> Dict[str, Any]:
        """获取限流统计信息"""
        return self.rate_limiter.get_stats()
    
    async def reset_limits(self, limiter_type: str = "all", identifier: str = None):
        """重置限流器"""
        await self.rate_limiter.reset_limiter(limiter_type, identifier)
        logger.info(f"Rate limits reset: {limiter_type}", identifier=identifier)
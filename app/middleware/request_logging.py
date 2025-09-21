"""请求日志中间件

实现详细的HTTP请求日志记录：
- 请求/响应日志
- 性能监控
- 错误追踪
- 分布式追踪
"""

import time
import json
from typing import Callable, Dict, Any
from uuid import uuid4

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import StreamingResponse

from loguru import logger
from app.core.logging import (
    set_request_context, generate_request_id,
    log_performance, log_audit
)
from app.core.config import settings


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """请求日志中间件"""
    
    def __init__(self, app, log_requests: bool = True, log_responses: bool = True):
        super().__init__(app)
        self.log_requests = log_requests
        self.log_responses = log_responses
        self.sensitive_headers = {
            'authorization', 'cookie', 'x-api-key', 'x-auth-token'
        }
        self.sensitive_paths = {
            '/docs', '/redoc', '/openapi.json'
        }
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """处理请求"""
        # 生成请求ID
        request_id = request.headers.get('X-Request-ID') or generate_request_id()
        
        # 设置请求上下文
        set_request_context(
            request_id=request_id,
            user_id=request.headers.get('X-User-ID'),
            session_id=request.headers.get('X-Session-ID')
        )
        
        # 记录请求开始时间
        start_time = time.time()
        
        # 提取请求信息
        request_info = await self._extract_request_info(request)
        
        # 记录请求日志
        if self.log_requests and not self._is_sensitive_path(request.url.path):
            logger.info(
                "HTTP Request",
                request_id=request_id,
                method=request.method,
                path=request.url.path,
                query_params=dict(request.query_params),
                headers=self._filter_sensitive_headers(dict(request.headers)),
                client_ip=self._get_client_ip(request),
                user_agent=request.headers.get('user-agent'),
                content_length=request.headers.get('content-length'),
                content_type=request.headers.get('content-type')
            )
        
        # 处理请求体（如果需要）
        if (request.method in ['POST', 'PUT', 'PATCH'] and 
            self.log_requests and 
            not self._is_sensitive_path(request.url.path)):
            
            body = await self._get_request_body(request)
            if body:
                logger.debug(
                    "Request Body",
                    request_id=request_id,
                    body=body[:1000] if len(body) > 1000 else body  # 限制长度
                )
        
        # 处理请求
        try:
            response = await call_next(request)
            
            # 计算处理时间
            process_time = time.time() - start_time
            
            # 记录响应日志
            if self.log_responses and not self._is_sensitive_path(request.url.path):
                await self._log_response(request, response, process_time, request_id)
            
            # 记录性能指标
            log_performance(
                operation=f"{request.method} {request.url.path}",
                duration=process_time,
                status_code=response.status_code,
                request_size=int(request.headers.get('content-length', 0)),
                response_size=self._get_response_size(response)
            )
            
            # 添加响应头
            response.headers['X-Request-ID'] = request_id
            response.headers['X-Process-Time'] = str(round(process_time * 1000, 2))
            
            return response
            
        except Exception as e:
            # 记录错误
            process_time = time.time() - start_time
            
            logger.error(
                "HTTP Request Error",
                request_id=request_id,
                method=request.method,
                path=request.url.path,
                error=str(e),
                error_type=type(e).__name__,
                process_time=round(process_time * 1000, 2)
            )
            
            # 记录审计日志（错误情况）
            log_audit(
                action="http_request_error",
                resource=request.url.path,
                error=str(e),
                method=request.method,
                client_ip=self._get_client_ip(request)
            )
            
            raise
    
    async def _extract_request_info(self, request: Request) -> Dict[str, Any]:
        """提取请求信息"""
        return {
            'method': request.method,
            'url': str(request.url),
            'path': request.url.path,
            'query_params': dict(request.query_params),
            'headers': dict(request.headers),
            'client_ip': self._get_client_ip(request),
            'user_agent': request.headers.get('user-agent'),
            'content_type': request.headers.get('content-type'),
            'content_length': request.headers.get('content-length')
        }
    
    async def _get_request_body(self, request: Request) -> str:
        """获取请求体"""
        try:
            content_type = request.headers.get('content-type', '')
            
            if 'application/json' in content_type:
                body = await request.body()
                if body:
                    try:
                        # 尝试解析JSON并格式化
                        json_data = json.loads(body.decode('utf-8'))
                        return json.dumps(json_data, ensure_ascii=False, indent=2)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        return body.decode('utf-8', errors='ignore')
            elif 'application/x-www-form-urlencoded' in content_type:
                body = await request.body()
                return body.decode('utf-8', errors='ignore')
            elif 'multipart/form-data' in content_type:
                return "[Multipart Form Data]"
            else:
                body = await request.body()
                if len(body) < 1000:  # 只记录小文件
                    return body.decode('utf-8', errors='ignore')
                else:
                    return f"[Binary Data: {len(body)} bytes]"
        except Exception as e:
            logger.warning(f"Failed to read request body: {e}")
            return "[Failed to read body]"
        
        return ""
    
    async def _log_response(self, request: Request, response: Response, 
                          process_time: float, request_id: str):
        """记录响应日志"""
        try:
            # 基本响应信息
            response_info = {
                'request_id': request_id,
                'status_code': response.status_code,
                'process_time_ms': round(process_time * 1000, 2),
                'headers': dict(response.headers),
                'size_bytes': self._get_response_size(response)
            }
            
            # 根据状态码选择日志级别
            if response.status_code < 400:
                log_level = 'info'
            elif response.status_code < 500:
                log_level = 'warning'
            else:
                log_level = 'error'
            
            getattr(logger, log_level)(
                "HTTP Response",
                **response_info
            )
            
            # 记录响应体（仅在调试模式下且状态码异常时）
            if (settings.debug and response.status_code >= 400 and 
                hasattr(response, 'body')):
                
                body = await self._get_response_body(response)
                if body:
                    logger.debug(
                        "Response Body",
                        request_id=request_id,
                        body=body[:1000] if len(body) > 1000 else body
                    )
            
        except Exception as e:
            logger.warning(f"Failed to log response: {e}")
    
    async def _get_response_body(self, response: Response) -> str:
        """获取响应体"""
        try:
            if isinstance(response, StreamingResponse):
                return "[Streaming Response]"
            
            if hasattr(response, 'body'):
                body = response.body
                if isinstance(body, bytes):
                    try:
                        return body.decode('utf-8')
                    except UnicodeDecodeError:
                        return f"[Binary Response: {len(body)} bytes]"
                else:
                    return str(body)
            
            return "[No Body]"
            
        except Exception as e:
            logger.warning(f"Failed to read response body: {e}")
            return "[Failed to read body]"
    
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
    
    def _filter_sensitive_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """过滤敏感头部信息"""
        filtered = {}
        for key, value in headers.items():
            if key.lower() in self.sensitive_headers:
                filtered[key] = '[REDACTED]'
            else:
                filtered[key] = value
        return filtered
    
    def _is_sensitive_path(self, path: str) -> bool:
        """检查是否为敏感路径"""
        return any(sensitive in path for sensitive in self.sensitive_paths)
    
    def _should_log_request(self, request: Request) -> bool:
        """判断是否应该记录请求"""
        # 跳过健康检查等路径
        skip_paths = {'/health', '/health/live', '/health/ready', '/metrics'}
        
        if request.url.path in skip_paths:
            return False
        
        # 跳过静态文件
        static_extensions = {'.css', '.js', '.png', '.jpg', '.ico', '.svg'}
        if any(request.url.path.endswith(ext) for ext in static_extensions):
            return False
        
        return True
    
    def _get_log_context(self, request: Request) -> Dict[str, Any]:
        """获取日志上下文"""
        return {
            'service': settings.app_name,
            'version': settings.app_version,
            'environment': settings.environment,
            'method': request.method,
            'path': request.url.path,
            'client_ip': self._get_client_ip(request),
            'user_agent': request.headers.get('user-agent', 'unknown')
        }
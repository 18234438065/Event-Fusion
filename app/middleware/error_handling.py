"""错误处理中间件

实现统一的异常处理和错误响应：
- 全局异常捕获
- 错误分类和处理
- 错误日志记录
- 标准化错误响应
- 错误监控和告警
"""

import traceback
from typing import Callable, Dict, Any, Optional
from datetime import datetime, timezone

from fastapi import Request, Response, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from loguru import logger
from app.core.config import settings
from app.core.logging import log_audit


class ErrorCategory:
    """错误分类"""
    VALIDATION = "validation_error"
    AUTHENTICATION = "authentication_error"
    AUTHORIZATION = "authorization_error"
    NOT_FOUND = "not_found_error"
    CONFLICT = "conflict_error"
    RATE_LIMIT = "rate_limit_error"
    EXTERNAL_SERVICE = "external_service_error"
    DATABASE = "database_error"
    NETWORK = "network_error"
    BUSINESS_LOGIC = "business_logic_error"
    SYSTEM = "system_error"
    UNKNOWN = "unknown_error"


class ErrorSeverity:
    """错误严重程度"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorHandler:
    """错误处理器"""
    
    def __init__(self):
        self.error_stats = {
            'total_errors': 0,
            'errors_by_category': {},
            'errors_by_status_code': {},
            'errors_by_path': {},
            'last_error_time': None
        }
        
        # 错误分类映射
        self.status_code_mapping = {
            400: ErrorCategory.VALIDATION,
            401: ErrorCategory.AUTHENTICATION,
            403: ErrorCategory.AUTHORIZATION,
            404: ErrorCategory.NOT_FOUND,
            409: ErrorCategory.CONFLICT,
            422: ErrorCategory.VALIDATION,
            429: ErrorCategory.RATE_LIMIT,
            500: ErrorCategory.SYSTEM,
            502: ErrorCategory.EXTERNAL_SERVICE,
            503: ErrorCategory.EXTERNAL_SERVICE,
            504: ErrorCategory.NETWORK
        }
        
        # 严重程度映射
        self.severity_mapping = {
            range(400, 500): ErrorSeverity.MEDIUM,
            range(500, 600): ErrorSeverity.HIGH,
            [429]: ErrorSeverity.LOW,
            [500, 502, 503]: ErrorSeverity.CRITICAL
        }
    
    def categorize_error(self, status_code: int, exception: Exception = None) -> str:
        """错误分类"""
        # 基于状态码分类
        category = self.status_code_mapping.get(status_code, ErrorCategory.UNKNOWN)
        
        # 基于异常类型细化分类
        if exception:
            exception_name = type(exception).__name__
            
            if 'Database' in exception_name or 'SQL' in exception_name:
                category = ErrorCategory.DATABASE
            elif 'Network' in exception_name or 'Connection' in exception_name:
                category = ErrorCategory.NETWORK
            elif 'Validation' in exception_name:
                category = ErrorCategory.VALIDATION
            elif 'Permission' in exception_name or 'Auth' in exception_name:
                category = ErrorCategory.AUTHORIZATION
        
        return category
    
    def get_severity(self, status_code: int) -> str:
        """获取错误严重程度"""
        for code_range, severity in self.severity_mapping.items():
            if isinstance(code_range, range) and status_code in code_range:
                return severity
            elif isinstance(code_range, list) and status_code in code_range:
                return severity
        
        return ErrorSeverity.MEDIUM
    
    def should_alert(self, status_code: int, category: str, severity: str) -> bool:
        """判断是否需要告警"""
        # 严重错误需要告警
        if severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
            return True
        
        # 系统错误需要告警
        if category in [ErrorCategory.SYSTEM, ErrorCategory.DATABASE]:
            return True
        
        # 频繁的外部服务错误需要告警
        if category == ErrorCategory.EXTERNAL_SERVICE:
            recent_errors = self.error_stats['errors_by_category'].get(category, 0)
            return recent_errors > 10  # 超过10次外部服务错误
        
        return False
    
    def record_error(self, status_code: int, category: str, path: str, 
                    exception: Exception = None):
        """记录错误统计"""
        self.error_stats['total_errors'] += 1
        self.error_stats['last_error_time'] = datetime.now(timezone.utc)
        
        # 按分类统计
        if category not in self.error_stats['errors_by_category']:
            self.error_stats['errors_by_category'][category] = 0
        self.error_stats['errors_by_category'][category] += 1
        
        # 按状态码统计
        if status_code not in self.error_stats['errors_by_status_code']:
            self.error_stats['errors_by_status_code'][status_code] = 0
        self.error_stats['errors_by_status_code'][status_code] += 1
        
        # 按路径统计
        if path not in self.error_stats['errors_by_path']:
            self.error_stats['errors_by_path'][path] = 0
        self.error_stats['errors_by_path'][path] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """获取错误统计"""
        return self.error_stats.copy()
    
    def reset_stats(self):
        """重置错误统计"""
        self.error_stats = {
            'total_errors': 0,
            'errors_by_category': {},
            'errors_by_status_code': {},
            'errors_by_path': {},
            'last_error_time': None
        }


class ErrorHandlingMiddleware(BaseHTTPMiddleware):
    """错误处理中间件"""
    
    def __init__(self, app, include_traceback: bool = None):
        super().__init__(app)
        self.include_traceback = include_traceback if include_traceback is not None else settings.debug
        self.error_handler = ErrorHandler()
        
        # 敏感信息过滤
        self.sensitive_patterns = [
            'password', 'token', 'secret', 'key', 'auth',
            'credential', 'session', 'cookie'
        ]
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """处理请求"""
        try:
            response = await call_next(request)
            return response
            
        except Exception as exc:
            return await self._handle_exception(request, exc)
    
    async def _handle_exception(self, request: Request, exc: Exception) -> JSONResponse:
        """处理异常"""
        # 提取请求信息
        request_info = self._extract_request_info(request)
        
        # 确定状态码和错误信息
        if isinstance(exc, (HTTPException, StarletteHTTPException)):
            status_code = exc.status_code
            detail = exc.detail
        else:
            status_code = 500
            detail = "Internal Server Error"
        
        # 错误分类
        category = self.error_handler.categorize_error(status_code, exc)
        severity = self.error_handler.get_severity(status_code)
        
        # 记录错误统计
        self.error_handler.record_error(status_code, category, request.url.path, exc)
        
        # 生成错误ID
        error_id = self._generate_error_id()
        
        # 记录错误日志
        await self._log_error(request, exc, status_code, category, severity, error_id, request_info)
        
        # 检查是否需要告警
        if self.error_handler.should_alert(status_code, category, severity):
            await self._send_alert(request, exc, status_code, category, severity, error_id)
        
        # 构建错误响应
        error_response = self._build_error_response(
            status_code, detail, category, severity, error_id, exc
        )
        
        return JSONResponse(
            status_code=status_code,
            content=error_response,
            headers=self._get_error_headers(error_id, category)
        )
    
    def _extract_request_info(self, request: Request) -> Dict[str, Any]:
        """提取请求信息"""
        return {
            'method': request.method,
            'url': str(request.url),
            'path': request.url.path,
            'query_params': dict(request.query_params),
            'headers': self._filter_sensitive_headers(dict(request.headers)),
            'client_ip': self._get_client_ip(request),
            'user_agent': request.headers.get('user-agent', 'unknown')
        }
    
    async def _log_error(self, request: Request, exc: Exception, status_code: int,
                        category: str, severity: str, error_id: str, 
                        request_info: Dict[str, Any]):
        """记录错误日志"""
        # 基本错误信息
        error_data = {
            'error_id': error_id,
            'status_code': status_code,
            'category': category,
            'severity': severity,
            'exception_type': type(exc).__name__,
            'exception_message': str(exc),
            'request_info': request_info
        }
        
        # 添加堆栈跟踪（仅在调试模式或严重错误时）
        if self.include_traceback or severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
            error_data['traceback'] = traceback.format_exc()
        
        # 根据严重程度选择日志级别
        if severity == ErrorSeverity.CRITICAL:
            log_level = 'critical'
        elif severity == ErrorSeverity.HIGH:
            log_level = 'error'
        elif severity == ErrorSeverity.MEDIUM:
            log_level = 'warning'
        else:
            log_level = 'info'
        
        getattr(logger, log_level)(
            f"HTTP Error [{category}]: {exc}",
            **error_data
        )
        
        # 记录审计日志
        log_audit(
            action="http_error",
            resource=request.url.path,
            error_id=error_id,
            status_code=status_code,
            category=category,
            severity=severity,
            client_ip=request_info['client_ip']
        )
    
    async def _send_alert(self, request: Request, exc: Exception, status_code: int,
                         category: str, severity: str, error_id: str):
        """发送告警"""
        try:
            alert_data = {
                'error_id': error_id,
                'service': settings.app_name,
                'environment': settings.environment,
                'status_code': status_code,
                'category': category,
                'severity': severity,
                'path': request.url.path,
                'method': request.method,
                'exception': str(exc),
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'client_ip': self._get_client_ip(request)
            }
            
            # 这里可以集成告警系统（如钉钉、企业微信、邮件等）
            logger.critical(
                f"ALERT: {severity.upper()} error in {settings.app_name}",
                **alert_data
            )
            
            # TODO: 实际项目中应该集成真实的告警系统
            
        except Exception as alert_exc:
            logger.error(f"Failed to send alert: {alert_exc}")
    
    def _build_error_response(self, status_code: int, detail: str, category: str,
                             severity: str, error_id: str, exc: Exception) -> Dict[str, Any]:
        """构建错误响应"""
        # 基本错误信息
        error_response = {
            "error": {
                "id": error_id,
                "code": status_code,
                "message": detail if not settings.is_production() else self._get_user_friendly_message(status_code),
                "category": category,
                "type": type(exc).__name__
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "path": None  # 将在调用处设置
        }
        
        # 在调试模式下添加详细信息
        if self.include_traceback:
            error_response["debug"] = {
                "exception_message": str(exc),
                "exception_type": type(exc).__name__,
                "severity": severity
            }
            
            # 添加堆栈跟踪（仅限严重错误或调试模式）
            if severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
                error_response["debug"]["traceback"] = traceback.format_exc().split('\n')
        
        # 添加帮助信息
        error_response["help"] = self._get_help_message(status_code, category)
        
        return error_response
    
    def _get_user_friendly_message(self, status_code: int) -> str:
        """获取用户友好的错误消息"""
        messages = {
            400: "请求参数错误，请检查输入数据",
            401: "身份验证失败，请重新登录",
            403: "权限不足，无法访问该资源",
            404: "请求的资源不存在",
            409: "资源冲突，请稍后重试",
            422: "数据验证失败，请检查输入格式",
            429: "请求过于频繁，请稍后重试",
            500: "服务器内部错误，请联系管理员",
            502: "外部服务不可用，请稍后重试",
            503: "服务暂时不可用，请稍后重试",
            504: "请求超时，请稍后重试"
        }
        
        return messages.get(status_code, "未知错误，请联系管理员")
    
    def _get_help_message(self, status_code: int, category: str) -> str:
        """获取帮助信息"""
        help_messages = {
            400: "请检查请求参数的格式和类型是否正确",
            401: "请确保提供了有效的认证信息",
            403: "请联系管理员获取相应权限",
            404: "请检查请求的URL路径是否正确",
            422: "请参考API文档检查数据格式",
            429: "请降低请求频率或稍后重试",
            500: "如果问题持续存在，请联系技术支持"
        }
        
        base_help = help_messages.get(status_code, "请联系技术支持获取帮助")
        
        if settings.debug:
            base_help += f" (错误分类: {category})"
        
        return base_help
    
    def _generate_error_id(self) -> str:
        """生成错误ID"""
        from uuid import uuid4
        return f"ERR_{uuid4().hex[:8].upper()}"
    
    def _get_error_headers(self, error_id: str, category: str) -> Dict[str, str]:
        """获取错误响应头"""
        return {
            "X-Error-ID": error_id,
            "X-Error-Category": category,
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY"
        }
    
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
    
    def _filter_sensitive_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """过滤敏感头部信息"""
        filtered = {}
        for key, value in headers.items():
            if any(pattern in key.lower() for pattern in self.sensitive_patterns):
                filtered[key] = '[REDACTED]'
            else:
                filtered[key] = value
        return filtered
    
    def get_error_stats(self) -> Dict[str, Any]:
        """获取错误统计"""
        stats = self.error_handler.get_stats()
        
        # 添加额外的统计信息
        if stats['total_errors'] > 0:
            stats['error_rate'] = {
                'by_category': {
                    category: (count / stats['total_errors']) * 100
                    for category, count in stats['errors_by_category'].items()
                },
                'by_status_code': {
                    str(code): (count / stats['total_errors']) * 100
                    for code, count in stats['errors_by_status_code'].items()
                }
            }
        
        return stats
    
    def reset_error_stats(self):
        """重置错误统计"""
        self.error_handler.reset_stats()
        logger.info("Error statistics reset")
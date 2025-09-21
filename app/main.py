"""主应用程序入口

企业级事件融合服务的主要入口点：
- FastAPI应用初始化
- 服务组件集成
- 中间件配置
- 生命周期管理
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from loguru import logger
from app.core.config import settings
from app.core.logging import (
    set_request_context, generate_request_id,
    log_performance, log_audit
)
from app.api.routes import router
from app.services.fusion.event_fusion_service import EventFusionService
from app.services.fusion.fusion_engine import FusionEngine
from app.services.fusion.event_cache import EventCache
from app.services.fusion.suppression_manager import SuppressionManager
from app.services.video.video_processor import VideoProcessor
from app.services.video.rtsp_manager import rtsp_manager
from app.services.video.storage_manager import video_storage_manager
from app.services.video.task_queue import video_task_queue
from app.services.notification.event_sender import event_sender
from app.services.monitoring.metrics_collector import metrics_collector
from infrastructure.kafka.kafka_manager import kafka_manager
from app.middleware.request_logging import RequestLoggingMiddleware
from app.middleware.rate_limiting import RateLimitingMiddleware
from app.middleware.error_handling import ErrorHandlingMiddleware


class ServiceManager:
    """服务管理器"""
    
    def __init__(self):
        self.event_cache: Optional[EventCache] = None
        self.suppression_manager: Optional[SuppressionManager] = None
        self.fusion_engine: Optional[FusionEngine] = None
        self.event_fusion_service: Optional[EventFusionService] = None
        self.video_processor: Optional[VideoProcessor] = None
        self.is_initialized = False
    
    async def initialize(self):
        """初始化所有服务"""
        try:
            logger.info("Initializing services...")
            
            # 初始化Kafka
            await kafka_manager.start()
            
            # 初始化事件缓存
            self.event_cache = EventCache()
            await self.event_cache.initialize_redis()
            
            # 初始化抑制管理器
            self.suppression_manager = SuppressionManager()
            await self.suppression_manager.initialize_redis()
            await self.suppression_manager.start()
            
            # 初始化融合引擎
            self.fusion_engine = FusionEngine()
            
            # 初始化事件融合服务
            self.event_fusion_service = EventFusionService(
                event_cache=self.event_cache,
                suppression_manager=self.suppression_manager,
                fusion_engine=self.fusion_engine
            )
            await self.event_fusion_service.start()
            
            # 初始化视频处理服务
            self.video_processor = VideoProcessor()
            await video_task_queue.start()
            await rtsp_manager.start()
            await video_storage_manager.start()
            
            # 初始化通知服务
            await event_sender.start()
            
            # 初始化监控服务
            await metrics_collector.start_collection()
            
            # 设置Kafka消费者
            await self._setup_kafka_consumers()
            
            self.is_initialized = True
            logger.info("All services initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize services: {e}")
            raise
    
    async def _setup_kafka_consumers(self):
        """设置Kafka消费者"""
        # 事件消费者
        await kafka_manager.create_consumer(
            topics=[settings.kafka.event_topic],
            consumer_name="event_consumer",
            message_handler=self._handle_event_message
        )
        
        logger.info("Kafka consumers setup completed")
    
    async def _handle_event_message(self, message):
        """处理事件消息"""
        try:
            if self.event_fusion_service:
                # 这里应该解析消息并调用事件融合服务
                logger.info(f"Received event message: {message.key}")
                # TODO: 实现消息处理逻辑
        except Exception as e:
            logger.error(f"Error handling event message: {e}")
    
    async def shutdown(self):
        """关闭所有服务"""
        try:
            logger.info("Shutting down services...")
            
            # 停止监控服务
            await metrics_collector.stop_collection()
            
            # 停止通知服务
            await event_sender.stop()
            
            # 停止视频处理服务
            await video_task_queue.stop()
            await rtsp_manager.stop()
            await video_storage_manager.stop()
            
            # 停止事件融合服务
            if self.event_fusion_service:
                await self.event_fusion_service.stop()
            
            if self.suppression_manager:
                await self.suppression_manager.stop()
                await self.suppression_manager.close()
            
            if self.event_cache:
                await self.event_cache.close()
            
            await kafka_manager.stop()
            
            logger.info("All services shut down successfully")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")


# 全局服务管理器
service_manager = ServiceManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动
    try:
        await service_manager.initialize()
        logger.info(f"🚀 {settings.app_name} v{settings.app_version} started")
        yield
    finally:
        # 关闭
        await service_manager.shutdown()
        logger.info(f"👋 {settings.app_name} stopped")


def create_app() -> FastAPI:
    """创建FastAPI应用"""
    
    app = FastAPI(
        title=settings.app_name,
        version=settings.app_version,
        description="企业级高速公路事件融合服务",
        docs_url="/docs" if settings.debug else None,
        redoc_url="/redoc" if settings.debug else None,
        lifespan=lifespan
    )
    
    # 添加中间件
    setup_middleware(app)
    
    # 添加路由
    app.include_router(router, prefix="/api/v1")
    
    # 添加异常处理器
    setup_exception_handlers(app)
    
    # 添加健康检查端点
    setup_health_endpoints(app)
    
    return app


def setup_middleware(app: FastAPI):
    """设置中间件"""
    
    # CORS中间件
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.debug else ["https://yourdomain.com"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Gzip压缩中间件
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    
    # 自定义中间件
    app.add_middleware(ErrorHandlingMiddleware)
    app.add_middleware(RateLimitingMiddleware)
    app.add_middleware(RequestLoggingMiddleware)


def setup_exception_handlers(app: FastAPI):
    """设置异常处理器"""
    
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        """HTTP异常处理器"""
        logger.warning(f"HTTP {exc.status_code}: {exc.detail}",
                      path=request.url.path,
                      method=request.method)
        
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.status_code,
                    "message": exc.detail,
                    "type": "http_error"
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "path": str(request.url.path)
            }
        )
    
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """请求验证异常处理器"""
        logger.warning(f"Validation error: {exc.errors()}",
                      path=request.url.path,
                      method=request.method)
        
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": 422,
                    "message": "请求数据验证失败",
                    "type": "validation_error",
                    "details": exc.errors()
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "path": str(request.url.path)
            }
        )
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        """通用异常处理器"""
        logger.error(f"Unhandled exception: {exc}",
                    path=request.url.path,
                    method=request.method,
                    exc_info=True)
        
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": 500,
                    "message": "服务器内部错误" if not settings.debug else str(exc),
                    "type": "internal_error"
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "path": str(request.url.path)
            }
        )


def setup_health_endpoints(app: FastAPI):
    """设置健康检查端点"""
    
    @app.get("/health")
    async def health_check():
        """健康检查"""
        try:
            # 检查服务状态
            kafka_health = await kafka_manager.health_check()
            
            fusion_health = {}
            if service_manager.event_fusion_service:
                fusion_health = await service_manager.event_fusion_service.health_check()
            
            cache_health = {}
            if service_manager.event_cache:
                cache_stats = await service_manager.event_cache.get_cache_stats()
                cache_health = {
                    'status': 'healthy',
                    'local_cache_size': cache_stats['local_cache']['size'],
                    'hit_rate': cache_stats['statistics']['hit_rate_percent']
                }
            
            overall_status = "healthy"
            if (kafka_health.get('status') != 'healthy' or 
                fusion_health.get('status') != 'healthy'):
                overall_status = "degraded"
            
            return {
                "status": overall_status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "version": settings.app_version,
                "environment": settings.environment,
                "services": {
                    "kafka": kafka_health,
                    "event_fusion": fusion_health,
                    "cache": cache_health
                }
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return JSONResponse(
                status_code=503,
                content={
                    "status": "unhealthy",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "error": str(e)
                }
            )
    
    @app.get("/health/ready")
    async def readiness_check():
        """就绪检查"""
        if service_manager.is_initialized:
            return {"status": "ready"}
        else:
            return JSONResponse(
                status_code=503,
                content={"status": "not_ready"}
            )
    
    @app.get("/health/live")
    async def liveness_check():
        """存活检查"""
        return {"status": "alive"}
    
    @app.get("/metrics")
    async def metrics():
        """指标端点"""
        try:
            kafka_stats = kafka_manager.get_stats()
            
            fusion_stats = {}
            if service_manager.event_fusion_service:
                stats = await service_manager.event_fusion_service.get_stats()
                fusion_stats = stats.dict()
            
            cache_stats = {}
            if service_manager.event_cache:
                cache_stats = await service_manager.event_cache.get_cache_stats()
            
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "kafka": kafka_stats,
                "event_fusion": fusion_stats,
                "cache": cache_stats
            }
            
        except Exception as e:
            logger.error(f"Metrics collection failed: {e}")
            return JSONResponse(
                status_code=500,
                content={"error": str(e)}
            )


# 创建应用实例
app = create_app()


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host=settings.fusion_service_host,
        port=settings.fusion_service_port,
        reload=settings.debug,
        workers=1 if settings.debug else settings.performance.max_workers,
        log_level=settings.log_level.lower(),
        access_log=settings.debug
    )


# 导入datetime用于时间处理
from datetime import datetime, timezone
from typing import Optional
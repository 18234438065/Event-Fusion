"""API路由

实现所有的RESTful API接口：
- 事件处理接口
- 监控统计接口
- 系统管理接口
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from loguru import logger
from app.models.event import EventData, EventResponse, EventStats
from app.core.logging import log_audit, log_performance, generate_request_id, set_request_context
from app.decorators.performance import async_performance_monitor
from app.decorators.retry import async_retry
from infrastructure.kafka.kafka_manager import kafka_manager


# 创建路由器
router = APIRouter()


# 依赖注入
async def get_service_manager():
    """获取服务管理器"""
    from app.main import service_manager
    if not service_manager.is_initialized:
        raise HTTPException(status_code=503, detail="服务尚未初始化")
    return service_manager


class EventProcessRequest(BaseModel):
    """事件处理请求"""
    events: List[EventData] = Field(..., description="事件列表")
    batch_id: Optional[str] = Field(None, description="批次ID")
    priority: int = Field(default=5, ge=1, le=10, description="优先级")


class EventProcessResponse(BaseModel):
    """事件处理响应"""
    success: bool = Field(..., description="处理是否成功")
    batch_id: str = Field(..., description="批次ID")
    processed_count: int = Field(..., description="处理成功数量")
    failed_count: int = Field(..., description="处理失败数量")
    results: List[EventResponse] = Field(..., description="处理结果列表")
    processing_time: float = Field(..., description="处理耗时(秒)")


class SystemStatusResponse(BaseModel):
    """系统状态响应"""
    status: str = Field(..., description="系统状态")
    timestamp: datetime = Field(..., description="时间戳")
    services: Dict[str, Any] = Field(..., description="服务状态")
    statistics: Dict[str, Any] = Field(..., description="统计信息")


# ============================================================================
# 事件处理接口
# ============================================================================

@router.post("/event", response_model=EventResponse, summary="处理单个事件")
@async_performance_monitor("api.process_single_event")
async def process_single_event(
    event: EventData,
    background_tasks: BackgroundTasks,
    service_manager = Depends(get_service_manager)
) -> EventResponse:
    """处理单个事件"""
    request_id = generate_request_id()
    set_request_context(request_id=request_id)
    
    try:
        logger.info(f"Processing single event: {event.alarmID}",
                   event_type=event.eventType,
                   stake_num=event.stakeNum)
        
        # 审计日志
        log_audit("process_event", "event", 
                 event_id=event.alarmID,
                 event_type=event.eventType,
                 stake_num=event.stakeNum)
        
        # 处理事件
        response = await service_manager.event_fusion_service.process_event(event)
        
        # 异步发送到Kafka
        if response.success and response.action in ["created", "fused"]:
            background_tasks.add_task(
                kafka_manager.send_processed_event,
                event.dict()
            )
        
        return response
        
    except Exception as e:
        logger.error(f"Error processing event {event.alarmID}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"事件处理失败: {str(e)}")


@router.post("/events/batch", response_model=EventProcessResponse, summary="批量处理事件")
@async_performance_monitor("api.process_batch_events")
async def process_batch_events(
    request: EventProcessRequest,
    background_tasks: BackgroundTasks,
    service_manager = Depends(get_service_manager)
) -> EventProcessResponse:
    """批量处理事件"""
    request_id = generate_request_id()
    batch_id = request.batch_id or request_id
    set_request_context(request_id=request_id)
    
    start_time = datetime.now(timezone.utc)
    
    try:
        logger.info(f"Processing batch events: {len(request.events)}",
                   batch_id=batch_id,
                   priority=request.priority)
        
        # 审计日志
        log_audit("process_batch_events", "events",
                 batch_id=batch_id,
                 event_count=len(request.events),
                 priority=request.priority)
        
        results = []
        processed_count = 0
        failed_count = 0
        
        # 处理每个事件
        for event in request.events:
            try:
                response = await service_manager.event_fusion_service.process_event(event)
                results.append(response)
                
                if response.success:
                    processed_count += 1
                    # 异步发送到Kafka
                    if response.action in ["created", "fused"]:
                        background_tasks.add_task(
                            kafka_manager.send_processed_event,
                            event.dict()
                        )
                else:
                    failed_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing event {event.alarmID}: {e}")
                failed_count += 1
                results.append(EventResponse(
                    success=False,
                    message=f"处理失败: {str(e)}",
                    event_id=event.alarmID,
                    action="error"
                ))
        
        processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
        
        return EventProcessResponse(
            success=failed_count == 0,
            batch_id=batch_id,
            processed_count=processed_count,
            failed_count=failed_count,
            results=results,
            processing_time=processing_time
        )
        
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"批量处理失败: {str(e)}")


# ============================================================================
# 监控和统计接口
# ============================================================================

@router.get("/stats", response_model=Dict[str, Any], summary="获取统计信息")
@async_performance_monitor("api.get_stats")
async def get_statistics(
    service_manager = Depends(get_service_manager)
) -> Dict[str, Any]:
    """获取系统统计信息"""
    try:
        # 获取事件融合统计
        fusion_stats = await service_manager.event_fusion_service.get_stats()
        
        # 获取缓存统计
        cache_stats = await service_manager.event_cache.get_cache_stats()
        
        # 获取抑制统计
        suppression_stats = await service_manager.suppression_manager.get_status()
        
        # 获取Kafka统计
        kafka_stats = kafka_manager.get_stats()
        
        # 获取融合引擎统计
        fusion_engine_stats = service_manager.fusion_engine.get_fusion_stats()
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_fusion": fusion_stats.dict(),
            "cache": cache_stats,
            "suppression": suppression_stats,
            "kafka": kafka_stats,
            "fusion_engine": fusion_engine_stats
        }
        
    except Exception as e:
        logger.error(f"Error getting statistics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取统计信息失败: {str(e)}")


@router.get("/events/active", response_model=Dict[str, Any], summary="获取活跃事件")
@async_performance_monitor("api.get_active_events")
async def get_active_events(
    event_type: Optional[str] = Query(None, description="事件类型过滤"),
    stake_num: Optional[str] = Query(None, description="桩号过滤"),
    limit: int = Query(100, ge=1, le=1000, description="返回数量限制"),
    service_manager = Depends(get_service_manager)
) -> Dict[str, Any]:
    """获取活跃事件列表"""
    try:
        active_events = await service_manager.event_fusion_service.get_active_events()
        
        # 应用过滤器
        filtered_events = active_events['events']
        
        if event_type:
            filtered_events = {
                k: v for k, v in filtered_events.items()
                if k.startswith(event_type)
            }
        
        if stake_num:
            filtered_events = {
                k: v for k, v in filtered_events.items()
                if stake_num in k
            }
        
        # 应用限制
        if len(filtered_events) > limit:
            # 按最后报告时间排序，取最新的
            sorted_events = sorted(
                filtered_events.items(),
                key=lambda x: x[1]['last_report_time'],
                reverse=True
            )
            filtered_events = dict(sorted_events[:limit])
        
        return {
            "total_count": active_events['count'],
            "filtered_count": len(filtered_events),
            "events": filtered_events,
            "filters": {
                "event_type": event_type,
                "stake_num": stake_num,
                "limit": limit
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting active events: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取活跃事件失败: {str(e)}")


@router.get("/suppression/status", response_model=Dict[str, Any], summary="获取抑制状态")
@async_performance_monitor("api.get_suppression_status")
async def get_suppression_status(
    service_manager = Depends(get_service_manager)
) -> Dict[str, Any]:
    """获取事件抑制状态"""
    try:
        return await service_manager.event_fusion_service.get_suppression_status()
    except Exception as e:
        logger.error(f"Error getting suppression status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取抑制状态失败: {str(e)}")


# ============================================================================
# 系统管理接口
# ============================================================================

@router.get("/system/status", response_model=SystemStatusResponse, summary="获取系统状态")
@async_performance_monitor("api.get_system_status")
async def get_system_status(
    service_manager = Depends(get_service_manager)
) -> SystemStatusResponse:
    """获取系统整体状态"""
    try:
        # 获取各服务状态
        kafka_health = await kafka_manager.health_check()
        fusion_health = await service_manager.event_fusion_service.health_check()
        cache_stats = await service_manager.event_cache.get_cache_stats()
        suppression_status = await service_manager.suppression_manager.get_status()
        
        # 确定整体状态
        overall_status = "healthy"
        if (kafka_health.get('status') != 'healthy' or 
            fusion_health.get('status') != 'healthy'):
            overall_status = "degraded"
        
        services = {
            "kafka": kafka_health,
            "event_fusion": fusion_health,
            "cache": {
                "status": "healthy",
                "local_cache_size": cache_stats['local_cache']['size'],
                "hit_rate_percent": cache_stats['statistics']['hit_rate_percent']
            },
            "suppression": {
                "status": "healthy",
                "active_suppressions": suppression_status['statistics']['active_suppressions']
            }
        }
        
        statistics = {
            "kafka": kafka_manager.get_stats(),
            "fusion_engine": service_manager.fusion_engine.get_fusion_stats(),
            "cache": cache_stats['statistics'],
            "suppression": suppression_status['statistics']
        }
        
        return SystemStatusResponse(
            status=overall_status,
            timestamp=datetime.now(timezone.utc),
            services=services,
            statistics=statistics
        )
        
    except Exception as e:
        logger.error(f"Error getting system status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取系统状态失败: {str(e)}")


@router.post("/system/reset-stats", summary="重置统计信息")
@async_performance_monitor("api.reset_stats")
async def reset_statistics(
    service_manager = Depends(get_service_manager)
) -> Dict[str, str]:
    """重置系统统计信息"""
    try:
        # 审计日志
        log_audit("reset_statistics", "system")
        
        # 重置各组件统计
        await service_manager.event_fusion_service.reset_stats()
        kafka_manager.reset_stats()
        service_manager.fusion_engine.reset_stats()
        
        logger.info("System statistics reset")
        
        return {
            "message": "统计信息已重置",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error resetting statistics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"重置统计信息失败: {str(e)}")


@router.post("/cache/clear", summary="清空缓存")
@async_performance_monitor("api.clear_cache")
async def clear_cache(
    service_manager = Depends(get_service_manager)
) -> Dict[str, str]:
    """清空系统缓存"""
    try:
        # 审计日志
        log_audit("clear_cache", "cache")
        
        # 清空缓存
        await service_manager.event_cache.clear_cache()
        service_manager.fusion_engine.clear_cache()
        
        logger.info("System cache cleared")
        
        return {
            "message": "缓存已清空",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error clearing cache: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"清空缓存失败: {str(e)}")


@router.post("/suppression/clear", summary="清除所有抑制规则")
@async_performance_monitor("api.clear_suppressions")
async def clear_suppressions(
    service_manager = Depends(get_service_manager)
) -> Dict[str, str]:
    """清除所有抑制规则"""
    try:
        # 审计日志
        log_audit("clear_suppressions", "suppression")
        
        # 清除抑制规则
        await service_manager.suppression_manager.clear_all_suppressions()
        
        logger.info("All suppressions cleared")
        
        return {
            "message": "所有抑制规则已清除",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error clearing suppressions: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"清除抑制规则失败: {str(e)}")


# ============================================================================
# 配置和调试接口
# ============================================================================

@router.get("/config", summary="获取配置信息")
@async_performance_monitor("api.get_config")
async def get_configuration() -> Dict[str, Any]:
    """获取系统配置信息"""
    from app.core.config import settings
    
    try:
        return {
            "app_name": settings.app_name,
            "app_version": settings.app_version,
            "environment": settings.environment,
            "debug": settings.debug,
            "fusion_rules": {
                "silence_window_seconds": settings.fusion_rules.silence_window_seconds,
                "new_event_threshold_seconds": settings.fusion_rules.new_event_threshold_seconds,
                "event_expiry_seconds": settings.fusion_rules.event_expiry_seconds,
                "stakes": settings.fusion_rules.stakes,
                "adjacent_fusion_events": settings.fusion_rules.adjacent_fusion_events
            },
            "kafka": {
                "bootstrap_servers": settings.kafka.bootstrap_servers,
                "consumer_group": settings.kafka.consumer_group,
                "event_topic": settings.kafka.event_topic,
                "processed_topic": settings.kafka.processed_topic,
                "video_topic": settings.kafka.video_topic
            },
            "performance": {
                "max_workers": settings.performance.max_workers,
                "max_concurrent_requests": settings.performance.max_concurrent_requests,
                "task_queue_size": settings.performance.task_queue_size,
                "task_worker_count": settings.performance.task_worker_count
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting configuration: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取配置信息失败: {str(e)}")


@router.get("/debug/kafka-topics", summary="获取Kafka主题信息")
@async_performance_monitor("api.get_kafka_topics")
async def get_kafka_topics() -> Dict[str, Any]:
    """获取Kafka主题信息（调试用）"""
    try:
        topics = await kafka_manager.list_topics()
        topic_info = {}
        
        for topic in topics:
            info = await kafka_manager.get_topic_info(topic)
            if info:
                topic_info[topic] = info
        
        return {
            "topics_count": len(topics),
            "topics": topic_info
        }
        
    except Exception as e:
        logger.error(f"Error getting Kafka topics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取Kafka主题信息失败: {str(e)}")


# ============================================================================
# 视频处理接口
# ============================================================================

@router.post("/video/process", summary="处理视频请求")
@async_performance_monitor("api.process_video")
async def process_video_request(
    request: Dict[str, Any],
    service_manager = Depends(get_service_manager)
) -> Dict[str, Any]:
    """处理视频生成请求"""
    try:
        from app.models.video import VideoRequest
        from app.services.video.task_queue import submit_video_task
        
        # 验证请求数据
        video_request = VideoRequest(**request)
        
        # 提交视频任务
        task_id = await submit_video_task(video_request)
        
        logger.info(f"Video task submitted: {task_id}", request_id=video_request.request_id)
        
        return {
            "success": True,
            "task_id": task_id,
            "request_id": video_request.request_id,
            "message": "视频处理任务已提交",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing video request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"视频处理请求失败: {str(e)}")


@router.get("/video/task/{task_id}", summary="获取视频任务状态")
@async_performance_monitor("api.get_video_task_status")
async def get_video_task_status(task_id: str) -> Dict[str, Any]:
    """获取视频任务状态"""
    try:
        from app.services.video.task_queue import get_video_task_status
        
        task_status = await get_video_task_status(task_id)
        
        if not task_status:
            raise HTTPException(status_code=404, detail="任务不存在")
        
        return task_status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting video task status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取任务状态失败: {str(e)}")


@router.get("/video/stats", summary="获取视频处理统计")
@async_performance_monitor("api.get_video_stats")
async def get_video_stats() -> Dict[str, Any]:
    """获取视频处理统计信息"""
    try:
        from app.services.video.task_queue import video_task_queue
        from app.services.video.storage_manager import get_storage_stats
        from app.services.video.rtsp_manager import rtsp_manager
        
        queue_stats = video_task_queue.get_queue_stats()
        storage_stats = await get_storage_stats()
        rtsp_stats = rtsp_manager.get_all_stats()
        
        return {
            "queue_stats": queue_stats,
            "storage_stats": storage_stats,
            "rtsp_stats": rtsp_stats,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting video stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取视频统计失败: {str(e)}")


# ============================================================================
# 通知服务接口
# ============================================================================

@router.get("/notification/stats", summary="获取通知服务统计")
@async_performance_monitor("api.get_notification_stats")
async def get_notification_stats() -> Dict[str, Any]:
    """获取通知服务统计信息"""
    try:
        from app.services.notification.event_sender import event_sender
        
        stats = event_sender.get_stats()
        health = await event_sender.health_check()
        
        return {
            "stats": stats.dict(),
            "health": health,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting notification stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取通知统计失败: {str(e)}")


@router.post("/notification/test-send", summary="测试发送通知")
@async_performance_monitor("api.test_notification_send")
async def test_notification_send(
    test_data: Dict[str, Any],
    service_manager = Depends(get_service_manager)
) -> Dict[str, Any]:
    """测试发送通知"""
    try:
        from app.services.notification.event_sender import event_sender
        from app.models.fusion import FusedEvent
        from app.models.event import EventKey
        
        # 创建测试融合事件
        event_data = EventData(**test_data)
        event_key = EventKey.from_event(event_data)
        
        fused_event = FusedEvent(
            key=event_key,
            data=event_data,
            first_report_time=datetime.now(timezone.utc),
            last_report_time=datetime.now(timezone.utc),
            last_update_time=datetime.now(timezone.utc),
            total_reports=1,
            is_suppressed=False
        )
        
        # 发送测试通知
        result = await event_sender.send_event(fused_event)
        
        return {
            "success": result.success,
            "result": result.dict(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error testing notification send: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"测试发送失败: {str(e)}")


# ============================================================================
# 监控服务接口
# ============================================================================

@router.get("/monitoring/metrics", summary="获取监控指标")
@async_performance_monitor("api.get_monitoring_metrics")
async def get_monitoring_metrics() -> Dict[str, Any]:
    """获取监控指标"""
    try:
        from app.services.monitoring.metrics_collector import metrics_collector
        
        # 获取系统摘要
        system_summary = metrics_collector.get_system_summary()
        
        # 获取自定义指标
        custom_metrics = metrics_collector.get_all_custom_metrics()
        
        # 获取健康状态
        health = await metrics_collector.health_check()
        
        return {
            "system_summary": system_summary,
            "custom_metrics_count": len(custom_metrics),
            "health": health,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting monitoring metrics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取监控指标失败: {str(e)}")


@router.get("/monitoring/prometheus", summary="获取Prometheus格式指标")
@async_performance_monitor("api.get_prometheus_metrics")
async def get_prometheus_metrics():
    """获取Prometheus格式的指标"""
    try:
        from app.services.monitoring.metrics_collector import metrics_collector
        from fastapi.responses import PlainTextResponse
        
        metrics_data = metrics_collector.get_prometheus_metrics()
        
        return PlainTextResponse(
            content=metrics_data,
            media_type="text/plain; version=0.0.4; charset=utf-8"
        )
        
    except Exception as e:
        logger.error(f"Error getting Prometheus metrics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取Prometheus指标失败: {str(e)}")


# ============================================================================
# 测试接口
# ============================================================================

@router.post("/test/send-kafka-message", summary="发送测试Kafka消息")
@async_performance_monitor("api.send_test_kafka_message")
async def send_test_kafka_message(
    topic: str = Query(..., description="目标主题"),
    message: str = Query(..., description="消息内容"),
    key: Optional[str] = Query(None, description="消息键")
) -> Dict[str, Any]:
    """发送测试Kafka消息（调试用）"""
    try:
        success = await kafka_manager.send_message(topic, message, key=key)
        
        return {
            "success": success,
            "topic": topic,
            "message": message,
            "key": key,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error sending test Kafka message: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"发送测试消息失败: {str(e)}")


@router.get("/test/generate-sample-event", response_model=EventData, summary="生成示例事件")
@async_performance_monitor("api.generate_sample_event")
async def generate_sample_event(
    event_type: str = Query("01", description="事件类型"),
    stake_num: str = Query("K1+000", description="桩号"),
    direction: int = Query(1, description="方向")
) -> EventData:
    """生成示例事件数据（测试用）"""
    try:
        from uuid import uuid4
        
        sample_event = EventData(
            alarmID=f"TEST_{uuid4().hex[:8].upper()}",
            stakeNum=stake_num,
            reportCount=1,
            eventTime=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            eventType=event_type,
            direction=direction,
            eventLevel="3" if event_type == "01" else None,
            photoUrl="http://example.com/test.jpg",
            videoUrl=None,
            remark1="测试事件",
            remark2=None,
            remark3=None
        )
        
        return sample_event
        
    except Exception as e:
        logger.error(f"Error generating sample event: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"生成示例事件失败: {str(e)}")


# 添加根路径重定向
@router.get("/", summary="API根路径")
async def api_root():
    """API根路径"""
    return {
        "message": "事件融合服务 API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
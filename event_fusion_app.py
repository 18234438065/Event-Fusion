#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
事件融合服务主应用
Event Fusion Service Main Application

实现完整的事件融合逻辑，包括去重、聚合、抑制、视频生成等功能
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

import redis
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 配置信息
class Config:
    # Redis配置
    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))
    
    # Kafka配置
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_HIGHWAY_EVENTS_TOPIC = "highway-events"
    KAFKA_PROCESSED_EVENTS_TOPIC = "processed-events"
    KAFKA_VIDEO_REQUESTS_TOPIC = "video-requests"
    KAFKA_AUDIT_TOPIC = "event-send-audit"
    
    # Elasticsearch配置
    ELASTICSEARCH_HOSTS = os.getenv("ELASTICSEARCH_HOSTS", "http://elasticsearch:9200")
    ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "event-fusion-logs")
    
    # 融合规则配置
    SILENCE_WINDOW_SECONDS = 60  # 1分钟静默窗口
    NEW_EVENT_THRESHOLD_SECONDS = 120  # 2分钟后作为新事件
    
    # 视频配置
    VIDEO_DURATION_SECONDS = 30  # 30秒视频
    VIDEO_STORAGE_PATH = "/app/data/videos"
    
    # 外部服务配置
    BUSINESS_PLATFORM_URL = os.getenv("BUSINESS_PLATFORM_URL", "http://mock-receiver:5000/receive")

config = Config()

# Pydantic模型
class EventInput(BaseModel):
    alarmID: str
    stakeNum: str
    reportCount: int = 1
    eventTime: str
    eventType: str
    direction: int = Field(..., ge=0, le=2)
    eventLevel: str = "1"
    photoUrl: str
    videoUrl: Optional[str] = None
    remark1: Optional[str] = None
    remark2: Optional[str] = None
    remark3: Optional[str] = None

class EventOutput(BaseModel):
    alarmID: str
    stakeNum: str
    reportCount: int
    eventTime: str
    eventType: str
    direction: int
    eventLevel: str
    photoUrl: str
    videoUrl: str
    remark1: Optional[str] = None
    remark2: Optional[str] = None
    remark3: Optional[str] = None

# 事件融合服务类
class EventFusionService:
    def __init__(self):
        self.redis_client = None
        self.kafka_producer = None
        self.es_client = None
        self.app = FastAPI(title="Event Fusion Service", version="1.0.0")
        self.setup_routes()
        
        # 事件类型配置
        self.event_types = {
            "01": "交通拥堵", "02": "车流量检测", "03": "车速检测",
            "04": "抛洒物事件检测", "05": "逆行事件检测", "06": "机动车违法事件",
            "07": "异常停车事件", "08": "行人行走驻留事件", "09": "行人闯入事件检测",
            "10": "非机动车闯入事件检测", "11": "施工占道事件检测", "12": "起火/烟雾事件检测",
            "13": "车道路面检测", "14": "基础及周边设施检测", "15": "交通事故"
        }
        
        # 相邻融合事件类型
        self.adjacent_fusion_types = {"01", "04", "05", "06", "07", "08", "09", "15"}
        
        # 需要生成视频的事件类型
        self.video_generation_types = {"01", "04", "05", "06", "07", "08", "09", "15"}
        
        # 桩号列表（用于相邻检查）
        self.stakes = ["K0+900", "K0+800", "K1+000", "K1+100", "K1+200"]

    async def startup(self):
        """启动时初始化连接"""
        try:
            # 初始化Redis连接
            self.redis_client = redis.Redis(
                host=config.REDIS_HOST,
                port=config.REDIS_PORT,
                db=config.REDIS_DB,
                decode_responses=True
            )
            await asyncio.sleep(0.1)  # 测试连接
            logger.info("Redis连接成功")
            
            # 初始化Kafka生产者
            self.kafka_producer = AIOKafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
            )
            await self.kafka_producer.start()
            logger.info("Kafka生产者启动成功")
            
            # 初始化Elasticsearch客户端
            self.es_client = AsyncElasticsearch([config.ELASTICSEARCH_HOSTS])
            logger.info(f"Elasticsearch客户端初始化成功: {config.ELASTICSEARCH_HOSTS}")
            
        except Exception as e:
            logger.error(f"启动失败: {e}")
            raise

    async def shutdown(self):
        """关闭时清理连接"""
        if self.kafka_producer:
            await self.kafka_producer.stop()
        if self.es_client:
            await self.es_client.close()
        if self.redis_client:
            self.redis_client.close()

    def setup_routes(self):
        """设置路由"""
        
        @self.app.on_event("startup")
        async def startup_event():
            await self.startup()
        
        @self.app.on_event("shutdown")
        async def shutdown_event():
            await self.shutdown()
        
        @self.app.get("/health")
        async def health_check():
            """健康检查"""
            return {
                "status": "healthy",
                "service": "event-fusion-service",
                "version": "1.0.0",
                "timestamp": datetime.now().isoformat(),
                "components": {
                    "redis": "connected" if self.redis_client else "disconnected",
                    "kafka": "connected" if self.kafka_producer else "disconnected",
                    "elasticsearch": "connected" if self.es_client else "disconnected"
                }
            }
        
        @self.app.post("/event")
        async def process_event(event: EventInput, background_tasks: BackgroundTasks):
            """处理事件融合"""
            try:
                # 记录接收到的事件
                await self.log_event("event_received", event.dict())
                
                # 执行事件融合逻辑
                result = await self.process_event_fusion(event)
                
                if result["action"] == "SUPPRESSED":
                    return JSONResponse(
                        status_code=200,
                        content={
                            "status": "suppressed",
                            "message": result["reason"],
                            "alarmID": event.alarmID
                        }
                    )
                elif result["action"] == "FILTERED":
                    return JSONResponse(
                        status_code=200,
                        content={
                            "status": "filtered",
                            "message": "Event within silence window",
                            "alarmID": event.alarmID
                        }
                    )
                else:
                    # 发送到业务平台
                    output_event = result["output_event"]
                    background_tasks.add_task(self.send_to_business_platform, output_event)
                    
                    # 异步生成视频
                    if event.eventType in self.video_generation_types:
                        background_tasks.add_task(self.generate_video, output_event)
                    
                    return JSONResponse(
                        status_code=200,
                        content={
                            "status": "processed",
                            "action": result["action"],
                            "alarmID": output_event["alarmID"],
                            "reportCount": output_event["reportCount"]
                        }
                    )
                    
            except Exception as e:
                logger.error(f"处理事件失败: {e}")
                await self.log_event("event_error", {"error": str(e), "event": event.dict()})
                raise HTTPException(status_code=500, detail=f"处理事件失败: {e}")

    async def process_event_fusion(self, event: EventInput) -> Dict[str, Any]:
        """执行事件融合逻辑"""
        
        # 1. 检查抑制规则
        suppression_result = await self.check_suppression_rules(event)
        if suppression_result["suppressed"]:
            await self.log_event("event_suppressed", {
                "event": event.dict(),
                "reason": suppression_result["reason"]
            })
            return {
                "action": "SUPPRESSED",
                "reason": suppression_result["reason"]
            }
        
        # 2. 事件去重和聚合检查
        cache_key = f"event:{event.stakeNum}:{event.eventType}:{event.direction}"
        cached_event = self.redis_client.get(cache_key)
        
        current_time = datetime.now()
        
        if cached_event:
            cached_data = json.loads(cached_event)
            last_time = datetime.fromisoformat(cached_data["last_time"])
            time_diff = (current_time - last_time).total_seconds()
            
            if time_diff < config.SILENCE_WINDOW_SECONDS:
                # 静默窗口内，过滤事件
                await self.log_event("event_filtered", {
                    "event": event.dict(),
                    "time_diff": time_diff
                })
                return {"action": "FILTERED"}
            
            elif time_diff < config.NEW_EVENT_THRESHOLD_SECONDS:
                # 聚合事件
                cached_data["report_count"] += 1
                cached_data["last_time"] = current_time.isoformat()
                cached_data["event_time"] = event.eventTime
                
                # 更新缓存
                self.redis_client.setex(
                    cache_key, 
                    config.NEW_EVENT_THRESHOLD_SECONDS,
                    json.dumps(cached_data)
                )
                
                output_event = event.dict()
                output_event["alarmID"] = cached_data["original_alarm_id"]
                output_event["reportCount"] = cached_data["report_count"]
                
                await self.log_event("event_aggregated", {
                    "event": event.dict(),
                    "aggregated_count": cached_data["report_count"]
                })
                
                return {
                    "action": "AGGREGATED",
                    "output_event": output_event
                }
        
        # 3. 新事件处理
        event_data = {
            "original_alarm_id": event.alarmID,
            "report_count": 1,
            "last_time": current_time.isoformat(),
            "event_time": event.eventTime
        }
        
        self.redis_client.setex(
            cache_key,
            config.NEW_EVENT_THRESHOLD_SECONDS,
            json.dumps(event_data)
        )
        
        output_event = event.dict()
        
        await self.log_event("event_new", {"event": event.dict()})
        
        return {
            "action": "NEW_EVENT",
            "output_event": output_event
        }

    async def check_suppression_rules(self, event: EventInput) -> Dict[str, Any]:
        """检查事件抑制规则"""
        
        # 规则1: 严重拥堵抑制停车和行人事件
        if event.eventType == "01" and event.eventLevel == "5":
            # 设置抑制标记
            suppression_key = f"suppression:severe_traffic:{event.stakeNum}:{event.direction}"
            self.redis_client.setex(suppression_key, 300, "active")  # 5分钟抑制
            
            return {"suppressed": False}  # 严重拥堵事件本身不被抑制
        
        # 检查是否被严重拥堵抑制
        if event.eventType in ["07", "08", "09"]:
            suppression_key = f"suppression:severe_traffic:{event.stakeNum}:{event.direction}"
            if self.redis_client.get(suppression_key):
                return {
                    "suppressed": True,
                    "reason": "被严重拥堵事件抑制"
                }
        
        # 规则2: 施工占道抑制其他事件
        if event.eventType == "11":
            # 设置施工抑制标记
            suppression_key = f"suppression:construction:{event.stakeNum}:{event.direction}"
            self.redis_client.setex(suppression_key, 1800, "active")  # 30分钟抑制
            
            return {"suppressed": False}  # 施工事件本身不被抑制
        
        # 检查是否被施工占道抑制
        suppression_key = f"suppression:construction:{event.stakeNum}:{event.direction}"
        if self.redis_client.get(suppression_key):
            return {
                "suppressed": True,
                "reason": "被施工占道事件抑制"
            }
        
        return {"suppressed": False}

    async def generate_video(self, event: Dict[str, Any]):
        """生成事件视频片段"""
        try:
            video_id = str(uuid.uuid4())
            video_filename = f"event_{event['alarmID']}_{video_id}.mp4"
            video_path = os.path.join(config.VIDEO_STORAGE_PATH, video_filename)
            
            # 模拟视频生成（实际应该调用RTSP处理服务）
            os.makedirs(config.VIDEO_STORAGE_PATH, exist_ok=True)
            
            # 发送视频生成请求到Kafka
            video_request = {
                "video_id": video_id,
                "alarm_id": event["alarmID"],
                "stake_num": event["stakeNum"],
                "event_time": event["eventTime"],
                "event_type": event["eventType"],
                "duration": config.VIDEO_DURATION_SECONDS,
                "output_path": video_path,
                "timestamp": datetime.now().isoformat()
            }
            
            await self.kafka_producer.send(
                config.KAFKA_VIDEO_REQUESTS_TOPIC,
                video_request
            )
            
            # 模拟视频生成延迟
            await asyncio.sleep(2)
            
            # 创建模拟视频文件
            with open(video_path, "w") as f:
                f.write(f"Mock video file for event {event['alarmID']}")
            
            # 生成视频URL
            video_url = f"http://video-service/videos/{video_filename}"
            
            # 更新事件的videoUrl
            event["videoUrl"] = video_url
            
            await self.log_event("video_generated", {
                "alarm_id": event["alarmID"],
                "video_url": video_url,
                "video_path": video_path
            })
            
            logger.info(f"视频生成完成: {video_url}")
            
        except Exception as e:
            logger.error(f"视频生成失败: {e}")
            await self.log_event("video_generation_error", {
                "alarm_id": event["alarmID"],
                "error": str(e)
            })

    async def send_to_business_platform(self, event: Dict[str, Any]):
        """发送事件到业务平台"""
        try:
            # 发送到Kafka进行审计
            audit_record = {
                "alarm_id": event["alarmID"],
                "target_url": config.BUSINESS_PLATFORM_URL,
                "event_data": event,
                "timestamp": datetime.now().isoformat(),
                "status": "sending"
            }
            
            await self.kafka_producer.send(
                config.KAFKA_AUDIT_TOPIC,
                audit_record
            )
            
            # 发送到已处理事件主题
            await self.kafka_producer.send(
                config.KAFKA_PROCESSED_EVENTS_TOPIC,
                event
            )
            
            await self.log_event("event_sent", {
                "alarm_id": event["alarmID"],
                "target_url": config.BUSINESS_PLATFORM_URL
            })
            
            logger.info(f"事件已发送到业务平台: {event['alarmID']}")
            
        except Exception as e:
            logger.error(f"发送事件到业务平台失败: {e}")
            await self.log_event("event_send_error", {
                "alarm_id": event["alarmID"],
                "error": str(e)
            })

    async def log_event(self, event_type: str, data: Dict[str, Any]):
        """记录事件到Elasticsearch"""
        try:
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "event_type": event_type,
                "service": "event-fusion-service",
                "data": data
            }
            
            if self.es_client:
                await self.es_client.index(
                    index=config.ELASTICSEARCH_INDEX,
                    document=log_entry
                )
        except Exception as e:
            logger.error(f"记录日志失败: {e}")

# 创建应用实例
service = EventFusionService()
app = service.app

if __name__ == "__main__":
    uvicorn.run(
        "event_fusion_app:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        access_log=True
    )

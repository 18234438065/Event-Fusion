#!/usr/bin/env python3
"""
模拟事件接收服务
Mock Event Receiver Service

接收来自事件融合服务的处理后事件
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EventReceived(BaseModel):
    """接收的事件模型"""
    alarmID: str = Field(..., description="事件唯一标识符")
    stakeNum: str = Field(..., description="桩号")
    reportCount: int = Field(..., description="上报次数")
    eventTime: str = Field(..., description="事件时间")
    eventType: str = Field(..., description="事件类型代码")
    direction: int = Field(..., description="方向")
    eventLevel: str = Field(..., description="事件等级")
    photoUrl: str = Field(..., description="图片URL")
    videoUrl: str = Field(None, description="视频URL")
    remark1: str = Field(None, description="备注1")
    remark2: str = Field(None, description="备注2")
    remark3: str = Field(None, description="备注3")

class MockEventReceiver:
    def __init__(self):
        self.app = FastAPI(title="Mock Event Receiver", version="1.0.0")
        self.received_events: List[Dict] = []
        self.event_types = {
            "01": "交通拥堵", "02": "车流量检测", "03": "车速检测",
            "04": "抛洒物事件", "05": "逆行事件", "06": "机动车违法",
            "07": "异常停车", "08": "行人行走", "09": "行人闯入",
            "10": "非机动车闯入", "11": "施工占道", "12": "起火/烟雾",
            "13": "车道检测", "14": "设施检测", "15": "交通事故"
        }
        self.setup_routes()
    
    def setup_routes(self):
        @self.app.get("/")
        async def root():
            return {
                "service": "Mock Event Receiver",
                "version": "1.0.0",
                "status": "running",
                "total_received": len(self.received_events),
                "timestamp": datetime.now().isoformat()
            }
        
        @self.app.get("/health")
        async def health_check():
            return {
                "status": "healthy",
                "service": "mock-event-receiver",
                "timestamp": datetime.now().isoformat()
            }
        
        @self.app.post("/receive")
        async def receive_event(event: EventReceived, request: Request):
            """接收处理后的事件"""
            try:
                event_data = event.model_dump()
                event_data["received_at"] = datetime.now().isoformat()
                event_data["event_type_name"] = self.event_types.get(event.eventType, "未知事件")
                
                # 记录接收的事件
                self.received_events.append(event_data)
                
                # 记录日志
                logger.info(f"📨 接收到事件: {event.alarmID} - {event_data['event_type_name']} - 桩号: {event.stakeNum} - 上报次数: {event.reportCount}")
                
                # 模拟业务处理
                if event.videoUrl:
                    logger.info(f"🎥 事件包含视频: {event.videoUrl}")
                
                return {
                    "status": "received",
                    "message": "事件接收成功",
                    "alarmID": event.alarmID,
                    "processed_at": datetime.now().isoformat()
                }
                
            except Exception as e:
                logger.error(f"❌ 事件接收失败: {e}")
                raise HTTPException(status_code=500, detail=f"事件接收失败: {str(e)}")
        
        @self.app.get("/events")
        async def get_received_events(limit: int = 50):
            """获取接收到的事件列表"""
            return {
                "total": len(self.received_events),
                "events": self.received_events[-limit:],
                "timestamp": datetime.now().isoformat()
            }
        
        @self.app.get("/stats")
        async def get_statistics():
            """获取接收统计"""
            stats = {
                "total_events": len(self.received_events),
                "event_types": {},
                "stake_nums": {},
                "last_received": None
            }
            
            for event in self.received_events:
                # 事件类型统计
                event_type = event.get("eventType", "unknown")
                event_type_name = self.event_types.get(event_type, "未知事件")
                stats["event_types"][event_type_name] = stats["event_types"].get(event_type_name, 0) + 1
                
                # 桩号统计
                stake_num = event.get("stakeNum", "unknown")
                stats["stake_nums"][stake_num] = stats["stake_nums"].get(stake_num, 0) + 1
                
                # 最后接收时间
                if not stats["last_received"] or event.get("received_at", "") > stats["last_received"]:
                    stats["last_received"] = event.get("received_at")
            
            return stats
        
        @self.app.delete("/events")
        async def clear_events():
            """清空接收的事件"""
            count = len(self.received_events)
            self.received_events.clear()
            logger.info(f"🗑️ 清空了 {count} 个事件记录")
            return {
                "status": "cleared",
                "message": f"已清空 {count} 个事件记录",
                "timestamp": datetime.now().isoformat()
            }

# 创建服务实例
receiver = MockEventReceiver()
app = receiver.app

if __name__ == "__main__":
    logger.info("🚀 启动模拟事件接收服务...")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=False,
        access_log=True
    )

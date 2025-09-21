#!/usr/bin/env python3
"""
Kafka演示脚本
Kafka Demo Script
"""

import asyncio
import json
import logging
from datetime import datetime

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaDemo:
    def __init__(self, bootstrap_servers="localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.topics = {
            "events": "highway-events",
            "processed": "processed-events", 
            "video": "video-requests",
            "audit": "event-send-audit"
        }
    
    async def produce_test_events(self, count=10):
        """生产测试事件"""
        producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
        
        try:
            await producer.start()
            logger.info(f"开始发送 {count} 个测试事件")
            
            for i in range(count):
                event = {
                    "alarmID": f"KAFKA_TEST_{i:03d}",
                    "stakeNum": f"K{i}+{100+i*10}",
                    "eventType": str((i % 15) + 1).zfill(2),
                    "direction": i % 3,
                    "eventLevel": str((i % 5) + 1),
                    "eventTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "photoUrl": f"http://example.com/photo_{i}.jpg",
                    "reportCount": 1
                }
                
                await producer.send(self.topics["events"], event)
                logger.info(f"发送事件: {event['alarmID']}")
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"发送事件失败: {e}")
        finally:
            await producer.stop()
    
    async def consume_events(self, topic_key="events", timeout=30):
        """消费事件"""
        topic = self.topics[topic_key]
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        try:
            await consumer.start()
            logger.info(f"开始消费主题: {topic}")
            
            async for msg in consumer:
                logger.info(f"收到消息: {msg.value}")
                
        except Exception as e:
            logger.error(f"消费消息失败: {e}")
        finally:
            await consumer.stop()

async def main():
    """主函数"""
    demo = KafkaDemo("kafka-service.event-fusion.svc.cluster.local:9092")
    
    # 创建两个任务：生产和消费
    producer_task = asyncio.create_task(demo.produce_test_events(5))
    consumer_task = asyncio.create_task(demo.consume_events("processed"))
    
    # 等待生产者完成
    await producer_task
    
    # 让消费者运行一段时间
    try:
        await asyncio.wait_for(consumer_task, timeout=10)
    except asyncio.TimeoutError:
        logger.info("消费者超时，停止演示")
        consumer_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
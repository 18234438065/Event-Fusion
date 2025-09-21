#!/usr/bin/env python3
"""
Kibana演示脚本 - 生成测试数据和查询示例
Kibana Demo Script - Generate test data and query examples
"""

import json
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KibanaDemo:
    def __init__(self, es_host="http://localhost:9200", kibana_host="http://localhost:5601"):
        self.es_host = es_host
        self.kibana_host = kibana_host
        self.index_name = "event-fusion-logs"
    
    def generate_test_log_data(self, count=50) -> List[Dict[str, Any]]:
        """生成测试日志数据"""
        logs = []
        event_types = {
            "01": "交通拥堵",
            "02": "车流量检测", 
            "03": "车速检测",
            "04": "抛洒物事件",
            "05": "逆行事件",
            "15": "交通事故"
        }
        
        stakes = ["K0+100", "K0+200", "K1+000", "K1+100", "K2+000"]
        actions = ["PROCESSED", "AGGREGATED", "SUPPRESSED", "NEW_EVENT"]
        
        base_time = datetime.now() - timedelta(hours=2)
        
        for i in range(count):
            event_type = list(event_types.keys())[i % len(event_types)]
            stake = stakes[i % len(stakes)]
            action = actions[i % len(actions)]
            
            log_entry = {
                "@timestamp": (base_time + timedelta(minutes=i*2)).isoformat(),
                "alarmID": f"DEMO{event_type}{datetime.now().strftime('%Y%m%d%H%M%S')}{i:04d}",
                "stakeNum": stake,
                "eventType": event_type,
                "eventTypeName": event_types[event_type],
                "direction": i % 3,
                "eventLevel": str((i % 5) + 1),
                "reportCount": (i % 3) + 1,
                "eventTime": (base_time + timedelta(minutes=i*2)).strftime("%Y-%m-%d %H:%M:%S"),
                "photoUrl": f"http://example.com/photos/{stake}_{event_type}.jpg",
                "videoUrl": f"http://video-service/videos/event_{stake}_{i}.mp4" if i % 4 == 0 else "",
                "processingAction": action,
                "processingSteps": "接收事件 | 标准处理流程 | 数据验证 | 输出到业务平台",
                "suppressed": action == "SUPPRESSED",
                "aggregated": action == "AGGREGATED",
                "videoGenerated": i % 4 == 0,
                "responseTime": 50 + (i % 100),
                "service": "event-fusion-service",
                "level": "info" if action == "PROCESSED" else "warn",
                "message": f"事件 {stake} 处理完成: {action}"
            }
            logs.append(log_entry)
        
        return logs
    
    def send_logs_to_es(self, logs: List[Dict[str, Any]]) -> bool:
        """发送日志到Elasticsearch"""
        try:
            for log in logs:
                response = requests.post(
                    f"{self.es_host}/{self.index_name}/_doc",
                    json=log,
                    headers={"Content-Type": "application/json"}
                )
                if response.status_code not in [200, 201]:
                    logger.error(f"发送日志失败: {response.text}")
                    return False
            
            logger.info(f"成功发送 {len(logs)} 条日志到ES")
            return True
            
        except Exception as e:
            logger.error(f"发送日志到ES失败: {e}")
            return False
    
    def create_index_pattern(self) -> bool:
        """创建Kibana索引模式"""
        try:
            # 创建索引模式
            index_pattern = {
                "attributes": {
                    "title": self.index_name,
                    "timeFieldName": "@timestamp"
                }
            }
            
            response = requests.post(
                f"{self.kibana_host}/api/saved_objects/index-pattern/{self.index_name}",
                json=index_pattern,
                headers={
                    "Content-Type": "application/json",
                    "kbn-xsrf": "true"
                }
            )
            
            if response.status_code in [200, 409]:  # 409表示已存在
                logger.info("索引模式创建成功")
                return True
            else:
                logger.error(f"创建索引模式失败: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"创建索引模式失败: {e}")
            return False
    
    def get_sample_queries(self) -> Dict[str, str]:
        """获取示例查询"""
        return {
            "所有事件": "_exists_:alarmID",
            "交通拥堵": "eventType:01",
            "被抑制的事件": "suppressed:true",
            "聚合事件": "aggregated:true",
            "高等级事件": "eventLevel:(4 OR 5)",
            "K1桩号事件": "stakeNum:K1*",
            "最近1小时": "@timestamp:[now-1h TO now]",
            "处理时间>100ms": "responseTime:>100",
            "包含视频的事件": "_exists_:videoUrl AND NOT videoUrl:\"\"",
            "错误和警告": "level:(error OR warn)"
        }
    
    def print_kibana_guide(self):
        """打印Kibana使用指南"""
        queries = self.get_sample_queries()
        
        print("\n" + "="*60)
        print("🔍 KIBANA 使用指南")
        print("="*60)
        print(f"Kibana地址: {self.kibana_host}")
        print(f"索引模式: {self.index_name}")
        print("\n📊 推荐的可视化面板:")
        print("1. 事件类型分布 (饼图)")
        print("2. 事件处理趋势 (线图)")
        print("3. 桩号事件热力图 (热力图)")
        print("4. 响应时间分布 (直方图)")
        print("5. 抑制率统计 (指标)")
        
        print("\n🔍 常用搜索查询:")
        for name, query in queries.items():
            print(f"  {name}: {query}")
        
        print("\n📈 建议的Dashboard:")
        print("- 实时监控看板")
        print("- 事件分析报告")
        print("- 性能监控面板")
        print("- 故障排查视图")
        print("="*60)

def main():
    """主函数"""
    # 使用K8s服务地址
    demo = KibanaDemo(
        es_host="http://10.1.1.160:30920",
        kibana_host="http://10.1.1.160:30601"
    )
    
    logger.info("开始Kibana演示...")
    
    # 1. 生成测试数据
    logger.info("生成测试日志数据...")
    test_logs = demo.generate_test_log_data(100)
    
    # 2. 发送到ES
    logger.info("发送数据到Elasticsearch...")
    if demo.send_logs_to_es(test_logs):
        logger.info("✅ 测试数据发送成功")
    else:
        logger.error("❌ 测试数据发送失败")
        return
    
    # 3. 创建索引模式
    logger.info("创建Kibana索引模式...")
    if demo.create_index_pattern():
        logger.info("✅ 索引模式创建成功")
    else:
        logger.info("⚠️ 索引模式创建失败（可能已存在）")
    
    # 4. 打印使用指南
    demo.print_kibana_guide()
    
    logger.info("🎉 Kibana演示完成！")

if __name__ == "__main__":
    main()

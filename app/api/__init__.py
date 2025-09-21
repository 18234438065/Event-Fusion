"""API模块

提供RESTful API接口：
- 事件处理接口
- 监控和统计接口
- 管理接口
- 健康检查接口
"""

from app.api.routes import router

__all__ = ["router"]
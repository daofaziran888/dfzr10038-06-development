"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, window_engine

# window_engine 是 /api/window-engine 固定前缀，必须先于 resource 的
# /api/{module} 动态路由注册，避免被模块名捕获。
ROUTERS = [window_engine, resource]

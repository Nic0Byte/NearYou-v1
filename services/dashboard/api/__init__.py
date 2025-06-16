from fastapi import APIRouter

from .routes import router as dashboard_router

# Router principale che aggrega tutti i sub-router
api_router = APIRouter()


api_router.include_router(dashboard_router, prefix="/user", tags=["user"])

# api_router.include_router(admin_router, prefix="/admin", tags=["admin"])
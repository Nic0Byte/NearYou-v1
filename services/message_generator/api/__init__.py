from fastapi import APIRouter

from .routes import router as generator_router

# Router principale che aggrega tutti i sub-router
api_router = APIRouter()

api_router.include_router(generator_router, prefix="", tags=["generator"])
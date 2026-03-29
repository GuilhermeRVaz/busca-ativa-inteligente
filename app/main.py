from fastapi import FastAPI

from app.routes import router
from core.config import settings


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="Simple FastAPI scaffold for intelligent active outreach.",
)

app.include_router(router)

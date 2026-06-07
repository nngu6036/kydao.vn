from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import get_settings
from app.db import connect_to_mongo, disconnect_from_mongo
from app.routes.public import router as public_router
from app.routes.auth import router as auth_router
from app.routes.admin import router as admin_router
from app.routes.content import router as content_router

settings = get_settings()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await connect_to_mongo()
    try:
        yield
    finally:
        await disconnect_from_mongo()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    return {"status": "ok"}

app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(content_router)
app.include_router(public_router)

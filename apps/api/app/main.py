from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from app.config import get_settings
from app.db import connect_to_mongo, disconnect_from_mongo
from app.routes.public import router as public_router
from app.routes.auth import router as auth_router
from app.routes.admin import router as admin_router
from app.routes.content import router as content_router

settings = get_settings()
logger = logging.getLogger("chess_elo.api")
logger.setLevel(logging.INFO)


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info(
        "Starting API app=%s mongodb_database=%s cors_origins=%s cognito_region=%s cognito_user_pool_configured=%s cognito_client_configured=%s",
        settings.app_name,
        settings.mongodb_database,
        settings.cors_origins,
        settings.aws_region,
        bool(settings.cognito_user_pool_id),
        bool(settings.cognito_client_id),
    )
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


@app.middleware("http")
async def log_request_debug(request: Request, call_next):
    origin = request.headers.get("origin")
    request_method = request.headers.get("access-control-request-method")
    request_headers = request.headers.get("access-control-request-headers")
    is_preflight = request.method == "OPTIONS" and request_method is not None

    if is_preflight:
        logger.warning(
            "CORS preflight received path=%s origin=%s requested_method=%s requested_headers=%s origin_allowed=%s configured_origins=%s",
            request.url.path,
            origin,
            request_method,
            request_headers,
            origin in settings.cors_origins if origin else False,
            settings.cors_origins,
        )

    response = await call_next(request)

    if is_preflight or request.url.path.startswith("/auth/"):
        logger.warning(
            "Request completed method=%s path=%s status=%s origin=%s requested_method=%s allow_origin=%s",
            request.method,
            request.url.path,
            response.status_code,
            origin,
            request_method,
            response.headers.get("access-control-allow-origin"),
        )

    return response


@app.get("/health")
async def health():
    return {"status": "ok"}

app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(content_router)
app.include_router(public_router)

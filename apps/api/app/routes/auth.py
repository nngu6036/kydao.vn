import base64
import hashlib
import hmac
import logging
from functools import lru_cache
from typing import Any

import boto3
from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from app.config import get_settings

router = APIRouter(prefix="/auth", tags=["auth"])
logger = logging.getLogger("chess_elo.api.auth")
logger.setLevel(logging.INFO)


class LoginRequest(BaseModel):
    username: str
    password: str


@lru_cache
def cognito_client():
    settings = get_settings()
    logger.info("Creating Cognito client region=%s", settings.aws_region)
    return boto3.client("cognito-idp", region_name=settings.aws_region)


def secret_hash(username: str, client_id: str, client_secret: str) -> str:
    digest = hmac.new(
        client_secret.encode("utf-8"),
        msg=f"{username}{client_id}".encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def cognito_login(username: str, password: str) -> dict[str, Any]:
    settings = get_settings()
    if not settings.cognito_client_id:
        logger.error("Cognito login requested but CHESS_ELO_COGNITO_CLIENT_ID is not configured")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication provider is not configured",
        )

    auth_parameters = {
        "USERNAME": username,
        "PASSWORD": password,
    }
    if settings.cognito_client_secret:
        auth_parameters["SECRET_HASH"] = secret_hash(
            username,
            settings.cognito_client_id,
            settings.cognito_client_secret,
        )

    try:
        logger.info(
            "Calling Cognito initiate_auth auth_flow=%s region=%s client_configured=%s client_secret_configured=%s user_pool_configured=%s",
            "USER_PASSWORD_AUTH",
            settings.aws_region,
            bool(settings.cognito_client_id),
            bool(settings.cognito_client_secret),
            bool(settings.cognito_user_pool_id),
        )
        return cognito_client().initiate_auth(
            ClientId=settings.cognito_client_id,
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters=auth_parameters,
        )
    except ClientError as exc:
        error = exc.response.get("Error", {})
        error_code = error.get("Code")
        error_message = error.get("Message", "")
        logger.warning("Cognito login error code=%s message=%s", error_code, error_message)
        if error_code in {
            "NotAuthorizedException",
            "UserNotFoundException",
            "PasswordResetRequiredException",
            "UserNotConfirmedException",
        }:
            print(
                "DEBUG auth invalid credentials "
                f"username={username!r} "
                f"password_present={bool(password)} "
                f"error_code={error_code} "
                f"error_message={error_message!r}"
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials or Cognito app client auth flow is not enabled",
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Authentication provider error",
        ) from exc


@router.post("/login")
async def login(payload: LoginRequest):
    logger.info("Auth login request received username_present=%s password_present=%s", bool(payload.username), bool(payload.password))
    if not payload.username or not payload.password:
        print(
            "DEBUG auth invalid login payload "
            f"username_present={bool(payload.username)} "
            f"password_present={bool(payload.password)}"
        )
    response = await run_in_threadpool(cognito_login, payload.username, payload.password)

    if response.get("ChallengeName"):
        logger.warning("Cognito login requires challenge challenge=%s", response["ChallengeName"])
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Cognito challenge required: {response['ChallengeName']}",
        )

    result = response.get("AuthenticationResult")
    if not result or not result.get("AccessToken"):
        logger.error("Cognito login returned invalid response has_authentication_result=%s", bool(result))
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Authentication provider returned an invalid response",
        )

    logger.info(
        "Auth login succeeded token_type=%s expires_in=%s has_id_token=%s has_refresh_token=%s",
        result.get("TokenType", "Bearer").lower(),
        result.get("ExpiresIn"),
        bool(result.get("IdToken")),
        bool(result.get("RefreshToken")),
    )
    return {
        "access_token": result["AccessToken"],
        "token_type": result.get("TokenType", "Bearer").lower(),
        "expires_in": result.get("ExpiresIn"),
        "id_token": result.get("IdToken"),
        "refresh_token": result.get("RefreshToken"),
    }

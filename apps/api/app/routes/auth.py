import base64
import hashlib
import hmac
from functools import lru_cache
from typing import Any

import boto3
from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from app.config import get_settings

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


@lru_cache
def cognito_client():
    settings = get_settings()
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
        return cognito_client().initiate_auth(
            ClientId=settings.cognito_client_id,
            AuthFlow="USER_PASSWORD_AUTH",
            AuthParameters=auth_parameters,
        )
    except ClientError as exc:
        error = exc.response.get("Error", {})
        error_code = error.get("Code")
        error_message = error.get("Message", "")
        print(f"Cognito login error: {error_code}: {error_message}")
        if error_code in {
            "NotAuthorizedException",
            "UserNotFoundException",
            "PasswordResetRequiredException",
            "UserNotConfirmedException",
        }:
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
    response = await run_in_threadpool(cognito_login, payload.username, payload.password)

    if response.get("ChallengeName"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Cognito challenge required: {response['ChallengeName']}",
        )

    result = response.get("AuthenticationResult")
    if not result or not result.get("AccessToken"):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Authentication provider returned an invalid response",
        )

    return {
        "access_token": result["AccessToken"],
        "token_type": result.get("TokenType", "Bearer").lower(),
        "expires_in": result.get("ExpiresIn"),
        "id_token": result.get("IdToken"),
        "refresh_token": result.get("RefreshToken"),
    }

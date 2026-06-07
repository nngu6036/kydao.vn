from functools import lru_cache
from typing import Any

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient
from jwt.exceptions import PyJWKClientError, PyJWTError

from app.config import get_settings

bearer_scheme = HTTPBearer(auto_error=False)


def _auth_error(detail: str = "Not authenticated") -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def _issuer(region: str, user_pool_id: str) -> str:
    return f"https://cognito-idp.{region}.amazonaws.com/{user_pool_id}"


@lru_cache
def _jwks_client(region: str, user_pool_id: str) -> PyJWKClient:
    return PyJWKClient(f"{_issuer(region, user_pool_id)}/.well-known/jwks.json")


def require_admin_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> dict[str, Any]:
    if credentials is None:
        raise _auth_error()

    settings = get_settings()
    if not settings.cognito_user_pool_id or not settings.cognito_client_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication provider is not configured",
        )

    token = credentials.credentials
    issuer = _issuer(settings.aws_region, settings.cognito_user_pool_id)

    try:
        signing_key = _jwks_client(settings.aws_region, settings.cognito_user_pool_id).get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            issuer=issuer,
            options={"verify_aud": False},
        )
    except (PyJWKClientError, PyJWTError) as exc:
        raise _auth_error("Invalid or expired token") from exc

    if claims.get("token_use") != "access":
        raise _auth_error("Invalid token type")

    if claims.get("client_id") != settings.cognito_client_id:
        raise _auth_error("Invalid token client")

    return claims

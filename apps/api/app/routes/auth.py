from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, HTTPException
from jose import jwt

router = APIRouter(prefix="/auth", tags=["auth"])

@router.post("/login")
async def login(payload: dict):
    if payload.get("username") != "admin" or payload.get("password") != "admin123":
        raise HTTPException(status_code=401, detail="Invalid credentials")
    exp = datetime.now(timezone.utc) + timedelta(days=1)
    token = jwt.encode({"sub": "admin", "exp": exp}, "change-me", algorithm="HS256")
    return {"access_token": token, "token_type": "bearer"}

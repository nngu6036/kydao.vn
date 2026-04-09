from fastapi import APIRouter
router = APIRouter(prefix="/admin", tags=["admin"])

@router.get("/players")
async def players():
    return []

@router.get("/games")
async def games():
    return []

@router.get("/tournaments")
async def tournaments():
    return []

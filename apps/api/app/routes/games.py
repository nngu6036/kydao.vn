from fastapi import APIRouter
router = APIRouter(prefix="/games")

@router.get("")
def index():
    return []

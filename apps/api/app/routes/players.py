from fastapi import APIRouter
router = APIRouter(prefix="/players")

@router.get("")
def index():
    return []

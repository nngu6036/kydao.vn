from fastapi import APIRouter
router = APIRouter(prefix="/public")

@router.get("")
def index():
    return []

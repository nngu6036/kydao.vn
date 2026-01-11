from fastapi import APIRouter
router = APIRouter(prefix="/auth")

@router.get("")
def index():
    return []

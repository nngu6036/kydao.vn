from fastapi import APIRouter
router = APIRouter(prefix="/tournaments")

@router.get("")
def index():
    return []

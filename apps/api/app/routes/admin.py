from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db import get_database
from app.models import Game, Page, Player, Tournament
from app.repositories import GameRepository, PlayerRepository, TournamentRepository
from app.security import require_admin_token

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin_token)])


def _page(items: list[dict], total: int, skip: int, limit: int) -> Page:
    current_page = (skip // limit) + 1
    pages = (total + limit - 1) // limit if total else 0
    return Page(
        items=items,
        total=total,
        skip=skip,
        limit=limit,
        page=current_page,
        page_size=limit,
        pages=pages,
    )


def _paging(
    page: int,
    page_size: int,
    skip: int | None,
    limit: int | None,
) -> tuple[int, int]:
    if skip is not None or limit is not None:
        resolved_limit = limit if limit is not None else page_size
        resolved_skip = skip if skip is not None else 0
        return resolved_skip, resolved_limit
    return (page - 1) * page_size, page_size


def _sort_dir(value: str) -> int:
    return -1 if value.lower() == "desc" else 1


@router.get("/players", response_model=Page)
async def players(
    query: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    skip: int | None = Query(default=None, ge=0),
    limit: int | None = Query(default=None, ge=1, le=200),
    sort_by: str | None = None,
    sort_dir: str = Query(default="asc", pattern="^(asc|desc)$"),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    skip, limit = _paging(page, page_size, skip, limit)
    items, total = await PlayerRepository(db).list(
        query=query,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_dir=_sort_dir(sort_dir),
    )
    return _page(items, total, skip, limit)


@router.get("/players/search", response_model=list[Player])
async def search_players_by_name(
    name: str = Query(default=""),
    limit: int = Query(default=10, ge=1, le=10),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    return await PlayerRepository(db).search_by_name(name=name, limit=limit)


@router.post("/players", response_model=Player)
async def create_player(payload: dict, db: AsyncIOMotorDatabase = Depends(get_database)):
    return await PlayerRepository(db).create(payload)


@router.get("/players/{player_id}", response_model=Player)
async def get_player(player_id: str, db: AsyncIOMotorDatabase = Depends(get_database)):
    player = await PlayerRepository(db).get(player_id)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return player


@router.put("/players/{player_id}", response_model=Player)
async def update_player(player_id: str, payload: dict, db: AsyncIOMotorDatabase = Depends(get_database)):
    player = await PlayerRepository(db).update(player_id, payload)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return player


@router.get("/games", response_model=Page)
async def games(
    query: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    skip: int | None = Query(default=None, ge=0),
    limit: int | None = Query(default=None, ge=1, le=200),
    sort_by: str | None = None,
    sort_dir: str = Query(default="asc", pattern="^(asc|desc)$"),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    skip, limit = _paging(page, page_size, skip, limit)
    items, total = await GameRepository(db).list(
        query=query,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_dir=_sort_dir(sort_dir),
    )
    return _page(items, total, skip, limit)


@router.post("/games", response_model=Game)
async def create_game(payload: dict, db: AsyncIOMotorDatabase = Depends(get_database)):
    try:
        return await GameRepository(db).create(payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/games/{game_id}", response_model=Game)
async def get_game(game_id: str, db: AsyncIOMotorDatabase = Depends(get_database)):
    game = await GameRepository(db).get(game_id)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    return game


@router.put("/games/{game_id}", response_model=Game)
async def update_game(game_id: str, payload: dict, db: AsyncIOMotorDatabase = Depends(get_database)):
    try:
        game = await GameRepository(db).update(game_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    return game


@router.get("/tournaments", response_model=Page)
async def tournaments(
    query: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    skip: int | None = Query(default=None, ge=0),
    limit: int | None = Query(default=None, ge=1, le=200),
    sort_by: str | None = None,
    sort_dir: str = Query(default="asc", pattern="^(asc|desc)$"),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    skip, limit = _paging(page, page_size, skip, limit)
    items, total = await TournamentRepository(db).list(
        query=query,
        skip=skip,
        limit=limit,
        sort_by=sort_by,
        sort_dir=_sort_dir(sort_dir),
    )
    return _page(items, total, skip, limit)


@router.get("/tournaments/search", response_model=list[Tournament])
async def search_tournaments_by_name(
    name: str = Query(default=""),
    limit: int = Query(default=10, ge=1, le=10),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    return await TournamentRepository(db).search_by_name(name=name, limit=limit)


@router.post("/tournaments", response_model=Tournament)
async def create_tournament(payload: dict, db: AsyncIOMotorDatabase = Depends(get_database)):
    return await TournamentRepository(db).create(payload)


@router.get("/tournaments/{tournament_id}", response_model=Tournament)
async def get_tournament(tournament_id: str, db: AsyncIOMotorDatabase = Depends(get_database)):
    tournament = await TournamentRepository(db).get(tournament_id)
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return tournament


@router.put("/tournaments/{tournament_id}", response_model=Tournament)
async def update_tournament(tournament_id: str, payload: dict, db: AsyncIOMotorDatabase = Depends(get_database)):
    tournament = await TournamentRepository(db).update(tournament_id, payload)
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return tournament

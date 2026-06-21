from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db import get_database
from app.models import Game, Page, Player, Tournament
from app.repositories import GameRepository, PlayerRepository, TournamentRepository

router = APIRouter(tags=["content"])


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


@router.get("/players", response_model=Page)
async def list_players(
    query: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    skip: int | None = Query(default=None, ge=0),
    limit: int | None = Query(default=None, ge=1, le=200),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    skip, limit = _paging(page, page_size, skip, limit)
    items, total = await PlayerRepository(db).list(query=query, skip=skip, limit=limit)
    return _page(items, total, skip, limit)


@router.get("/players/elo-rankings", response_model=Page)
async def list_elo_ranking_players(
    query: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    skip: int | None = Query(default=None, ge=0),
    limit: int | None = Query(default=None, ge=1, le=200),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    skip, limit = _paging(page, page_size, skip, limit)
    items, total = await PlayerRepository(db).list_elo_rankings(query=query, skip=skip, limit=limit)
    return _page(items, total, skip, limit)


@router.get("/players/{player_id}", response_model=Player)
async def get_player(player_id: str, db: AsyncIOMotorDatabase = Depends(get_database)):
    player = await PlayerRepository(db).get(player_id)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return player


@router.get("/players/{player_id}/games", response_model=Page)
async def list_games_by_player(
    player_id: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    skip: int | None = Query(default=None, ge=0),
    limit: int | None = Query(default=None, ge=1, le=200),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    skip, limit = _paging(page, page_size, skip, limit)
    items, total = await GameRepository(db).list_by_player(player_id, skip=skip, limit=limit)
    return _page(items, total, skip, limit)


@router.get("/tournaments", response_model=Page)
async def list_tournaments(
    query: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    skip: int | None = Query(default=None, ge=0),
    limit: int | None = Query(default=None, ge=1, le=200),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    skip, limit = _paging(page, page_size, skip, limit)
    items, total = await TournamentRepository(db).list(query=query, skip=skip, limit=limit)
    return _page(items, total, skip, limit)


@router.get("/tournaments/{tournament_id}", response_model=Tournament)
async def get_tournament(tournament_id: str, db: AsyncIOMotorDatabase = Depends(get_database)):
    tournament = await TournamentRepository(db).get(tournament_id)
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    return tournament


@router.get("/tournaments/{tournament_id}/games", response_model=Page)
async def list_games_by_tournament(
    tournament_id: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    skip: int | None = Query(default=None, ge=0),
    limit: int | None = Query(default=None, ge=1, le=200),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    skip, limit = _paging(page, page_size, skip, limit)
    items, total = await GameRepository(db).list_by_tournament(tournament_id, skip=skip, limit=limit)
    return _page(items, total, skip, limit)


@router.get("/games", response_model=Page)
async def list_games(
    query: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    skip: int | None = Query(default=None, ge=0),
    limit: int | None = Query(default=None, ge=1, le=200),
    db: AsyncIOMotorDatabase = Depends(get_database),
):
    skip, limit = _paging(page, page_size, skip, limit)
    items, total = await GameRepository(db).list(query=query, skip=skip, limit=limit)
    return _page(items, total, skip, limit)


@router.get("/games/{game_id}", response_model=Game)
async def get_game(game_id: str, db: AsyncIOMotorDatabase = Depends(get_database)):
    game = await GameRepository(db).get(game_id)
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    return game

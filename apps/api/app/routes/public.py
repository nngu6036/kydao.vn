from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db import get_database
from app.repositories import GameRepository, PlayerRepository, TournamentRepository

router = APIRouter(prefix="/public", tags=["public"])

@router.get("/search")
async def search(query: str = "", db: AsyncIOMotorDatabase = Depends(get_database)):
    players, _ = await PlayerRepository(db).list(query=query, limit=10)
    games, _ = await GameRepository(db).list(query=query, limit=10)
    tournaments, _ = await TournamentRepository(db).list(query=query, limit=10)

    return {
        "query": query,
        "players": players,
        "games": games,
        "tournaments": tournaments,
        "featured": [],
        "ongoingTournaments": [],
    }

@router.get("/homepage")
async def homepage():
    return {
        "topMagazineLinks": [],
        "mainNavLinks": [],
        "promoLinks": [],
        "featuredTabs": [],
        "ongoingTournaments": [],
        "upcomingTournaments": [],
        "finishedTournaments": [],
        "playerCategories": [],
        "learningLinkStrips": [],
        "threeColumnSections": [],
        "friendLinks": [],
    }

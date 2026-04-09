from fastapi import APIRouter

router = APIRouter(prefix="/public", tags=["public"])

@router.get("/search")
async def search(query: str = ""):
    return {
        "query": query,
        "players": [],
        "games": [],
        "tournaments": [],
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

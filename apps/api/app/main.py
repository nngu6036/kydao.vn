from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import auth, players, games, tournaments, public

app = FastAPI(title="Chess ELO API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
    allow_methods=["*"],
)

app.include_router(auth.router)
app.include_router(players.router)
app.include_router(games.router)
app.include_router(tournaments.router)
app.include_router(public.router)

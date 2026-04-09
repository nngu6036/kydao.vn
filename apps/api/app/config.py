from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CHESS_ELO_",
        env_file=".env",
        extra="ignore",
    )

    app_name: str = "Chess ELO API"
    cors_origins: list[str] = Field(
        default_factory=lambda: [
            "http://localhost:4200",
            "http://localhost:4201",
        ]
    )
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_database: str = "chess_elo"
    mongodb_app_name: str = "chess-elo-api"


@lru_cache
def get_settings() -> Settings:
    return Settings()

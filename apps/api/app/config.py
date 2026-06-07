from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


API_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = API_ROOT.parents[1] if len(API_ROOT.parents) > 1 else API_ROOT
ENV_FILES = (REPO_ROOT / ".env", API_ROOT / ".env")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CHESS_ELO_",
        env_file=ENV_FILES,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return env_settings, dotenv_settings, init_settings, file_secret_settings

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
    aws_region: str = "ap-southeast-2"
    cognito_client_id: str = ""
    cognito_user_pool_id: str = ""
    cognito_client_secret: str | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()

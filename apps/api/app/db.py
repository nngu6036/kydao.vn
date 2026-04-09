from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.config import get_settings

_client: AsyncIOMotorClient | None = None
_database: AsyncIOMotorDatabase | None = None


async def connect_to_mongo() -> None:
    global _client, _database

    settings = get_settings()
    _client = AsyncIOMotorClient(
        settings.mongodb_uri,
        appname=settings.mongodb_app_name,
    )
    await _client.admin.command("ping")
    _database = _client[settings.mongodb_database]


async def disconnect_from_mongo() -> None:
    global _client, _database

    if _client is not None:
        _client.close()

    _client = None
    _database = None


def get_database() -> AsyncIOMotorDatabase:
    if _database is None:
        raise RuntimeError("MongoDB is not connected")

    return _database

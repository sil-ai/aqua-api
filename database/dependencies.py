# dependencies.py

import os

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Load environment variables from .env file
load_dotenv()

DATABASE_URL = os.getenv("AQUA_DB")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"Environment variable {name}={raw!r} is not a valid integer")


# Pytest's TestClient spawns a new event loop per request; asyncpg
# connections can't migrate across loops, so test runs must use NullPool.
# Production runs a single persistent loop per worker and benefits from
# pooling — avoids the asyncpg+TLS handshake on every request.
#
# Default sizing is conservative: 8 uvicorn workers × (2 + 3) = 40 max
# connections per container, well under the typical DigitalOcean managed
# Postgres limit (~25 on small tiers, ~97 mid-tier). Tune via env vars if
# the running tier and replica count have headroom.
if os.getenv("AQUA_DB_POOLCLASS", "").lower() == "null":
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
else:
    engine = create_async_engine(
        DATABASE_URL,
        pool_size=_env_int("AQUA_DB_POOL_SIZE", 2),
        max_overflow=_env_int("AQUA_DB_MAX_OVERFLOW", 3),
        pool_timeout=_env_int("AQUA_DB_POOL_TIMEOUT", 10),
        pool_recycle=_env_int("AQUA_DB_POOL_RECYCLE", 1800),
        pool_pre_ping=True,
    )
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_db():
    db: AsyncSession = AsyncSessionLocal()
    try:
        yield db
    finally:
        await db.close()

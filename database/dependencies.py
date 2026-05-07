# dependencies.py

import os

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

# Load environment variables from .env file
load_dotenv()

DATABASE_URL = os.getenv("AQUA_DB")

# Pytest's TestClient spawns a new event loop per request; asyncpg
# connections can't migrate across loops, so test runs must use NullPool.
# Production runs a single persistent loop per worker and benefits from
# pooling — avoids re-running asyncpg+TLS handshake on every request.
if os.getenv("AQUA_DB_POOLCLASS", "").lower() == "null":
    engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
else:
    engine = create_async_engine(
        DATABASE_URL,
        pool_size=int(os.getenv("AQUA_DB_POOL_SIZE", "5")),
        max_overflow=int(os.getenv("AQUA_DB_MAX_OVERFLOW", "10")),
        pool_timeout=int(os.getenv("AQUA_DB_POOL_TIMEOUT", "30")),
        pool_recycle=int(os.getenv("AQUA_DB_POOL_RECYCLE", "1800")),
        pool_pre_ping=True,
    )
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_db():
    db: AsyncSession = AsyncSessionLocal()
    try:
        yield db
    finally:
        await db.close()

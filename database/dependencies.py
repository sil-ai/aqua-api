# dependencies.py

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from config import settings

DATABASE_URL = settings.aqua_db


# Pytest's TestClient spawns a new event loop per request; asyncpg
# connections can't migrate across loops, so test runs must use NullPool.
# Production runs a single persistent loop per worker and benefits from
# pooling — avoids the asyncpg+TLS handshake on every request.
#
# Default sizing: with 8 uvicorn workers, steady-state is ~16 conns per
# container (8 × pool_size 2) and the burst ceiling is 40 (8 × (2 + 3)).
# RDS default max_connections is LEAST({DBInstanceClassMemory/9531392},
# 5000) — roughly 170 on db.t3.small, 340 on db.t3.medium, 675 on
# db.m5.large — so 40/container leaves comfortable headroom even on
# small instance classes. Tune the env vars if running many containers
# or if other consumers (alembic, batch jobs, replicas) eat the budget.
if settings.aqua_db_poolclass and settings.aqua_db_poolclass.lower() == "null":
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
else:
    engine = create_async_engine(
        DATABASE_URL,
        pool_size=settings.aqua_db_pool_size,
        max_overflow=settings.aqua_db_max_overflow,
        pool_timeout=settings.aqua_db_pool_timeout,
        pool_recycle=settings.aqua_db_pool_recycle,
        pool_pre_ping=True,
    )
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_db():
    db: AsyncSession = AsyncSessionLocal()
    try:
        yield db
    finally:
        await db.close()

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
# Default sizing: with 8 uvicorn workers, steady-state is ~40 conns per
# container (8 × pool_size 5) and the burst ceiling is 120 (8 × (5 + 10)).
# RDS default max_connections is LEAST({DBInstanceClassMemory/9531392},
# 5000) — roughly 170 on db.t3.small, 340 on db.t3.medium, 675 on
# db.m5.large — so 120/container leaves comfortable headroom even on
# small instance classes. NOTE: this budget is per-container — N concurrent
# App Runner instances multiply it (2 × 120 already exceeds t3.small's ~170),
# and there is no fleet-wide saturation alert yet. See #747. Tune the env
# vars if running many containers or if other consumers (alembic, batch
# jobs, replicas) eat the budget.
#
# An earlier 2+3 default starved /v3/textsearch (with comparison) under
# moderate concurrency: a handful of slow searches consumed a worker's
# whole pool, and the next request — even just the auth lookup — timed
# out in get_db. Pair the bigger pool with a server-side statement_timeout
# so a single slow query can't pin a connection indefinitely.
# statement_timeout applies per physical connection, so wire it on both engine
# branches — otherwise AQUA_DB_POOLCLASS=null (NullPool) would drop the runaway-
# query safety net exactly when it removes the pool ceiling too.
connect_args = {}
if settings.aqua_db_statement_timeout_ms > 0:
    connect_args["server_settings"] = {
        "statement_timeout": str(settings.aqua_db_statement_timeout_ms)
    }
if settings.aqua_db_poolclass and settings.aqua_db_poolclass.lower() == "null":
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(
        DATABASE_URL, poolclass=NullPool, connect_args=connect_args
    )
else:
    engine = create_async_engine(
        DATABASE_URL,
        pool_size=settings.aqua_db_pool_size,
        max_overflow=settings.aqua_db_max_overflow,
        pool_timeout=settings.aqua_db_pool_timeout,
        pool_recycle=settings.aqua_db_pool_recycle,
        pool_pre_ping=True,
        connect_args=connect_args,
    )
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_db():
    db: AsyncSession = AsyncSessionLocal()
    try:
        yield db
    finally:
        await db.close()

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
# The shared RDS instance reports max_connections=829, so even a handful of
# containers (staging + prod share it) stay well under budget. Tune the env
# vars if running many more containers or if other consumers (alembic, batch
# jobs, replicas) eat the budget.
#
# An earlier 2+3 default (5 conns/worker) starved /v3/textsearch under the
# overnight batch: a few multi-second searches consumed a worker's whole
# pool and the next request — even just the auth lookup — timed out at 10s in
# get_db, surfacing as 500 "QueuePool limit of size 2 overflow 3 reached".
# Pair the bigger pool with a server-side statement_timeout so one slow query
# can't pin a pooled connection indefinitely.
if settings.aqua_db_poolclass and settings.aqua_db_poolclass.lower() == "null":
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
else:
    connect_args = {}
    if settings.aqua_db_statement_timeout_ms > 0:
        connect_args["server_settings"] = {
            "statement_timeout": str(settings.aqua_db_statement_timeout_ms)
        }
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

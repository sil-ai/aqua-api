# NOTE: The application does all DB access through the async stack
# (`create_async_engine` / `AsyncSession`, see `database/dependencies.py`).
# This module intentionally exposes only `Base` for the ORM metadata, which
# Alembic imports in `alembic/migrations/env.py`.
#
# A sync `create_engine(settings.aqua_db)` scaffold (`engine`, `db_session`,
# `init_db`) used to live here but was dead code: `settings.aqua_db` is an
# async URL (`postgresql+asyncpg://…`), so the sync engine would have raised on
# first connect, and nothing ever connected through it. Removed to drop the
# footgun (see issue #848).
from database.models import Base

__all__ = ["Base"]

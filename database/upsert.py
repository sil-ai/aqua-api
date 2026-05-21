"""Shared helpers for idempotent bulk inserts.

`batch_insert_on_conflict_nothing` keeps the two retry-safe push endpoints
(assessment_result and eflomal_cooccurrence; see issue #721) consistent so
that one drifting from the other doesn't reintroduce silent duplicate
rows on Modal-worker retries.
"""

from sqlalchemy.dialects.postgresql import insert as pg_insert

# PostgreSQL's wire protocol caps a single statement at 32,767 parameters,
# so each chunk's parameter count (cols_per_row * batch_size) must stay
# below that. The hard-coded 5,000-row default at the call sites is itself
# parameter-bounded by this helper.
_PG_MAX_PARAMS = 32_767


async def batch_insert_on_conflict_nothing(
    db,
    model_cls,
    rows,
    conflict_cols,
    batch_size,
    return_ids: bool = False,
):
    """Bulk-insert ``rows`` into ``model_cls`` with ON CONFLICT DO NOTHING.

    Rows whose ``conflict_cols`` tuple already exists are silently skipped
    (first-write-wins).  Used by every retry-safe push endpoint so a Modal
    worker that re-pushes a partial batch can't insert ghost duplicates
    (issue #721).

    Parameters
    ----------
    db : AsyncSession
        Open SQLAlchemy async session; the caller is responsible for
        committing.
    model_cls : declarative model
        Target table.  Must have a unique index keyed on ``conflict_cols``.
    rows : list[dict]
        Per-row column values, sized to the caller's HTTP body cap.
    conflict_cols : list[str]
        Columns that form the unique key — must match the unique index
        Postgres should use for ON CONFLICT inference.
    batch_size : int
        Caller's preferred chunk size; clamped down to keep each statement
        below the 32,767-parameter wire-protocol limit.
    return_ids : bool, default False
        When True the statement adds ``RETURNING id`` and the function
        returns the IDs of rows that actually landed (skipped conflicts
        are omitted).  When False no IDs are projected and ``[]`` is
        returned — slightly cheaper for hot paths that don't need them.
    """
    cols_per_row = len(model_cls.__table__.columns)
    chunk = min(batch_size, _PG_MAX_PARAMS // cols_per_row)
    inserted_ids: list = []
    for i in range(0, len(rows), chunk):
        batch = rows[i : i + chunk]
        stmt = pg_insert(model_cls).values(batch)
        stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
        if return_ids:
            stmt = stmt.returning(model_cls.id)
            result = await db.execute(stmt)
            inserted_ids.extend(r[0] for r in result.fetchall())
        else:
            await db.execute(stmt)
    return inserted_ids

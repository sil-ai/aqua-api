"""Backfill NULL ``hide`` / ``flag`` values in ``alignment_top_source_scores``.

Context: issue #596. Rows pushed via ``POST /v3/assessment/{id}/alignment-scores``
landed with ``hide=NULL`` because the endpoint omitted the column. Reads through
``GET /alignmentscores`` then 500 because Pydantic rejects ``None`` on
``WordAlignment.hide: bool``.

The application fix in PR #597 sets ``hide=False`` on every new insert and
``ALTER COLUMN ... SET DEFAULT false`` covers any future caller that omits the
column. This script heals the *existing* NULL rows without taking the
multi-hour ``VALIDATE CONSTRAINT`` scan that a formal ``NOT NULL`` migration
would require on a 1.7B-row table.

Strategy
--------
* Walk the primary key from ``MAX(id)`` down to ``MIN(id)`` (newest rows first
  — the breakage started ~2026-04-01 so most NULLs are in the upper id range).
* Each batch reads at most ``--batch-size`` rows by id range and updates only
  those where ``hide IS NULL OR flag IS NULL``. The id-range filter uses the
  primary key index so each batch's read cost is bounded regardless of table
  size.
* Each batch commits in its own transaction. Locks are released between
  batches; concurrent inserts from the live application are unaffected.
* ``--sleep`` adds a delay between batches to keep I/O headroom for prod
  traffic.
* ``--start-id`` resumes from a checkpoint if a previous run was interrupted.
* ``--stop-after-clean N`` exits early after N consecutive batches with zero
  updates — useful once you've passed below the broken-id range and the rest
  of the table is known to be clean.

Recommended setup (do once, before running this script)
-------------------------------------------------------
On a 1.7B-row table the per-batch ``WHERE id BETWEEN ... AND (hide IS NULL
OR flag IS NULL)`` filter is much cheaper if there's a partial index on the
broken rows. Build it concurrently so it doesn't block writes::

    CREATE INDEX CONCURRENTLY ix_alignment_top_source_scores_null_hide_flag
    ON alignment_top_source_scores (id)
    WHERE hide IS NULL OR flag IS NULL;

The build still does one full read of the table — schedule it for a
low-traffic window even though it doesn't take a blocking lock. After the
backfill finishes the predicate matches no rows, so the index self-empties
and can be dropped::

    DROP INDEX ix_alignment_top_source_scores_null_hide_flag;

Usage
-----
::

    AQUA_DB="postgresql+asyncpg://..." \\
      .venv/bin/python scripts/backfill_alignment_hide_flag.py \\
        --batch-size 100000 --sleep 0.1 --stop-after-clean 50

Pause with Ctrl-C; rerun with ``--start-id <last printed lo id>`` to resume.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

_SQL_BOUNDS = "SELECT MIN(id), MAX(id) FROM alignment_top_source_scores"
_SQL_UPDATE = """
    UPDATE alignment_top_source_scores
    SET hide = COALESCE(hide, false),
        flag = COALESCE(flag, false)
    WHERE id BETWEEN :lo AND :hi
      AND (hide IS NULL OR flag IS NULL)
"""


async def backfill(
    *, batch_size: int, sleep_s: float, start_id: int | None, stop_after_clean: int
) -> None:
    db_url = os.environ.get("AQUA_DB")
    if not db_url:
        raise SystemExit("AQUA_DB env var is required")

    engine = create_async_engine(db_url)

    stopping = False

    def _handle_sigint(*_):
        nonlocal stopping
        stopping = True
        print("\nInterrupt received; finishing current batch and exiting...")

    signal.signal(signal.SIGINT, _handle_sigint)

    async with engine.begin() as conn:
        result = await conn.execute(text(_SQL_BOUNDS))
        min_id, max_id = result.first() or (None, None)
    if min_id is None:
        print("Table is empty; nothing to do.")
        return

    upper = start_id if start_id is not None else max_id
    print(f"Range: id {min_id} .. {max_id}; starting from {upper}")

    total_updated = 0
    consecutive_clean = 0
    start = time.time()
    current = upper

    while current >= min_id and not stopping:
        lo = max(current - batch_size + 1, min_id)
        async with engine.begin() as conn:
            result = await conn.execute(text(_SQL_UPDATE), {"lo": lo, "hi": current})
            updated = result.rowcount or 0
        total_updated += updated
        elapsed = time.time() - start
        print(
            f"id [{lo:>12,} .. {current:>12,}]: "
            f"updated={updated:>6,}  total={total_updated:>10,}  "
            f"elapsed={elapsed:>7.1f}s"
        )
        if updated == 0:
            consecutive_clean += 1
            if stop_after_clean and consecutive_clean >= stop_after_clean:
                print(
                    f"\nStopping early: {consecutive_clean} consecutive batches "
                    f"with no NULL rows. Below this id the table appears clean."
                )
                break
        else:
            consecutive_clean = 0
        current = lo - 1
        if sleep_s:
            await asyncio.sleep(sleep_s)

    print(f"\nDone. Total updated: {total_updated:,}. Last lo id: {current + 1}.")
    await engine.dispose()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--batch-size", type=int, default=100_000)
    p.add_argument("--sleep", type=float, default=0.1, dest="sleep_s")
    p.add_argument("--start-id", type=int, default=None)
    p.add_argument("--stop-after-clean", type=int, default=50)
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(
        backfill(
            batch_size=args.batch_size,
            sleep_s=args.sleep_s,
            start_id=args.start_id,
            stop_after_clean=args.stop_after_clean,
        )
    )

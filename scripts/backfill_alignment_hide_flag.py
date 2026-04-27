"""Backfill NULL ``hide`` / ``flag`` values in ``alignment_top_source_scores``.

Context: issue #596. Rows pushed via ``POST /v3/assessment/{id}/alignment-scores``
landed with ``hide=NULL`` because the endpoint omitted the column. Reads through
``GET /alignmentscores`` then 500 because Pydantic rejects ``None`` on
``WordAlignment.hide: bool``.

The application fix in PR #597 sets ``hide=False`` on every new insert and
``ALTER COLUMN ... SET DEFAULT false`` covers any future caller that omits the
column. This script heals the *existing* NULL rows.

Strategy
--------
The bug is per-assessment: the push endpoint writes all rows for one
assessment atomically, so within any single assessment every row is either
all-NULL-hide or all-False-hide. That means we never need to scan the
1.7B-row table to find NULL rows — we iterate the (much smaller) list of
candidate assessments and update each one's rows via the existing
``ix_alignment_top_source_scores_assessment_id`` index.

* Selects word-alignment assessments since ``--since`` (default
  ``2026-03-25`` — a week before the push endpoint launched, with margin).
* For each assessment, runs one UPDATE that uses the assessment_id index;
  touched rows are bounded by that assessment's row count (typically a
  few hundred thousand), so each UPDATE finishes in seconds.
* Each UPDATE is its own transaction — locks released between
  assessments. Concurrent inserts from the live application are unaffected.
* ``--sleep`` adds a delay between assessments to keep I/O headroom for
  prod traffic.
* ``--start-after-id`` resumes from a checkpoint if a previous run was
  interrupted (skips assessments with id <= that value).
* ``--dry-run`` reports the count of broken rows per candidate assessment
  without writing.

Usage
-----
::

    AQUA_DB="postgresql+asyncpg://..." \\
      .venv/bin/python scripts/backfill_alignment_hide_flag.py

    # Or to inspect first without writing:
    .venv/bin/python scripts/backfill_alignment_hide_flag.py --dry-run

Pause with Ctrl-C; rerun with ``--start-after-id <last printed assessment id>``
to resume.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import os
import signal
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

_SQL_CANDIDATES = """
    SELECT id
    FROM assessment
    WHERE type = 'word-alignment'
      AND requested_time >= :since
      AND id > :start_after_id
    ORDER BY id
"""

_SQL_UPDATE = """
    UPDATE alignment_top_source_scores
    SET hide = COALESCE(hide, false),
        flag = COALESCE(flag, false)
    WHERE assessment_id = :aid
      AND (hide IS NULL OR flag IS NULL)
"""

_SQL_COUNT_BROKEN = """
    SELECT COUNT(*)
    FROM alignment_top_source_scores
    WHERE assessment_id = :aid
      AND (hide IS NULL OR flag IS NULL)
"""


async def backfill(
    *, since: dt.date, sleep_s: float, start_after_id: int, dry_run: bool
) -> None:
    db_url = os.environ.get("AQUA_DB")
    if not db_url:
        raise SystemExit("AQUA_DB env var is required")

    engine = create_async_engine(db_url)

    stopping = False

    def _handle_sigint(*_):
        nonlocal stopping
        stopping = True
        print("\nInterrupt received; finishing current assessment and exiting...")

    signal.signal(signal.SIGINT, _handle_sigint)

    async with engine.begin() as conn:
        result = await conn.execute(
            text(_SQL_CANDIDATES),
            {"since": since, "start_after_id": start_after_id},
        )
        candidates = [row[0] for row in result.all()]

    print(
        f"Candidate word-alignment assessments since {since}: {len(candidates)}"
        + (f" (resuming after id {start_after_id})" if start_after_id else "")
    )
    if not candidates:
        print("Nothing to do.")
        await engine.dispose()
        return

    total_updated = 0
    assessments_touched = 0
    start = time.time()

    for aid in candidates:
        if stopping:
            break
        async with engine.begin() as conn:
            if dry_run:
                count = (
                    await conn.execute(text(_SQL_COUNT_BROKEN), {"aid": aid})
                ).scalar() or 0
                updated = count
            else:
                result = await conn.execute(text(_SQL_UPDATE), {"aid": aid})
                updated = result.rowcount or 0
        if updated:
            assessments_touched += 1
        total_updated += updated
        elapsed = time.time() - start
        marker = "[dry-run] " if dry_run else ""
        print(
            f"{marker}assessment_id={aid:>7}  "
            f"rows={'(would update) ' if dry_run else ''}{updated:>7,}  "
            f"total={total_updated:>10,}  "
            f"elapsed={elapsed:>6.1f}s"
        )
        if sleep_s and not dry_run:
            await asyncio.sleep(sleep_s)

    verb = "would update" if dry_run else "updated"
    print(
        f"\nDone. {verb.capitalize()} {total_updated:,} rows across "
        f"{assessments_touched} assessments."
    )
    await engine.dispose()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument(
        "--since",
        type=dt.date.fromisoformat,
        default=dt.date(2026, 3, 25),
        help="ISO date; only assessments requested at/after this are considered.",
    )
    p.add_argument("--sleep", type=float, default=0.0, dest="sleep_s")
    p.add_argument("--start-after-id", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(
        backfill(
            since=args.since,
            sleep_s=args.sleep_s,
            start_after_id=args.start_after_id,
            dry_run=args.dry_run,
        )
    )

"""Bulk verse upload helpers used by the /v3/revision POST route.

The hot path is one upload of a full Bible (~41,899 vrefs, ~31,000 non-empty
verses) bound into INSERTs against `verse_text`. Tail latencies up to 100s+
have been observed under CI contention; the previous shape of this code
issued ~41 commits per upload (one per 1000-row batch) and re-read the vref
skeleton from disk on every request. Both have been collapsed:

- The vref skeleton is parsed once at import and cached as a list of
  (book, chapter, verse_str, verse_reference) tuples.
- `text_loading` issues one INSERT batch at a time without committing,
  using a 5000-row batch to stay under Postgres' 65535-parameter cap
  (6 cols × 5000 = 30k params, leaves headroom). The caller owns the
  transaction boundary so the BibleRevision row and its verses commit
  together (one WAL fsync) and rollback together on error.
"""

import asyncio
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from sqlalchemy.sql import insert

from database.models import VerseText

# Insert batch size: 5000 rows × 6 columns = 30,000 bound parameters,
# safely under the Postgres protocol limit of 65,535 per statement.
_INSERT_BATCH_SIZE = 5000

# Resolve fixtures/vref.txt relative to this file so import succeeds
# regardless of the process's cwd (test runners, alembic env, scripts).
_VREF_PATH = Path(__file__).resolve().parent / "fixtures" / "vref.txt"


_VrefSlot = Optional[Tuple[str, int, int, str]]


def _parse_vref_skeleton() -> List[_VrefSlot]:
    """Parse fixtures/vref.txt once into a list of (book, chapter, verse,
    verse_reference) tuples, with `None` placeholders for non-canonical
    lines (e.g. `<range>` markers) so the skeleton stays 1:1 with the
    file's line ordering. Trailing blank lines are stripped so a stray
    newline at end-of-file can't silently inflate the skeleton length
    and break the input length check."""
    rows: List[_VrefSlot] = []
    with open(_VREF_PATH, mode="r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                rows.append(None)
                continue
            try:
                book_chap, verse_str = line.split(":", 1)
                book, chapter_str = book_chap.split(" ", 1)
                rows.append(
                    (
                        book,
                        int(chapter_str),
                        int(verse_str),
                        f"{book} {chapter_str}:{verse_str}",
                    )
                )
            except (ValueError, IndexError):
                # Range markers etc — not a real vref slot.
                rows.append(None)
    while rows and rows[-1] is None:
        rows.pop()
    return rows


_VREF_SKELETON: List[_VrefSlot] = _parse_vref_skeleton()


def _build_verse_records(verses: Iterable, revision_id: int) -> List[dict]:
    """Pair the cached vref skeleton with the uploaded verse strings, dropping
    rows where the verse is empty/whitespace or the vref slot is a non-canonical
    line (e.g. range marker). Returns a list of dicts ready for executemany.

    `verses` must be the same length as the vref skeleton (41,899 entries) —
    callers pad missing/empty verses with empty strings or None.
    """
    verses = list(verses)
    if len(verses) != len(_VREF_SKELETON):
        raise ValueError(
            f"Expected {len(_VREF_SKELETON)} input lines (one per vref), "
            f"got {len(verses)}"
        )
    records: List[dict] = []
    for slot, verse in zip(_VREF_SKELETON, verses):
        if slot is None:
            continue
        if verse is None:
            continue
        # Treat whitespace-only / NaN floats as empty.
        if isinstance(verse, float):
            # numpy.nan is float and != itself
            if verse != verse:  # NaN check without importing numpy
                continue
            text = str(verse)
        else:
            text = str(verse).replace("\n", "")
        if not text.strip():
            continue
        book, chapter, verse_num, verse_reference = slot
        records.append(
            {
                "text": text,
                "revision_id": revision_id,
                "verse_reference": verse_reference,
                "book": book,
                "chapter": chapter,
                "verse": verse_num,
            }
        )
    return records


async def async_text_dataframe(verses, bible_revision):
    """Build INSERT-ready records for a full Bible upload.

    Kept under the legacy name (and signature) so existing callers don't
    break. `bible_revision` may be a list-per-row (legacy pandas-style) or
    a scalar; we just use the first non-empty entry as the revision id since
    every row of a single upload shares the same revision.
    """
    if isinstance(bible_revision, (list, tuple)):
        revision_id = next((r for r in bible_revision if r is not None), None)
    else:
        revision_id = bible_revision
    if revision_id is None:
        raise ValueError("bible_revision must contain at least one revision id")

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, _build_verse_records, verses, int(revision_id)
    )


async def text_loading(verse_records, db):
    """Insert pre-built verse records in batches without committing.

    The caller owns the transaction boundary: one commit at the end of the
    full upload means one WAL fsync per request instead of one per batch.
    Under CI contention (multiple concurrent uploads × 41 fsyncs each) the
    old per-batch commit produced 100s+ tail latencies that App Runner's
    120s LB timeout was killing as 502s.
    """
    if not verse_records:
        return
    for start in range(0, len(verse_records), _INSERT_BATCH_SIZE):
        batch = verse_records[start : start + _INSERT_BATCH_SIZE]
        await db.execute(insert(VerseText), batch)


async def upload_bible(verses, bible_revision, db):
    verse_records = await async_text_dataframe(verses, bible_revision)
    await text_loading(verse_records, db)
    await db.commit()

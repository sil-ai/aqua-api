"""backfill first_vref on rangeless agent-critique runs

Revision ID: caf9540c5392
Revises: 96ccb8f8ca2a
Create Date: 2026-10-23 09:57:23.541413

Older agent-critique runs (before ranges were stored, around 21 April 2026)
have no first_vref in kwargs. The web app finds a run's chapter only from
v4 options.first_vref, so without one those runs drop off the AI Notes page
(aqua-api#1019, aqua-django-app#517). This sets the range from the verses
the run's agent_translations rows cover:

* One chapter: first_vref = "<BOOK> <chapter>:1" and no last_vref, the same
  shape the app submits for a chapter run.
* Several chapters: first_vref / last_vref = the first and last verse
  covered, in canonical order (book_reference.number, not string order).

Runs with no translations are left alone. They have no critique notes
either (checked 2026-10-23), and since 24 April a run without first_vref is
the runner's pre-translation-only mode, which is correctly rangeless.
Only finished runs are touched, so a partial run never gets a range.
Deleted runs and runs under deleted revisions are skipped, as are runs with
a translation vref missing from verse_reference. An empty-string
first_vref is treated as missing, as the create-time dedup already does.
The rest of kwargs is kept as it is.

Pure data update, idempotent: a backfilled row has a first_vref, so a
second run matches nothing.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "caf9540c5392"
down_revision: Union[str, None] = "96ccb8f8ca2a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Module-level so the test can run exactly this statement. The chapter is
# built as `|| ':' || '1'` because op.execute wraps the string in text(),
# which would read a literal ':1' as a bind parameter.
BACKFILL_SQL = """
WITH candidates AS (
    SELECT a.id
    FROM assessment a
    JOIN bible_revision r ON r.id = a.revision_id
    WHERE a.type = 'agent-critique'
      AND a.status = 'finished'
      AND a.deleted IS NOT TRUE
      AND r.deleted IS NOT TRUE
      AND coalesce(a.kwargs ->> 'first_vref', '') = ''
),
ranges AS (
    SELECT
        t.assessment_id,
        CASE
            WHEN count(DISTINCT vr.chapter) = 1 THEN jsonb_build_object(
                'first_vref', min(vr.chapter) || ':' || '1'
            )
            ELSE jsonb_build_object(
                'first_vref',
                (array_agg(vr.full_verse_id
                    ORDER BY br.number, cr.number, vr.number))[1],
                'last_vref',
                (array_agg(vr.full_verse_id
                    ORDER BY br.number DESC, cr.number DESC, vr.number DESC))[1]
            )
        END AS vref_range
    FROM candidates c
    JOIN agent_translations t ON t.assessment_id = c.id
    LEFT JOIN verse_reference vr ON vr.full_verse_id = t.vref
    LEFT JOIN chapter_reference cr ON cr.full_chapter_id = vr.chapter
    LEFT JOIN book_reference br ON br.abbreviation = vr.book_reference
    GROUP BY t.assessment_id
    -- agent_translations.vref has no FK. Skip a run with any vref we can't
    -- place rather than derive its range from the rest of its rows.
    HAVING count(*) = count(vr.full_verse_id)
)
UPDATE assessment a
SET kwargs = CASE
        WHEN jsonb_typeof(a.kwargs) = 'object' THEN a.kwargs
        ELSE '{}'::jsonb
    END || ranges.vref_range
FROM ranges
WHERE a.id = ranges.assessment_id
  AND coalesce(a.kwargs ->> 'first_vref', '') = ''
"""


def upgrade() -> None:
    op.execute(BACKFILL_SQL)


def downgrade() -> None:
    # Irreversible: a backfilled range can't be told apart from one the
    # caller submitted. No-op so the migration can be unwound without
    # erroring; rows keep their first_vref.
    pass

"""tighten language_affixes unique key to (iso, form, position)

Revision ID: c4f2a8e9b1d7
Revises: a6c1e7d3b4f2
Create Date: 2026-05-13 10:00:00.000000

Collapses any existing rows that share (iso_639_3, form, position) into a single
row, then replaces the (iso, form, position, gloss) unique index with one on
(iso, form, position). Going forward, polysemy is rejected at the API layer
with a 409, mirroring how lexeme card duplicates are handled.

"""
from typing import Sequence, Union

from alembic import op

revision: str = "c4f2a8e9b1d7"
down_revision: Union[str, None] = "a6c1e7d3b4f2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Merge duplicate rows for the same (iso, form, position). Winner is the
    # lowest-id row in each group: union its examples with the losers',
    # sum n_runs (capped at the smallint ceiling), then drop the losers.
    op.execute(
        """
        WITH duplicate_groups AS (
            SELECT iso_639_3, form, position, MIN(id) AS winner_id
            FROM language_affixes
            GROUP BY iso_639_3, form, position
            HAVING COUNT(*) > 1
        ),
        losers AS (
            SELECT a.id AS loser_id, dg.winner_id
            FROM language_affixes a
            JOIN duplicate_groups dg
              ON a.iso_639_3 = dg.iso_639_3
             AND a.form = dg.form
             AND a.position = dg.position
            WHERE a.id != dg.winner_id
        ),
        merged AS (
            UPDATE language_affixes w
            SET examples = (
                    SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb)
                    FROM (
                        SELECT jsonb_array_elements(
                            COALESCE(a2.examples, '[]'::jsonb)
                        ) AS elem
                        FROM language_affixes a2
                        WHERE a2.iso_639_3 = w.iso_639_3
                          AND a2.form = w.form
                          AND a2.position = w.position
                    ) sub
                ),
                n_runs = LEAST(
                    32767,
                    (
                        SELECT SUM(a2.n_runs)::int
                        FROM language_affixes a2
                        WHERE a2.iso_639_3 = w.iso_639_3
                          AND a2.form = w.form
                          AND a2.position = w.position
                    )
                ),
                updated_at = NOW()
            FROM duplicate_groups dg
            WHERE w.id = dg.winner_id
        )
        DELETE FROM language_affixes a
        USING losers l
        WHERE a.id = l.loser_id
        """
    )

    # Examples may have ended up as an empty JSONB array '[]' for winners that
    # had no examples and no losers had examples — normalise those back to NULL
    # to match the prior on-insert convention.
    op.execute(
        """
        UPDATE language_affixes
        SET examples = NULL
        WHERE examples = '[]'::jsonb
        """
    )

    op.drop_index(
        "ux_language_affixes_iso_form_position_gloss",
        table_name="language_affixes",
    )
    op.create_index(
        "ux_language_affixes_iso_form_position",
        "language_affixes",
        ["iso_639_3", "form", "position"],
        unique=True,
    )


def downgrade() -> None:
    # Lossy: rows merged in upgrade() cannot be restored. Restores the old
    # 4-column unique index so the schema lines up, but any polysemy rows
    # that existed before upgrade are permanently gone.
    op.drop_index(
        "ux_language_affixes_iso_form_position",
        table_name="language_affixes",
    )
    op.create_index(
        "ux_language_affixes_iso_form_position_gloss",
        "language_affixes",
        ["iso_639_3", "form", "position", "gloss"],
        unique=True,
    )

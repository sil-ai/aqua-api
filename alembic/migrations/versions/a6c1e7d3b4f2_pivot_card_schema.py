"""Phase 0 schema for pivot-language card architecture

Revision ID: a6c1e7d3b4f2
Revises: 1d460bf9ea55
Create Date: 2026-05-13

Additive schema work that unblocks the pivot-language card architecture
being developed in aqua-assessments (see aqua-api#665):

  * Card identity shifts from "built against this specific Bible version"
    to "built against this language" — the source-side reading pool is a
    curated set of openly-licensed Bibles, so version-specificity doesn't
    apply on that side. Target-side identity stays revision-keyed.
  * Cards become a per-target-revision artifact built once against a
    system-chosen canonical pivot language; cheap MT-derived translations
    for other languages will be layered on top in subsequent issues via
    the new `card_translations` / `card_translation_examples` tables.

Concrete changes in this revision:

  1. Add ``source_language_iso`` (NOT NULL after backfill) and
     ``build_version`` (nullable) to ``agent_lexeme_cards``.
  2. Backfill ``source_language_iso`` from ``bible_version.iso_language``
     via ``source_version_id``. Embedded pre-check refuses to proceed if
     any orphaned rows exist (empirically zero today).
  3. Swap the unique constraint from version-keyed
     ``(LOWER(target_lemma), source_version_id, target_version_id)`` to
     language-keyed
     ``(LOWER(target_lemma), source_language_iso, target_version_id)``.
     Embedded pre-check refuses to proceed if the new constraint would
     create collisions.
  4. Install a BEFORE INSERT/UPDATE trigger that auto-fills
     ``source_language_iso`` from the row's ``source_version_id`` when
     callers don't supply it. This is what lets existing writers — which
     don't know about the new column yet — keep working unchanged
     against the NOT NULL constraint. Writers will be updated in a
     follow-up issue to set it explicitly.
  5. Create ``card_translations`` and ``card_translation_examples``
     tables. No readers/writers yet; this revision is purely structural.

Everything runs in a single upgrade transaction so a partially-applied
state is impossible. (Index ops are plain CREATE/DROP INDEX rather than
CONCURRENTLY for that reason; the table is small enough — TRUNCATE'd in
e3a9f5d2c8b1 and only repopulated by new agent-critique runs — that the
brief ACCESS EXCLUSIVE lock is acceptable.)
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "a6c1e7d3b4f2"
down_revision: Union[str, None] = "1d460bf9ea55"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Trigger keeps existing writers working: pre-pivot code paths insert
# rows without source_language_iso. The trigger derives it from the
# row's source_version_id so the NOT NULL constraint holds without app
# changes. Mirrored in database/models.py via DDL events so test DBs
# (which use Base.metadata.create_all, not migrations) get it too.
_TRIGGER_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION fill_lexeme_card_source_language_iso()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.source_language_iso IS NULL AND NEW.source_version_id IS NOT NULL THEN
        SELECT iso_language INTO NEW.source_language_iso
        FROM bible_version WHERE id = NEW.source_version_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

_TRIGGER_CREATE_SQL = """
CREATE TRIGGER trg_fill_lexeme_card_source_language_iso
BEFORE INSERT OR UPDATE OF source_version_id ON agent_lexeme_cards
FOR EACH ROW
EXECUTE FUNCTION fill_lexeme_card_source_language_iso();
"""


def upgrade() -> None:
    bind = op.get_bind()

    # 1. Empirical pre-check: every row must resolve to a bible_version
    # with a populated iso_language, otherwise the backfill leaves rows
    # with NULL source_language_iso and the NOT NULL flip below fails.
    # Empirically zero today; abort if anything has crept in.
    orphans = bind.exec_driver_sql(
        """
        SELECT COUNT(*) FROM agent_lexeme_cards c
        WHERE c.source_version_id NOT IN (SELECT id FROM bible_version)
           OR (SELECT iso_language FROM bible_version
               WHERE id = c.source_version_id) IS NULL
        """
    ).scalar()
    if orphans:
        raise RuntimeError(
            f"agent_lexeme_cards: {orphans} row(s) have a source_version_id "
            "that does not resolve to a bible_version with iso_language set. "
            "Resolve these by hand before re-running the migration."
        )

    # 2. Add the new columns (nullable for now so we can backfill).
    op.add_column(
        "agent_lexeme_cards",
        sa.Column("source_language_iso", sa.CHAR(3), nullable=True),
    )
    op.add_column(
        "agent_lexeme_cards",
        sa.Column("build_version", sa.Text(), nullable=True),
    )

    # 3. Backfill source_language_iso from bible_version.iso_language.
    op.execute(
        """
        UPDATE agent_lexeme_cards c
        SET source_language_iso = bv.iso_language
        FROM bible_version bv
        WHERE c.source_version_id = bv.id
        """
    )

    # 4. Now that every row has a value, lock the NOT NULL in.
    op.alter_column("agent_lexeme_cards", "source_language_iso", nullable=False)

    # 5. Install the auto-fill trigger so existing writers (which don't
    # set source_language_iso) keep working. Must come before any later
    # writes — including ones from concurrent rolling-deploy code paths.
    op.execute(_TRIGGER_FUNCTION_SQL)
    op.execute(_TRIGGER_CREATE_SQL)

    # 6. Empirical pre-check for the constraint swap: would the
    # language-keyed unique index create collisions? Empirically returns
    # zero rows today; abort with a clear error if anything has changed.
    duplicates = bind.exec_driver_sql(
        """
        SELECT bv.iso_language, c.target_version_id, LOWER(c.target_lemma),
               COUNT(*) AS n
        FROM agent_lexeme_cards c
        JOIN bible_version bv ON c.source_version_id = bv.id
        GROUP BY bv.iso_language, c.target_version_id, LOWER(c.target_lemma)
        HAVING COUNT(*) > 1
        """
    ).fetchall()
    if duplicates:
        sample = ", ".join(
            f"({lang!r}, target_version_id={tvid}, lemma={lemma!r}, count={n})"
            for lang, tvid, lemma, n in duplicates[:5]
        )
        raise RuntimeError(
            f"agent_lexeme_cards: {len(duplicates)} collision group(s) would "
            f"violate the new language-keyed unique constraint. Examples: "
            f"{sample}. Reconcile by hand before re-running the migration."
        )

    # 7. Swap the unique index from version-keyed (v4) to language-keyed (v5).
    op.drop_index("ix_agent_lexeme_cards_unique_v4", table_name="agent_lexeme_cards")
    op.execute(
        """
        CREATE UNIQUE INDEX ix_agent_lexeme_cards_unique_v5
        ON agent_lexeme_cards
            (LOWER(target_lemma), source_language_iso, target_version_id)
        """
    )

    # 8. Holder for cheap MT-derived translations of canonical cards.
    # Canonical content stays in agent_lexeme_cards directly — the
    # U-shaped model rejected by the issue would make canonical "just
    # another row" here at the cost of more invasive reader changes.
    op.create_table(
        "card_translations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "card_id",
            sa.Integer(),
            sa.ForeignKey("agent_lexeme_cards.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("language_iso", sa.CHAR(3), nullable=False),
        sa.Column("source_lemma", sa.Text(), nullable=True),
        sa.Column(
            "source_surface_forms",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "senses",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("parent_build_version", sa.Text(), nullable=True),
        sa.Column("build_version", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "last_updated",
            sa.TIMESTAMP(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_card_translations_unique",
        "card_translations",
        ["card_id", "language_iso"],
        unique=True,
    )
    op.create_index(
        "ix_card_translations_card_id",
        "card_translations",
        ["card_id"],
    )

    # 9. Per-example translation rows. The target-language text lives on
    # the canonical example via the example_id FK; this table stores
    # only the MT'd source-side text. That avoids duplicating low-
    # resource target text and lets corrections to a canonical example
    # flow through translation consumers automatically.
    op.create_table(
        "card_translation_examples",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "card_translation_id",
            sa.Integer(),
            sa.ForeignKey("card_translations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "example_id",
            sa.Integer(),
            sa.ForeignKey("agent_lexeme_card_examples.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_text", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_card_translation_examples_unique",
        "card_translation_examples",
        ["card_translation_id", "example_id"],
        unique=True,
    )
    op.create_index(
        "ix_card_translation_examples_translation",
        "card_translation_examples",
        ["card_translation_id"],
    )


def downgrade() -> None:
    # 1. Drop the new translation tables (cascade FKs handled by op.drop_table).
    op.drop_index(
        "ix_card_translation_examples_translation",
        table_name="card_translation_examples",
    )
    op.drop_index(
        "ix_card_translation_examples_unique",
        table_name="card_translation_examples",
    )
    op.drop_table("card_translation_examples")

    op.drop_index("ix_card_translations_card_id", table_name="card_translations")
    op.drop_index("ix_card_translations_unique", table_name="card_translations")
    op.drop_table("card_translations")

    # 2. Reverse the unique-index swap. Both directions use the same
    # row data because v4's (source_version_id, target_version_id)
    # tuple is one-to-one with v5's (source_language_iso,
    # target_version_id) for any row that's actually in the table
    # (source_version_id determines source_language_iso by FK).
    op.execute("DROP INDEX IF EXISTS ix_agent_lexeme_cards_unique_v5")
    op.execute(
        """
        CREATE UNIQUE INDEX ix_agent_lexeme_cards_unique_v4
        ON agent_lexeme_cards
            (LOWER(target_lemma), source_version_id, target_version_id)
        """
    )

    # 3. Drop the auto-fill trigger before its target column goes away.
    op.execute(
        "DROP TRIGGER IF EXISTS trg_fill_lexeme_card_source_language_iso "
        "ON agent_lexeme_cards"
    )
    op.execute("DROP FUNCTION IF EXISTS fill_lexeme_card_source_language_iso()")

    # 4. Drop the new columns. Backfilled data is lost on downgrade but
    # is recoverable from bible_version.iso_language if needed.
    op.drop_column("agent_lexeme_cards", "build_version")
    op.drop_column("agent_lexeme_cards", "source_language_iso")

"""Scope language_affixes uniqueness per target_version_id

Revision ID: d4f7b1e9c3a2
Revises: e8a3f1c2d790
Create Date: 2026-06-10

Background
----------
Reads (#693 -- ``GET /affixes-by-version``) and bulk DELETE (#797 --
``DELETE /affixes-by-version``) are already version-scoped, but the
write/conflict path was still iso-scoped: the unique index
``ux_language_affixes_iso_form_position`` was on
``(iso_639_3, form, position)``. So a ``POST /v3/affixes`` for one
``target_version_id`` would 409 against rows owned by *another* version
of the same ISO, and the patch-fallback path silently mutated the other
version's rows.

This migration version-scopes the write path, mirroring lexeme cards'
``(LOWER(target_lemma), source_language_iso, target_version_id)``.

What changes
------------
- Drop ``ux_language_affixes_iso_form_position``
  (was ``(iso_639_3, form, position)`` unique, all rows).
- Drop ``ux_language_affixes_version_form_position_gloss``
  (was ``(target_version_id, form, position, gloss)`` unique).
  Polysemy is rejected at the API layer (#c4f2a8e9b1d7), so the gloss
  column in this composite no longer serves a real purpose. The new
  ``ux_language_affixes_version_form_position`` replaces it.
- Add ``ux_language_affixes_version_form_position``: partial unique on
  ``(target_version_id, form, position) WHERE target_version_id IS NOT NULL``.
- Add ``ux_language_affixes_iso_form_position_legacy``: partial unique on
  ``(iso_639_3, form, position) WHERE target_version_id IS NULL``.
  Migration ``e2ad1ed4a64a`` deleted all NULL-stamped legacy rows, but
  the API still allows NULL-stamped writes (POST/PUT without
  ``revision_id``), so this partial keeps the legacy bucket's uniqueness.

Existing data already satisfies both partial indexes: after
``c4f2a8e9b1d7``, there is at most one row per ``(iso, form, position)``,
which implies at most one row per ``(target_version_id, form, position)``
too.

Downgrade restores the iso-keyed unique and the legacy 4-column
``ux_language_affixes_version_form_position_gloss`` index, after deduping
rows the new partial uniques deliberately allowed to coexist (NULL+stamped
at the same ``(iso, form, position)``; multiple stamped versions sharing
``(iso, form, position)``). That dedup is lossy by design -- the rows it
drops are reproducible by re-running the agent.
"""

from typing import Sequence, Union

from alembic import op

revision: str = "d4f7b1e9c3a2"
down_revision: Union[str, None] = "e8a3f1c2d790"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index(
        "ux_language_affixes_iso_form_position",
        table_name="language_affixes",
    )
    op.drop_index(
        "ux_language_affixes_version_form_position_gloss",
        table_name="language_affixes",
    )
    op.create_index(
        "ux_language_affixes_version_form_position",
        "language_affixes",
        ["target_version_id", "form", "position"],
        unique=True,
        postgresql_where="target_version_id IS NOT NULL",
    )
    op.create_index(
        "ux_language_affixes_iso_form_position_legacy",
        "language_affixes",
        ["iso_639_3", "form", "position"],
        unique=True,
        postgresql_where="target_version_id IS NULL",
    )


def downgrade() -> None:
    op.drop_index(
        "ux_language_affixes_iso_form_position_legacy",
        table_name="language_affixes",
    )
    op.drop_index(
        "ux_language_affixes_version_form_position",
        table_name="language_affixes",
    )
    # The new write semantics deliberately allow a stamped row and a
    # legacy NULL row to coexist at the same (iso, form, position) -- see
    # test_put_affixes_scoped_to_revision_leaves_legacy_null_row_intact.
    # Restoring the non-partial iso-keyed unique would fail on any such
    # pair. Drop the NULL-stamped row in those cases (it is reproducible
    # by re-running the agent without revision_id, and migration
    # e2ad1ed4a64a already established the precedent of deleting NULL
    # legacy data).
    op.execute(
        """
        DELETE FROM language_affixes
        WHERE target_version_id IS NULL
          AND (iso_639_3, form, position) IN (
              SELECT iso_639_3, form, position
              FROM language_affixes
              WHERE target_version_id IS NOT NULL
          )
        """
    )
    # If two stamped rows from different versions share (iso, form,
    # position), the iso-keyed unique can only be restored by dropping
    # all but one. Keep the lowest-id row and delete the others.
    op.execute(
        """
        DELETE FROM language_affixes a
        USING language_affixes b
        WHERE a.iso_639_3 = b.iso_639_3
          AND a.form = b.form
          AND a.position = b.position
          AND a.id > b.id
        """
    )
    op.create_index(
        "ux_language_affixes_version_form_position_gloss",
        "language_affixes",
        ["target_version_id", "form", "position", "gloss"],
        unique=True,
    )
    op.create_index(
        "ux_language_affixes_iso_form_position",
        "language_affixes",
        ["iso_639_3", "form", "position"],
        unique=True,
    )

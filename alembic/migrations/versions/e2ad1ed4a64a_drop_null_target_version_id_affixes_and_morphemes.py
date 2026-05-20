"""Drop legacy NULL-stamped language_affixes / language_morphemes rows

Revision ID: e2ad1ed4a64a
Revises: c8e1f4a72b9d
Create Date: 2026-05-20

Background
----------
``language_affixes`` and ``language_morphemes`` rows are now version-stamped
(``target_version_id`` set on insert). Reads at ``GET /affixes-by-version``
and ``GET /morphemes-by-version`` still do a "soft union" — they include
both rows stamped to the requested version *and* legacy rows with
``target_version_id IS NULL`` that share the same ISO. The DELETE that
backs ``build_mode="rebuild"`` (``DELETE /tokenizer/training-artifacts/
{version_id}``) only clears rows stamped to ``version_id``, so the legacy
NULL rows survive a rebuild and the agent picks them right back up via
the soft union.

Why drop instead of stamp
-------------------------
These rows are reproducible: the agent's affix-inventory and grammar-
discovery steps regenerate them from scratch on the next training run for
each version. Migrating them onto a specific ``target_version_id`` would
require guessing which version "owns" them (often there's more than one
version per ISO with no clear primary), so we take the simpler path —
delete them and let the agent rebuild against each version cleanly.

What this migration does NOT do
-------------------------------
- Does not add a ``NOT NULL`` constraint. The POST/PUT ``/affixes`` API
  contract still accepts an optional ``revision_id`` (and infers
  ``target_version_id`` from it). Tightening the schema would require a
  matching API change to reject NULL-target writes — deferred so this
  migration stays small.
- Does not change the soft-union reads. Those still ``OR target_version_id
  IS NULL``; the branch will simply match nothing after this migration.
  Drop the branch in a follow-up once we're confident no new NULLs appear.

Downgrade is intentionally a no-op: the deleted rows are reproducible, so
restoring them isn't meaningful.
"""

from alembic import op

revision = "e2ad1ed4a64a"
down_revision = "c8e1f4a72b9d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "DELETE FROM language_affixes WHERE target_version_id IS NULL"
    )
    op.execute(
        "DELETE FROM language_morphemes WHERE target_version_id IS NULL"
    )


def downgrade() -> None:
    # No-op: the deleted rows are reproducible by re-running the agent.
    pass

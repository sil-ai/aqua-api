"""Critique API: replace omission/addition/replacement buckets with unified MQM-aligned Issue

Revision ID: e8a3f1c2d790
Revises: 91243bb07389
Create Date: 2026-06-06

Drops issue_type / its index / the text-fields CHECK constraint.  Adds
dimension, subtype, detector (String) and evidence (JSONB).  Makes severity
nullable.  Adds indices on dimension and subtype.

Existing rows are preserved by mapping the legacy issue_type onto the new
MQM taxonomy:
    omission    -> dimension='accuracy', subtype='omission'
    addition    -> dimension='accuracy', subtype='addition'
    replacement -> dimension='accuracy', subtype='mistranslation'

detector and evidence stay NULL for legacy rows (truthful provenance — these
issues predate detector tagging).

Caveat on `replacement -> mistranslation`: the legacy taxonomy did not
distinguish term-consistency failures (which MQM puts under
`dimension=terminology`) from generic semantic mistranslations.  Mapping
everything to `accuracy/mistranslation` is the most defensible default —
the underlying `source_text` and `draft_text` are preserved, so a later
re-classification job can split this bucket without data loss.

Operator note: this migration is content-mutable under a stable revision
ID.  Any environment that previously applied the delete-based draft of
e8a3f1c2d790 against non-empty data has already lost those rows — alembic
will consider it up-to-date and the backfill below will not re-fire.  As
of merge time the only environments at e8a3f1c2d790 were dev + CI
(ephemeral, empty); prod and other long-lived envs are still on
91243bb07389 and will receive the backfill.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "e8a3f1c2d790"
down_revision: Union[str, None] = "91243bb07389"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_FORWARD_MAP_SQL = """
    UPDATE agent_critique_issue
    SET dimension = 'accuracy',
        subtype = CASE issue_type
            WHEN 'omission'    THEN 'omission'
            WHEN 'addition'    THEN 'addition'
            WHEN 'replacement' THEN 'mistranslation'
            ELSE NULL
        END
"""


def upgrade() -> None:
    # 1. Drop the legacy CHECK constraint that ties issue_type to the
    #    source_text/draft_text NULL pattern.  It no longer applies once we
    #    drop issue_type, and the new schema permits any combination
    #    (e.g. punctuation issues with no spans).
    op.drop_constraint(
        "ck_critique_issue_text_fields", "agent_critique_issue", type_="check"
    )

    # 2. Add new columns nullable so we can backfill before applying NOT NULL.
    op.add_column(
        "agent_critique_issue",
        sa.Column("dimension", sa.String(length=50), nullable=True),
    )
    op.add_column(
        "agent_critique_issue",
        sa.Column("subtype", sa.String(length=100), nullable=True),
    )
    op.add_column(
        "agent_critique_issue",
        sa.Column("detector", sa.String(length=50), nullable=True),
    )
    op.add_column(
        "agent_critique_issue",
        sa.Column("evidence", JSONB(), nullable=True),
    )

    # 3. Backfill from issue_type.  All three legacy buckets are accuracy
    #    issues under the MQM taxonomy.
    op.execute(_FORWARD_MAP_SQL)

    # 4. Sanity check: no row should be left unmapped.  If the table held
    #    something other than the three documented issue_types we want to
    #    abort loudly rather than ship NULL dimensions.
    conn = op.get_bind()
    unmapped = conn.execute(
        sa.text(
            "SELECT count(*) FROM agent_critique_issue "
            "WHERE dimension IS NULL OR subtype IS NULL"
        )
    ).scalar()
    if unmapped:
        msg = (
            f"{unmapped} rows in agent_critique_issue could not be mapped "
            "from issue_type to (dimension, subtype). Inspect the table for "
            "unexpected issue_type values before re-running."
        )
        # Print before raising so the operator sees the message above the
        # traceback noise alembic emits for any migration exception.
        print(f"\nMIGRATION ABORT: {msg}\n")
        raise RuntimeError(msg)

    # 5. Enforce NOT NULL now that all rows are populated.
    op.alter_column(
        "agent_critique_issue",
        "dimension",
        existing_type=sa.String(length=50),
        nullable=False,
    )
    op.alter_column(
        "agent_critique_issue",
        "subtype",
        existing_type=sa.String(length=100),
        nullable=False,
    )

    # 6. Drop the legacy column and its index.
    op.drop_index("ix_agent_critique_issue_type", table_name="agent_critique_issue")
    op.drop_column("agent_critique_issue", "issue_type")

    # 7. Relax severity — the agent now passes through NULL when the model
    #    omits it (we deliberately do not coerce to 0 or 1).
    op.alter_column(
        "agent_critique_issue",
        "severity",
        existing_type=sa.Integer(),
        nullable=True,
    )

    # 8. New indices for the new filter columns.
    op.create_index(
        "ix_agent_critique_issue_dimension",
        "agent_critique_issue",
        ["dimension"],
    )
    op.create_index(
        "ix_agent_critique_issue_subtype",
        "agent_critique_issue",
        ["subtype"],
    )


def downgrade() -> None:
    # Downgrade is best-effort: any row whose (dimension, subtype) pair is
    # outside the three legacy accuracy buckets cannot round-trip into the
    # old schema without inventing data.  Abort rather than silently mis-label
    # such rows.
    conn = op.get_bind()
    unmappable = conn.execute(
        sa.text(
            """
            SELECT count(*) FROM agent_critique_issue
            WHERE NOT (
                dimension = 'accuracy'
                AND subtype IN ('omission', 'addition', 'mistranslation')
            )
            """
        )
    ).scalar()
    if unmappable:
        msg = (
            f"Cannot downgrade: {unmappable} rows use a (dimension, subtype) "
            "pair that has no legacy issue_type equivalent. Remove or remap "
            "those rows before downgrading."
        )
        print(f"\nMIGRATION ABORT: {msg}\n")
        raise RuntimeError(msg)

    op.drop_index("ix_agent_critique_issue_subtype", table_name="agent_critique_issue")
    op.drop_index(
        "ix_agent_critique_issue_dimension", table_name="agent_critique_issue"
    )

    # severity must go back to NOT NULL.  Backfill any NULL severity rows to
    # 3 (mid-point of the 1..5 scale) before tightening — the old API treated
    # severity as required, so callers can never have meant "unknown" via
    # NULL there.  This is a one-way lossy coercion documented for operators
    # reading the downgrade output.
    op.execute("UPDATE agent_critique_issue SET severity = 3 WHERE severity IS NULL")
    op.alter_column(
        "agent_critique_issue",
        "severity",
        existing_type=sa.Integer(),
        nullable=False,
    )

    # server_default lets the NOT NULL column be added safely; we clear it
    # again immediately after the backfill so new inserts must supply a
    # value, matching the original schema.
    op.add_column(
        "agent_critique_issue",
        sa.Column(
            "issue_type",
            sa.String(length=15),
            nullable=False,
            server_default="omission",
        ),
    )
    # The unmappable pre-flight check above guarantees every subtype is one of
    # the three legacy buckets; the explicit `ELSE NULL` makes that contract
    # visible and ensures any future drift surfaces as a NOT NULL violation
    # on the column rather than getting silently fixed by `server_default`.
    op.execute(
        """
        UPDATE agent_critique_issue
        SET issue_type = CASE subtype
            WHEN 'omission'       THEN 'omission'
            WHEN 'addition'       THEN 'addition'
            WHEN 'mistranslation' THEN 'replacement'
            ELSE NULL
        END
        """
    )
    op.alter_column("agent_critique_issue", "issue_type", server_default=None)

    op.drop_column("agent_critique_issue", "evidence")
    op.drop_column("agent_critique_issue", "detector")
    op.drop_column("agent_critique_issue", "subtype")
    op.drop_column("agent_critique_issue", "dimension")

    op.create_index(
        "ix_agent_critique_issue_type", "agent_critique_issue", ["issue_type"]
    )
    op.create_check_constraint(
        "ck_critique_issue_text_fields",
        "agent_critique_issue",
        """
        (issue_type = 'omission'    AND source_text IS NOT NULL AND draft_text IS NULL) OR
        (issue_type = 'addition'    AND source_text IS NULL     AND draft_text IS NOT NULL) OR
        (issue_type = 'replacement' AND source_text IS NOT NULL AND draft_text IS NOT NULL) OR
        (source_text IS NULL AND draft_text IS NULL)
        """,
    )

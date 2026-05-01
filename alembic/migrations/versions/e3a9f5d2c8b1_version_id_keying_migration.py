"""shift train/predict artifact keying from language to version_id

Revision ID: e3a9f5d2c8b1
Revises: c1f3a8d2e9b6
Create Date: 2026-04-30 23:00:00.000000

Implements the schema half of aqua-api#613. Train/predict artifacts have
been keyed on (source_language, target_language) ISO codes, which causes
two different bible_version rows in the same language pair to share
artifacts. After this migration they are keyed on
(source_version_id, target_version_id) so different versions in the same
language pair are isolated.

Tables touched:
  - eflomal_assessment        (backfill via assessment.revision_id/reference_id)
  - tfidf_artifact_runs       (backfill via assessment chain; source-only)
  - tfidf_svd_staging         (transient rows; no backfill)
  - training_job              (backfill via revision FK)
  - agent_lexeme_cards        (TRUNCATE — no version chain available; rows
                               will be repopulated from new agent-critique
                               training runs)
  - agent_word_alignments     (TRUNCATE — same reason)
  - agent_translations        (backfill reference_version_id via
                               assessment.reference_id chain)

The cross-repo aqua-assessments runner must update its lookups in lockstep
(see sil-ai/aqua-assessments#232).
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e3a9f5d2c8b1"
down_revision: Union[str, None] = "c1f3a8d2e9b6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _drop_invalid_index(bind, name: str) -> None:
    """If a previous CONCURRENTLY build was interrupted Postgres leaves the
    index in an INVALID state — IF NOT EXISTS will then skip the rebuild
    and the planner could still try to use the broken one. Detect and drop
    so this migration is safely retryable. Mirrors the pattern in
    b7a3e9d6f1c2_drop_training_job_status_fields.py."""
    is_invalid = bind.exec_driver_sql(
        f"SELECT 1 FROM pg_class c "
        f"JOIN pg_index i ON i.indexrelid = c.oid "
        f"WHERE c.relname = '{name}' AND NOT i.indisvalid"
    ).scalar()
    if is_invalid:
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")


def upgrade() -> None:
    bind = op.get_bind()

    # ------------------------------------------------------------------
    # Phase 1 — drop language-keyed indexes CONCURRENTLY (no ACCESS
    # EXCLUSIVE on hot tables during a rolling deploy).
    # ------------------------------------------------------------------
    with op.get_context().autocommit_block():
        for name in (
            "ix_eflomal_assessment_language_pair",
            "ix_tfidf_artifact_runs_lang",
            "ix_tfidf_artifact_runs_lang_created",
            "ix_training_job_lang_pair",
            "ix_agent_lexeme_cards_unique_v3",
            "ix_agent_lexeme_cards_lang_confidence",
            "ux_agent_word_alignments_lang_words",
            "ix_agent_word_alignments_lang_source",
            "ix_agent_word_alignments_lang_target",
            "ix_agent_word_alignments_lang_score",
            "ix_agent_translations_unique",
            "ix_agent_translations_rev_lang_script_vref",
        ):
            _drop_invalid_index(bind, name)
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")

    # ------------------------------------------------------------------
    # Phase 2 — column changes inside a single transaction so that
    # backfills + NOT NULL flips are atomic with column adds/drops.
    # ------------------------------------------------------------------

    # --- eflomal_assessment ---
    op.add_column(
        "eflomal_assessment",
        sa.Column(
            "source_version_id",
            sa.Integer(),
            sa.ForeignKey("bible_version.id"),
            nullable=True,
        ),
    )
    op.add_column(
        "eflomal_assessment",
        sa.Column(
            "target_version_id",
            sa.Integer(),
            sa.ForeignKey("bible_version.id"),
            nullable=True,
        ),
    )
    op.execute(
        """
        UPDATE eflomal_assessment ea
        SET source_version_id = src_br.bible_version_id,
            target_version_id = tgt_br.bible_version_id
        FROM assessment a
        JOIN bible_revision src_br ON a.revision_id = src_br.id
        JOIN bible_revision tgt_br ON a.reference_id = tgt_br.id
        WHERE a.id = ea.assessment_id
        """
    )
    # Sanity check: every row must have backfilled cleanly. If any rows
    # are still NULL the alter_column to NOT NULL below will fail —
    # surface the count up front for easier debugging.
    orphans = bind.exec_driver_sql(
        "SELECT COUNT(*) FROM eflomal_assessment "
        "WHERE source_version_id IS NULL OR target_version_id IS NULL"
    ).scalar()
    if orphans:
        raise RuntimeError(
            f"eflomal_assessment: {orphans} row(s) failed version_id backfill — "
            "investigate orphaned rows before re-running"
        )
    op.alter_column("eflomal_assessment", "source_version_id", nullable=False)
    op.alter_column("eflomal_assessment", "target_version_id", nullable=False)
    op.drop_column("eflomal_assessment", "source_language")
    op.drop_column("eflomal_assessment", "target_language")

    # --- tfidf_artifact_runs (source only — TF-IDF vectorizers are
    # source-side artifacts) ---
    op.add_column(
        "tfidf_artifact_runs",
        sa.Column(
            "source_version_id",
            sa.Integer(),
            sa.ForeignKey("bible_version.id"),
            nullable=True,
        ),
    )
    op.execute(
        """
        UPDATE tfidf_artifact_runs tar
        SET source_version_id = src_br.bible_version_id
        FROM assessment a
        JOIN bible_revision src_br ON a.revision_id = src_br.id
        WHERE a.id = tar.assessment_id
        """
    )
    orphans = bind.exec_driver_sql(
        "SELECT COUNT(*) FROM tfidf_artifact_runs WHERE source_version_id IS NULL"
    ).scalar()
    if orphans:
        raise RuntimeError(
            f"tfidf_artifact_runs: {orphans} row(s) failed version_id backfill"
        )
    op.alter_column("tfidf_artifact_runs", "source_version_id", nullable=False)
    op.drop_column("tfidf_artifact_runs", "source_language")

    # --- tfidf_svd_staging (transient; no backfill) ---
    op.add_column(
        "tfidf_svd_staging",
        sa.Column(
            "source_version_id",
            sa.Integer(),
            sa.ForeignKey("bible_version.id"),
            nullable=True,
        ),
    )
    op.drop_column("tfidf_svd_staging", "source_language")

    # --- training_job ---
    op.add_column(
        "training_job",
        sa.Column(
            "source_version_id",
            sa.Integer(),
            sa.ForeignKey("bible_version.id"),
            nullable=True,
        ),
    )
    op.add_column(
        "training_job",
        sa.Column(
            "target_version_id",
            sa.Integer(),
            sa.ForeignKey("bible_version.id"),
            nullable=True,
        ),
    )
    op.execute(
        """
        UPDATE training_job tj
        SET source_version_id = src_br.bible_version_id,
            target_version_id = tgt_br.bible_version_id
        FROM bible_revision src_br, bible_revision tgt_br
        WHERE src_br.id = tj.source_revision_id
          AND tgt_br.id = tj.target_revision_id
        """
    )
    orphans = bind.exec_driver_sql(
        "SELECT COUNT(*) FROM training_job "
        "WHERE source_version_id IS NULL OR target_version_id IS NULL"
    ).scalar()
    if orphans:
        raise RuntimeError(f"training_job: {orphans} row(s) failed version_id backfill")
    op.alter_column("training_job", "source_version_id", nullable=False)
    op.alter_column("training_job", "target_version_id", nullable=False)
    op.drop_column("training_job", "source_language")
    op.drop_column("training_job", "target_language")

    # --- agent_lexeme_cards (truncate; CASCADE drops examples) ---
    # No FK or join chain back to a bible_version — pre-migration rows
    # cannot be unambiguously assigned to a version pair. Confirmed
    # acceptable on aqua-api#613: cards rebuild from new agent-critique
    # training runs.
    op.execute("TRUNCATE TABLE agent_lexeme_cards CASCADE")
    op.add_column(
        "agent_lexeme_cards",
        sa.Column(
            "source_version_id",
            sa.Integer(),
            sa.ForeignKey("bible_version.id"),
            nullable=False,
        ),
    )
    op.add_column(
        "agent_lexeme_cards",
        sa.Column(
            "target_version_id",
            sa.Integer(),
            sa.ForeignKey("bible_version.id"),
            nullable=False,
        ),
    )
    op.drop_column("agent_lexeme_cards", "source_language")
    op.drop_column("agent_lexeme_cards", "target_language")

    # --- agent_word_alignments (truncate; no dependents) ---
    op.execute("TRUNCATE TABLE agent_word_alignments")
    op.add_column(
        "agent_word_alignments",
        sa.Column(
            "source_version_id",
            sa.Integer(),
            sa.ForeignKey("bible_version.id"),
            nullable=False,
        ),
    )
    op.add_column(
        "agent_word_alignments",
        sa.Column(
            "target_version_id",
            sa.Integer(),
            sa.ForeignKey("bible_version.id"),
            nullable=False,
        ),
    )
    op.drop_column("agent_word_alignments", "source_language")
    op.drop_column("agent_word_alignments", "target_language")

    # --- agent_translations (single reference_version_id; backfillable) ---
    op.add_column(
        "agent_translations",
        sa.Column(
            "reference_version_id",
            sa.Integer(),
            sa.ForeignKey("bible_version.id"),
            nullable=True,
        ),
    )
    op.execute(
        """
        UPDATE agent_translations at
        SET reference_version_id = ref_br.bible_version_id
        FROM assessment a
        JOIN bible_revision ref_br ON a.reference_id = ref_br.id
        WHERE a.id = at.assessment_id
        """
    )
    orphans = bind.exec_driver_sql(
        "SELECT COUNT(*) FROM agent_translations WHERE reference_version_id IS NULL"
    ).scalar()
    if orphans:
        raise RuntimeError(
            f"agent_translations: {orphans} row(s) failed version_id backfill"
        )
    op.alter_column("agent_translations", "reference_version_id", nullable=False)
    op.drop_column("agent_translations", "language")

    # ------------------------------------------------------------------
    # Phase 3 — rebuild indexes CONCURRENTLY on the new columns. Outside
    # the column-change transaction so each CREATE INDEX takes only a
    # SHARE UPDATE EXCLUSIVE lock; readers/writers proceed.
    # ------------------------------------------------------------------
    with op.get_context().autocommit_block():
        for name, table, ddl in (
            (
                "ix_eflomal_assessment_version_pair",
                "eflomal_assessment",
                "(source_version_id, target_version_id)",
            ),
            (
                "ix_tfidf_artifact_runs_version",
                "tfidf_artifact_runs",
                "(source_version_id)",
            ),
            (
                "ix_tfidf_artifact_runs_version_created",
                "tfidf_artifact_runs",
                "(source_version_id, created_at)",
            ),
            (
                "ix_training_job_version_pair",
                "training_job",
                "(source_version_id, target_version_id)",
            ),
            (
                "ix_agent_lexeme_cards_version_confidence",
                "agent_lexeme_cards",
                "(source_version_id, target_version_id, confidence DESC)",
            ),
            (
                "ix_agent_word_alignments_version_source",
                "agent_word_alignments",
                "(source_version_id, target_version_id, source_word)",
            ),
            (
                "ix_agent_word_alignments_version_target",
                "agent_word_alignments",
                "(source_version_id, target_version_id, target_word)",
            ),
            (
                "ix_agent_word_alignments_version_score",
                "agent_word_alignments",
                "(source_version_id, target_version_id, score DESC)",
            ),
            (
                "ix_agent_translations_rev_refversion_script_vref",
                "agent_translations",
                "(revision_id, reference_version_id, script, vref)",
            ),
        ):
            _drop_invalid_index(bind, name)
            op.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} " f"ON {table} {ddl}"
            )

        # Unique constraints replacing the dropped language-keyed unique
        # indexes. Created CONCURRENTLY for the same lock-avoidance
        # reason; UNIQUE indexes can be created concurrently in PG.
        for name, table, ddl in (
            (
                "ix_agent_lexeme_cards_unique_v4",
                "agent_lexeme_cards",
                "(LOWER(target_lemma), source_version_id, target_version_id)",
            ),
            (
                "ux_agent_word_alignments_version_words",
                "agent_word_alignments",
                "(source_version_id, target_version_id, source_word, target_word)",
            ),
            (
                "ix_agent_translations_unique",
                "agent_translations",
                "(revision_id, reference_version_id, script, vref, version)",
            ),
        ):
            _drop_invalid_index(bind, name)
            op.execute(
                f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {name} "
                f"ON {table} {ddl}"
            )


def downgrade() -> None:
    """Reverses column / index shape but cannot recover truncated rows.

    agent_lexeme_cards and agent_word_alignments lose all rows on upgrade
    (no version chain to backfill from). Downgrading restores the schema
    but leaves those tables empty; callers will repopulate from new runs
    in either direction.
    """
    bind = op.get_bind()

    with op.get_context().autocommit_block():
        for name in (
            "ix_eflomal_assessment_version_pair",
            "ix_tfidf_artifact_runs_version",
            "ix_tfidf_artifact_runs_version_created",
            "ix_training_job_version_pair",
            "ix_agent_lexeme_cards_version_confidence",
            "ix_agent_lexeme_cards_unique_v4",
            "ix_agent_word_alignments_version_source",
            "ix_agent_word_alignments_version_target",
            "ix_agent_word_alignments_version_score",
            "ux_agent_word_alignments_version_words",
            "ix_agent_translations_rev_refversion_script_vref",
            "ix_agent_translations_unique",
        ):
            _drop_invalid_index(bind, name)
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")

    # --- agent_translations ---
    op.add_column(
        "agent_translations",
        sa.Column(
            "language",
            sa.String(3),
            sa.ForeignKey("iso_language.iso639"),
            nullable=True,
        ),
    )
    op.execute(
        """
        UPDATE agent_translations at
        SET language = bv.iso_language
        FROM bible_version bv
        WHERE bv.id = at.reference_version_id
        """
    )
    op.alter_column("agent_translations", "language", nullable=False)
    op.drop_column("agent_translations", "reference_version_id")

    # --- agent_word_alignments — schema restored, rows already gone ---
    op.add_column(
        "agent_word_alignments",
        sa.Column(
            "source_language",
            sa.String(3),
            sa.ForeignKey("iso_language.iso639"),
            nullable=True,
        ),
    )
    op.add_column(
        "agent_word_alignments",
        sa.Column(
            "target_language",
            sa.String(3),
            sa.ForeignKey("iso_language.iso639"),
            nullable=True,
        ),
    )
    op.drop_column("agent_word_alignments", "source_version_id")
    op.drop_column("agent_word_alignments", "target_version_id")

    # --- agent_lexeme_cards ---
    op.add_column(
        "agent_lexeme_cards",
        sa.Column(
            "source_language",
            sa.String(3),
            sa.ForeignKey("iso_language.iso639"),
            nullable=True,
        ),
    )
    op.add_column(
        "agent_lexeme_cards",
        sa.Column(
            "target_language",
            sa.String(3),
            sa.ForeignKey("iso_language.iso639"),
            nullable=True,
        ),
    )
    op.drop_column("agent_lexeme_cards", "source_version_id")
    op.drop_column("agent_lexeme_cards", "target_version_id")

    # --- training_job ---
    op.add_column(
        "training_job",
        sa.Column(
            "source_language",
            sa.String(3),
            sa.ForeignKey("iso_language.iso639"),
            nullable=True,
        ),
    )
    op.add_column(
        "training_job",
        sa.Column(
            "target_language",
            sa.String(3),
            sa.ForeignKey("iso_language.iso639"),
            nullable=True,
        ),
    )
    op.execute(
        """
        UPDATE training_job tj
        SET source_language = src_bv.iso_language,
            target_language = tgt_bv.iso_language
        FROM bible_version src_bv, bible_version tgt_bv
        WHERE src_bv.id = tj.source_version_id
          AND tgt_bv.id = tj.target_version_id
        """
    )
    op.alter_column("training_job", "source_language", nullable=False)
    op.alter_column("training_job", "target_language", nullable=False)
    op.drop_column("training_job", "source_version_id")
    op.drop_column("training_job", "target_version_id")

    # --- tfidf_svd_staging ---
    op.add_column(
        "tfidf_svd_staging",
        sa.Column("source_language", sa.String(3), nullable=True),
    )
    op.drop_column("tfidf_svd_staging", "source_version_id")

    # --- tfidf_artifact_runs ---
    op.add_column(
        "tfidf_artifact_runs",
        sa.Column("source_language", sa.String(3), nullable=True),
    )
    op.execute(
        """
        UPDATE tfidf_artifact_runs tar
        SET source_language = src_bv.iso_language
        FROM bible_version src_bv
        WHERE src_bv.id = tar.source_version_id
        """
    )
    op.drop_column("tfidf_artifact_runs", "source_version_id")

    # --- eflomal_assessment ---
    op.add_column(
        "eflomal_assessment",
        sa.Column("source_language", sa.String(3), nullable=True),
    )
    op.add_column(
        "eflomal_assessment",
        sa.Column("target_language", sa.String(3), nullable=True),
    )
    op.execute(
        """
        UPDATE eflomal_assessment ea
        SET source_language = src_bv.iso_language,
            target_language = tgt_bv.iso_language
        FROM bible_version src_bv, bible_version tgt_bv
        WHERE src_bv.id = ea.source_version_id
          AND tgt_bv.id = ea.target_version_id
        """
    )
    op.drop_column("eflomal_assessment", "source_version_id")
    op.drop_column("eflomal_assessment", "target_version_id")

    # Restore previously-dropped language-keyed indexes
    with op.get_context().autocommit_block():
        for name, table, ddl in (
            (
                "ix_eflomal_assessment_language_pair",
                "eflomal_assessment",
                "(source_language, target_language)",
            ),
            (
                "ix_tfidf_artifact_runs_lang",
                "tfidf_artifact_runs",
                "(source_language)",
            ),
            (
                "ix_tfidf_artifact_runs_lang_created",
                "tfidf_artifact_runs",
                "(source_language, created_at)",
            ),
            (
                "ix_training_job_lang_pair",
                "training_job",
                "(source_language, target_language)",
            ),
            (
                "ix_agent_lexeme_cards_lang_confidence",
                "agent_lexeme_cards",
                "(source_language, target_language, confidence DESC)",
            ),
            (
                "ix_agent_word_alignments_lang_source",
                "agent_word_alignments",
                "(source_language, target_language, source_word)",
            ),
            (
                "ix_agent_word_alignments_lang_target",
                "agent_word_alignments",
                "(source_language, target_language, target_word)",
            ),
            (
                "ix_agent_word_alignments_lang_score",
                "agent_word_alignments",
                "(source_language, target_language, score DESC)",
            ),
            (
                "ix_agent_translations_rev_lang_script_vref",
                "agent_translations",
                "(revision_id, language, script, vref)",
            ),
        ):
            _drop_invalid_index(bind, name)
            op.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} " f"ON {table} {ddl}"
            )
        for name, table, ddl in (
            (
                "ix_agent_lexeme_cards_unique_v3",
                "agent_lexeme_cards",
                "(LOWER(target_lemma), source_language, target_language)",
            ),
            (
                "ux_agent_word_alignments_lang_words",
                "agent_word_alignments",
                "(source_language, target_language, source_word, target_word)",
            ),
            (
                "ix_agent_translations_unique",
                "agent_translations",
                "(revision_id, language, script, vref, version)",
            ),
        ):
            _drop_invalid_index(bind, name)
            op.execute(
                f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {name} "
                f"ON {table} {ddl}"
            )

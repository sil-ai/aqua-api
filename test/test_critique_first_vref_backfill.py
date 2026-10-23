"""Tests for the #1019 data migration that backfills ``first_vref`` on old
agent-critique runs.

The suite builds its schema with ``create_all``, so no migration ever runs here.
These tests load the migration module and execute its ``BACKFILL_SQL`` against
rows inserted directly, which is the same statement ``upgrade()`` runs.

``kwargs`` is read back as raw JSON text, not through the ORM, so SQL ``NULL``
(``None`` here) and JSON ``null`` (``"null"``) stay distinguishable. Both shapes
exist on the live table.
"""

import importlib.util
import itertools
import json
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import null, text

from database.models import AgentTranslation, Assessment, BibleRevision

MIGRATION = (
    Path(__file__).resolve().parent.parent
    / "alembic/migrations/versions/caf9540c5392_backfill_first_vref_on_rangeless_agent_.py"
)

_spec = importlib.util.spec_from_file_location("backfill_1019", MIGRATION)
_migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_migration)
BACKFILL_SQL = _migration.BACKFILL_SQL

#: Pass as ``kwargs`` to store SQL NULL. A plain ``None`` stores JSON ``null``.
SQL_NULL = null()

_names = itertools.count()


@pytest.fixture
def make_run(test_db_session, test_revision_id, test_revision_id_2, test_version_id):
    """Insert an assessment plus one agent_translations row per vref."""

    def _make(
        vrefs=(),
        kwargs=SQL_NULL,
        type="agent-critique",
        status="finished",
        deleted=False,
        revision_id=test_revision_id,
    ):
        run = Assessment(
            revision_id=revision_id,
            reference_id=test_revision_id_2,
            type=type,
            status=status,
            kwargs=kwargs,
            deleted=deleted,
        )
        test_db_session.add(run)
        test_db_session.commit()
        # ix_agent_translations_unique spans revision + vref + version, not the
        # assessment, so a fresh version keeps runs sharing vrefs apart.
        version = next(_names)
        for vref in vrefs:
            test_db_session.add(
                AgentTranslation(
                    assessment_id=run.id,
                    revision_id=revision_id,
                    reference_version_id=test_version_id,
                    script="Latn",
                    vref=vref,
                    version=version,
                    draft_text=f"draft {vref}",
                )
            )
        test_db_session.commit()
        return run.id

    return _make


def _backfill(db):
    db.execute(text(BACKFILL_SQL))
    db.commit()


def _stored(db, assessment_id):
    """The row's kwargs as raw JSON text: None for SQL NULL, "null" for JSON null."""
    return db.execute(
        text("SELECT kwargs::text FROM assessment WHERE id = :id"),
        {"id": assessment_id},
    ).scalar_one()


def _kwargs(db, assessment_id):
    return json.loads(_stored(db, assessment_id))


def test_fixture_stores_both_null_shapes(test_db_session, make_run):
    assert _stored(test_db_session, make_run()) is None
    assert _stored(test_db_session, make_run(kwargs=None)) == "null"


@pytest.mark.parametrize("kwargs", [SQL_NULL, None, {}], ids=["sql", "json", "empty"])
def test_one_chapter_gets_chapter_start_and_no_last_vref(
    test_db_session, make_run, kwargs
):
    run_id = make_run(["JHN 1:19", "JHN 1:5", "JHN 1:20"], kwargs=kwargs)
    _backfill(test_db_session)
    assert _kwargs(test_db_session, run_id) == {"first_vref": "JHN 1:1"}


def test_other_kwargs_are_kept(test_db_session, make_run):
    run_id = make_run(
        ["MAT 5:3"], kwargs={"response_language": "Swahili", "transcribed_audio": True}
    )
    _backfill(test_db_session)
    assert _kwargs(test_db_session, run_id) == {
        "response_language": "Swahili",
        "transcribed_audio": True,
        "first_vref": "MAT 5:1",
    }


@pytest.mark.parametrize("empty", ["", None])
def test_empty_first_vref_counts_as_missing(test_db_session, make_run, empty):
    run_id = make_run(["MRK 2:1"], kwargs={"first_vref": empty})
    _backfill(test_db_session)
    assert _kwargs(test_db_session, run_id) == {"first_vref": "MRK 2:1"}


def test_several_chapters_use_canonical_order(test_db_session, make_run):
    # String order would put "1JN" before "JHN" and "JHN 10" before "JHN 9".
    run_id = make_run(["JHN 10:2", "1JN 1:3", "JHN 9:4", "JHN 21:25"])
    _backfill(test_db_session)
    assert _kwargs(test_db_session, run_id) == {
        "first_vref": "JHN 9:4",
        "last_vref": "1JN 1:3",
    }


def test_several_chapters_in_one_book(test_db_session, make_run):
    run_id = make_run(["ACT 12:3", "ACT 7:9", "ACT 9:1"])
    _backfill(test_db_session)
    assert _kwargs(test_db_session, run_id) == {
        "first_vref": "ACT 7:9",
        "last_vref": "ACT 12:3",
    }


def test_existing_range_is_untouched(test_db_session, make_run):
    kwargs = {"first_vref": "LUK 3:1", "last_vref": "LUK 3:10"}
    run_id = make_run(["LUK 4:1"], kwargs=kwargs)
    _backfill(test_db_session)
    assert _kwargs(test_db_session, run_id) == kwargs


@pytest.mark.parametrize("kwargs", [SQL_NULL, None], ids=["sql", "json"])
def test_null_kwargs_without_translations_stay_as_they_are(
    test_db_session, make_run, kwargs
):
    run_id = make_run([], kwargs=kwargs)
    before = _stored(test_db_session, run_id)
    _backfill(test_db_session)
    assert _stored(test_db_session, run_id) == before


# Each case below has translations, so only the condition under test keeps the
# run from being backfilled. A non-null kwargs makes "untouched" provable.
UNTOUCHED = {"response_language": "French"}


def test_run_without_translations_is_untouched(test_db_session, make_run):
    run_id = make_run([], kwargs=UNTOUCHED)
    _backfill(test_db_session)
    assert _kwargs(test_db_session, run_id) == UNTOUCHED


@pytest.mark.parametrize("status", ["running", "queued", "failed", "cancelled"])
def test_unfinished_run_is_untouched(test_db_session, make_run, status):
    run_id = make_run(["ROM 1:1"], kwargs=UNTOUCHED, status=status)
    _backfill(test_db_session)
    assert _kwargs(test_db_session, run_id) == UNTOUCHED


def test_deleted_run_is_untouched(test_db_session, make_run):
    run_id = make_run(["ROM 8:1"], kwargs=UNTOUCHED, deleted=True)
    _backfill(test_db_session)
    assert _kwargs(test_db_session, run_id) == UNTOUCHED


def test_run_under_deleted_revision_is_untouched(
    test_db_session, make_run, test_version_id
):
    revision = BibleRevision(
        bible_version_id=test_version_id,
        name=f"deleted-{next(_names)}",
        date=datetime.now(),
        deleted=True,
    )
    test_db_session.add(revision)
    test_db_session.commit()
    run_id = make_run(["ROM 9:1"], kwargs=UNTOUCHED, revision_id=revision.id)
    _backfill(test_db_session)
    assert _kwargs(test_db_session, run_id) == UNTOUCHED


def test_other_assessment_types_are_untouched(test_db_session, make_run):
    run_id = make_run(["ACT 2:1"], kwargs=UNTOUCHED, type="semantic-similarity")
    _backfill(test_db_session)
    assert _kwargs(test_db_session, run_id) == UNTOUCHED


def test_run_with_an_unknown_vref_is_untouched(test_db_session, make_run):
    # Without the guard this would read as one chapter, PHP 1.
    run_id = make_run(["PHP 1:1", "PHP 2:1-3"], kwargs=UNTOUCHED)
    _backfill(test_db_session)
    assert _kwargs(test_db_session, run_id) == UNTOUCHED


def test_second_run_changes_nothing(test_db_session, make_run):
    run_id = make_run(["GAL 1:1", "GAL 2:1"])
    _backfill(test_db_session)
    first = _kwargs(test_db_session, run_id)
    _backfill(test_db_session)
    assert (
        _kwargs(test_db_session, run_id)
        == first
        == {
            "first_vref": "GAL 1:1",
            "last_vref": "GAL 2:1",
        }
    )

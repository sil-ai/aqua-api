"""Tests for the in-process ``tfidf`` leg of ``POST /v4/predictions`` (#992).

The ranking is real, except in the failure-isolation test, which replaces it: each test
stores real verse text and a real fitted vectorizer pair. Modal is still patched, to prove ``tfidf`` never reaches it and to stand
in for the other apps.
"""

import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sklearn.feature_extraction.text import TfidfVectorizer

from database.models import (
    Assessment,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    Group,
    TfidfArtifactRun,
    TfidfVectorizerArtifact,
)
from database.models import UserDB as UserModel
from database.models import (
    VerseText,
)
from predict_routes.v4 import predict_service, tfidf_predict
from utils.tfidf_tokenizer import unicode_word_tokenizer

_names = iter(range(10_000))

#: ``GEN 1:1`` is the query throughout; each later verse shares fewer of its words, so
#: the expected ranking is the mapping's own order. GEN 1:4 shares none.
TARGET = {
    "GEN 1:1": "light darkness waters firmament",
    "GEN 1:2": "light darkness waters serpent",
    "GEN 1:3": "light darkness garden harvest",
    "GEN 1:4": "vineyard shepherd mountain",
}
#: The same shape in a second language. GEN 1:5 exists only here, GEN 1:4 only in
#: ``TARGET``.
SOURCE = {
    "GEN 1:1": "mwanga giza maji anga",
    "GEN 1:2": "mwanga giza maji nyoka",
    "GEN 1:3": "mwanga giza bustani mavuno",
    "GEN 1:5": "shamba mchungaji mlima",
}


@pytest.fixture(autouse=True)
def _clear_fn_cache():
    predict_service._fn_cache.clear()
    yield
    predict_service._fn_cache.clear()


# --- fixtures: real rows, real vectorizers ------------------------------------------


def _owner(db):
    return db.query(UserModel).filter_by(username="testuser1").first().id


def _version(db):
    n = next(_names)
    version = BibleVersion(
        name=f"V4PT {n}",
        iso_language="eng",
        iso_script="Latn",
        abbreviation=f"V4PT{n}",
        owner_id=_owner(db),
        machine_translation=False,
        is_reference=False,
        deleted=False,
    )
    db.add(version)
    db.commit()
    group = db.query(Group).filter_by(name="Group1").first()
    db.add(BibleVersionAccess(bible_version_id=version.id, group_id=group.id))
    db.commit()
    return version.id


def _revision(db, version_id, corpus=None, *, deleted=False):
    revision = BibleRevision(
        bible_version_id=version_id,
        name=f"V4PT {next(_names)}",
        date=datetime.now(),
        published=False,
        machine_translation=False,
        deleted=deleted,
    )
    db.add(revision)
    db.commit()
    for vref, text in (corpus or {}).items():
        book, rest = vref.split(" ")
        chapter, verse = rest.split(":")
        db.add(
            VerseText(
                revision_id=revision.id,
                verse_reference=vref,
                text=text,
                book=book,
                chapter=int(chapter),
                verse=int(verse),
            )
        )
    db.commit()
    return revision.id


def _assessment(db, revision_id, *, type_="tfidf", status="finished", **fields):
    row = Assessment(
        revision_id=revision_id,
        type=type_,
        status=status,
        requested_time=datetime.now(),
        end_time=fields.pop("end_time", datetime.now()),
        owner_id=_owner(db),
        deleted=False,
        **fields,
    )
    db.add(row)
    db.commit()
    return row.id


def _recipe(db, assessment_id, version_id, corpus):
    """Fit and store the two vectorizers as the runner does, with no SVD."""
    texts = list(corpus.values())
    word = TfidfVectorizer(
        ngram_range=(1, 2), tokenizer=unicode_word_tokenizer, token_pattern=None
    ).fit(texts)
    char = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 6)).fit(texts)
    db.add(
        TfidfArtifactRun(
            assessment_id=assessment_id,
            source_version_id=version_id,
            n_components=None,
            n_word_features=len(word.vocabulary_),
            n_char_features=len(char.vocabulary_),
            n_corpus_vrefs=len(texts),
            sklearn_version="1.6.1",
        )
    )
    db.commit()
    for kind, fitted, analyzer, ngrams in (
        ("word", word, "word", [1, 2]),
        ("char", char, "char_wb", [3, 6]),
    ):
        db.add(
            TfidfVectorizerArtifact(
                assessment_id=assessment_id,
                kind=kind,
                vocabulary={k: int(v) for k, v in fitted.vocabulary_.items()},
                idf=fitted.idf_.tolist(),
                params={
                    "analyzer": analyzer,
                    "ngram_range": ngrams,
                    "lowercase": True,
                    "max_df": 1.0,
                    "min_df": 1,
                },
            )
        )
    db.commit()


class _Side:
    """A version, its revision holding ``corpus``, and optionally a tfidf assessment."""

    def __init__(self, db, corpus, *, assessed=True, artifacts=True, **fields):
        self.version_id = _version(db)
        self.revision_id = _revision(db, self.version_id, corpus)
        self.assessment_id = None
        if assessed:
            self.assessment_id = _assessment(db, self.revision_id, **fields)
            if artifacts:
                _recipe(db, self.assessment_id, self.version_id, corpus)


# --- requests -------------------------------------------------------------------------


def _modal_mock():
    """A ``modal.Function`` stand-in that records every app name looked up."""
    mock = MagicMock()
    mock.looked_up = []

    def from_name(app_name, fn_name, environment_name=None):
        mock.looked_up.append(app_name)
        fn = MagicMock()
        fn.remote.aio = AsyncMock(return_value={"app": app_name})
        fn.spawn.aio = AsyncMock(return_value=MagicMock(object_id="fc-test"))
        return fn

    mock.from_name = from_name
    return mock


def _pair(target_text=TARGET["GEN 1:1"], source_text=None, vref="GEN 1:1"):
    pair = {"target_text": target_text, "source_text": source_text}
    if vref is not None:
        pair["vref"] = vref
    return pair


def _post(client, token, *, pairs=None, apps=("tfidf",), modal=None, **fields):
    body = {
        "pairs": pairs or [_pair()],
        "apps": list(apps),
        "include_translation": False,
        **fields,
    }
    with patch(
        "predict_routes.v4.predict_service.modal.Function", modal or _modal_mock()
    ):
        return client.post(
            "/v4/predictions", json=body, headers={"Authorization": f"Bearer {token}"}
        )


def _tfidf(client, token, **fields):
    """The ``tfidf`` entry of a 200 response."""
    response = _post(client, token, **fields)
    assert response.status_code == 200, response.text
    return response.json()["results"]["tfidf"]


def _ok(client, token, **fields):
    """The ``tfidf`` leg's ``data``, asserting the leg succeeded."""
    result = _tfidf(client, token, **fields)
    assert result["status"] == "ok", result
    return result["data"]


def _both(client, token, target, source, *, source_text=SOURCE["GEN 1:1"], **fields):
    """``_ok`` for a request naming both sides by revision."""
    fields.setdefault("pairs", [_pair(source_text=source_text)])
    return _ok(
        client,
        token,
        revision_id=target.revision_id,
        reference_id=source.revision_id,
        **fields,
    )


def _vrefs(neighbours):
    return [n["vref"] for n in neighbours]


# --- tests ----------------------------------------------------------------------------


def test_tfidf_never_reaches_modal_and_the_other_apps_still_do(
    client, regular_token1, db_session
):
    target = _Side(db_session, TARGET)
    modal = _modal_mock()
    response = _post(
        client,
        regular_token1,
        apps=("tfidf", "ngrams"),
        modal=modal,
        revision_id=target.revision_id,
    )
    results = response.json()["results"]
    assert results["tfidf"]["status"] == "ok"
    assert results["ngrams"]["data"] == {"app": "ngrams"}
    assert modal.looked_up == ["ngrams"]


class TestShape:
    """The Modal app's response shape, key for key."""

    def test_both_sides_at_every_level(self, client, regular_token1, db_session):
        target, source = _Side(db_session, TARGET), _Side(db_session, SOURCE)
        data = _both(client, regular_token1, target, source)
        assert data["target_assessment_id"] == target.assessment_id
        assert data["target_revision_id"] == target.revision_id
        assert data["source_assessment_id"] == source.assessment_id
        assert data["source_revision_id"] == source.revision_id
        assert set(data) == {
            "target_assessment_id",
            "target_revision_id",
            "source_assessment_id",
            "source_revision_id",
            "pairs",
        }
        (pair,) = data["pairs"]
        assert set(pair) == {"vref", "target_neighbours", "source_neighbours"}
        neighbours = pair["target_neighbours"] + pair["source_neighbours"]
        for n in neighbours:
            assert set(n) == {
                "vref",
                "similarity",
                "target_revision_text",
                "source_revision_text",
            }
            assert isinstance(n["similarity"], float)
        # Both sides rank in order, and every hit carries both revisions' text.
        for hits in (pair["target_neighbours"], pair["source_neighbours"]):
            assert _vrefs(hits)[:3] == ["GEN 1:1", "GEN 1:2", "GEN 1:3"]
            assert hits[0]["target_revision_text"] == TARGET["GEN 1:1"]
            assert hits[0]["source_revision_text"] == SOURCE["GEN 1:1"]
        assert pair["target_neighbours"][0]["similarity"] == pytest.approx(1.0)

    def test_pair_order_and_vref_only_when_sent(
        self, client, regular_token1, db_session
    ):
        target = _Side(db_session, TARGET)
        pairs = [_pair(TARGET["GEN 1:4"], vref="A"), _pair(vref=None)]
        data = _ok(client, regular_token1, pairs=pairs, revision_id=target.revision_id)
        first, second = data["pairs"]
        assert first["vref"] == "A"
        assert first["target_neighbours"][0]["vref"] == "GEN 1:4"
        assert "vref" not in second
        assert second["target_neighbours"][0]["vref"] == "GEN 1:1"

    def test_text_missing_from_one_revision_is_null(
        self, client, regular_token1, db_session
    ):
        target, source = _Side(db_session, TARGET), _Side(db_session, SOURCE)
        pairs = [_pair(TARGET["GEN 1:4"], source_text=SOURCE["GEN 1:5"])]
        (pair,) = _both(client, regular_token1, target, source, pairs=pairs)["pairs"]
        assert pair["target_neighbours"][0]["vref"] == "GEN 1:4"
        assert pair["target_neighbours"][0]["source_revision_text"] is None
        assert pair["source_neighbours"][0]["vref"] == "GEN 1:5"
        assert pair["source_neighbours"][0]["target_revision_text"] is None


class TestTargetSide:
    def test_target_only(self, client, regular_token1, db_session):
        target = _Side(db_session, TARGET)
        data = _ok(client, regular_token1, revision_id=target.revision_id)
        assert data["source_assessment_id"] is None
        assert data["source_revision_id"] is None
        (pair,) = data["pairs"]
        assert pair["source_neighbours"] == []
        assert all(n["source_revision_text"] is None for n in pair["target_neighbours"])

    def test_assessment_id_reports_its_own_revision(
        self, client, regular_token1, db_session
    ):
        """Not the request's revision_id, which the Modal app reported."""
        target = _Side(db_session, TARGET)
        data = _ok(
            client,
            regular_token1,
            assessment_id=target.assessment_id,
            revision_id=_revision(db_session, target.version_id),
        )
        assert data["target_assessment_id"] == target.assessment_id
        assert data["target_revision_id"] == target.revision_id
        first = data["pairs"][0]["target_neighbours"][0]
        assert first["target_revision_text"] == TARGET["GEN 1:1"]

    def test_version_resolves_its_latest_live_revision(
        self, client, regular_token1, db_session
    ):
        target = _Side(db_session, TARGET)
        _revision(db_session, target.version_id, deleted=True)
        data = _ok(client, regular_token1, target_version_id=target.version_id)
        assert data["target_revision_id"] == target.revision_id

    def test_the_latest_revision_is_the_highest_id_not_the_newest_date(
        self, client, regular_token1, db_session
    ):
        """A later upload dated in the past still counts as the latest revision.

        Under the Modal app's ``date`` rule this would resolve to the assessed revision
        and succeed; under the highest-id rule it resolves to the new, unassessed one.
        """
        target = _Side(db_session, TARGET)
        backdated = _revision(db_session, target.version_id)
        db_session.query(BibleRevision).filter_by(id=backdated).update(
            {"date": datetime.now() - timedelta(days=60)}
        )
        db_session.commit()
        result = _tfidf(client, regular_token1, target_version_id=target.version_id)
        assert result["status"] == "not_trained"
        assert f"revision_id={backdated}" in result["error"]

    def test_the_latest_finished_assessment_wins(
        self, client, regular_token1, db_session
    ):
        """Latest by ``end_time``, not by id: the higher id here finished earlier."""
        target = _Side(db_session, TARGET)
        older = _assessment(
            db_session, target.revision_id, end_time=datetime.now() - timedelta(days=2)
        )
        _recipe(db_session, older, target.version_id, TARGET)
        _assessment(db_session, target.revision_id, status="failed")
        data = _ok(client, regular_token1, revision_id=target.revision_id)
        assert older > target.assessment_id
        assert data["target_assessment_id"] == target.assessment_id

    @pytest.mark.parametrize(
        "case,error",
        [
            ("queued", "No finished tfidf assessment"),
            ("running", "No finished tfidf assessment"),
            ("failed", "No finished tfidf assessment"),
            (
                "no selector",
                "requires assessment_id, revision_id, or target_version_id",
            ),
            ("version without revisions", "requires assessment_id"),
            ("no artifacts", "No TF-IDF artifacts"),
            ("no artifacts, all texts blank", "No TF-IDF artifacts"),
            ("not a tfidf assessment", "is not a tfidf assessment"),
        ],
    )
    def test_not_trained(self, client, regular_token1, db_session, case, error):
        fields = {}
        if case in ("queued", "running", "failed"):
            fields["revision_id"] = _Side(db_session, TARGET, status=case).revision_id
        elif case == "version without revisions":
            fields["target_version_id"] = _version(db_session)
        elif case.startswith("no artifacts"):
            side = _Side(db_session, TARGET, artifacts=False)
            fields["revision_id"] = side.revision_id
            if "blank" in case:
                # The Modal app loaded artifacts before reading any text.
                fields["pairs"] = [_pair(" "), _pair("")]
        elif case == "not a tfidf assessment":
            side = _Side(db_session, TARGET, type_="ngrams", artifacts=False)
            fields["assessment_id"] = side.assessment_id
        result = _tfidf(client, regular_token1, **fields)
        assert result["status"] == "not_trained"
        assert error in result["error"]
        assert result["data"] is None

    def test_a_blank_target_text_gets_no_neighbours(
        self, client, regular_token1, db_session
    ):
        target = _Side(db_session, TARGET)
        pairs = [_pair("   "), _pair()]
        data = _ok(client, regular_token1, pairs=pairs, revision_id=target.revision_id)
        assert data["pairs"][0]["target_neighbours"] == []
        assert data["pairs"][1]["target_neighbours"]


class TestSourceSide:
    def test_source_version_resolves_its_latest_revision(
        self, client, regular_token1, db_session
    ):
        target, source = _Side(db_session, TARGET), _Side(db_session, SOURCE)
        data = _ok(
            client,
            regular_token1,
            pairs=[_pair(source_text=SOURCE["GEN 1:1"])],
            revision_id=target.revision_id,
            source_version_id=source.version_id,
        )
        assert data["source_revision_id"] == source.revision_id
        assert data["source_assessment_id"] == source.assessment_id
        assert data["pairs"][0]["source_neighbours"]

    @pytest.mark.parametrize(
        "assessed,artifacts,source_text",
        [
            (False, False, SOURCE["GEN 1:1"]),  # no source assessment
            (True, False, SOURCE["GEN 1:1"]),  # assessment without artifacts
            (True, False, None),  # ...checked even when nothing is ranked
        ],
    )
    def test_best_effort_keeps_the_revision_for_parallel_text(
        self, client, regular_token1, db_session, assessed, artifacts, source_text
    ):
        target = _Side(db_session, TARGET)
        source = _Side(db_session, SOURCE, assessed=assessed, artifacts=artifacts)
        data = _both(client, regular_token1, target, source, source_text=source_text)
        assert data["source_assessment_id"] is None
        assert data["source_revision_id"] == source.revision_id
        (pair,) = data["pairs"]
        assert pair["source_neighbours"] == []
        assert pair["target_neighbours"][0]["source_revision_text"] == SOURCE["GEN 1:1"]

    @pytest.mark.parametrize("blank", [None, "", "  \n"])
    def test_a_blank_source_text_gets_no_source_neighbours(
        self, client, regular_token1, db_session, blank
    ):
        target, source = _Side(db_session, TARGET), _Side(db_session, SOURCE)
        pairs = [_pair(source_text=blank), _pair(source_text=SOURCE["GEN 1:1"])]
        data = _both(client, regular_token1, target, source, pairs=pairs)
        blank_pair, full_pair = data["pairs"]
        assert blank_pair["source_neighbours"] == []
        assert blank_pair["target_neighbours"]
        assert full_pair["source_neighbours"]


class TestTrainingRows:
    def test_training_assessments_rank_through_the_cascade(
        self, client, regular_token1, db_session
    ):
        target = _Side(db_session, TARGET, is_training=True)
        source = _Side(db_session, SOURCE, is_training=True)
        data = _both(client, regular_token1, target, source)
        assert data["target_assessment_id"] == target.assessment_id
        assert data["source_assessment_id"] == source.assessment_id
        assert data["pairs"][0]["source_neighbours"]

    def test_a_training_assessment_by_id_is_still_404(
        self, client, regular_token1, db_session
    ):
        """``authorize_selectors`` hides training rows (Decision 3); decided on #992."""
        target = _Side(db_session, TARGET, is_training=True)
        response = _post(client, regular_token1, assessment_id=target.assessment_id)
        assert response.status_code == 404, response.text
        assert response.json()["error"]["code"] == "ASSESSMENT_NOT_FOUND"


class TestLimit:
    def test_limit_cuts_each_side(self, client, regular_token1, db_session):
        target, source = _Side(db_session, TARGET), _Side(db_session, SOURCE)
        (full,) = _both(client, regular_token1, target, source)["pairs"]
        assert len(full["target_neighbours"]) > 1
        assert len(full["source_neighbours"]) > 1
        (cut,) = _both(client, regular_token1, target, source, limit=1)["pairs"]
        assert _vrefs(cut["target_neighbours"]) == ["GEN 1:1"]
        assert _vrefs(cut["source_neighbours"]) == ["GEN 1:1"]

    @pytest.mark.parametrize(
        "sent,ranked_with", [(None, tfidf_predict.TFIDF_DEFAULT_LIMIT), (100, 100)]
    )
    def test_default_and_maximum(
        self, client, regular_token1, db_session, sent, ranked_with
    ):
        target, source = _Side(db_session, TARGET), _Side(db_session, SOURCE)
        fields = {} if sent is None else {"limit": sent}
        real = tfidf_predict.assessment_service._rank_texts
        with patch.object(
            tfidf_predict.assessment_service, "_rank_texts", AsyncMock(wraps=real)
        ) as spy:
            _both(client, regular_token1, target, source, **fields)
        # One call per side, both with the same limit.
        assert [call.kwargs["limit"] for call in spy.await_args_list] == [
            ranked_with,
            ranked_with,
        ]

    def test_above_100_is_422_not_a_clamp(self, client, regular_token1):
        """``similar-verses``' rule; the cap moved down from 10,000 on #990."""
        assert _post(client, regular_token1, limit=101).status_code == 422

    def test_low_scores_are_not_dropped(self, client, regular_token1, db_session):
        """The Modal app cut everything below 0.18; this leg cuts nothing (#992)."""
        target = _Side(db_session, TARGET)
        data = _ok(client, regular_token1, revision_id=target.revision_id)
        neighbours = data["pairs"][0]["target_neighbours"]
        assert _vrefs(neighbours) == ["GEN 1:1", "GEN 1:2", "GEN 1:3", "GEN 1:4"]
        assert neighbours[-1]["similarity"] < 0.18


def test_each_revision_is_read_once_over_the_union_of_hits(
    client, regular_token1, db_session
):
    target, source = _Side(db_session, TARGET), _Side(db_session, SOURCE)
    pairs = [
        _pair(TARGET[vref], source_text=SOURCE.get(vref), vref=vref)
        for vref in ("GEN 1:1", "GEN 1:2", "GEN 1:4")
    ]
    real = tfidf_predict.assessment_service._verse_texts
    with patch.object(
        tfidf_predict.assessment_service, "_verse_texts", AsyncMock(wraps=real)
    ) as spy:
        data = _both(client, regular_token1, target, source, pairs=pairs)
    hits = sorted(
        {
            n["vref"]
            for pair in data["pairs"]
            for n in pair["target_neighbours"] + pair["source_neighbours"]
        }
    )
    assert [call.args[1:] for call in spy.await_args_list] == [
        (target.revision_id, hits),
        (source.revision_id, hits),
    ]


def test_more_than_eight_pairs_use_the_corpus_index(client, regular_token1, db_session):
    """Past ``TWO_STAGE_MAX_QUERIES`` the ranking is the whole-revision index."""
    target = _Side(db_session, TARGET)
    pairs = [_pair(vref=f"P{i}") for i in range(9)]
    data = _ok(client, regular_token1, pairs=pairs, revision_id=target.revision_id)
    assert len(data["pairs"]) == 9
    for pair in data["pairs"]:
        assert pair["target_neighbours"][0]["vref"] == "GEN 1:1"
        assert pair["target_neighbours"][0]["similarity"] == pytest.approx(1.0)


def test_runs_beside_the_slow_agent_spawn(client, regular_token1, db_session):
    """Both complete when the slow-leg spawn runs beside the leg.

    This shows the two coexist, not that they use separate sessions: the test database
    uses NullPool and the timing here rarely overlaps them.
    """
    target = _Side(db_session, TARGET)
    response = _post(
        client,
        regular_token1,
        pairs=[_pair(source_text="mwanga")],
        apps=("tfidf", "agent-critique"),
        include_translation=True,
        revision_id=target.revision_id,
    )
    body = response.json()
    assert body["results"]["tfidf"]["status"] == "ok"
    assert body["job"]["state"] == "RUNNING"


@pytest.mark.parametrize("failure", ["exception", "timeout"])
def test_a_failing_leg_is_isolated_like_a_modal_one(
    client, regular_token1, db_session, monkeypatch, failure
):
    target = _Side(db_session, TARGET)
    monkeypatch.setitem(
        predict_service.PER_APP_TIMEOUT_S, predict_service.PredictApp.tfidf, 0.2
    )

    async def hang(*args, **kwargs):
        await asyncio.sleep(30)

    effect = RuntimeError("postgres://secret@host") if failure == "exception" else hang
    with patch.object(
        tfidf_predict.assessment_service, "_rank_texts", AsyncMock(side_effect=effect)
    ):
        response = _post(
            client,
            regular_token1,
            apps=("tfidf", "ngrams"),
            revision_id=target.revision_id,
        )
    results = response.json()["results"]
    assert results["tfidf"]["status"] == "error"
    expected = "RuntimeError" if failure == "exception" else "timeout after 0.2s"
    assert results["tfidf"]["error"] == expected
    assert results["ngrams"]["status"] == "ok"

"""Tests for the in-process ``tfidf`` leg of ``POST /v4/predictions`` (#992).

The leg used to be a Modal call; now :mod:`predict_routes.v4.tfidf_predict` ranks with
the code behind ``similar-verses`` and returns the Modal app's response shape. So unlike
``test_predict_routes_v4.py``, nothing here mocks the ranking: each test stores real
verse text and a real fitted vectorizer pair, and the rankings are computed. Modal is
still patched, to prove ``tfidf`` never reaches it and to stand in for the other apps.

What each group pins down:

* ``TestNoModal`` — ``tfidf`` is never dispatched, and the apps beside it still are.
* ``TestShape`` — the response keys, exactly, at the top level, per pair and per
  neighbour.
* ``TestTargetSide`` — the target cascade, and every way it fails as ``not_trained``.
* ``TestSourceSide`` — the source cascade's best-effort rules.
* ``TestTrainingRows`` — training assessments rank through the cascade, and are still a
  404 by id.
* ``TestLimit`` — the default, the cap, and that nothing is dropped for scoring low.
* ``TestParallelText`` — one text read per revision over the union of every hit.
* ``TestBatch`` — the whole-revision index path, and the slow-leg spawn running beside
  the leg.
"""

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

PREDICTIONS = "/v4/predictions"

_names = iter(range(10_000))

#: The target revision's text. ``GEN 1:1`` is the query used throughout, and each later
#: verse shares fewer of its words, so the expected order is the mapping's own.
TARGET_CORPUS = {
    "GEN 1:1": "light darkness waters firmament",
    "GEN 1:2": "light darkness waters serpent",
    "GEN 1:3": "light darkness garden harvest",
    "GEN 1:4": "vineyard shepherd mountain",
}
#: The source revision's text, built the same way in a second language.
SOURCE_CORPUS = {
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


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


def _user_id(db_session, username):
    return db_session.query(UserModel).filter_by(username=username).first().id


def _make_version(db_session, group_name="Group1"):
    n = next(_names)
    version = BibleVersion(
        name=f"V4PT Version {n}",
        iso_language="eng",
        iso_script="Latn",
        abbreviation=f"V4PT{n}",
        owner_id=_user_id(db_session, "testuser1"),
        machine_translation=False,
        is_reference=False,
        deleted=False,
    )
    db_session.add(version)
    db_session.commit()
    group = db_session.query(Group).filter_by(name=group_name).first()
    db_session.add(BibleVersionAccess(bible_version_id=version.id, group_id=group.id))
    db_session.commit()
    return version.id


def _make_revision(db_session, version_id, corpus=None, *, deleted=False):
    revision = BibleRevision(
        bible_version_id=version_id,
        name=f"V4PT Revision {next(_names)}",
        date=datetime.now(),
        published=False,
        machine_translation=False,
        deleted=deleted,
    )
    db_session.add(revision)
    db_session.commit()
    for vref, text in (corpus or {}).items():
        book, rest = vref.split(" ")
        chapter, verse = rest.split(":")
        db_session.add(
            VerseText(
                revision_id=revision.id,
                verse_reference=vref,
                text=text,
                book=book,
                chapter=int(chapter),
                verse=int(verse),
            )
        )
    db_session.commit()
    return revision.id


def _make_assessment(
    db_session,
    revision_id,
    *,
    type_="tfidf",
    status="finished",
    is_training=False,
    end_time=None,
):
    assessment = Assessment(
        revision_id=revision_id,
        type=type_,
        status=status,
        requested_time=datetime.now(),
        end_time=end_time or datetime.now(),
        owner_id=_user_id(db_session, "testuser1"),
        is_training=is_training,
        deleted=False,
    )
    db_session.add(assessment)
    db_session.commit()
    return assessment.id


def _store_recipe(db_session, assessment_id, version_id, corpus):
    """Fit and store the two vectorizers the way the runner does, with no SVD."""
    texts = list(corpus.values())
    word = TfidfVectorizer(
        analyzer="word",
        ngram_range=(1, 2),
        lowercase=True,
        tokenizer=unicode_word_tokenizer,
        token_pattern=None,
    ).fit(texts)
    char = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 6), lowercase=True).fit(
        texts
    )
    db_session.add(
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
    db_session.commit()
    for kind, vectorizer, analyzer, ngram_range in (
        ("word", word, "word", [1, 2]),
        ("char", char, "char_wb", [3, 6]),
    ):
        db_session.add(
            TfidfVectorizerArtifact(
                assessment_id=assessment_id,
                kind=kind,
                vocabulary={k: int(v) for k, v in vectorizer.vocabulary_.items()},
                idf=vectorizer.idf_.tolist(),
                params={
                    "analyzer": analyzer,
                    "ngram_range": ngram_range,
                    "lowercase": True,
                    "max_df": 1.0,
                    "min_df": 1,
                },
            )
        )
    db_session.commit()


class _Side:
    def __init__(self, version_id, revision_id, assessment_id):
        self.version_id = version_id
        self.revision_id = revision_id
        self.assessment_id = assessment_id


def _side(db_session, corpus, *, assessed=True, artifacts=True, **assessment_kwargs):
    """A version, a revision holding ``corpus``, and optionally its tfidf assessment."""
    version_id = _make_version(db_session)
    revision_id = _make_revision(db_session, version_id, corpus)
    assessment_id = None
    if assessed:
        assessment_id = _make_assessment(db_session, revision_id, **assessment_kwargs)
        if artifacts:
            _store_recipe(db_session, assessment_id, version_id, corpus)
    return _Side(version_id, revision_id, assessment_id)


def _modal_mock():
    """A ``modal.Function`` stand-in that records every app name it is asked for."""
    mock_cls = MagicMock()
    mock_cls.looked_up = []

    def from_name(app_name, fn_name, environment_name=None):
        mock_cls.looked_up.append(app_name)
        fn = MagicMock()
        fn.remote.aio = AsyncMock(return_value={"app": app_name})
        fn.spawn.aio = AsyncMock(return_value=MagicMock(object_id="fc-test"))
        return fn

    mock_cls.from_name = from_name
    return mock_cls


def _post(client, token, body, modal_mock=None):
    with patch(
        "predict_routes.v4.predict_service.modal.Function",
        modal_mock if modal_mock is not None else _modal_mock(),
    ):
        return client.post(PREDICTIONS, json=body, headers=_auth(token))


def _pair(target_text=TARGET_CORPUS["GEN 1:1"], source_text=None, vref="GEN 1:1"):
    pair = {"target_text": target_text, "source_text": source_text}
    if vref is not None:
        pair["vref"] = vref
    return pair


def _tfidf(client, token, pairs=None, **selectors):
    body = {
        "pairs": pairs or [_pair()],
        "apps": ["tfidf"],
        "include_translation": False,
        **selectors,
    }
    response = _post(client, token, body)
    assert response.status_code == 200, response.text
    return response.json()["results"]["tfidf"]


def _ok(result):
    assert result["status"] == "ok", result
    return result["data"]


def _vrefs(neighbours):
    return [n["vref"] for n in neighbours]


class TestNoModal:
    def test_tfidf_is_never_looked_up_on_modal(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS)
        mock = _modal_mock()
        response = _post(
            client,
            regular_token1,
            {
                "pairs": [_pair()],
                "apps": ["tfidf", "ngrams"],
                "revision_id": target.revision_id,
                "include_translation": False,
            },
            mock,
        )
        assert response.status_code == 200, response.text
        results = response.json()["results"]
        assert results["tfidf"]["status"] == "ok"
        assert results["ngrams"]["data"] == {"app": "ngrams"}
        assert "tfidf" not in mock.looked_up
        assert mock.looked_up == ["ngrams"]

    def test_an_unresolvable_target_still_never_reaches_modal(
        self, client, regular_token1
    ):
        mock = _modal_mock()
        response = _post(
            client,
            regular_token1,
            {"pairs": [_pair()], "apps": ["tfidf"], "include_translation": False},
            mock,
        )
        assert response.json()["results"]["tfidf"]["status"] == "not_trained"
        assert mock.looked_up == []


class TestShape:
    """The Modal app's response shape, key for key, so `data` means what it did."""

    def test_keys_at_every_level(self, client, regular_token1, db_session):
        target = _side(db_session, TARGET_CORPUS)
        source = _side(db_session, SOURCE_CORPUS)
        result = _tfidf(
            client,
            regular_token1,
            pairs=[_pair(source_text=SOURCE_CORPUS["GEN 1:1"])],
            revision_id=target.revision_id,
            reference_id=source.revision_id,
        )
        assert result["error"] is None
        data = _ok(result)
        assert set(data) == {
            "target_assessment_id",
            "target_revision_id",
            "source_assessment_id",
            "source_revision_id",
            "pairs",
        }
        assert data["target_assessment_id"] == target.assessment_id
        assert data["target_revision_id"] == target.revision_id
        assert data["source_assessment_id"] == source.assessment_id
        assert data["source_revision_id"] == source.revision_id
        (pair,) = data["pairs"]
        assert set(pair) == {"vref", "target_neighbours", "source_neighbours"}
        assert pair["vref"] == "GEN 1:1"
        for neighbour in pair["target_neighbours"] + pair["source_neighbours"]:
            assert set(neighbour) == {
                "vref",
                "similarity",
                "target_revision_text",
                "source_revision_text",
            }
            assert isinstance(neighbour["similarity"], float)
            assert 0.0 <= neighbour["similarity"] <= 1.0 + 1e-6

    def test_vref_is_absent_when_the_caller_sent_none(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS)
        data = _ok(
            _tfidf(
                client,
                regular_token1,
                pairs=[_pair(vref=None)],
                revision_id=target.revision_id,
            )
        )
        assert "vref" not in data["pairs"][0]

    def test_pairs_come_back_in_submission_order(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS)
        pairs = [
            _pair(TARGET_CORPUS["GEN 1:4"], vref="A"),
            _pair(TARGET_CORPUS["GEN 1:1"], vref="B"),
        ]
        data = _ok(
            _tfidf(client, regular_token1, pairs=pairs, revision_id=target.revision_id)
        )
        assert [p["vref"] for p in data["pairs"]] == ["A", "B"]
        assert data["pairs"][0]["target_neighbours"][0]["vref"] == "GEN 1:4"
        assert data["pairs"][1]["target_neighbours"][0]["vref"] == "GEN 1:1"


class TestTargetSide:
    def test_target_only(self, client, regular_token1, db_session):
        target = _side(db_session, TARGET_CORPUS)
        data = _ok(_tfidf(client, regular_token1, revision_id=target.revision_id))
        assert data["source_assessment_id"] is None
        assert data["source_revision_id"] is None
        (pair,) = data["pairs"]
        assert pair["source_neighbours"] == []
        neighbours = pair["target_neighbours"]
        # The identical verse first, at cosine 1; nothing is excluded, since the pair's
        # vref is a caller label.
        assert neighbours[0]["vref"] == "GEN 1:1"
        assert neighbours[0]["similarity"] == pytest.approx(1.0)
        assert _vrefs(neighbours)[:3] == ["GEN 1:1", "GEN 1:2", "GEN 1:3"]
        assert neighbours[0]["target_revision_text"] == TARGET_CORPUS["GEN 1:1"]
        assert all(n["source_revision_text"] is None for n in neighbours)

    def test_assessment_id_is_used_directly(self, client, regular_token1, db_session):
        target = _side(db_session, TARGET_CORPUS)
        data = _ok(_tfidf(client, regular_token1, assessment_id=target.assessment_id))
        assert data["target_assessment_id"] == target.assessment_id
        assert data["target_revision_id"] == target.revision_id

    def test_assessment_id_reports_its_own_revision(
        self, client, regular_token1, db_session
    ):
        """Not the request's revision_id, which the Modal app reported. See the module."""
        target = _side(db_session, TARGET_CORPUS)
        other_revision = _make_revision(db_session, target.version_id)
        data = _ok(
            _tfidf(
                client,
                regular_token1,
                assessment_id=target.assessment_id,
                revision_id=other_revision,
            )
        )
        assert data["target_revision_id"] == target.revision_id
        first = data["pairs"][0]["target_neighbours"][0]
        assert first["target_revision_text"] == TARGET_CORPUS["GEN 1:1"]

    def test_target_version_resolves_its_latest_revision(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS)
        # A newer revision that is deleted is skipped.
        _make_revision(db_session, target.version_id, deleted=True)
        data = _ok(_tfidf(client, regular_token1, target_version_id=target.version_id))
        assert data["target_revision_id"] == target.revision_id
        assert data["target_assessment_id"] == target.assessment_id

    def test_the_latest_finished_assessment_wins(
        self, client, regular_token1, db_session
    ):
        target = _side(
            db_session, TARGET_CORPUS, end_time=datetime.now() - timedelta(days=2)
        )
        newer = _make_assessment(
            db_session, target.revision_id, end_time=datetime.now()
        )
        _store_recipe(db_session, newer, target.version_id, TARGET_CORPUS)
        _make_assessment(db_session, target.revision_id, status="failed")
        data = _ok(_tfidf(client, regular_token1, revision_id=target.revision_id))
        assert data["target_assessment_id"] == newer

    @pytest.mark.parametrize("status", ["queued", "running", "failed"])
    def test_no_finished_assessment_is_not_trained(
        self, client, regular_token1, db_session, status
    ):
        target = _side(db_session, TARGET_CORPUS, status=status)
        result = _tfidf(client, regular_token1, revision_id=target.revision_id)
        assert result["status"] == "not_trained"
        assert "No finished tfidf assessment" in result["error"]
        assert result["data"] is None

    def test_no_selector_at_all_is_not_trained(self, client, regular_token1):
        result = _tfidf(client, regular_token1)
        assert result["status"] == "not_trained"
        assert "requires assessment_id, revision_id, or target_version_id" in (
            result["error"]
        )

    def test_a_version_with_no_revision_is_not_trained(
        self, client, regular_token1, db_session
    ):
        version_id = _make_version(db_session)
        result = _tfidf(client, regular_token1, target_version_id=version_id)
        assert result["status"] == "not_trained"

    def test_an_assessment_without_artifacts_is_not_trained(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS, artifacts=False)
        result = _tfidf(client, regular_token1, revision_id=target.revision_id)
        assert result["status"] == "not_trained"
        assert "No TF-IDF artifacts" in result["error"]

    def test_a_non_tfidf_assessment_id_is_not_trained(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS, type_="ngrams", artifacts=False)
        result = _tfidf(client, regular_token1, assessment_id=target.assessment_id)
        assert result["status"] == "not_trained"
        assert "is not a tfidf assessment" in result["error"]

    def test_all_blank_texts_still_need_artifacts(
        self, client, regular_token1, db_session
    ):
        """The Modal app loaded artifacts before reading any text, so an artifact-less
        target was ``not_trained`` however blank the texts were."""
        target = _side(db_session, TARGET_CORPUS, artifacts=False)
        result = _tfidf(
            client,
            regular_token1,
            pairs=[_pair(" "), _pair("")],
            revision_id=target.revision_id,
        )
        assert result["status"] == "not_trained"
        assert "No TF-IDF artifacts" in result["error"]

    def test_a_blank_target_text_gets_no_neighbours(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS)
        data = _ok(
            _tfidf(
                client,
                regular_token1,
                pairs=[_pair("   "), _pair()],
                revision_id=target.revision_id,
            )
        )
        assert data["pairs"][0]["target_neighbours"] == []
        assert data["pairs"][1]["target_neighbours"]


class TestSourceSide:
    def test_target_and_source(self, client, regular_token1, db_session):
        target = _side(db_session, TARGET_CORPUS)
        source = _side(db_session, SOURCE_CORPUS)
        data = _ok(
            _tfidf(
                client,
                regular_token1,
                pairs=[_pair(source_text=SOURCE_CORPUS["GEN 1:1"])],
                revision_id=target.revision_id,
                reference_id=source.revision_id,
            )
        )
        (pair,) = data["pairs"]
        source_hits = pair["source_neighbours"]
        assert _vrefs(source_hits)[:3] == ["GEN 1:1", "GEN 1:2", "GEN 1:3"]
        # Each hit carries both revisions' text for its vref, whichever side found it.
        first = source_hits[0]
        assert first["source_revision_text"] == SOURCE_CORPUS["GEN 1:1"]
        assert first["target_revision_text"] == TARGET_CORPUS["GEN 1:1"]
        target_first = pair["target_neighbours"][0]
        assert target_first["source_revision_text"] == SOURCE_CORPUS["GEN 1:1"]

    def test_missing_text_on_one_side_is_null(self, client, regular_token1, db_session):
        """GEN 1:5 exists only in the source revision, GEN 1:4 only in the target."""
        target = _side(db_session, TARGET_CORPUS)
        source = _side(db_session, SOURCE_CORPUS)
        data = _ok(
            _tfidf(
                client,
                regular_token1,
                pairs=[
                    _pair(
                        TARGET_CORPUS["GEN 1:4"], source_text=SOURCE_CORPUS["GEN 1:5"]
                    )
                ],
                revision_id=target.revision_id,
                reference_id=source.revision_id,
            )
        )
        (pair,) = data["pairs"]
        target_first = pair["target_neighbours"][0]
        assert target_first["vref"] == "GEN 1:4"
        assert target_first["source_revision_text"] is None
        source_first = pair["source_neighbours"][0]
        assert source_first["vref"] == "GEN 1:5"
        assert source_first["target_revision_text"] is None

    def test_source_version_resolves_its_latest_revision(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS)
        source = _side(db_session, SOURCE_CORPUS)
        data = _ok(
            _tfidf(
                client,
                regular_token1,
                pairs=[_pair(source_text=SOURCE_CORPUS["GEN 1:1"])],
                revision_id=target.revision_id,
                source_version_id=source.version_id,
            )
        )
        assert data["source_revision_id"] == source.revision_id
        assert data["source_assessment_id"] == source.assessment_id
        assert data["pairs"][0]["source_neighbours"]

    def test_no_source_assessment_keeps_the_revision_for_parallel_text(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS)
        source = _side(db_session, SOURCE_CORPUS, assessed=False)
        data = _ok(
            _tfidf(
                client,
                regular_token1,
                pairs=[_pair(source_text=SOURCE_CORPUS["GEN 1:1"])],
                revision_id=target.revision_id,
                reference_id=source.revision_id,
            )
        )
        assert data["source_assessment_id"] is None
        assert data["source_revision_id"] == source.revision_id
        (pair,) = data["pairs"]
        assert pair["source_neighbours"] == []
        first = pair["target_neighbours"][0]
        assert first["source_revision_text"] == SOURCE_CORPUS["GEN 1:1"]

    def test_a_source_assessment_without_artifacts_degrades(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS)
        source = _side(db_session, SOURCE_CORPUS, artifacts=False)
        result = _tfidf(
            client,
            regular_token1,
            pairs=[_pair(source_text=SOURCE_CORPUS["GEN 1:1"])],
            revision_id=target.revision_id,
            reference_id=source.revision_id,
        )
        data = _ok(result)
        assert data["source_assessment_id"] is None
        assert data["source_revision_id"] == source.revision_id
        assert data["pairs"][0]["source_neighbours"] == []
        assert data["pairs"][0]["target_neighbours"]

    def test_an_artifact_less_source_is_not_reported_when_no_text_is_ranked(
        self, client, regular_token1, db_session
    ):
        """No pair sends source text, so nothing is ranked on that side, but the source
        assessment is still checked, as the Modal app checked it."""
        target = _side(db_session, TARGET_CORPUS)
        source = _side(db_session, SOURCE_CORPUS, artifacts=False)
        data = _ok(
            _tfidf(
                client,
                regular_token1,
                revision_id=target.revision_id,
                reference_id=source.revision_id,
            )
        )
        assert data["source_assessment_id"] is None
        assert data["source_revision_id"] == source.revision_id

    @pytest.mark.parametrize("blank", [None, "", "  \n"])
    def test_a_blank_source_text_gets_no_source_neighbours(
        self, client, regular_token1, db_session, blank
    ):
        target = _side(db_session, TARGET_CORPUS)
        source = _side(db_session, SOURCE_CORPUS)
        data = _ok(
            _tfidf(
                client,
                regular_token1,
                pairs=[
                    _pair(source_text=blank, vref="blank"),
                    _pair(source_text=SOURCE_CORPUS["GEN 1:1"], vref="full"),
                ],
                revision_id=target.revision_id,
                reference_id=source.revision_id,
            )
        )
        blank_pair, full_pair = data["pairs"]
        assert blank_pair["source_neighbours"] == []
        assert blank_pair["target_neighbours"]
        assert full_pair["source_neighbours"]
        assert data["source_assessment_id"] == source.assessment_id


class TestFailureIsolation:
    """The leg fails the way a Modal leg fails: in its own entry, never the request."""

    def test_an_unexpected_exception_reports_only_its_type(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS)
        with patch.object(
            tfidf_predict.assessment_service,
            "_rank_texts",
            AsyncMock(side_effect=RuntimeError("postgres://secret@host")),
        ):
            response = _post(
                client,
                regular_token1,
                {
                    "pairs": [_pair()],
                    "apps": ["tfidf", "ngrams"],
                    "revision_id": target.revision_id,
                    "include_translation": False,
                },
            )
        assert response.status_code == 200, response.text
        results = response.json()["results"]
        assert results["tfidf"]["status"] == "error"
        assert results["tfidf"]["error"] == "RuntimeError"
        assert results["ngrams"]["status"] == "ok"

    def test_a_slow_leg_times_out_like_any_other(
        self, client, regular_token1, db_session, monkeypatch
    ):
        import asyncio

        target = _side(db_session, TARGET_CORPUS)
        monkeypatch.setitem(
            predict_service.PER_APP_TIMEOUT_S, predict_service.PredictApp.tfidf, 0.2
        )

        async def hang(*args, **kwargs):
            await asyncio.sleep(30)

        with patch.object(
            tfidf_predict.assessment_service, "_rank_texts", AsyncMock(side_effect=hang)
        ):
            response = _post(
                client,
                regular_token1,
                {
                    "pairs": [_pair()],
                    "apps": ["tfidf", "ngrams"],
                    "revision_id": target.revision_id,
                    "include_translation": False,
                },
            )
        assert response.status_code == 200, response.text
        results = response.json()["results"]
        assert results["tfidf"]["status"] == "error"
        assert results["tfidf"]["error"] == "timeout after 0.2s"
        assert results["tfidf"]["duration_ms"] < 5000
        assert results["ngrams"]["status"] == "ok"


class TestTrainingRows:
    def test_a_training_assessment_ranks_through_the_cascade(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS, is_training=True)
        data = _ok(_tfidf(client, regular_token1, revision_id=target.revision_id))
        assert data["target_assessment_id"] == target.assessment_id
        assert data["pairs"][0]["target_neighbours"][0]["vref"] == "GEN 1:1"

    def test_a_training_source_assessment_ranks_too(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS)
        source = _side(db_session, SOURCE_CORPUS, is_training=True)
        data = _ok(
            _tfidf(
                client,
                regular_token1,
                pairs=[_pair(source_text=SOURCE_CORPUS["GEN 1:1"])],
                revision_id=target.revision_id,
                reference_id=source.revision_id,
            )
        )
        assert data["source_assessment_id"] == source.assessment_id
        assert data["pairs"][0]["source_neighbours"]

    def test_a_training_assessment_by_id_is_still_404(
        self, client, regular_token1, db_session
    ):
        """``authorize_selectors`` resolves the id through ``get_assessment``, which
        hides training rows (Decision 3). #992 leaves that predicate alone."""
        target = _side(db_session, TARGET_CORPUS, is_training=True)
        response = _post(
            client,
            regular_token1,
            {
                "pairs": [_pair()],
                "apps": ["tfidf"],
                "assessment_id": target.assessment_id,
                "include_translation": False,
            },
        )
        assert response.status_code == 404, response.text
        assert response.json()["error"]["code"] == "ASSESSMENT_NOT_FOUND"


class TestLimit:
    def test_limit_caps_each_side(self, client, regular_token1, db_session):
        target = _side(db_session, TARGET_CORPUS)
        source = _side(db_session, SOURCE_CORPUS)
        selectors = {
            "pairs": [_pair(source_text=SOURCE_CORPUS["GEN 1:1"])],
            "revision_id": target.revision_id,
            "reference_id": source.revision_id,
        }
        (unlimited,) = _ok(_tfidf(client, regular_token1, **selectors))["pairs"]
        assert len(unlimited["target_neighbours"]) > 1
        assert len(unlimited["source_neighbours"]) > 1
        (pair,) = _ok(_tfidf(client, regular_token1, limit=1, **selectors))["pairs"]
        assert _vrefs(pair["target_neighbours"]) == ["GEN 1:1"]
        assert _vrefs(pair["source_neighbours"]) == ["GEN 1:1"]

    @pytest.mark.parametrize(
        "sent,ranked_with",
        [(None, tfidf_predict.TFIDF_DEFAULT_LIMIT), (7, 7), (100, 100)],
    )
    def test_limit_default_and_cap(
        self, client, regular_token1, db_session, sent, ranked_with
    ):
        target = _side(db_session, TARGET_CORPUS)
        selectors = {"revision_id": target.revision_id}
        if sent is not None:
            selectors["limit"] = sent
        real = tfidf_predict.assessment_service._rank_texts
        with patch.object(
            tfidf_predict.assessment_service,
            "_rank_texts",
            AsyncMock(wraps=real),
        ) as spy:
            _ok(_tfidf(client, regular_token1, **selectors))
        assert spy.await_args.kwargs["limit"] == ranked_with

    def test_a_limit_above_100_is_422_not_a_clamp(self, client, regular_token1):
        """``similar-verses``' rule: out of range is refused, never quietly narrowed.
        The cap moved down from 10,000 on #990."""
        response = _post(
            client,
            regular_token1,
            {
                "pairs": [_pair()],
                "apps": ["tfidf"],
                "limit": 101,
                "include_translation": False,
            },
        )
        assert response.status_code == 422, response.text

    def test_low_scores_are_not_dropped(self, client, regular_token1, db_session):
        """The Modal app cut everything below 0.18; this leg cuts nothing (#992).

        GEN 1:4 shares no word with the query, so it scores far below the old cutoff,
        and it still comes back, last.
        """
        target = _side(db_session, TARGET_CORPUS)
        data = _ok(_tfidf(client, regular_token1, revision_id=target.revision_id))
        neighbours = data["pairs"][0]["target_neighbours"]
        assert _vrefs(neighbours) == ["GEN 1:1", "GEN 1:2", "GEN 1:3", "GEN 1:4"]
        assert neighbours[-1]["similarity"] < 0.18


class TestParallelText:
    def test_each_revision_is_read_once_over_the_union_of_hits(
        self, client, regular_token1, db_session
    ):
        target = _side(db_session, TARGET_CORPUS)
        source = _side(db_session, SOURCE_CORPUS)
        pairs = [
            _pair(TARGET_CORPUS[vref], source_text=SOURCE_CORPUS.get(vref), vref=vref)
            for vref in ("GEN 1:1", "GEN 1:2", "GEN 1:4")
        ]
        real = tfidf_predict.assessment_service._verse_texts
        with patch.object(
            tfidf_predict.assessment_service,
            "_verse_texts",
            AsyncMock(wraps=real),
        ) as spy:
            data = _ok(
                _tfidf(
                    client,
                    regular_token1,
                    pairs=pairs,
                    revision_id=target.revision_id,
                    reference_id=source.revision_id,
                )
            )
        hit_vrefs = {
            n["vref"]
            for pair in data["pairs"]
            for n in pair["target_neighbours"] + pair["source_neighbours"]
        }
        assert spy.await_count == 2
        revisions = [call.args[1] for call in spy.await_args_list]
        assert revisions == [target.revision_id, source.revision_id]
        for call in spy.await_args_list:
            assert call.args[2] == sorted(hit_vrefs)


class TestBatch:
    def test_more_than_eight_pairs_use_the_corpus_index(
        self, client, regular_token1, db_session
    ):
        """Past ``TWO_STAGE_MAX_QUERIES`` the ranking switches to the whole-revision
        index; the leg must answer the same way through it."""
        target = _side(db_session, TARGET_CORPUS)
        pairs = [_pair(vref=f"P{i}") for i in range(9)]
        data = _ok(
            _tfidf(client, regular_token1, pairs=pairs, revision_id=target.revision_id)
        )
        assert len(data["pairs"]) == 9
        for pair in data["pairs"]:
            assert pair["target_neighbours"][0]["vref"] == "GEN 1:1"
            assert pair["target_neighbours"][0]["similarity"] == pytest.approx(1.0)

    def test_runs_beside_the_slow_agent_spawn(self, client, regular_token1, db_session):
        """The spawn writes on the request's session while this leg reads; the leg's
        own session is what makes that safe."""
        target = _side(db_session, TARGET_CORPUS)
        mock = _modal_mock()
        response = _post(
            client,
            regular_token1,
            {
                "pairs": [_pair(source_text="mwanga")],
                "apps": ["tfidf", "agent-critique"],
                "revision_id": target.revision_id,
                "include_translation": True,
            },
            mock,
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["results"]["tfidf"]["status"] == "ok"
        assert body["job"]["state"] == "RUNNING"
        assert "tfidf" not in mock.looked_up

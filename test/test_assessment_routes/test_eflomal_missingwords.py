"""Tests for the use_eflomal=True branch of GET /v3/missingwords."""

import pytest

from database.models import (
    Assessment,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    EflomalAssessment,
    EflomalDictionary,
    EflomalTargetWordCount,
    Group,
    UserDB,
    VerseText,
)

EFLOMAL_REV_ID = 8801  # revision (target / swh)
EFLOMAL_REF_ID = 8800  # reference (source / eng)
EFLOMAL_VERSION_ID = 8800
EFLOMAL_ASSESSMENT_ID = 8800


def _seed_eflomal_fixture(db_session):
    """Create a single-verse eflomal assessment with engineered orphans.

    Engineered scenario for vref 'GEN 1:1':
      reference (eng)  = "god created"
      revision  (swh)  = "mungu kosmos borderline"
      dictionary       = god→mungu(100), created→aliumba(100),
                         world→kosmos(100), alpha→borderline(10)
      target counts    = mungu:100, kosmos:200, borderline:50
                         (alignment freq for kosmos = 100/200 = 0.5,
                          alignment freq for borderline = 10/50 = 0.2)
      reference corpus is just one verse, so src_word_counts derived on the
      fly are {"god": 1, "created": 1}.

    Greedy alignment matches god↔mungu only. With default min_frequency=0.5:
      - target side: 'kosmos' is an orphan (known src 'world' not in source);
        'borderline' is dropped at min_frequency=0.5 but emitted at 0.1.
      - source side: 'created' is an orphan (known tgt 'aliumba' not in
        target); 'god' was aligned, so skipped.
    """
    if (
        db_session.query(Assessment)
        .filter(Assessment.id == EFLOMAL_ASSESSMENT_ID)
        .first()
    ):
        return EFLOMAL_ASSESSMENT_ID

    user = db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    user_id = user.id if user else None

    version = BibleVersion(
        id=EFLOMAL_VERSION_ID,
        abbreviation="EFLM",
        name="Eflomal Missingwords Test",
        owner_id=user_id,
    )
    db_session.add(version)

    db_session.add(
        BibleRevision(id=EFLOMAL_REF_ID, bible_version_id=EFLOMAL_VERSION_ID)
    )
    db_session.add(
        BibleRevision(id=EFLOMAL_REV_ID, bible_version_id=EFLOMAL_VERSION_ID)
    )

    db_session.add(
        VerseText(
            text="god created",
            revision_id=EFLOMAL_REF_ID,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        )
    )
    db_session.add(
        VerseText(
            text="mungu kosmos borderline",
            revision_id=EFLOMAL_REV_ID,
            verse_reference="GEN 1:1",
            book="GEN",
            chapter=1,
            verse=1,
        )
    )

    assessment = Assessment(
        id=EFLOMAL_ASSESSMENT_ID,
        revision_id=EFLOMAL_REV_ID,
        reference_id=EFLOMAL_REF_ID,
        type="word-alignment",
        status="finished",
        assessment_version="1",
    )
    db_session.add(assessment)

    group = db_session.query(Group.id).first()
    if group:
        db_session.add(
            BibleVersionAccess(bible_version_id=EFLOMAL_VERSION_ID, group_id=group[0])
        )

    db_session.commit()

    eflomal = EflomalAssessment(
        assessment_id=EFLOMAL_ASSESSMENT_ID,
        source_language="eng",
        target_language="swh",
    )
    db_session.add(eflomal)
    db_session.commit()
    db_session.refresh(eflomal)

    db_session.add_all(
        [
            EflomalDictionary(
                assessment_id=eflomal.id,
                source_word="god",
                target_word="mungu",
                count=100,
                probability=0.95,
            ),
            EflomalDictionary(
                assessment_id=eflomal.id,
                source_word="created",
                target_word="aliumba",
                count=100,
                probability=0.9,
            ),
            EflomalDictionary(
                assessment_id=eflomal.id,
                source_word="world",
                target_word="kosmos",
                count=100,
                probability=0.9,
            ),
            EflomalDictionary(
                assessment_id=eflomal.id,
                source_word="alpha",
                target_word="borderline",
                count=10,
                probability=0.6,
            ),
        ]
    )
    db_session.add_all(
        [
            EflomalTargetWordCount(assessment_id=eflomal.id, word="mungu", count=100),
            EflomalTargetWordCount(assessment_id=eflomal.id, word="kosmos", count=200),
            EflomalTargetWordCount(
                assessment_id=eflomal.id, word="borderline", count=50
            ),
        ]
    )
    db_session.commit()

    return EFLOMAL_ASSESSMENT_ID


@pytest.fixture(scope="module")
def eflomal_fixture(test_db_session):
    return _seed_eflomal_fixture(test_db_session)


def _get(client, token, **params):
    return client.get(
        "/v3/missingwords",
        params=params,
        headers={"Authorization": f"Bearer {token}"},
    )


def test_eflomal_direction_target(client, regular_token1, eflomal_fixture):
    response = _get(
        client,
        regular_token1,
        revision_id=EFLOMAL_REV_ID,
        reference_id=EFLOMAL_REF_ID,
        use_eflomal=True,
        direction="target",
    )
    assert response.status_code == 200, response.text
    body = response.json()
    notes = sorted((r["note"], r["target"][0]["word"]) for r in body["results"])
    assert notes == [("orphan_target", "kosmos")]
    assert body["total_count"] == 1
    only = body["results"][0]
    assert only["vref"] == "GEN 1:1"
    assert "world(100)" in only["source"]
    assert pytest.approx(only["score"], abs=1e-4) == 0.5


def test_eflomal_direction_source(client, regular_token1, eflomal_fixture):
    response = _get(
        client,
        regular_token1,
        revision_id=EFLOMAL_REV_ID,
        reference_id=EFLOMAL_REF_ID,
        use_eflomal=True,
        direction="source",
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["total_count"] == 1
    only = body["results"][0]
    assert only["note"] == "orphan_source"
    assert only["source"] == "created"
    assert "aliumba(100)" in only["target"][0]["word"]
    assert only["vref"] == "GEN 1:1"


def test_eflomal_direction_both(client, regular_token1, eflomal_fixture):
    response = _get(
        client,
        regular_token1,
        revision_id=EFLOMAL_REV_ID,
        reference_id=EFLOMAL_REF_ID,
        use_eflomal=True,
        direction="both",
    )
    assert response.status_code == 200, response.text
    body = response.json()
    notes = sorted(r["note"] for r in body["results"])
    assert notes == ["orphan_source", "orphan_target"]
    assert body["total_count"] == 2


def test_eflomal_threshold_override_admits_borderline(
    client, regular_token1, eflomal_fixture
):
    # min_frequency lowered to 0.1 so the alignment_frequency=0.2 borderline
    # word now passes; we should see kosmos AND borderline.
    response = _get(
        client,
        regular_token1,
        revision_id=EFLOMAL_REV_ID,
        reference_id=EFLOMAL_REF_ID,
        use_eflomal=True,
        direction="target",
        threshold=0.1,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    found = sorted(r["target"][0]["word"] for r in body["results"])
    assert found == ["borderline", "kosmos"]


def test_eflomal_invalid_direction_returns_422(client, regular_token1, eflomal_fixture):
    response = _get(
        client,
        regular_token1,
        revision_id=EFLOMAL_REV_ID,
        reference_id=EFLOMAL_REF_ID,
        use_eflomal=True,
        direction="garbage",
    )
    assert response.status_code == 422


def test_eflomal_missing_assessment_returns_404(
    client, regular_token1, test_db_session
):
    # Use revision IDs that have no eflomal assessment attached.
    response = _get(
        client,
        regular_token1,
        revision_id=999999,
        reference_id=999998,
        use_eflomal=True,
        direction="both",
    )
    assert response.status_code == 404

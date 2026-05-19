import dataclasses
from datetime import date

import pandas as pd
import pytest

from database.models import (
    AlignmentThresholdScores,
    AlignmentTopSourceScores,
    Assessment,
    AssessmentResult,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    Group,
    UserDB,
)


@dataclasses.dataclass(frozen=True)
class AssessmentsDataset:
    """IDs of the rows created by the `assessments_dataset` fixture.

    Tests reference these instead of hardcoding the fixture-file IDs the
    previous setup helper used (115/505/138/772/3863). Auto-incremented
    IDs are robust to whatever state earlier test modules left the
    bible_version / bible_revision / assessment sequences in.
    """

    assessment_id: int
    revision_id: int
    reference_id: int
    revision_version_id: int
    reference_version_id: int


@pytest.fixture(scope="module")
def assessments_dataset(test_db_session):
    """Word-alignment assessment + ~2150 AssessmentResult rows from
    fixtures/assessment_results.json, with auto-incremented IDs.

    Module-scoped so the JSON load only runs once per test module. Tests
    are read-only against this dataset; if a test needs to mutate, it
    should create its own assessment.
    """
    user = test_db_session.query(UserDB).filter(UserDB.username == "testuser1").first()
    group = test_db_session.query(Group).first()

    revision_version = BibleVersion(
        name="Ngoreme",
        iso_language="ngq",
        iso_script="Latn",
        abbreviation="ngq-ngqNT",
        owner_id=user.id if user else None,
    )
    reference_version = BibleVersion(
        name="Swahili Neno (reference)",
        iso_language="swh",
        iso_script="Latn",
        abbreviation="system_swh-ONEN",
        owner_id=user.id if user else None,
    )
    test_db_session.add_all([revision_version, reference_version])
    test_db_session.flush()

    revision = BibleRevision(
        date=date(2023, 1, 23),
        bible_version_id=revision_version.id,
        published=False,
    )
    reference = BibleRevision(
        date=date(2023, 2, 3),
        bible_version_id=reference_version.id,
        published=True,
    )
    test_db_session.add_all([revision, reference])
    test_db_session.flush()

    assessment = Assessment(
        revision_id=revision.id,
        reference_id=reference.id,
        type="word-alignment",
        status="finished",
    )
    test_db_session.add(assessment)
    test_db_session.flush()

    test_db_session.add_all(
        [
            BibleVersionAccess(bible_version_id=revision_version.id, group_id=group.id),
            BibleVersionAccess(
                bible_version_id=reference_version.id, group_id=group.id
            ),
        ]
    )

    # Remap assessment_id; drop fixture row IDs so auto-increment handles
    # them, avoiding any collision with assessment_result_id_seq state.
    result_df = pd.read_json(
        "fixtures/assessment_results.json", orient="records", lines=True
    )
    result_df = result_df.drop(columns=["id"], errors="ignore")
    result_df["assessment_id"] = assessment.id
    for _, row in result_df.iterrows():
        test_db_session.add(AssessmentResult(**row.to_dict()))
    test_db_session.commit()

    return AssessmentsDataset(
        assessment_id=assessment.id,
        revision_id=revision.id,
        reference_id=reference.id,
        revision_version_id=revision_version.id,
        reference_version_id=reference_version.id,
    )


def test_regular_user_flow(client, regular_token1, regular_token2, assessments_dataset):
    first_assessment_id = assessments_dataset.assessment_id
    params = {
        "assessment_id": first_assessment_id,
        "aggregate": "chapter",
        "reverse": False,
    }
    response = client.get(
        "/v3/result",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    # Check that the response is as expected
    assert response.status_code == 200
    response_data = response.json()

    assert response_data["total_count"] > 0
    assert response_data["results"], "No results found in response."
    assert (
        len(response_data["results"]) == response_data["total_count"]
    ), "Results length is not as expected."
    assert response_data["results"][0]["vref"]
    assert response_data["results"][0]["score"] >= 0
    assert response_data["results"][0]["score"] <= 1
    assert response_data["results"][0]["assessment_id"] == first_assessment_id

    # check that second user does not have access
    response = client.get(
        "/v3/result",
        params=params,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403

    # check that user can access the result with reverse
    params["reverse"] = True
    response = client.get(
        "/v3/result",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["results"][0]["vref"]
    assert response_data["results"][0]["score"] >= 0
    assert response_data["results"][0]["score"] <= 1

    # check that user can access the result with aggregate=book
    params["aggregate"] = "book"
    params["reverse"] = False
    response = client.get(
        "/v3/result",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    response_data = response.json()
    assert response_data["results"][0]["vref"]
    assert response_data["results"][0]["score"] >= 0
    assert response_data["results"][0]["score"] <= 1
    assert response_data["results"][0]["assessment_id"] == first_assessment_id

    # check that usec can access alignmentscores
    params = {
        "assessment_id": first_assessment_id,
        "reverse": False,
    }

    response = client.get(
        "/v3/alignmentscores",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200

    # check that second user has no access to alignmentscores
    response = client.get(
        "/v3/alignmentscores",
        params=params,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


@pytest.fixture(scope="module")
def alignment_data(test_db_session, assessments_dataset):
    """AlignmentTopSourceScores rows for the assessments_dataset assessment.

    Realistic word-alignment shape: a handful of source words with one
    or more target translations and scores. Padded with extra rows so
    each well-supported word clears the min_support filter the API
    applies (~20.0 score sum). Returns the dataset for convenience.
    """
    assessment_id = assessments_dataset.assessment_id
    alignment_data = [
        # Word "god" aligns strongly to "dios" and weakly to "señor"
        {
            "assessment_id": assessment_id,
            "source": "god",
            "target": "dios",
            "score": 0.85,
            "vref": None,
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "god",
            "target": "dios",
            "score": 0.9,
            "vref": "GEN 1:2",
            "book": "GEN",
            "chapter": 1,
            "verse": 2,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "god",
            "target": "dios",
            "score": 0.88,
            "vref": "GEN 1:3",
            "book": "GEN",
            "chapter": 1,
            "verse": 3,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "god",
            "target": "señor",
            "score": 0.15,
            "vref": "GEN 1:4",
            "book": "GEN",
            "chapter": 1,
            "verse": 4,
            "flag": False,
            "hide": False,
            "note": None,
        },
        # Word "created" aligns to "creó" with high confidence
        {
            "assessment_id": assessment_id,
            "source": "created",
            "target": "creó",
            "score": 0.92,
            "vref": "GEN 1:1",
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "created",
            "target": "creó",
            "score": 0.89,
            "vref": "GEN 1:5",
            "book": "GEN",
            "chapter": 1,
            "verse": 5,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "created",
            "target": "hizo",
            "score": 0.1,
            "vref": "GEN 1:6",
            "book": "GEN",
            "chapter": 1,
            "verse": 6,
            "flag": False,
            "hide": False,
            "note": None,
        },
        # Word "heaven" with multiple possible translations
        {
            "assessment_id": assessment_id,
            "source": "heaven",
            "target": "cielo",
            "score": 0.75,
            "vref": "GEN 1:1",
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "heaven",
            "target": "cielo",
            "score": 0.8,
            "vref": "GEN 1:7",
            "book": "GEN",
            "chapter": 1,
            "verse": 7,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "heaven",
            "target": "cielos",
            "score": 0.2,
            "vref": "GEN 1:8",
            "book": "GEN",
            "chapter": 1,
            "verse": 8,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "heaven",
            "target": "cielo",
            "score": 0.78,
            "vref": "GEN 1:9",
            "book": "GEN",
            "chapter": 1,
            "verse": 9,
            "flag": False,
            "hide": False,
            "note": None,
        },
        # Word "earth" with good alignment
        {
            "assessment_id": assessment_id,
            "source": "earth",
            "target": "tierra",
            "score": 0.95,
            "vref": "GEN 1:1",
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "earth",
            "target": "tierra",
            "score": 0.93,
            "vref": "GEN 1:10",
            "book": "GEN",
            "chapter": 1,
            "verse": 10,
            "flag": False,
            "hide": False,
            "note": None,
        },
        # Word "beginning" - lower support (fewer occurrences)
        {
            "assessment_id": assessment_id,
            "source": "beginning",
            "target": "principio",
            "score": 0.85,
            "vref": "GEN 1:1",
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "beginning",
            "target": "comienzo",
            "score": 0.15,
            "vref": "GEN 1:1",
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        # Word with very low support - should be filtered out by min_support
        {
            "assessment_id": assessment_id,
            "source": "rare",
            "target": "raro",
            "score": 0.5,
            "vref": "GEN 1:11",
            "book": "GEN",
            "chapter": 1,
            "verse": 11,
            "flag": False,
            "hide": False,
            "note": None,
        },
    ]

    # Add more entries to increase support mass above min_support threshold (20.0)
    # Each word needs total score sum >= 20 to pass the filter
    for verse in range(11, 35):  # Add more verses for words that need higher support
        alignment_data.extend(
            [
                {
                    "assessment_id": assessment_id,
                    "source": "god",
                    "target": "dios",
                    "score": 0.87,
                    "vref": f"GEN 1:{verse}",
                    "book": "GEN",
                    "chapter": 1,
                    "verse": verse,
                    "flag": False,
                    "hide": False,
                    "note": None,
                },
                {
                    "assessment_id": assessment_id,
                    "source": "created",
                    "target": "creó",
                    "score": 0.90,
                    "vref": f"GEN 1:{verse}",
                    "book": "GEN",
                    "chapter": 1,
                    "verse": verse,
                    "flag": False,
                    "hide": False,
                    "note": None,
                },
                {
                    "assessment_id": assessment_id,
                    "source": "earth",
                    "target": "tierra",
                    "score": 0.94,
                    "vref": f"GEN 1:{verse}",
                    "book": "GEN",
                    "chapter": 1,
                    "verse": verse,
                    "flag": False,
                    "hide": False,
                    "note": None,
                },
            ]
        )

    for data in alignment_data:
        data["vref"] = None
        test_db_session.add(AlignmentTopSourceScores(**data))

    test_db_session.commit()
    return assessments_dataset


def test_textalignmentmatches_basic(client, regular_token1, alignment_data):
    """Test basic functionality of the textalignmentmatches endpoint."""
    params = {
        "revision_id": alignment_data.revision_id,
        "reference_id": alignment_data.reference_id,
    }

    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Check basic structure
    assert "results" in response_data
    assert "total_count" in response_data
    assert response_data["total_count"] > 0
    assert len(response_data["results"]) == response_data["total_count"]

    # Check that results are properly structured
    for result in response_data["results"]:
        assert "source_word" in result
        assert "target_word" in result
        assert "rank" in result
        assert "probability" in result
        assert "support_mass" in result
        assert "support_hits" in result
        assert "strength_mass" in result
        assert "strength_margin_mass" in result
        assert "strength_confidence" in result

        # Validate data types and ranges
        assert isinstance(result["source_word"], str)
        assert isinstance(result["target_word"], str)
        assert isinstance(result["rank"], int)
        assert result["rank"] >= 1
        assert result["rank"] <= 3  # default top_k
        assert 0.0 <= result["probability"] <= 1.0
        assert result["support_mass"] >= 0.0
        assert result["support_hits"] >= 0


def test_textalignmentmatches_top_k_parameter(client, regular_token1, alignment_data):
    """Test that the top_k parameter correctly limits results per source word."""
    # Test with top_k=1 (only best match per source)
    params = {
        "revision_id": alignment_data.revision_id,
        "reference_id": alignment_data.reference_id,
        "top_k": 1,
    }

    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # All results should have rank=1
    for result in response_data["results"]:
        assert result["rank"] == 1

    # Test with top_k=2
    params["top_k"] = 2
    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # All results should have rank <= 2
    for result in response_data["results"]:
        assert result["rank"] <= 2


def test_textalignmentmatches_min_support_filter(
    client, regular_token1, alignment_data
):
    """Test that min_support parameter filters out low-support source words."""
    # Test with very high min_support - should get fewer results
    params = {
        "revision_id": alignment_data.revision_id,
        "reference_id": alignment_data.reference_id,
        "min_support": 100.0,  # High threshold
    }

    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # All results should have high support
    for result in response_data["results"]:
        assert result["support_mass"] >= 100.0

    # Test with low min_support - should get more results
    params["min_support"] = 1.0
    response_low = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response_low.status_code == 200
    response_low_data = response_low.json()

    # Should have more or equal results with lower threshold
    assert response_low_data["total_count"] >= response_data["total_count"]


def test_textalignmentmatches_min_probability_filter(
    client, regular_token1, alignment_data
):
    """Test that min_probability parameter filters out low-probability alignments."""
    # Test with high min_probability
    params = {
        "revision_id": alignment_data.revision_id,
        "reference_id": alignment_data.reference_id,
        "min_probability": 0.7,
    }

    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # All results should have probability > 0.7
    for result in response_data["results"]:
        assert result["probability"] > 0.7


def test_textalignmentmatches_authorization(
    client, regular_token1, regular_token2, alignment_data
):
    """Test that authorization is properly enforced."""
    params = {
        "revision_id": alignment_data.revision_id,
        "reference_id": alignment_data.reference_id,
    }

    # First user should have access
    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200

    # Second user should NOT have access (not in the group)
    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_textalignmentmatches_no_assessment_found(
    client, regular_token1, assessments_dataset
):
    """Test that 404 is returned when no matching assessment exists."""

    # Use non-existent revision/reference combination
    params = {
        "revision_id": 99999,
        "reference_id": 99999,
    }

    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 404
    assert "No completed word-alignment assessment found" in response.json()["detail"]


def test_textalignmentmatches_strength_metrics(client, regular_token1, alignment_data):
    """Test that strength metrics are calculated and present in results."""
    params = {
        "revision_id": alignment_data.revision_id,
        "reference_id": alignment_data.reference_id,
    }

    response = client.get(
        "/v3/textalignmentmatches",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Check that strength metrics are present and reasonable
    for result in response_data["results"]:
        # strength_mass should be <= support_mass (since probability <= 1)
        assert result["strength_mass"] <= result["support_mass"]

        # strength_margin_mass should be >= 0 (margin is non-negative)
        assert result["strength_margin_mass"] >= 0

        # For rank 1 results, margin should be larger than for rank 2+
        # (because rank 1 has the highest probability)

        # strength_confidence should be a reasonable value
        assert isinstance(result["strength_confidence"], (int, float))


@pytest.fixture(scope="module")
def threshold_score_data(test_db_session, assessments_dataset):
    """AlignmentThresholdScores rows for the assessments_dataset assessment."""
    assessment_id = assessments_dataset.assessment_id
    threshold_rows = [
        {
            "assessment_id": assessment_id,
            "source": "god",
            "target": "dios",
            "score": 0.55,
            "vref": None,
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
            "flag": False,
            "hide": False,
            "note": None,
        },
        {
            "assessment_id": assessment_id,
            "source": "earth",
            "target": "tierra",
            "score": 0.62,
            "vref": None,
            "book": "GEN",
            "chapter": 1,
            "verse": 2,
            "flag": False,
            "hide": False,
            "note": None,
        },
    ]
    for row in threshold_rows:
        test_db_session.add(AlignmentThresholdScores(**row))
    test_db_session.commit()
    return assessments_dataset


def test_alignmentscores_score_type(
    client,
    regular_token1,
    regular_token2,
    test_db_session,
    alignment_data,
    threshold_score_data,
):
    """`/v3/alignmentscores` returns top-source rows by default and threshold rows
    when score_type=threshold; book/chapter/verse filters, pagination, and auth
    apply uniformly to both tables."""
    first_assessment_id = alignment_data.assessment_id

    top_count = (
        test_db_session.query(AlignmentTopSourceScores)
        .filter(AlignmentTopSourceScores.assessment_id == first_assessment_id)
        .count()
    )
    threshold_count = (
        test_db_session.query(AlignmentThresholdScores)
        .filter(AlignmentThresholdScores.assessment_id == first_assessment_id)
        .count()
    )
    # Sanity check that the two tables have distinct row counts so we can tell
    # which one the route read from.
    assert top_count > threshold_count
    assert threshold_count == 2

    headers = {"Authorization": f"Bearer {regular_token1}"}

    # Default (no score_type) → top-source scores
    response = client.get(
        "/v3/alignmentscores",
        params={"assessment_id": first_assessment_id},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json()["total_count"] == top_count

    # Explicit score_type=top → top-source scores
    response = client.get(
        "/v3/alignmentscores",
        params={"assessment_id": first_assessment_id, "score_type": "top"},
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json()["total_count"] == top_count

    # score_type=threshold → threshold scores
    response = client.get(
        "/v3/alignmentscores",
        params={"assessment_id": first_assessment_id, "score_type": "threshold"},
        headers=headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total_count"] == threshold_count
    sources = sorted(r["source"] for r in body["results"])
    assert sources == ["earth", "god"]

    # score_type=threshold + book/chapter filter → both threshold rows are GEN 1
    response = client.get(
        "/v3/alignmentscores",
        params={
            "assessment_id": first_assessment_id,
            "score_type": "threshold",
            "book": "GEN",
            "chapter": 1,
        },
        headers=headers,
    )
    assert response.status_code == 200
    assert response.json()["total_count"] == 2

    # score_type=threshold + book/chapter/verse filter → only the verse-1 row
    response = client.get(
        "/v3/alignmentscores",
        params={
            "assessment_id": first_assessment_id,
            "score_type": "threshold",
            "book": "GEN",
            "chapter": 1,
            "verse": 1,
        },
        headers=headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total_count"] == 1
    assert body["results"][0]["source"] == "god"

    # score_type=threshold + pagination → page_size=1 returns 1 row, total still 2
    response = client.get(
        "/v3/alignmentscores",
        params={
            "assessment_id": first_assessment_id,
            "score_type": "threshold",
            "page": 1,
            "page_size": 1,
        },
        headers=headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total_count"] == threshold_count
    assert len(body["results"]) == 1

    # score_type=threshold + unauthorized user → 403 (same as default path)
    response = client.get(
        "/v3/alignmentscores",
        params={"assessment_id": first_assessment_id, "score_type": "threshold"},
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403

    # Invalid score_type → 422
    response = client.get(
        "/v3/alignmentscores",
        params={"assessment_id": first_assessment_id, "score_type": "bogus"},
        headers=headers,
    )
    assert response.status_code == 422


@dataclasses.dataclass(frozen=True)
class TextLengthsDataset:
    """IDs created by the `text_lengths_data` fixture."""

    revision_id: int
    reference_id: int
    revision_assessment_id: int
    reference_assessment_id: int


@pytest.fixture(scope="module")
def text_lengths_data(test_db_session, assessments_dataset):
    """Two text-lengths Assessment rows (one per revision) plus
    TextLengthsTable rows with zero values arranged to exercise verse
    range merging in /v3/compare_text_lengths.
    """
    from database.models import TextLengthsTable

    revision_assessment = Assessment(
        revision_id=assessments_dataset.revision_id,
        reference_id=None,
        type="text-lengths",
        status="finished",
        assessment_version="1",
    )
    reference_assessment = Assessment(
        revision_id=assessments_dataset.reference_id,
        reference_id=None,
        type="text-lengths",
        status="finished",
        assessment_version="1",
    )
    test_db_session.add_all([revision_assessment, reference_assessment])
    test_db_session.flush()

    rev_aid = revision_assessment.id
    ref_aid = reference_assessment.id

    # Create text lengths data with some zero values to test verse range merging
    # Note: Revision and reference have zeros in DIFFERENT places to test realistic scenarios
    # Revision data (GAL 1:1-10)
    revision_data = [
        {
            "assessment_id": rev_aid,
            "vref": "GAL 1:1",
            "word_lengths": 10,
            "char_lengths": 50,
            "word_lengths_z": 0.5,
            "char_lengths_z": 0.3,
        },
        {
            "assessment_id": rev_aid,
            "vref": "GAL 1:2",
            "word_lengths": 0,
            "char_lengths": 0,
            "word_lengths_z": 0.0,
            "char_lengths_z": 0.0,
        },  # Zero in revision
        {
            "assessment_id": rev_aid,
            "vref": "GAL 1:3",
            "word_lengths": 0,
            "char_lengths": 0,
            "word_lengths_z": 0.0,
            "char_lengths_z": 0.0,
        },  # Zero in revision
        {
            "assessment_id": rev_aid,
            "vref": "GAL 1:4",
            "word_lengths": 15,
            "char_lengths": 75,
            "word_lengths_z": 1.0,
            "char_lengths_z": 0.8,
        },
        {
            "assessment_id": rev_aid,
            "vref": "GAL 1:5",
            "word_lengths": 12,
            "char_lengths": 60,
            "word_lengths_z": 0.7,
            "char_lengths_z": 0.5,
        },
        {
            "assessment_id": rev_aid,
            "vref": "GAL 1:6",
            "word_lengths": 8,
            "char_lengths": 40,
            "word_lengths_z": 0.2,
            "char_lengths_z": 0.1,
        },
        {
            "assessment_id": rev_aid,
            "vref": "GAL 1:7",
            "word_lengths": 0,
            "char_lengths": 0,
            "word_lengths_z": 0.0,
            "char_lengths_z": 0.0,
        },  # Zero in revision
        {
            "assessment_id": rev_aid,
            "vref": "GAL 1:8",
            "word_lengths": 14,
            "char_lengths": 70,
            "word_lengths_z": 0.9,
            "char_lengths_z": 0.7,
        },
        {
            "assessment_id": rev_aid,
            "vref": "GAL 1:9",
            "word_lengths": 11,
            "char_lengths": 55,
            "word_lengths_z": 0.6,
            "char_lengths_z": 0.4,
        },
        {
            "assessment_id": rev_aid,
            "vref": "GAL 1:10",
            "word_lengths": 13,
            "char_lengths": 65,
            "word_lengths_z": 0.8,
            "char_lengths_z": 0.6,
        },
    ]

    # Reference data (GAL 1:1-10) - zeros in DIFFERENT verses than revision
    reference_data = [
        {
            "assessment_id": ref_aid,
            "vref": "GAL 1:1",
            "word_lengths": 9,
            "char_lengths": 45,
            "word_lengths_z": 0.4,
            "char_lengths_z": 0.2,
        },
        {
            "assessment_id": ref_aid,
            "vref": "GAL 1:2",
            "word_lengths": 8,
            "char_lengths": 40,
            "word_lengths_z": 0.3,
            "char_lengths_z": 0.1,
        },  # Non-zero in reference
        {
            "assessment_id": ref_aid,
            "vref": "GAL 1:3",
            "word_lengths": 0,
            "char_lengths": 0,
            "word_lengths_z": 0.0,
            "char_lengths_z": 0.0,
        },  # Zero in reference (and in revision)
        {
            "assessment_id": ref_aid,
            "vref": "GAL 1:4",
            "word_lengths": 14,
            "char_lengths": 70,
            "word_lengths_z": 0.9,
            "char_lengths_z": 0.7,
        },
        {
            "assessment_id": ref_aid,
            "vref": "GAL 1:5",
            "word_lengths": 0,
            "char_lengths": 0,
            "word_lengths_z": 0.0,
            "char_lengths_z": 0.0,
        },  # Zero in reference (not in revision)
        {
            "assessment_id": ref_aid,
            "vref": "GAL 1:6",
            "word_lengths": 7,
            "char_lengths": 35,
            "word_lengths_z": 0.1,
            "char_lengths_z": 0.0,
        },
        {
            "assessment_id": ref_aid,
            "vref": "GAL 1:7",
            "word_lengths": 9,
            "char_lengths": 45,
            "word_lengths_z": 0.4,
            "char_lengths_z": 0.2,
        },  # Non-zero in reference
        {
            "assessment_id": ref_aid,
            "vref": "GAL 1:8",
            "word_lengths": 13,
            "char_lengths": 65,
            "word_lengths_z": 0.8,
            "char_lengths_z": 0.6,
        },
        {
            "assessment_id": ref_aid,
            "vref": "GAL 1:9",
            "word_lengths": 0,
            "char_lengths": 0,
            "word_lengths_z": 0.0,
            "char_lengths_z": 0.0,
        },  # Zero in reference (not in revision)
        {
            "assessment_id": ref_aid,
            "vref": "GAL 1:10",
            "word_lengths": 12,
            "char_lengths": 60,
            "word_lengths_z": 0.7,
            "char_lengths_z": 0.5,
        },
    ]

    # Add EPH data for book aggregation testing
    revision_data.extend(
        [
            {
                "assessment_id": rev_aid,
                "vref": "EPH 1:1",
                "word_lengths": 9,
                "char_lengths": 45,
                "word_lengths_z": 0.4,
                "char_lengths_z": 0.2,
            },
            {
                "assessment_id": rev_aid,
                "vref": "EPH 1:2",
                "word_lengths": 10,
                "char_lengths": 50,
                "word_lengths_z": 0.5,
                "char_lengths_z": 0.3,
            },
        ]
    )

    reference_data.extend(
        [
            {
                "assessment_id": ref_aid,
                "vref": "EPH 1:1",
                "word_lengths": 8,
                "char_lengths": 40,
                "word_lengths_z": 0.3,
                "char_lengths_z": 0.1,
            },
            {
                "assessment_id": ref_aid,
                "vref": "EPH 1:2",
                "word_lengths": 9,
                "char_lengths": 45,
                "word_lengths_z": 0.4,
                "char_lengths_z": 0.2,
            },
        ]
    )

    for data in revision_data:
        test_db_session.add(TextLengthsTable(**data))

    for data in reference_data:
        test_db_session.add(TextLengthsTable(**data))

    test_db_session.commit()

    return TextLengthsDataset(
        revision_id=assessments_dataset.revision_id,
        reference_id=assessments_dataset.reference_id,
        revision_assessment_id=rev_aid,
        reference_assessment_id=ref_aid,
    )


def test_compare_text_lengths_basic(client, regular_token1, text_lengths_data):
    """Test basic functionality of compare_text_lengths endpoint."""

    params = {
        "revision_id": text_lengths_data.revision_id,
        "reference_id": text_lengths_data.reference_id,
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Check basic structure
    assert "results" in response_data
    assert "total_count" in response_data
    assert response_data["total_count"] > 0
    assert len(response_data["results"]) > 0

    # Check that results contain differences
    for result in response_data["results"]:
        assert "vref" in result
        assert "vrefs" in result
        assert "word_lengths" in result  # This is the difference
        assert "char_lengths" in result  # This is the difference
        assert "word_lengths_z" in result  # Z-score of the difference
        assert "char_lengths_z" in result  # Z-score of the difference
        assert "assessment_id" in result


def test_compare_text_lengths_verse_range_merging(
    client, regular_token1, text_lengths_data
):
    """Test that zero values trigger verse range merging and summing.

    Note: The revision has zeros at verses 2, 3, 7
    The reference has zeros at verses 3, 5, 9

    After merging:
    - 1:1-3 (1:1 + 1:2 + 1:3), 1:4-5 (1:4 + 1:5), 1:6-7 (1:6 + 1:7), 1:8-9 (1:8 + 1:9)

    The final comparison will show merged ranges where either has zeros.
    """

    params = {
        "revision_id": text_lengths_data.revision_id,
        "reference_id": text_lengths_data.reference_id,
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Check that we have results with vrefs (merged verses should have multiple vrefs)
    for result in response_data["results"]:
        assert "vrefs" in result and "vref" in result

    assert response_data["results"][0]["vref"] == "GAL 1:1"
    assert response_data["results"][0]["vrefs"] == ["GAL 1:1", "GAL 1:2", "GAL 1:3"]
    assert response_data["results"][0]["word_lengths"] == (10 + 0 + 0) - (9 + 8 + 0)
    assert response_data["results"][0]["char_lengths"] == (50 + 0 + 0) - (45 + 40 + 0)
    assert response_data["results"][0]["word_lengths_z"] == pytest.approx(
        -1.0782472811532828, abs=0.001
    )
    assert response_data["results"][0]["char_lengths_z"] == pytest.approx(
        -1.0782472811532833, abs=0.001
    )

    assert response_data["results"][1]["vref"] == "GAL 1:4"
    assert response_data["results"][1]["vrefs"] == ["GAL 1:4", "GAL 1:5"]
    assert response_data["results"][1]["word_lengths"] == (15 + 12) - (14 + 0)
    assert response_data["results"][1]["char_lengths"] == (75 + 60) - (70 + 0)
    assert response_data["results"][1]["word_lengths_z"] == pytest.approx(
        1.3565046440315498, abs=0.001
    )
    assert response_data["results"][1]["char_lengths_z"] == pytest.approx(
        1.3565046440315496, abs=0.001
    )

    assert response_data["results"][2]["vref"] == "GAL 1:6"
    assert response_data["results"][2]["vrefs"] == ["GAL 1:6", "GAL 1:7"]
    assert response_data["results"][2]["word_lengths"] == (8 + 0) - (7 + 9)
    assert response_data["results"][2]["char_lengths"] == (40 + 0) - (35 + 45)
    assert response_data["results"][2]["word_lengths_z"] == pytest.approx(
        -1.199984877412525, abs=0.001
    )
    assert response_data["results"][2]["char_lengths_z"] == pytest.approx(
        -1.1999848774125246, abs=0.001
    )

    assert response_data["results"][3]["vref"] == "GAL 1:8"
    assert response_data["results"][3]["vrefs"] == ["GAL 1:8", "GAL 1:9"]
    assert response_data["results"][3]["word_lengths"] == (14 + 11) - (13 + 0)
    assert response_data["results"][3]["char_lengths"] == (70 + 55) - (65 + 0)
    assert response_data["results"][3]["word_lengths_z"] == pytest.approx(
        1.234767047772308, abs=0.001
    )
    assert response_data["results"][3]["char_lengths_z"] == pytest.approx(
        1.2347670477723078, abs=0.001
    )


def test_compare_text_lengths_book_filter(client, regular_token1, text_lengths_data):
    """Test filtering results by book."""

    # Test with GAL book
    params = {
        "revision_id": text_lengths_data.revision_id,
        "reference_id": text_lengths_data.reference_id,
        "book": "GAL",
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # All results should be from GAL
    for result in response_data["results"]:
        vref = result.get("vref") or (
            result.get("vrefs")[0] if result.get("vrefs") else None
        )
        if vref:
            assert vref.startswith("GAL"), f"Expected GAL verse, got {vref}"

    # Test with EPH book
    params["book"] = "EPH"
    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # All results should be from EPH
    for result in response_data["results"]:
        vref = result.get("vref") or (
            result.get("vrefs")[0] if result.get("vrefs") else None
        )
        if vref:
            assert vref.startswith("EPH"), f"Expected EPH verse, got {vref}"


def test_compare_text_lengths_chapter_aggregation(
    client, regular_token1, text_lengths_data
):
    """Test chapter-level aggregation."""

    params = {
        "revision_id": text_lengths_data.revision_id,
        "reference_id": text_lengths_data.reference_id,
        "aggregate": "chapter",
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Results should be aggregated by chapter
    chapter_vrefs = set()
    for result in response_data["results"]:
        vref = result.get("vref")
        if vref:
            # Chapter aggregation should return "BOOK CHAPTER" format (e.g., "GAL 1")
            assert vref.count(" ") == 1, f"Expected chapter-level vref, got {vref}"
            assert (
                ":" not in vref
            ), f"Expected no verse number in chapter aggregation, got {vref}"
            chapter_vrefs.add(vref)

    # Should have at least GAL 1 and EPH 1
    assert len(chapter_vrefs) >= 2


def test_compare_text_lengths_book_aggregation(
    client, regular_token1, text_lengths_data
):
    """Test book-level aggregation."""

    params = {
        "revision_id": text_lengths_data.revision_id,
        "reference_id": text_lengths_data.reference_id,
        "aggregate": "book",
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Results should be aggregated by book
    book_vrefs = set()
    for result in response_data["results"]:
        vref = result.get("vref")
        if vref:
            # Book aggregation should return just the book name (e.g., "GAL")
            assert " " not in vref, f"Expected book-level vref, got {vref}"
            book_vrefs.add(vref)

    # Should have at least GAL and EPH
    assert "GAL" in book_vrefs
    assert "EPH" in book_vrefs


def test_compare_text_lengths_text_aggregation(
    client, regular_token1, text_lengths_data
):
    """Test text-level aggregation (entire text)."""

    params = {
        "revision_id": text_lengths_data.revision_id,
        "reference_id": text_lengths_data.reference_id,
        "aggregate": "text",
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Should return only one result for the entire text
    assert response_data["total_count"] == 1
    assert len(response_data["results"]) == 1

    result = response_data["results"][0]
    # Text aggregation should have None for vref
    assert result.get("vref") is None


def test_compare_text_lengths_pagination(client, regular_token1, text_lengths_data):
    """Test pagination of results."""

    # Get first page
    params = {
        "revision_id": text_lengths_data.revision_id,
        "reference_id": text_lengths_data.reference_id,
        "page": 1,
        "page_size": 3,
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    total_count = response_data["total_count"]
    assert len(response_data["results"]) <= 3
    first_page_results = response_data["results"]

    # Get second page if there are enough results
    if total_count > 3:
        params["page"] = 2
        response = client.get(
            "/v3/compare_text_lengths",
            params=params,
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

        assert response.status_code == 200
        second_page_data = response.json()

        # Second page should have different results
        assert second_page_data["results"] != first_page_results
        assert second_page_data["total_count"] == total_count


def test_compare_text_lengths_authorization(
    client, regular_token1, regular_token2, text_lengths_data
):
    """Test that authorization is properly enforced."""

    params = {
        "revision_id": text_lengths_data.revision_id,
        "reference_id": text_lengths_data.reference_id,
    }

    # First user should have access
    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200

    # Second user should NOT have access
    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_compare_text_lengths_no_assessment_found(
    client, regular_token1, assessments_dataset
):
    """Test that 404 is returned when no matching assessment exists."""

    # Use non-existent revision/reference combination
    params = {
        "revision_id": 99999,
        "reference_id": 99999,
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 404
    assert "No completed text-lengths assessment found" in response.json()["detail"]


def test_compare_text_lengths_difference_calculation(
    client, regular_token1, text_lengths_data
):
    """Test that differences are correctly calculated (revision - reference)."""

    params = {
        "revision_id": text_lengths_data.revision_id,
        "reference_id": text_lengths_data.reference_id,
        "book": "GAL",
        "chapter": 1,
        "verse": 4,  # A verse with no zeros
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    assert len(response_data["results"]) > 0
    result = response_data["results"][0]

    # GAL 1:4 has revision: word_lengths=15, char_lengths=75
    # GAL 1:4 has reference: word_lengths=14, char_lengths=70
    # Difference should be: word_lengths=1, char_lengths=5
    assert result["word_lengths"] == 1.0
    assert result["char_lengths"] == 5.0

    # Z-scores should be calculated
    assert "word_lengths_z" in result
    assert "char_lengths_z" in result


def test_compare_text_lengths_with_assessment_ids(
    client, regular_token1, text_lengths_data
):
    """Test compare_text_lengths with assessment IDs instead of revision IDs."""

    # Use assessment IDs directly (skip the revision/reference lookup path).
    params = {
        "revision_assessment_id": text_lengths_data.revision_assessment_id,
        "reference_assessment_id": text_lengths_data.reference_assessment_id,
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200
    response_data = response.json()

    # Check basic structure
    assert "results" in response_data
    assert "total_count" in response_data
    assert response_data["total_count"] > 0
    assert len(response_data["results"]) > 0

    # Check that results contain differences
    for result in response_data["results"]:
        assert "vref" in result
        assert "vrefs" in result
        assert "word_lengths" in result
        assert "char_lengths" in result
        assert "word_lengths_z" in result
        assert "char_lengths_z" in result
        assert "assessment_id" in result


def test_compare_text_lengths_mixed_id_types_error(
    client, regular_token1, text_lengths_data
):
    """Test that providing both revision IDs and assessment IDs raises an error."""

    params = {
        "revision_id": text_lengths_data.revision_id,
        "reference_id": text_lengths_data.reference_id,
        "revision_assessment_id": text_lengths_data.revision_assessment_id,
        "reference_assessment_id": text_lengths_data.reference_assessment_id,
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "Cannot provide both" in response.json()["detail"]


def test_compare_text_lengths_incomplete_revision_pair_error(
    client, regular_token1, text_lengths_data
):
    """Test that providing only one ID from the revision pair raises an error."""

    # Test with only revision_id (no reference_id)
    params = {
        "revision_id": text_lengths_data.revision_id,
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    # The first check catches incomplete pairs
    assert "Must provide either" in response.json()["detail"]


def test_compare_text_lengths_incomplete_assessment_pair_error(
    client, regular_token1, text_lengths_data
):
    """Test that providing only one ID from the assessment pair raises an error."""

    # Test with only revision_assessment_id (no reference_assessment_id)
    params = {
        "revision_assessment_id": text_lengths_data.revision_assessment_id,
    }

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "Must provide either" in response.json()["detail"]


def test_compare_text_lengths_no_ids_error(client, regular_token1, assessments_dataset):
    """Test that providing no IDs raises an error."""

    params = {}

    response = client.get(
        "/v3/compare_text_lengths",
        params=params,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 400
    assert "Must provide either" in response.json()["detail"]


# ----- /v3/ngrams_result --------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_ngrams_total_count_cache():
    """Reset the per-process total_count cache before every test in this
    module. Several ngrams_result tests share a finished assessment via
    `_setup_ngrams_assessment`, and warming the cache in one test would
    otherwise hide newly-inserted rows from a later test. Cheap to run
    everywhere — non-ngrams tests just see an empty dict."""
    from assessment_routes.v3.results_query_routes import (
        _ngrams_total_count_cache,
    )

    _ngrams_total_count_cache.clear()
    yield
    _ngrams_total_count_cache.clear()


_NGRAMS_SEEDS = [
    ("the lord", 2, ["GEN 1:1", "GEN 1:2"]),
    ("of god", 2, ["GEN 1:3"]),
    ("son of man", 3, ["MAT 8:20", "MAT 9:6", "MAT 10:23"]),
    ("in the beginning", 3, ["GEN 1:1"]),
    ("light of the world", 4, ["JHN 8:12"]),
    ("kingdom of heaven", 3, ["MAT 4:17", "MAT 5:3"]),
    ("bread of life", 3, ["JHN 6:35"]),
    ("good shepherd", 2, ["JHN 10:11"]),
    ("alpha and omega", 3, ["REV 1:8", "REV 22:13"]),
    ("the way", 2, ["JHN 14:6"]),
    ("the truth", 2, ["JHN 14:6"]),
    ("the life", 2, ["JHN 14:6"]),
]


@pytest.fixture(scope="module")
def ngrams_dataset(test_db_session, assessments_dataset):
    """A finished ngrams Assessment with 12 ngrams (each with 1–3 vrefs)
    — small but enough rows to exercise multi-page pagination at
    page_size=5. Returns (assessment_id, seeds)."""
    from database.models import NgramsTable, NgramVrefTable

    assessment = Assessment(
        revision_id=assessments_dataset.revision_id,
        reference_id=assessments_dataset.reference_id,
        type="ngrams",
        status="finished",
        assessment_version="1",
    )
    test_db_session.add(assessment)
    test_db_session.flush()

    for ngram, size, vrefs in _NGRAMS_SEEDS:
        ng = NgramsTable(assessment_id=assessment.id, ngram=ngram, ngram_size=size)
        test_db_session.add(ng)
        test_db_session.flush()
        for v in vrefs:
            test_db_session.add(NgramVrefTable(ngram_id=ng.id, vref=v))
    test_db_session.commit()
    return assessment.id, list(_NGRAMS_SEEDS)


def test_ngrams_result_unpaginated_returns_all(client, regular_token1, ngrams_dataset):
    """No page params → returns every ngram in the assessment with full
    vref lists, ordered by ngram id."""
    assessment_id, seeds = ngrams_dataset

    response = client.get(
        "/v3/ngrams_result",
        params={"assessment_id": assessment_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["total_count"] == len(seeds)
    assert len(body["results"]) == len(seeds)
    # Insertion order = id order, since auto-increment assigns ids in that order.
    for got, (ngram, size, vrefs) in zip(body["results"], seeds):
        assert got["ngram"] == ngram
        assert got["ngram_size"] == size
        # Order of vrefs within a single ngram isn't guaranteed by the
        # query (we don't sort the vref lookup), so compare as sets.
        assert set(got["vrefs"]) == set(vrefs)


def test_ngrams_result_pagination_covers_corpus_without_overlap(
    client, regular_token1, ngrams_dataset
):
    """Walk the assessment a page at a time and assert: every ngram
    appears exactly once, in id order, and each page <= page_size. This
    is the regression guard for the two-step pagination — if the leaf
    pagination ever drifts out of sync with the vref join, ngrams would
    silently duplicate, vanish, or get the wrong vrefs."""
    assessment_id, seeds = ngrams_dataset

    page_size = 5
    seen_ngrams: list[str] = []
    seen_ids: list[int] = []
    seen_vrefs_by_ngram: dict[str, set] = {}

    page = 1
    while True:
        response = client.get(
            "/v3/ngrams_result",
            params={
                "assessment_id": assessment_id,
                "page": page,
                "page_size": page_size,
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["total_count"] == len(seeds)
        assert len(body["results"]) <= page_size
        if not body["results"]:
            break
        for r in body["results"]:
            seen_ngrams.append(r["ngram"])
            seen_ids.append(r["id"])
            seen_vrefs_by_ngram[r["ngram"]] = set(r["vrefs"])
        page += 1

    assert sorted(seen_ngrams) == sorted(n for n, _, _ in seeds)
    assert len(seen_ids) == len(seeds), "duplicate or missing ngrams across pages"
    assert seen_ids == sorted(seen_ids), "ngrams not returned in id order"

    # Each ngram's vrefs must match what we seeded — joining with the
    # vref table after pagination must not lose or merge vrefs across
    # ngrams.
    for ngram, _, vrefs in seeds:
        assert seen_vrefs_by_ngram[ngram] == set(vrefs), ngram


def test_ngrams_result_unauthorized_assessment_returns_403(
    client, regular_token2, ngrams_dataset
):
    """Users outside the assessment's group can't read its ngrams."""
    assessment_id, _ = ngrams_dataset

    response = client.get(
        "/v3/ngrams_result",
        params={"assessment_id": assessment_id},
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


@pytest.mark.parametrize(
    "params",
    [
        {"page": 2},  # page without page_size
        {"page_size": 5},  # page_size without page
    ],
    ids=["page_without_size", "size_without_page"],
)
def test_ngrams_result_partial_pagination_args_rejected(
    client, regular_token1, ngrams_dataset, params
):
    """`page` and `page_size` must be provided together. Without this
    check, supplying `page=2` alone silently bypassed the offset/limit
    branch in fetch_ngrams_page and returned the entire table."""
    assessment_id, _ = ngrams_dataset

    response = client.get(
        "/v3/ngrams_result",
        params={"assessment_id": assessment_id, **params},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 400
    assert "page" in response.json()["detail"].lower()


@pytest.mark.parametrize(
    "params,reason",
    [
        ({"page": 0, "page_size": 5}, "page=0 would compute negative OFFSET"),
        ({"page": -1, "page_size": 5}, "page negative"),
        ({"page": 1, "page_size": 0}, "page_size=0 is meaningless"),
        ({"page": 1, "page_size": -5}, "page_size negative"),
        ({"page": 1, "page_size": 10001}, "page_size above 10k cap"),
    ],
    ids=[
        "page_zero",
        "page_negative",
        "size_zero",
        "size_negative",
        "size_above_cap",
    ],
)
def test_ngrams_result_pagination_bounds_rejected(
    client, regular_token1, ngrams_dataset, params, reason
):
    """page>=1, page_size in [1, 10000] are enforced at the FastAPI
    boundary so a bad client can't (a) crash the query with a
    negative OFFSET (Postgres rejects it → 500) or (b) blow up the
    vref-fetch IN-list with an unbounded page_size."""
    assessment_id, _ = ngrams_dataset

    response = client.get(
        "/v3/ngrams_result",
        params={"assessment_id": assessment_id, **params},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 422, reason


def test_ngrams_result_includes_vrefless_ngram(
    client, regular_token1, test_db_session, ngrams_dataset
):
    """A ngram with no rows in ngram_vref_table now appears in results
    with `vrefs=[]` instead of being silently dropped (the old INNER
    JOIN behaviour). Pinned because the docstring on fetch_ngrams_page
    promises this contract — switching back to INNER JOIN would be a
    silent regression that callers couldn't easily notice."""
    from database.models import NgramsTable

    assessment_id, seeds = ngrams_dataset

    # Add a vrefless ngram. The schema permits it (no NOT NULL on the
    # vref relationship from this side; ngrams_table doesn't require
    # at-least-one ngram_vref_table row).
    orphan = NgramsTable(
        assessment_id=assessment_id,
        ngram="orphan_ngram_with_no_vrefs",
        ngram_size=4,
    )
    test_db_session.add(orphan)
    test_db_session.commit()

    response = client.get(
        "/v3/ngrams_result",
        params={"assessment_id": assessment_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200, response.text
    body = response.json()

    by_ngram = {r["ngram"]: r for r in body["results"]}
    assert "orphan_ngram_with_no_vrefs" in by_ngram
    assert by_ngram["orphan_ngram_with_no_vrefs"]["vrefs"] == []
    # And total_count includes the orphan, so len(results) == total_count
    # in the unpaginated case — the inconsistency the old INNER JOIN
    # produced is gone.
    assert body["total_count"] == len(seeds) + 1
    assert len(body["results"]) == body["total_count"]


def test_ngrams_result_caches_total_count_for_finished_assessment(
    client, regular_token1, test_db_session, ngrams_dataset
):
    """For a `status='finished'` assessment, total_count is memoized
    per-worker by assessment_id (see #651). Once the cache is warm,
    rows added behind the cache's back stay hidden from total_count
    until the entry is invalidated — which is the intended behaviour
    because counts on finished assessments are immutable by contract
    (a rerun produces a new assessment row, not new rows on the old
    one)."""
    from assessment_routes.v3.results_query_routes import (
        _ngrams_total_count_cache,
    )
    from database.models import NgramsTable

    assessment_id, _ = ngrams_dataset

    # Prime the cache.
    primed = client.get(
        "/v3/ngrams_result",
        params={"assessment_id": assessment_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert primed.status_code == 200, primed.text
    primed_count = primed.json()["total_count"]
    assert assessment_id in _ngrams_total_count_cache

    # Insert a row behind the cache's back.
    test_db_session.add(
        NgramsTable(
            assessment_id=assessment_id,
            ngram="cache_probe_ngram",
            ngram_size=2,
        )
    )
    test_db_session.commit()

    # Cached count should still come back unchanged.
    cached = client.get(
        "/v3/ngrams_result",
        params={"assessment_id": assessment_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert cached.status_code == 200, cached.text
    assert cached.json()["total_count"] == primed_count

    # Invalidating the entry forces a fresh COUNT and now reflects the
    # newly-added row.
    _ngrams_total_count_cache.pop(assessment_id, None)
    refreshed = client.get(
        "/v3/ngrams_result",
        params={"assessment_id": assessment_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert refreshed.status_code == 200, refreshed.text
    assert refreshed.json()["total_count"] == primed_count + 1


def test_ngrams_result_missing_assessment_returns_404_for_admin(
    client, admin_token, assessments_dataset
):
    """Admins are authorized for every assessment without an existence
    check (`is_user_authorized_for_assessment` short-circuits True),
    so a request for a nonexistent assessment_id has to be handled by
    the count helper itself — otherwise the missing row would raise
    NoResultFound and surface as a 500."""

    # Pick an id far above any seeded assessment.
    missing_id = 9_999_999

    response = client.get(
        "/v3/ngrams_result",
        params={"assessment_id": missing_id},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == 404, response.text
    assert str(missing_id) in response.json()["detail"]


def test_ngrams_result_does_not_cache_in_progress_assessment(
    client, regular_token1, test_db_session, assessments_dataset
):
    """Counts for non-finished assessments must not be memoized — an
    in-progress assessment can still grow rows, and serving a stale
    count would confuse pagination during a live training run."""
    from assessment_routes.v3.results_query_routes import (
        _ngrams_total_count_cache,
    )
    from database.models import NgramsTable

    in_progress = Assessment(
        revision_id=assessments_dataset.revision_id,
        reference_id=assessments_dataset.reference_id,
        type="ngrams",
        status="queued",
        assessment_version="1",
    )
    test_db_session.add(in_progress)
    test_db_session.commit()
    test_db_session.refresh(in_progress)
    test_db_session.add(
        NgramsTable(
            assessment_id=in_progress.id, ngram="probe_in_progress", ngram_size=1
        )
    )
    test_db_session.commit()

    response = client.get(
        "/v3/ngrams_result",
        params={"assessment_id": in_progress.id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["total_count"] == 1
    # Must not have been memoized — only finished assessments are cached.
    assert in_progress.id not in _ngrams_total_count_cache

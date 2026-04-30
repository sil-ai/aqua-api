import pytest

from database.models import (
    AlignmentThresholdScores,
    AlignmentTopSourceScores,
    Assessment,
    AssessmentResult,
    TextLengthsTable,
    TfidfPcaVector,
)

prefix = "v3"


@pytest.fixture(scope="module")
def push_assessment_id(test_db_session, test_revision_id, test_revision_id_2):
    """Create an assessment for push results tests."""
    assessment = Assessment(
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
        type="sentence-length",
        status="running",
    )
    test_db_session.add(assessment)
    test_db_session.commit()
    test_db_session.refresh(assessment)
    return assessment.id


# ---------------------------------------------------------------------------
# POST /assessment/{id}/results
# ---------------------------------------------------------------------------


def test_push_results(client, regular_token1, push_assessment_id, test_db_session):
    body = [
        {
            "vref": "GEN 1:1",
            "score": 0.95,
            "flag": False,
            "source": "In the beginning",
            "target": "Hapo mwanzo",
        },
        {
            "vref": "GEN 1:2",
            "score": 0.80,
        },
    ]
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/results",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["ids"] == []
    # Response no longer carries IDs, so confirm rows landed via the DB.
    test_db_session.expire_all()
    persisted = (
        test_db_session.query(AssessmentResult)
        .filter(
            AssessmentResult.assessment_id == push_assessment_id,
            AssessmentResult.vref.in_(["GEN 1:1", "GEN 1:2"]),
        )
        .count()
    )
    assert persisted == 2


def test_push_results_empty_body(client, regular_token1, push_assessment_id):
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/results",
        json=[],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["ids"] == []


def test_push_results_nonexistent_assessment(client, regular_token1):
    response = client.post(
        f"{prefix}/assessment/999999/results",
        json=[{"vref": "GEN 1:1", "score": 0.5}],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_push_results_unauthorized(client, regular_token2, push_assessment_id):
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/results",
        json=[{"vref": "GEN 1:1", "score": 0.5}],
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_push_results_no_auth(client, push_assessment_id):
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/results",
        json=[{"vref": "GEN 1:1", "score": 0.5}],
    )
    assert response.status_code == 401


def test_push_results_invalid_vref(client, regular_token1, push_assessment_id):
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/results",
        json=[{"vref": "INVALID", "score": 0.5}],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 400


def test_push_results_body_too_large(client, regular_token1, push_assessment_id):
    body = [{"vref": "GEN 1:1", "score": 0.5}] * 5001
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/results",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "5001" in detail
    assert "5000" in detail
    assert "split into batches" in detail


# ---------------------------------------------------------------------------
# POST /assessment/{id}/alignment-scores
# ---------------------------------------------------------------------------


def test_push_alignment_scores(client, regular_token1, push_assessment_id):
    body = [
        {
            "vref": "GEN 1:1",
            "score": 0.99,
            "source": "beginning",
            "target": "mwanzo",
        },
    ]
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/alignment-scores",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["ids"] == []


def test_push_alignment_scores_round_trips_to_get(
    client, regular_token1, push_assessment_id
):
    """Regression for #596: pushed rows must be readable via GET /alignmentscores.

    Before the fix the push omitted ``hide`` from the insert dict, so the column
    landed NULL, and ``WordAlignment.hide: bool`` failed Pydantic validation on
    read, surfacing as HTTP 500.
    """
    body = [
        {
            "vref": "GEN 1:2",
            "score": 0.97,
            "source": "earth",
            "target": "tierra",
        },
        {
            "vref": "GEN 1:3",
            "score": 0.85,
            "source": "heaven",
            "target": "cielo",
        },
    ]
    push = client.post(
        f"{prefix}/assessment/{push_assessment_id}/alignment-scores",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert push.status_code == 200

    read = client.get(
        f"{prefix}/alignmentscores",
        params={"assessment_id": push_assessment_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert read.status_code == 200, read.text
    payload = read.json()
    assert payload["total_count"] >= len(body)
    pushed = {(r["source"], r["target"]) for r in body}
    returned = {(r["source"], r["target"]) for r in payload["results"]}
    assert pushed.issubset(returned)
    for row in payload["results"]:
        assert row["hide"] is False
        assert row["flag"] is False


# ---------------------------------------------------------------------------
# POST /assessment/{id}/alignment-threshold-scores
# ---------------------------------------------------------------------------


def test_push_alignment_threshold_scores(client, regular_token1, push_assessment_id):
    body = [
        {
            "vref": "GEN 1:6",
            "score": 1.0,
            "source": "water.”",
            "target": "nsɨ.”",
        },
        {
            "vref": "GEN 1:6",
            "score": 0.579,
            "source": "water",
            "target": "aminzi",
        },
    ]
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/alignment-threshold-scores",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["ids"] == []


def test_push_alignment_threshold_scores_round_trips_to_get(
    client, regular_token1, push_assessment_id
):
    """Pushed threshold rows must be readable via GET /alignmentscores?score_type=threshold."""
    body = [
        {
            "vref": "GEN 1:7",
            "score": 0.91,
            "source": "firmament",
            "target": "anga",
        },
        {
            "vref": "GEN 1:7",
            "score": 0.62,
            "source": "firmament",
            "target": "uwazi",
        },
    ]
    push = client.post(
        f"{prefix}/assessment/{push_assessment_id}/alignment-threshold-scores",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert push.status_code == 200, push.text

    read = client.get(
        f"{prefix}/alignmentscores",
        params={"assessment_id": push_assessment_id, "score_type": "threshold"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert read.status_code == 200, read.text
    payload = read.json()
    assert payload["total_count"] >= len(body)
    pushed = {(r["source"], r["target"]) for r in body}
    returned = {(r["source"], r["target"]) for r in payload["results"]}
    assert pushed.issubset(returned)
    # Multi-target survival: the whole point of the threshold table (vs the
    # deduped top-source table) is that two rows with the same (vref, source)
    # but different targets must both persist. Assert it directly rather than
    # relying on the subset check alone.
    firmament_targets = {
        r["target"]
        for r in payload["results"]
        if r["vref"] == "GEN 1:7" and r["source"] == "firmament"
    }
    assert {"anga", "uwazi"}.issubset(firmament_targets)
    for row in payload["results"]:
        assert row["hide"] is False
        assert row["flag"] is False


def test_push_alignment_threshold_scores_empty_body(
    client, regular_token1, push_assessment_id
):
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/alignment-threshold-scores",
        json=[],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["ids"] == []


def test_push_alignment_threshold_scores_unauthorized(
    client, regular_token2, push_assessment_id
):
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/alignment-threshold-scores",
        json=[{"vref": "GEN 1:1", "score": 0.5}],
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_push_alignment_threshold_scores_nonexistent(client, regular_token1):
    response = client.post(
        f"{prefix}/assessment/999999/alignment-threshold-scores",
        json=[{"vref": "GEN 1:1", "score": 0.5}],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_push_alignment_threshold_scores_no_auth(client, push_assessment_id):
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/alignment-threshold-scores",
        json=[{"vref": "GEN 1:1", "score": 0.5}],
    )
    assert response.status_code == 401


def test_push_alignment_threshold_scores_invalid_vref(
    client, regular_token1, push_assessment_id
):
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/alignment-threshold-scores",
        json=[{"vref": "NOTAVREF", "score": 0.5}],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 400


# ---------------------------------------------------------------------------
# POST /assessment/{id}/text-lengths
# ---------------------------------------------------------------------------


def test_push_text_lengths(
    client, regular_token1, push_assessment_id, test_db_session
):
    body = [
        {
            "vref": "GEN 1:1",
            "word_lengths": 5.0,
            "char_lengths": 25.0,
            "word_lengths_z": 0.3,
            "char_lengths_z": -0.1,
        },
        {
            "vref": "GEN 1:2",
            "word_lengths": 8.0,
            "char_lengths": 40.0,
            "word_lengths_z": 1.2,
            "char_lengths_z": 0.5,
        },
    ]
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/text-lengths",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["ids"] == []
    # Response no longer carries IDs, so confirm rows landed via the DB.
    test_db_session.expire_all()
    persisted = (
        test_db_session.query(TextLengthsTable)
        .filter(
            TextLengthsTable.assessment_id == push_assessment_id,
            TextLengthsTable.vref.in_(["GEN 1:1", "GEN 1:2"]),
        )
        .count()
    )
    assert persisted == 2


# ---------------------------------------------------------------------------
# POST /assessment/{id}/tfidf-vectors
# ---------------------------------------------------------------------------


def test_push_tfidf_vectors(
    client, regular_token1, push_assessment_id, test_db_session
):
    body = [
        {
            "vref": "GEN 1:1",
            "vector": [0.1] * 300,
        },
    ]
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/tfidf-vectors",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["ids"] == []
    # Response no longer carries IDs, so confirm rows landed via the DB.
    test_db_session.expire_all()
    persisted = (
        test_db_session.query(TfidfPcaVector)
        .filter(
            TfidfPcaVector.assessment_id == push_assessment_id,
            TfidfPcaVector.vref == "GEN 1:1",
        )
        .count()
    )
    assert persisted == 1


# ---------------------------------------------------------------------------
# POST /assessment/{id}/ngrams
# ---------------------------------------------------------------------------


def test_push_ngrams(client, regular_token1, push_assessment_id):
    body = [
        {
            "ngram": "the word",
            "ngram_size": 2,
            "vrefs": ["GEN 1:1", "GEN 1:2"],
        },
        {
            "ngram": "in the beginning",
            "ngram_size": 3,
            "vrefs": ["GEN 1:1"],
        },
    ]
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/ngrams",
        json=body,
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["ids"]) == 2


def test_push_ngrams_empty(client, regular_token1, push_assessment_id):
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/ngrams",
        json=[],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["ids"] == []


def test_push_ngrams_nonexistent_assessment(client, regular_token1):
    response = client.post(
        f"{prefix}/assessment/999999/ngrams",
        json=[{"ngram": "test", "ngram_size": 1, "vrefs": ["GEN 1:1"]}],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_push_ngrams_unauthorized(client, regular_token2, push_assessment_id):
    response = client.post(
        f"{prefix}/assessment/{push_assessment_id}/ngrams",
        json=[{"ngram": "test", "ngram_size": 1, "vrefs": ["GEN 1:1"]}],
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# DELETE /assessment/{id}/results
# ---------------------------------------------------------------------------


def test_delete_results(client, regular_token1, push_assessment_id, test_db_session):
    # First insert some results to delete
    insert_resp = client.post(
        f"{prefix}/assessment/{push_assessment_id}/results",
        json=[
            {"vref": "GEN 1:3", "score": 0.5},
            {"vref": "GEN 1:4", "score": 0.6},
        ],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert insert_resp.status_code == 200
    test_db_session.expire_all()
    ids = [
        row.id
        for row in test_db_session.query(AssessmentResult)
        .filter(
            AssessmentResult.assessment_id == push_assessment_id,
            AssessmentResult.vref.in_(["GEN 1:3", "GEN 1:4"]),
        )
        .all()
    ]

    response = client.request(
        "DELETE",
        f"{prefix}/assessment/{push_assessment_id}/results",
        json={"ids": ids},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["deleted"] == 2


def test_delete_results_empty_ids(client, regular_token1, push_assessment_id):
    response = client.request(
        "DELETE",
        f"{prefix}/assessment/{push_assessment_id}/results",
        json={"ids": []},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["deleted"] == 0


def test_delete_results_nonexistent_assessment(client, regular_token1):
    response = client.request(
        "DELETE",
        f"{prefix}/assessment/999999/results",
        json={"ids": [1]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_delete_results_unauthorized(client, regular_token2, push_assessment_id):
    response = client.request(
        "DELETE",
        f"{prefix}/assessment/{push_assessment_id}/results",
        json={"ids": [1]},
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


# ---------------------------------------------------------------------------
# DELETE /assessment/{id}/alignment-scores
# ---------------------------------------------------------------------------


def test_delete_alignment_scores(
    client, regular_token1, push_assessment_id, test_db_session
):
    insert_resp = client.post(
        f"{prefix}/assessment/{push_assessment_id}/alignment-scores",
        json=[
            {
                "vref": "GEN 1:1",
                "score": 0.9,
                "source": "delete-beginning",
                "target": "delete-mwanzo",
            },
            {
                "vref": "GEN 1:2",
                "score": 0.85,
                "source": "delete-earth",
                "target": "delete-dunia",
            },
        ],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert insert_resp.status_code == 200
    test_db_session.expire_all()
    ids = [
        row.id
        for row in test_db_session.query(AlignmentTopSourceScores)
        .filter(
            AlignmentTopSourceScores.assessment_id == push_assessment_id,
            AlignmentTopSourceScores.source.in_(["delete-beginning", "delete-earth"]),
        )
        .all()
    ]

    response = client.request(
        "DELETE",
        f"{prefix}/assessment/{push_assessment_id}/alignment-scores",
        json={"ids": ids},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["deleted"] == 2


def test_delete_alignment_scores_nonexistent(client, regular_token1):
    response = client.request(
        "DELETE",
        f"{prefix}/assessment/999999/alignment-scores",
        json={"ids": [1]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /assessment/{id}/alignment-threshold-scores
# ---------------------------------------------------------------------------


def test_delete_alignment_threshold_scores(
    client, regular_token1, push_assessment_id, test_db_session
):
    insert_resp = client.post(
        f"{prefix}/assessment/{push_assessment_id}/alignment-threshold-scores",
        json=[
            {
                "vref": "GEN 1:8",
                "score": 0.9,
                "source": "heaven",
                "target": "mbingu",
            },
            {
                "vref": "GEN 1:8",
                "score": 0.7,
                "source": "heaven",
                "target": "anga",
            },
        ],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert insert_resp.status_code == 200
    test_db_session.expire_all()
    ids = [
        row.id
        for row in test_db_session.query(AlignmentThresholdScores)
        .filter(
            AlignmentThresholdScores.assessment_id == push_assessment_id,
            AlignmentThresholdScores.vref == "GEN 1:8",
            AlignmentThresholdScores.source == "heaven",
        )
        .all()
    ]

    response = client.request(
        "DELETE",
        f"{prefix}/assessment/{push_assessment_id}/alignment-threshold-scores",
        json={"ids": ids},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["deleted"] == 2


def test_delete_alignment_threshold_scores_nonexistent(client, regular_token1):
    response = client.request(
        "DELETE",
        f"{prefix}/assessment/999999/alignment-threshold-scores",
        json={"ids": [1]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_delete_alignment_threshold_scores_unauthorized(
    client, regular_token2, push_assessment_id
):
    response = client.request(
        "DELETE",
        f"{prefix}/assessment/{push_assessment_id}/alignment-threshold-scores",
        json={"ids": [1]},
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert response.status_code == 403


def test_delete_alignment_threshold_scores_empty_ids(
    client, regular_token1, push_assessment_id
):
    response = client.request(
        "DELETE",
        f"{prefix}/assessment/{push_assessment_id}/alignment-threshold-scores",
        json={"ids": []},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["deleted"] == 0


# ---------------------------------------------------------------------------
# DELETE /assessment/{id}/text-lengths
# ---------------------------------------------------------------------------


def test_delete_text_lengths(
    client, regular_token1, push_assessment_id, test_db_session
):
    # Use a vref unique to this test so the lookup can't pick up rows
    # inserted by other tests sharing the module-scoped assessment.
    delete_vref = "GEN 1:30"
    insert_resp = client.post(
        f"{prefix}/assessment/{push_assessment_id}/text-lengths",
        json=[
            {
                "vref": delete_vref,
                "word_lengths": 5.0,
                "char_lengths": 25.0,
                "word_lengths_z": 0.3,
                "char_lengths_z": -0.1,
            },
        ],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert insert_resp.status_code == 200
    test_db_session.expire_all()
    ids = [
        row.id
        for row in test_db_session.query(TextLengthsTable)
        .filter(
            TextLengthsTable.assessment_id == push_assessment_id,
            TextLengthsTable.vref == delete_vref,
        )
        .all()
    ]

    response = client.request(
        "DELETE",
        f"{prefix}/assessment/{push_assessment_id}/text-lengths",
        json={"ids": ids},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["deleted"] == 1


def test_delete_text_lengths_nonexistent(client, regular_token1):
    response = client.request(
        "DELETE",
        f"{prefix}/assessment/999999/text-lengths",
        json={"ids": [1]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /assessment/{id}/tfidf-vectors
# ---------------------------------------------------------------------------


def test_delete_tfidf_vectors(
    client, regular_token1, push_assessment_id, test_db_session
):
    # Use a vref unique to this test so the lookup can't pick up rows
    # inserted by other tests sharing the module-scoped assessment.
    delete_vref = "GEN 1:31"
    insert_resp = client.post(
        f"{prefix}/assessment/{push_assessment_id}/tfidf-vectors",
        json=[{"vref": delete_vref, "vector": [0.1] * 300}],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert insert_resp.status_code == 200
    test_db_session.expire_all()
    ids = [
        row.id
        for row in test_db_session.query(TfidfPcaVector)
        .filter(
            TfidfPcaVector.assessment_id == push_assessment_id,
            TfidfPcaVector.vref == delete_vref,
        )
        .all()
    ]

    response = client.request(
        "DELETE",
        f"{prefix}/assessment/{push_assessment_id}/tfidf-vectors",
        json={"ids": ids},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["deleted"] == 1


def test_delete_tfidf_vectors_nonexistent(client, regular_token1):
    response = client.request(
        "DELETE",
        f"{prefix}/assessment/999999/tfidf-vectors",
        json={"ids": [1]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# DELETE /assessment/{id}/ngrams
# ---------------------------------------------------------------------------


def test_delete_ngrams(client, regular_token1, push_assessment_id):
    # Insert ngrams with vrefs
    insert_resp = client.post(
        f"{prefix}/assessment/{push_assessment_id}/ngrams",
        json=[
            {"ngram": "delete me", "ngram_size": 2, "vrefs": ["GEN 1:1", "GEN 1:2"]},
            {"ngram": "also delete", "ngram_size": 2, "vrefs": ["GEN 1:3"]},
        ],
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert insert_resp.status_code == 200
    ids = insert_resp.json()["ids"]

    # Delete should cascade to vref rows
    response = client.request(
        "DELETE",
        f"{prefix}/assessment/{push_assessment_id}/ngrams",
        json={"ids": ids},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    assert response.json()["deleted"] == 2


def test_delete_ngrams_nonexistent(client, regular_token1):
    response = client.request(
        "DELETE",
        f"{prefix}/assessment/999999/ngrams",
        json={"ids": [1]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404

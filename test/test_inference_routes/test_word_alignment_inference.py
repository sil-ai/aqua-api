# test_word_alignment_inference.py

import inference_routes.v3.inference_routes as inference_module

prefix = "v3"


def test_word_alignment_inference_success_by_language(
    client, regular_token1, test_eflomal_inference_assessment_id
):
    """POST with valid language pair returns a well-formed response."""
    # Clear the cache so this test loads fresh from DB
    inference_module._artifact_cache.clear()

    response = client.post(
        f"{prefix}/inference/word-alignment",
        json={
            "text1": "In the beginning God created the heavens and the earth",
            "text2": "Hapo mwanzo Mungu aliumba mbingu na nchi",
            "source_language": "eng",
            "target_language": "swh",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()

    assert "verse_score" in data
    assert "avg_link_score" in data
    assert "coverage" in data
    assert "alignment_links" in data
    assert "missing_words" in data

    assert 0.0 <= data["verse_score"] <= 1.0
    assert 0.0 <= data["coverage"] <= 1.0

    for link in data["alignment_links"]:
        assert "source_word" in link
        assert "target_word" in link
        assert "score" in link
        assert 0.0 <= link["score"] <= 1.0


def test_word_alignment_inference_success_by_assessment_id(
    client, regular_token1, test_eflomal_inference_assessment_id
):
    """POST with assessment_id returns a valid response."""
    response = client.post(
        f"{prefix}/inference/word-alignment",
        json={
            "text1": "God created",
            "text2": "Mungu aliumba",
            "source_language": "eng",
            "target_language": "swh",
            "assessment_id": test_eflomal_inference_assessment_id,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["verse_score"] > 0
    # "God"->"Mungu" and "created"->"aliumba" should both match
    assert len(data["alignment_links"]) == 2


def test_word_alignment_inference_known_pair_scores(
    client, regular_token1, test_eflomal_inference_assessment_id
):
    """God->Mungu is the highest-probability pair; verify it appears in links."""
    response = client.post(
        f"{prefix}/inference/word-alignment",
        json={
            "text1": "God",
            "text2": "Mungu",
            "source_language": "eng",
            "target_language": "swh",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["alignment_links"]) == 1
    assert data["alignment_links"][0]["source_word"] == "God"
    assert data["alignment_links"][0]["target_word"] == "Mungu"
    assert data["alignment_links"][0]["score"] > 0.8


def test_word_alignment_inference_empty_texts(
    client, regular_token1, test_eflomal_inference_assessment_id
):
    """Empty texts return zero scores with empty link/missing lists."""
    response = client.post(
        f"{prefix}/inference/word-alignment",
        json={
            "text1": "",
            "text2": "",
            "source_language": "eng",
            "target_language": "swh",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["verse_score"] == 0.0
    assert data["alignment_links"] == []


def test_word_alignment_inference_unknown_language_pair_returns_404(
    client, regular_token1
):
    """Language pair with no trained model returns 404."""
    response = client.post(
        f"{prefix}/inference/word-alignment",
        json={
            "text1": "hello",
            "text2": "hola",
            "source_language": "xyz",
            "target_language": "abc",
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_word_alignment_inference_unknown_assessment_id_returns_404(
    client, regular_token1
):
    """Non-existent assessment_id returns 404."""
    response = client.post(
        f"{prefix}/inference/word-alignment",
        json={
            "text1": "hello",
            "text2": "hola",
            "source_language": "eng",
            "target_language": "swh",
            "assessment_id": 999999,
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 404


def test_word_alignment_inference_missing_fields_returns_422(client, regular_token1):
    """Missing required fields return 422."""
    response = client.post(
        f"{prefix}/inference/word-alignment",
        json={"text1": "hello"},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert response.status_code == 422


def test_word_alignment_inference_no_auth_returns_401(client):
    """Request without auth token returns 401."""
    response = client.post(
        f"{prefix}/inference/word-alignment",
        json={
            "text1": "God created",
            "text2": "Mungu aliumba",
            "source_language": "eng",
            "target_language": "swh",
        },
    )
    assert response.status_code == 401

# test_semantic_similarity_integration.py
#
# Tests using realistic Kinga-Swahili verse data with mocked Modal responses.
# Verifies the API correctly forwards requests and handles all response types.

from unittest.mock import AsyncMock, patch

import pytest

prefix = "v3"

MOCK_MODULE = "predict_routes.v3.predict_routes.modal.Function"

# -- Test data: Kinga (zga) <-> Swahili (swh) Psalms verse pairs --

GOOD_PAIRS = [
    {
        "text1": "Inʼgatalʉsu ʉvɨ nikwɨdɨka ɨlisongo lya avagalo, ikange nikwɨma mu nzɨla jya vatulanongwa, paange kwɨhanza mu lʉlʉndamano lwa vanya sʉngʉ.",
        "text2": "Amebarikiwa ambaye hayafuati maneno ya waovu, ambaye haendi katika njia za watenda dhambi, ambaye hakai katika ambao wanadharau.",
        "vref": "PSA 1:1",
        "mock_score": 0.45,
    },
    {
        "text1": "Apeene ʉmwene ikʉtsihovokela indagɨlo tsa Ntwa ʉNgʉlʉve, ikʉtsisaagɨlɨla siitso pa muunyi na pa kɨlo.",
        "text2": "Bali sheria ya BWANA ndiyo ambayo inampendeza, huiwaza mchana na usiku.",
        "vref": "PSA 1:2",
        "mock_score": 0.38,
    },
    {
        "text1": "Lɨno najiiva ewo kʉ avagalo! Avo vahwaniine nʉ mweleelo ʉgupuulwa nɨ mepo.",
        "text2": "Lakini waovu hawako kama hivyo. Wao wako kama vile makapi ambayo yanapeperushwa na upepo.",
        "vref": "PSA 1:4",
        "mock_score": 0.52,
    },
    {
        "text1": 'Vitsova viita, "Tʉmamaalʉle ɨminyololo gya veene, valeke ʉkʉtʉtavala lʉsiku!"',
        "text2": 'Wanasema, Tujifungue katika utawala wao, tutupilie mbali minyororo yao!"',
        "vref": "PSA 2:3",
        "mock_score": 0.41,
    },
]


@pytest.mark.parametrize("pair", GOOD_PAIRS, ids=[p["vref"] for p in GOOD_PAIRS])
def test_matched_pair_returns_score(
    client, regular_token1, pair, test_version_id, test_version_id_2
):
    """Correctly matched verse pairs should return a valid score."""
    with patch(MOCK_MODULE) as mock_function_cls:
        mock_fn = AsyncMock()
        mock_fn.remote.aio = AsyncMock(return_value={"score": pair["mock_score"]})
        mock_function_cls.from_name.return_value = mock_fn

        response = client.post(
            f"/{prefix}/predict/semantic-similarity",
            json={
                "text1": pair["text1"],
                "text2": pair["text2"],
                "source_version_id": test_version_id,
                "target_version_id": test_version_id_2,
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["score"] == pair["mock_score"]

    # Verify Modal was called with the correct text and version IDs
    mock_fn.remote.aio.assert_called_once_with(
        pair["text1"],
        pair["text2"],
        source_version_id=test_version_id,
        target_version_id=test_version_id_2,
    )


def test_mismatched_pair_forwarded_correctly(
    client, regular_token1, test_version_id, test_version_id_2
):
    """Mismatched verse pair is forwarded to Modal with correct params."""
    with patch(MOCK_MODULE) as mock_function_cls:
        mock_fn = AsyncMock()
        mock_fn.remote.aio = AsyncMock(return_value={"score": 0.12})
        mock_function_cls.from_name.return_value = mock_fn

        # PSA 1:1 zga text with PSA 1:2 swh text (mismatched)
        response = client.post(
            f"/{prefix}/predict/semantic-similarity",
            json={
                "text1": GOOD_PAIRS[0]["text1"],
                "text2": GOOD_PAIRS[1]["text2"],
                "source_version_id": test_version_id,
                "target_version_id": test_version_id_2,
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 200
    assert response.json()["score"] == 0.12


def test_unknown_version_pair_returns_422(
    client, regular_token1, test_version_id, test_version_id_2
):
    """Version pair with no trained model returns 422."""
    with patch(MOCK_MODULE) as mock_function_cls:
        mock_fn = AsyncMock()
        mock_fn.remote.aio = AsyncMock(
            return_value={
                "error": (
                    f"No fine-tuned model found for "
                    f"{test_version_id}_{test_version_id_2}"
                )
            }
        )
        mock_function_cls.from_name.return_value = mock_fn

        response = client.post(
            f"/{prefix}/predict/semantic-similarity",
            json={
                "text1": "hello",
                "text2": "world",
                "source_version_id": test_version_id,
                "target_version_id": test_version_id_2,
            },
            headers={"Authorization": f"Bearer {regular_token1}"},
        )

    assert response.status_code == 422
    assert "No fine-tuned model found" in response.json()["detail"]

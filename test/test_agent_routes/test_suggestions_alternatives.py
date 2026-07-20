"""Tests for span-level critique suggestions and whole-verse translation alternatives (aqua-api#811)."""

prefix = "v3"


def _create_translation(client, token, assessment_id, vref, alternatives=None):
    body = {"assessment_id": assessment_id, "vref": vref, "draft_text": "test"}
    if alternatives is not None:
        body["alternatives"] = alternatives
    resp = client.post(
        f"{prefix}/agent/translation",
        json=body,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, f"Failed to create translation: {resp.json()}"
    return resp.json()


def test_critique_suggestions_round_trip(client, regular_token1, test_assessment_id):
    """suggestions posted on a critique issue are persisted and returned on GET."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 3:16"
    )["id"]

    issue = {
        "dimension": "accuracy",
        "subtype": "mistranslation/sense",
        "source_text": "went",
        "draft_text": "кӧчкелен",
        "comments": "overstates the motion",
        "severity": 3,
        "suggestions": [
            {"text": "келген", "note": "neutral 'came/went'"},
            {"text": "барган"},
        ],
    }
    post_resp = client.post(
        f"{prefix}/agent/critique",
        json={"agent_translation_id": translation_id, "issues": [issue]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert post_resp.status_code == 200, post_resp.json()
    created = post_resp.json()[0]
    assert created["suggestions"] == [
        {"text": "келген", "note": "neutral 'came/went'"},
        {"text": "барган", "note": None},
    ]

    get_resp = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}"
        f"&agent_translation_id={translation_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert get_resp.status_code == 200
    fetched = next(i for i in get_resp.json() if i["id"] == created["id"])
    assert fetched["suggestions"] == [
        {"text": "келген", "note": "neutral 'came/went'"},
        {"text": "барган", "note": None},
    ]


def test_critique_suggestions_optional(client, regular_token1, test_assessment_id):
    """Omitting suggestions leaves the field NULL (backward compatible)."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 3:17"
    )["id"]
    issue = {
        "dimension": "accuracy",
        "subtype": "omission",
        "source_text": "world",
        "severity": 2,
    }
    resp = client.post(
        f"{prefix}/agent/critique",
        json={"agent_translation_id": translation_id, "issues": [issue]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200
    issue_id = resp.json()[0]["id"]
    assert resp.json()[0]["suggestions"] is None

    # NULL also round-trips through GET as null (not [] or a missing key).
    get_resp = client.get(
        f"{prefix}/agent/critique?assessment_id={test_assessment_id}"
        f"&agent_translation_id={translation_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert get_resp.status_code == 200
    fetched = next(i for i in get_resp.json() if i["id"] == issue_id)
    assert fetched["suggestions"] is None


def test_translation_alternatives_round_trip(
    client, regular_token1, test_assessment_id
):
    """alternatives posted on a single translation are persisted and returned on GET."""
    alternatives = [
        {"text": "These are the names of Israel's sons.", "note": "smoother phrasing"},
        {"text": "The names of Israel's sons are these."},
    ]
    created = _create_translation(
        client, regular_token1, test_assessment_id, "EXO 1:1", alternatives=alternatives
    )
    assert created["alternatives"] == [
        {"text": "These are the names of Israel's sons.", "note": "smoother phrasing"},
        {"text": "The names of Israel's sons are these.", "note": None},
    ]

    get_resp = client.get(
        f"{prefix}/agent/translations?assessment_id={test_assessment_id}&vref=EXO 1:1",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert get_resp.status_code == 200
    fetched = next(t for t in get_resp.json() if t["id"] == created["id"])
    assert fetched["alternatives"] == [
        {"text": "These are the names of Israel's sons.", "note": "smoother phrasing"},
        {"text": "The names of Israel's sons are these.", "note": None},
    ]


def test_translation_alternatives_optional(client, regular_token1, test_assessment_id):
    """Omitting alternatives leaves the field NULL (backward compatible)."""
    created = _create_translation(client, regular_token1, test_assessment_id, "EXO 1:2")
    assert created["alternatives"] is None

    get_resp = client.get(
        f"{prefix}/agent/translations?assessment_id={test_assessment_id}&vref=EXO 1:2",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert get_resp.status_code == 200
    fetched = next(t for t in get_resp.json() if t["id"] == created["id"])
    assert fetched["alternatives"] is None


def test_critique_multiple_issues_each_keep_own_suggestions(
    client, regular_token1, test_assessment_id
):
    """Each issue in a multi-issue POST keeps its own suggestions (no cross-bleed)."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 3:18"
    )["id"]
    issues = [
        {
            "dimension": "accuracy",
            "subtype": "omission",
            "source_text": "first",
            "suggestions": [{"text": "A"}],
        },
        {
            "dimension": "accuracy",
            "subtype": "mistranslation",
            "source_text": "second",
            "draft_text": "x",
            "suggestions": [{"text": "B"}, {"text": "C", "note": "alt"}],
        },
    ]
    resp = client.post(
        f"{prefix}/agent/critique",
        json={"agent_translation_id": translation_id, "issues": issues},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.json()
    by_source = {i["source_text"]: i for i in resp.json()}
    assert by_source["first"]["suggestions"] == [{"text": "A", "note": None}]
    assert by_source["second"]["suggestions"] == [
        {"text": "B", "note": None},
        {"text": "C", "note": "alt"},
    ]


def test_suggestion_text_control_chars_sanitized(
    client, regular_token1, test_assessment_id
):
    """Control characters in suggestion text/note are stripped like other text fields."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 3:19"
    )["id"]
    issue = {
        "dimension": "accuracy",
        "subtype": "mistranslation",
        "source_text": "s",
        "draft_text": "d",
        "suggestions": [{"text": "foo\nbar\tbaz", "note": "line\rnote"}],
    }
    resp = client.post(
        f"{prefix}/agent/critique",
        json={"agent_translation_id": translation_id, "issues": [issue]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.json()
    assert resp.json()[0]["suggestions"] == [
        {"text": "foo bar baz", "note": "line note"}
    ]


def test_empty_list_normalized_to_null(client, regular_token1, test_assessment_id):
    """An explicit empty list persists as NULL for both alternatives and suggestions."""
    created = _create_translation(
        client, regular_token1, test_assessment_id, "EXO 1:3", alternatives=[]
    )
    assert created["alternatives"] is None

    translation_id = created["id"]
    issue = {
        "dimension": "accuracy",
        "subtype": "omission",
        "source_text": "s",
        "suggestions": [],
    }
    resp = client.post(
        f"{prefix}/agent/critique",
        json={"agent_translation_id": translation_id, "issues": [issue]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200
    assert resp.json()[0]["suggestions"] is None


def test_blank_suggestion_text_rejected(client, regular_token1, test_assessment_id):
    """A whitespace-only suggestion text is rejected with 422 (min_length)."""
    translation_id = _create_translation(
        client, regular_token1, test_assessment_id, "JHN 3:20"
    )["id"]
    issue = {
        "dimension": "accuracy",
        "subtype": "omission",
        "source_text": "s",
        "suggestions": [{"text": "   "}],
    }
    resp = client.post(
        f"{prefix}/agent/critique",
        json={"agent_translation_id": translation_id, "issues": [issue]},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 422


def test_bulk_translations_alternatives(client, regular_token1, test_assessment_id):
    """alternatives flow through the bulk translations endpoint."""
    resp = client.post(
        f"{prefix}/agent/translations",
        json={
            "assessment_id": test_assessment_id,
            "translations": [
                {
                    "vref": "EXO 2:1",
                    "draft_text": "a",
                    "alternatives": [{"text": "alt-a", "note": "n"}],
                },
                {"vref": "EXO 2:2", "draft_text": "b"},
            ],
        },
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.json()
    by_vref = {t["vref"]: t for t in resp.json()}
    assert by_vref["EXO 2:1"]["alternatives"] == [{"text": "alt-a", "note": "n"}]
    assert by_vref["EXO 2:2"]["alternatives"] is None

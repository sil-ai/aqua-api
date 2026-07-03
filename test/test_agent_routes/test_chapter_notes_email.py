"""Tests for the whole-chapter AI-notes HTML email endpoint (aqua-api#817)."""

import pytest

from database.models import VerseText

prefix = "v3"

DRAFT_TEXTS = {
    1: "Chapter notes draft verse one",
    2: "Chapter notes draft verse two",
    3: "Chapter notes draft verse three",
}
REFERENCE_TEXTS = {
    1: "Chapter notes reference verse one",
    2: "Chapter notes reference verse two",
    3: "Chapter notes reference verse three",
}


@pytest.fixture(scope="module")
def chapter_verse_texts(test_db_session, test_revision_id, test_revision_id_2):
    """Insert draft + reference verse text for JON 1:1-3."""
    rows = []
    for verse, text in DRAFT_TEXTS.items():
        rows.append(
            VerseText(
                text=text,
                revision_id=test_revision_id,
                verse_reference=f"JON 1:{verse}",
                book="JON",
                chapter=1,
                verse=verse,
            )
        )
    for verse, text in REFERENCE_TEXTS.items():
        rows.append(
            VerseText(
                text=text,
                revision_id=test_revision_id_2,
                verse_reference=f"JON 1:{verse}",
                book="JON",
                chapter=1,
                verse=verse,
            )
        )
    test_db_session.add_all(rows)
    test_db_session.commit()


@pytest.fixture(scope="module")
def notes_setup(client, regular_token1, test_assessment_id, chapter_verse_texts):
    """Create translations and critique issues for the JON 1 chapter email."""
    headers = {"Authorization": f"Bearer {regular_token1}"}

    translation_resp = client.post(
        f"{prefix}/agent/translation",
        json={
            "assessment_id": test_assessment_id,
            "vref": "JON 1:1",
            "draft_text": DRAFT_TEXTS[1],
            "literal_translation": "Auto back translation of verse one",
            "english_translation": "English translation with [?wanenge?] marker",
            "alternatives": [
                {"text": "An alternative rendering of verse one", "note": "smoother"}
            ],
        },
        headers=headers,
    )
    assert translation_resp.status_code == 200, translation_resp.json()
    translation_id = translation_resp.json()["id"]

    translation2_resp = client.post(
        f"{prefix}/agent/translation",
        json={
            "assessment_id": test_assessment_id,
            "vref": "JON 1:2",
            "draft_text": DRAFT_TEXTS[2],
            "literal_translation": "Auto back translation of verse two",
        },
        headers=headers,
    )
    assert translation2_resp.status_code == 200, translation2_resp.json()

    issues = [
        {
            "dimension": "accuracy",
            "subtype": "mistranslation/hallucination-numbers",
            "source_text": "reference verse one",
            "draft_text": "draft verse one",
            "comments": "Severity five problem comment",
            "severity": 5,
            "suggestions": [{"text": "Suggested fix text", "note": "match the source"}],
            "evidence": ["evidence line one", "evidence line two"],
        },
        {
            "dimension": "terminology",
            "subtype": "wrong-key-term",
            "comments": "Severity two suggestion comment",
            "severity": 2,
        },
        {
            "dimension": "linguistic_conventions",
            "subtype": "spelling",
            "comments": "No severity comment with <script>alert('x')</script>",
        },
        {
            "dimension": "accuracy",
            "subtype": "omission",
            "comments": "Dismissed severity four comment",
            "severity": 4,
        },
    ]
    critique_resp = client.post(
        f"{prefix}/agent/critique",
        json={"agent_translation_id": translation_id, "issues": issues},
        headers=headers,
    )
    assert critique_resp.status_code == 200, critique_resp.json()
    created = critique_resp.json()

    dismissed = next(i for i in created if i["comments"].startswith("Dismissed"))
    resolve_resp = client.patch(
        f"{prefix}/agent/critique/{dismissed['id']}/resolve",
        json={"resolution_notes": "handled"},
        headers=headers,
    )
    assert resolve_resp.status_code == 200, resolve_resp.json()

    return {"translation_id": translation_id, "issues": created}


def _get_email(client, token, **params):
    return client.get(
        f"{prefix}/agent/chapter_notes_email",
        params={"book": "JON", "chapter": 1, **params},
        headers={"Authorization": f"Bearer {token}"},
    )


def test_chapter_notes_email_html(
    client, regular_token1, test_assessment_id, notes_setup
):
    resp = _get_email(client, regular_token1, assessment_id=test_assessment_id)
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("text/html")
    assert "Jonah 1" in resp.headers["x-email-subject"]
    html = resp.text

    # Every verse renders draft + reference text, with notes or without
    for text in list(DRAFT_TEXTS.values()) + list(REFERENCE_TEXTS.values()):
        assert text in html

    # Header and summary banner
    assert "Jonah 1" in html
    assert "loading_test" in html
    assert "1 problem" in html
    assert "1 suggestion" in html

    # Notes: severity 5 shown with dimension/priority/suggestions/evidence
    assert "Severity five problem comment" in html
    assert "Accuracy" in html
    assert "P5" in html
    assert "mistranslation › hallucination numbers" in html
    assert "Suggested fix text" in html
    assert "match the source" in html
    assert "evidence line one" in html

    # min_severity default 3 excludes severity 2 but keeps NULL severity
    assert "Severity two suggestion comment" not in html
    assert "No severity comment" in html

    # Dismissed notes excluded by default
    assert "Dismissed severity four comment" not in html

    # Back translation and alternatives shown; English hidden (reference is eng)
    assert "Auto back translation of verse one" in html
    assert "Auto back translation of verse two" in html
    assert "An alternative rendering of verse one" in html
    assert "smoother" in html
    assert "English translation with" not in html


def test_chapter_notes_email_escapes_html(
    client, regular_token1, test_assessment_id, notes_setup
):
    resp = _get_email(client, regular_token1, assessment_id=test_assessment_id)
    assert resp.status_code == 200
    assert "<script>alert" not in resp.text
    assert "&lt;script&gt;alert" in resp.text


def test_chapter_notes_email_min_severity(
    client, regular_token1, test_assessment_id, notes_setup
):
    resp = _get_email(
        client, regular_token1, assessment_id=test_assessment_id, min_severity=1
    )
    assert resp.status_code == 200
    assert "Severity two suggestion comment" in resp.text


def test_chapter_notes_email_include_dismissed(
    client, regular_token1, test_assessment_id, notes_setup
):
    resp = _get_email(
        client,
        regular_token1,
        assessment_id=test_assessment_id,
        include_dismissed=True,
    )
    assert resp.status_code == 200
    assert "Dismissed severity four comment" in resp.text
    assert "dismissed" in resp.text


def test_chapter_notes_email_json_format(
    client, regular_token1, test_assessment_id, notes_setup
):
    resp = _get_email(
        client, regular_token1, assessment_id=test_assessment_id, format="json"
    )
    assert resp.status_code == 200
    body = resp.json()
    assert set(body) == {"subject", "html"}
    assert "Jonah 1" in body["subject"]
    assert "Severity five problem comment" in body["html"]


def test_chapter_notes_email_by_revision_pair(
    client,
    regular_token1,
    test_assessment_id,
    test_revision_id,
    test_revision_id_2,
    notes_setup,
):
    resp = _get_email(
        client,
        regular_token1,
        revision_id=test_revision_id,
        reference_id=test_revision_id_2,
    )
    assert resp.status_code == 200
    assert "Severity five problem comment" in resp.text


def test_chapter_notes_email_param_validation(
    client, regular_token1, test_assessment_id, test_revision_id, notes_setup
):
    # Neither assessment_id nor revision pair
    resp = _get_email(client, regular_token1)
    assert resp.status_code == 400

    # Both assessment_id and revision pair
    resp = _get_email(
        client,
        regular_token1,
        assessment_id=test_assessment_id,
        revision_id=test_revision_id,
        reference_id=test_revision_id,
    )
    assert resp.status_code == 400

    # Bad format
    resp = _get_email(
        client, regular_token1, assessment_id=test_assessment_id, format="pdf"
    )
    assert resp.status_code == 400

    # Bad min_severity
    resp = _get_email(
        client, regular_token1, assessment_id=test_assessment_id, min_severity=6
    )
    assert resp.status_code == 400


def test_chapter_notes_email_unauthorized(
    client, regular_token2, test_assessment_id, notes_setup
):
    resp = _get_email(client, regular_token2, assessment_id=test_assessment_id)
    assert resp.status_code == 403


def test_chapter_notes_email_empty_chapter(
    client, regular_token1, test_assessment_id, notes_setup
):
    # No verse text loaded for JON 2 at all -> 404
    resp = client.get(
        f"{prefix}/agent/chapter_notes_email",
        params={"book": "JON", "chapter": 2, "assessment_id": test_assessment_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 404


def test_chapter_notes_email_no_notes_still_renders(
    client,
    regular_token1,
    test_db_session,
    test_revision_id,
    test_revision_id_2,
    test_assessment_id,
    notes_setup,
):
    # A chapter with verse text but zero qualifying notes returns a valid email
    test_db_session.add(
        VerseText(
            text="Chapter three draft verse",
            revision_id=test_revision_id,
            verse_reference="JON 3:1",
            book="JON",
            chapter=3,
            verse=1,
        )
    )
    test_db_session.commit()

    resp = client.get(
        f"{prefix}/agent/chapter_notes_email",
        params={"book": "JON", "chapter": 3, "assessment_id": test_assessment_id},
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200
    assert "Chapter three draft verse" in resp.text
    assert "No notes at or above priority 3" in resp.text


def test_critique_chapter_filter(
    client, regular_token1, test_assessment_id, notes_setup
):
    """/agent/critique now accepts a chapter filter (added for the email)."""
    headers = {"Authorization": f"Bearer {regular_token1}"}
    resp = client.get(
        f"{prefix}/agent/critique",
        params={"assessment_id": test_assessment_id, "book": "JON", "chapter": 1},
        headers=headers,
    )
    assert resp.status_code == 200
    issues = resp.json()
    assert len(issues) >= 4
    assert all(i["chapter"] == 1 for i in issues)

    resp = client.get(
        f"{prefix}/agent/critique",
        params={"assessment_id": test_assessment_id, "book": "JON", "chapter": 2},
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json() == []

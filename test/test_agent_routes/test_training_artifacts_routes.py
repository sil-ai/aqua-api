"""Tests for Phase 2 version-keyed read endpoints (issue #687):

- GET /v3/tokenizer/training-artifacts/{version_id}
- GET /v3/tokenizer/morphemes-by-version/{version_id}
- GET /v3/affixes-by-version/{version_id}
"""

from database.models import (
    BibleRevision,
    BibleVersion,
    LanguageAffix,
    LanguageMorpheme,
    LanguageProfile,
    TrainingArtifact,
)

prefix = "v3"


def _resolve_version_id(db_session, revision_id):
    return (
        db_session.query(BibleRevision.bible_version_id)
        .filter(BibleRevision.id == revision_id)
        .scalar()
    )


def _resolve_iso(db_session, version_id):
    return (
        db_session.query(BibleVersion.iso_language)
        .filter(BibleVersion.id == version_id)
        .scalar()
    )


def _cleanup(db_session, version_id, iso):
    # Drop training_artifacts for *every* bible_version under this ISO,
    # not just `version_id`, so cross-version tests that seed extra
    # versions don't leak rows. version_id is accepted for symmetry with
    # the existing API and to keep call sites self-documenting.
    del version_id
    version_ids = [
        row.id
        for row in db_session.query(BibleVersion.id)
        .filter(BibleVersion.iso_language == iso)
        .all()
    ]
    if version_ids:
        db_session.query(TrainingArtifact).filter(
            TrainingArtifact.target_version_id.in_(version_ids)
        ).delete(synchronize_session="fetch")
    db_session.query(LanguageAffix).filter(LanguageAffix.iso_639_3 == iso).delete()
    db_session.query(LanguageMorpheme).filter(
        LanguageMorpheme.iso_639_3 == iso
    ).delete()
    db_session.query(LanguageProfile).filter(LanguageProfile.iso_639_3 == iso).delete()
    db_session.commit()


# ---- /tokenizer/training-artifacts/{version_id} ----------------------------


def test_training_artifacts_returns_version_keyed_row(
    client, regular_token1, test_revision_id, db_session
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(LanguageProfile(iso_639_3=iso, name="English", grammar_sketch="iso"))
    db_session.flush()
    db_session.add(
        TrainingArtifact(
            target_version_id=version_id,
            grammar_sketch="version-specific",
            source_model="gpt-5-mini",
        )
    )
    db_session.commit()

    resp = client.get(
        f"/{prefix}/tokenizer/training-artifacts/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["target_version_id"] == version_id
    assert body["iso_639_3"] == iso
    assert body["grammar_sketch"] == "version-specific"
    assert body["source"] == "training_artifacts"
    assert body["source_model"] == "gpt-5-mini"

    _cleanup(db_session, version_id, iso)


def test_training_artifacts_falls_back_to_language_profile(
    client, regular_token1, test_revision_id, db_session
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    # Only language-keyed grammar_sketch — no training_artifacts row.
    db_session.add(
        LanguageProfile(iso_639_3=iso, name="English", grammar_sketch="legacy"),
    )
    db_session.commit()

    resp = client.get(
        f"/{prefix}/tokenizer/training-artifacts/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["grammar_sketch"] == "legacy"
    assert body["source"] == "language_profile"
    assert body["source_model"] is None

    _cleanup(db_session, version_id, iso)


def test_training_artifacts_falls_back_when_version_row_has_null_sketch(
    client, regular_token1, test_revision_id, db_session
):
    """A training_artifacts row that exists but has grammar_sketch=NULL
    should trigger fallback to the language-keyed sketch — and crucially
    must surface the version-keyed source_model so provenance isn't lost."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(
        LanguageProfile(iso_639_3=iso, name="English", grammar_sketch="legacy"),
    )
    db_session.flush()
    db_session.add(
        TrainingArtifact(
            target_version_id=version_id,
            grammar_sketch=None,
            source_model="provenance-model",
        ),
    )
    db_session.commit()

    resp = client.get(
        f"/{prefix}/tokenizer/training-artifacts/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["grammar_sketch"] == "legacy"
    assert body["source"] == "language_profile"
    # Even though the sketch came from the fallback, the version-keyed
    # source_model must be preserved.
    assert body["source_model"] == "provenance-model"

    _cleanup(db_session, version_id, iso)


def test_training_artifacts_404_when_neither_store_has_data(
    client, regular_token1, test_revision_id, db_session
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    resp = client.get(
        f"/{prefix}/tokenizer/training-artifacts/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 404, resp.text


def test_training_artifacts_403_when_version_unknown(
    client, regular_token1, db_session
):
    """Unknown version_id returns 403, not 404 — same response code as
    "not authorized" so callers can't enumerate valid version_ids."""
    resp = client.get(
        f"/{prefix}/tokenizer/training-artifacts/999999999",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 403, resp.text


def test_training_artifacts_403_when_user_lacks_version_access(
    client, regular_token2, test_revision_id, db_session
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    resp = client.get(
        f"/{prefix}/tokenizer/training-artifacts/{version_id}",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403, resp.text


# ---- /tokenizer/morphemes-by-version/{version_id} --------------------------


def test_morphemes_by_version_soft_union_with_null(
    client, regular_token1, test_revision_id, db_session
):
    """Returns version-stamped rows PLUS legacy NULL-stamped rows for
    the same ISO. Other versions' rows must not bleed in."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    db_session.add_all(
        [
            LanguageMorpheme(
                iso_639_3=iso,
                morpheme="versioned",
                morpheme_class="LEXICAL",
                target_version_id=version_id,
            ),
            LanguageMorpheme(
                iso_639_3=iso,
                morpheme="legacy",
                morpheme_class="LEXICAL",
                target_version_id=None,
            ),
        ]
    )
    db_session.commit()

    resp = client.get(
        f"/{prefix}/tokenizer/morphemes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    forms = {m["morpheme"] for m in body["morphemes"]}
    assert forms == {"versioned", "legacy"}
    assert body["total"] == 2
    assert body["iso_639_3"] == iso

    _cleanup(db_session, version_id, iso)


def test_morphemes_by_version_excludes_other_versions(
    client,
    regular_token1,
    test_revision_id,
    test_version_id_2,
    db_session,
):
    """Rows stamped to a different version_id must not appear in this
    version's results, even though they share the ISO."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    db_session.add_all(
        [
            LanguageMorpheme(
                iso_639_3=iso,
                morpheme="mine",
                morpheme_class="LEXICAL",
                target_version_id=version_id,
            ),
            LanguageMorpheme(
                iso_639_3=iso,
                morpheme="theirs",
                morpheme_class="LEXICAL",
                target_version_id=test_version_id_2,
            ),
        ]
    )
    db_session.commit()

    resp = client.get(
        f"/{prefix}/tokenizer/morphemes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    forms = {m["morpheme"] for m in resp.json()["morphemes"]}
    assert forms == {"mine"}

    _cleanup(db_session, version_id, iso)


def test_morphemes_by_version_403_when_user_lacks_version_access(
    client, regular_token2, test_revision_id, db_session
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    resp = client.get(
        f"/{prefix}/tokenizer/morphemes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403, resp.text


def test_morphemes_by_version_403_when_version_unknown(
    client, regular_token1, db_session
):
    resp = client.get(
        f"/{prefix}/tokenizer/morphemes-by-version/999999999",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 403, resp.text


# ---- /affixes-by-version/{version_id} --------------------------------------


def test_affixes_by_version_soft_union_with_null(
    client, regular_token1, test_revision_id, db_session
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    db_session.add_all(
        [
            LanguageAffix(
                iso_639_3=iso,
                form="v-",
                position="prefix",
                gloss="versioned-stamp",
                target_version_id=version_id,
            ),
            LanguageAffix(
                iso_639_3=iso,
                form="-l",
                position="suffix",
                gloss="legacy-null",
                target_version_id=None,
            ),
        ]
    )
    db_session.commit()

    resp = client.get(
        f"/{prefix}/affixes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    forms = {a["form"] for a in body["affixes"]}
    assert forms == {"v-", "-l"}
    assert body["total"] == 2

    _cleanup(db_session, version_id, iso)


def test_affixes_by_version_excludes_other_versions(
    client,
    regular_token1,
    test_revision_id,
    test_version_id_2,
    db_session,
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    db_session.add_all(
        [
            LanguageAffix(
                iso_639_3=iso,
                form="mine-",
                position="prefix",
                gloss="own",
                target_version_id=version_id,
            ),
            LanguageAffix(
                iso_639_3=iso,
                form="theirs-",
                position="prefix",
                gloss="other",
                target_version_id=test_version_id_2,
            ),
        ]
    )
    db_session.commit()

    resp = client.get(
        f"/{prefix}/affixes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    forms = {a["form"] for a in resp.json()["affixes"]}
    assert forms == {"mine-"}

    _cleanup(db_session, version_id, iso)


def test_affixes_by_version_position_filter(
    client, regular_token1, test_revision_id, db_session
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    db_session.add_all(
        [
            LanguageAffix(
                iso_639_3=iso,
                form="pre-",
                position="prefix",
                gloss="g1",
                target_version_id=version_id,
            ),
            LanguageAffix(
                iso_639_3=iso,
                form="-suf",
                position="suffix",
                gloss="g2",
                target_version_id=version_id,
            ),
        ]
    )
    db_session.commit()

    resp = client.get(
        f"/{prefix}/affixes-by-version/{version_id}?position=prefix",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    affixes = resp.json()["affixes"]
    assert len(affixes) == 1
    assert affixes[0]["form"] == "pre-"

    _cleanup(db_session, version_id, iso)


def test_affixes_by_version_403_when_user_lacks_version_access(
    client, regular_token2, test_revision_id, db_session
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    resp = client.get(
        f"/{prefix}/affixes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403, resp.text


def test_affixes_by_version_403_when_version_unknown(
    client, regular_token1, db_session
):
    resp = client.get(
        f"/{prefix}/affixes-by-version/999999999",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 403, resp.text


# ---- Additional coverage: admin bypass, filter params, empty results -----


def test_training_artifacts_admin_can_read_any_version(
    client, admin_token, test_revision_id, db_session
):
    """Admin bypass via is_user_authorized_for_bible_version's is_admin
    short-circuit. Without an admin-access test, a future tightening of
    the auth helper could silently lock admins out."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(
        LanguageProfile(iso_639_3=iso, name="English", grammar_sketch="for-admins"),
    )
    db_session.commit()

    resp = client.get(
        f"/{prefix}/tokenizer/training-artifacts/{version_id}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["grammar_sketch"] == "for-admins"

    _cleanup(db_session, version_id, iso)


def test_training_artifacts_admin_gets_404_for_unknown_version(
    client, admin_token, db_session
):
    """Admins are authorized for any version, so they CAN distinguish
    "doesn't exist" (404) from "no data" — regular users get 403 for
    both, but admin queries should fall through to the version lookup."""
    resp = client.get(
        f"/{prefix}/tokenizer/training-artifacts/999999999",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 404, resp.text


def test_morphemes_by_version_class_filter(
    client, regular_token1, test_revision_id, db_session
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    db_session.add_all(
        [
            LanguageMorpheme(
                iso_639_3=iso,
                morpheme="rootword",
                morpheme_class="LEXICAL",
                target_version_id=version_id,
            ),
            LanguageMorpheme(
                iso_639_3=iso,
                morpheme="affixbit",
                morpheme_class="GRAMMATICAL",
                target_version_id=version_id,
            ),
        ]
    )
    db_session.commit()

    resp = client.get(
        f"/{prefix}/tokenizer/morphemes-by-version/{version_id}?class=LEXICAL",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    morphemes = resp.json()["morphemes"]
    assert len(morphemes) == 1
    assert morphemes[0]["morpheme"] == "rootword"

    _cleanup(db_session, version_id, iso)


def test_morphemes_by_version_limit(
    client, regular_token1, test_revision_id, db_session
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    db_session.add_all(
        [
            LanguageMorpheme(
                iso_639_3=iso,
                morpheme=f"m{i}",
                morpheme_class="LEXICAL",
                target_version_id=version_id,
            )
            for i in range(5)
        ]
    )
    db_session.commit()

    resp = client.get(
        f"/{prefix}/tokenizer/morphemes-by-version/{version_id}?limit=2",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["morphemes"]) == 2
    assert body["total"] == 2

    _cleanup(db_session, version_id, iso)


def test_morphemes_by_version_empty_result(
    client, regular_token1, test_revision_id, db_session
):
    """Valid version + authorized user + no rows → 200 with empty list,
    not 404. Documents the contract for Phase 3 callers."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    resp = client.get(
        f"/{prefix}/tokenizer/morphemes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 0
    assert body["morphemes"] == []


def test_affixes_by_version_empty_result(
    client, regular_token1, test_revision_id, db_session
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    resp = client.get(
        f"/{prefix}/affixes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 0
    assert body["affixes"] == []

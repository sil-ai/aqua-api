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


def _cleanup(db_session, version_id, iso, extra_version_ids=()):
    # Tests that seed rows for additional versions can pass their IDs in
    # `extra_version_ids` so the corresponding training_artifacts rows
    # are also cleaned up.
    cleanup_version_ids = [version_id, *extra_version_ids]
    db_session.query(TrainingArtifact).filter(
        TrainingArtifact.target_version_id.in_(cleanup_version_ids)
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
    _cleanup(db_session, version_id, iso, extra_version_ids=(test_version_id_2,))

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

    _cleanup(db_session, version_id, iso, extra_version_ids=(test_version_id_2,))


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
    _cleanup(db_session, version_id, iso, extra_version_ids=(test_version_id_2,))

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

    _cleanup(db_session, version_id, iso, extra_version_ids=(test_version_id_2,))


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


# ---- DELETE /tokenizer/training-artifacts/{version_id} (Phase 4) ----------


def test_delete_training_artifacts_clears_version_stamped_rows(
    client, regular_token1, test_revision_id, db_session
):
    """All three kinds of agent-discovered artifacts (the per-version
    training_artifacts row, version-stamped affixes, version-stamped
    morphemes) are deleted atomically. Returns row counts."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    db_session.add(
        TrainingArtifact(
            target_version_id=version_id,
            grammar_sketch="to-be-cleared",
            source_model="gpt-5-mini",
        )
    )
    db_session.add_all(
        [
            LanguageMorpheme(
                iso_639_3=iso,
                morpheme="m1",
                morpheme_class="LEXICAL",
                target_version_id=version_id,
            ),
            LanguageMorpheme(
                iso_639_3=iso,
                morpheme="m2",
                morpheme_class="GRAMMATICAL",
                target_version_id=version_id,
            ),
        ]
    )
    db_session.add_all(
        [
            LanguageAffix(
                iso_639_3=iso,
                form="a1-",
                position="prefix",
                gloss="g1",
                target_version_id=version_id,
            ),
            LanguageAffix(
                iso_639_3=iso,
                form="-a2",
                position="suffix",
                gloss="g2",
                target_version_id=version_id,
            ),
            LanguageAffix(
                iso_639_3=iso,
                form="a3-",
                position="prefix",
                gloss="g3",
                target_version_id=version_id,
            ),
        ]
    )
    db_session.commit()

    resp = client.delete(
        f"/{prefix}/tokenizer/training-artifacts/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {
        "target_version_id": version_id,
        "training_artifacts_deleted": 1,
        "affixes_deleted": 3,
        "morphemes_deleted": 2,
    }

    db_session.expire_all()
    assert (
        db_session.query(TrainingArtifact)
        .filter(TrainingArtifact.target_version_id == version_id)
        .count()
        == 0
    )
    assert (
        db_session.query(LanguageMorpheme)
        .filter(LanguageMorpheme.target_version_id == version_id)
        .count()
        == 0
    )
    assert (
        db_session.query(LanguageAffix)
        .filter(LanguageAffix.target_version_id == version_id)
        .count()
        == 0
    )

    _cleanup(db_session, version_id, iso)


def test_delete_training_artifacts_preserves_null_and_other_version_rows(
    client,
    regular_token1,
    test_revision_id,
    test_version_id_2,
    db_session,
):
    """Legacy `target_version_id IS NULL` rows and rows stamped to
    other versions of the same ISO must NOT be touched — they belong
    to the soft-union for all versions of the ISO and to other versions
    respectively. Only version-stamped rows for the target version go."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso, extra_version_ids=(test_version_id_2,))

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    db_session.add_all(
        [
            LanguageMorpheme(
                iso_639_3=iso,
                morpheme="version_stamped",
                morpheme_class="LEXICAL",
                target_version_id=version_id,
            ),
            LanguageMorpheme(
                iso_639_3=iso,
                morpheme="legacy_null",
                morpheme_class="LEXICAL",
                target_version_id=None,
            ),
            LanguageMorpheme(
                iso_639_3=iso,
                morpheme="other_version",
                morpheme_class="LEXICAL",
                target_version_id=test_version_id_2,
            ),
        ]
    )
    db_session.add_all(
        [
            LanguageAffix(
                iso_639_3=iso,
                form="v-",
                position="prefix",
                gloss="versioned",
                target_version_id=version_id,
            ),
            LanguageAffix(
                iso_639_3=iso,
                form="-n",
                position="suffix",
                gloss="legacy",
                target_version_id=None,
            ),
            LanguageAffix(
                iso_639_3=iso,
                form="o-",
                position="prefix",
                gloss="other",
                target_version_id=test_version_id_2,
            ),
        ]
    )
    db_session.commit()

    resp = client.delete(
        f"/{prefix}/tokenizer/training-artifacts/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["affixes_deleted"] == 1
    assert body["morphemes_deleted"] == 1
    assert body["training_artifacts_deleted"] == 0  # no row was created above

    db_session.expire_all()
    morphemes = {
        row.morpheme
        for row in db_session.query(LanguageMorpheme).filter(
            LanguageMorpheme.iso_639_3 == iso
        )
    }
    assert morphemes == {"legacy_null", "other_version"}
    affixes = {
        row.form
        for row in db_session.query(LanguageAffix).filter(
            LanguageAffix.iso_639_3 == iso
        )
    }
    assert affixes == {"-n", "o-"}

    _cleanup(db_session, version_id, iso, extra_version_ids=(test_version_id_2,))


def test_delete_training_artifacts_idempotent_when_nothing_exists(
    client, regular_token1, test_revision_id, db_session
):
    """Calling DELETE on a version with no artifacts returns 200 with
    zero counts — rebuild semantics are 'ensure nothing exists', not
    'something was there'."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    resp = client.delete(
        f"/{prefix}/tokenizer/training-artifacts/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "target_version_id": version_id,
        "training_artifacts_deleted": 0,
        "affixes_deleted": 0,
        "morphemes_deleted": 0,
    }


def test_delete_training_artifacts_403_when_user_lacks_version_access(
    client, regular_token2, test_revision_id, db_session
):
    """A user without access to the version can't clear its artifacts.
    Same 403-for-unknown-version pattern as the Phase 2 GET endpoints."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    resp = client.delete(
        f"/{prefix}/tokenizer/training-artifacts/{version_id}",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403, resp.text


def test_delete_training_artifacts_403_when_version_unknown(
    client, regular_token1, db_session
):
    resp = client.delete(
        f"/{prefix}/tokenizer/training-artifacts/999999999",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 403, resp.text


def test_delete_training_artifacts_admin_gets_404_for_unknown_version(
    client, admin_token, db_session
):
    """Admins bypass the auth helper, so they reach the version lookup
    and get a real 404 for non-existent versions — matches the Phase 2
    GET endpoint behavior."""
    resp = client.delete(
        f"/{prefix}/tokenizer/training-artifacts/999999999",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 404, resp.text


def test_delete_training_artifacts_does_not_touch_lexeme_cards(
    client, regular_token1, test_revision_id, db_session
):
    """The DELETE clears tokenizer artifacts only — agent_lexeme_card
    rows belonging to the same version stay intact (they have their
    own DELETE endpoint per the Phase 4 contract). Pins this invariant
    so a future refactor that adds an accidental cascade through some
    relationship would be caught immediately."""
    from database.models import AgentLexemeCard

    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    db_session.add(
        TrainingArtifact(target_version_id=version_id, grammar_sketch="to-be-cleared")
    )
    db_session.add(
        AgentLexemeCard(
            target_lemma="staysput",
            source_version_id=version_id,
            target_version_id=version_id,
            source_language_iso=iso,
        )
    )
    db_session.commit()

    resp = client.delete(
        f"/{prefix}/tokenizer/training-artifacts/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["training_artifacts_deleted"] == 1

    db_session.expire_all()
    card = (
        db_session.query(AgentLexemeCard)
        .filter(AgentLexemeCard.target_lemma == "staysput")
        .one_or_none()
    )
    assert card is not None, "Lexeme card was incorrectly deleted by the rebuild DELETE"

    db_session.query(AgentLexemeCard).filter(
        AgentLexemeCard.target_lemma == "staysput"
    ).delete()
    db_session.commit()
    _cleanup(db_session, version_id, iso)


def test_delete_training_artifacts_cascades_to_word_morpheme_index(
    client, regular_token1, test_revision_id, db_session
):
    """Deleting a version-stamped morpheme cascades through
    `word_morpheme_index.morpheme_id` (declared `ondelete=CASCADE`).
    The DELETE response count reflects morpheme deletions only — but
    dependent index rows must also go, or queries against them
    post-rebuild would return ghosts. Pins the cascade behavior so an
    FK weakening (CASCADE → SET NULL / DEFAULT) would fail here."""
    from database.models import WordMorphemeIndex

    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    morpheme = LanguageMorpheme(
        iso_639_3=iso,
        morpheme="indexed",
        morpheme_class="LEXICAL",
        target_version_id=version_id,
    )
    db_session.add(morpheme)
    db_session.flush()
    db_session.add(
        WordMorphemeIndex(
            iso_639_3=iso,
            word="indexed-word",
            morpheme_id=morpheme.id,
            position=0,
            total_morphemes=1,
        )
    )
    db_session.commit()
    morpheme_id = morpheme.id

    resp = client.delete(
        f"/{prefix}/tokenizer/training-artifacts/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["morphemes_deleted"] == 1

    db_session.expire_all()
    assert (
        db_session.query(WordMorphemeIndex)
        .filter(WordMorphemeIndex.morpheme_id == morpheme_id)
        .count()
        == 0
    ), "WordMorphemeIndex rows should cascade-delete with the morpheme"

    _cleanup(db_session, version_id, iso)


# ---- DELETE /affixes-by-version/{version_id} (issue #796) ------------------


def test_delete_affixes_by_version_clears_stamped_and_iso_keyed_rows(
    client,
    regular_token1,
    test_revision_id,
    test_version_id_2,
    db_session,
):
    """The DELETE mirrors the GET soft-union: version-stamped rows AND
    legacy iso-keyed (NULL target) rows both go, while rows stamped to
    other versions of the same ISO survive. Counts are split by bucket."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso, extra_version_ids=(test_version_id_2,))

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    db_session.add_all(
        [
            LanguageAffix(
                iso_639_3=iso,
                form="v1-",
                position="prefix",
                gloss="versioned-1",
                target_version_id=version_id,
            ),
            LanguageAffix(
                iso_639_3=iso,
                form="-v2",
                position="suffix",
                gloss="versioned-2",
                target_version_id=version_id,
            ),
            LanguageAffix(
                iso_639_3=iso,
                form="-leg",
                position="suffix",
                gloss="legacy-null",
                target_version_id=None,
            ),
            LanguageAffix(
                iso_639_3=iso,
                form="other-",
                position="prefix",
                gloss="other-version",
                target_version_id=test_version_id_2,
            ),
        ]
    )
    db_session.commit()

    resp = client.delete(
        f"/{prefix}/affixes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "target_version_id": version_id,
        "iso_639_3": iso,
        "version_stamped_deleted": 2,
        "iso_keyed_deleted": 1,
        "total_deleted": 3,
    }

    db_session.expire_all()
    remaining = {
        row.form
        for row in db_session.query(LanguageAffix).filter(
            LanguageAffix.iso_639_3 == iso
        )
    }
    assert remaining == {"other-"}

    resp = client.get(
        f"/{prefix}/affixes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200
    assert resp.json()["total"] == 0

    _cleanup(db_session, version_id, iso, extra_version_ids=(test_version_id_2,))


def test_delete_affixes_by_version_idempotent_when_nothing_exists(
    client, regular_token1, test_revision_id, db_session
):
    """Rebuild semantics are 'ensure nothing exists' — zero counts on an
    empty target, and zero counts again on a repeat call."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    for _ in range(2):
        resp = client.delete(
            f"/{prefix}/affixes-by-version/{version_id}",
            headers={"Authorization": f"Bearer {regular_token1}"},
        )
        assert resp.status_code == 200, resp.text
        assert resp.json() == {
            "target_version_id": version_id,
            "iso_639_3": iso,
            "version_stamped_deleted": 0,
            "iso_keyed_deleted": 0,
            "total_deleted": 0,
        }


def test_delete_affixes_by_version_preserves_other_isos(
    client, regular_token1, test_revision_id, db_session
):
    """Legacy NULL-target rows belonging to a different ISO are outside
    this version's soft-union and must survive the wipe."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    other_iso = "swh" if iso != "swh" else "eng"
    _cleanup(db_session, version_id, iso)
    db_session.query(LanguageAffix).filter(
        LanguageAffix.iso_639_3 == other_iso
    ).delete()
    db_session.query(LanguageProfile).filter(
        LanguageProfile.iso_639_3 == other_iso
    ).delete()
    db_session.commit()

    db_session.add_all(
        [
            LanguageProfile(iso_639_3=iso, name="English"),
            LanguageProfile(iso_639_3=other_iso, name="Swahili"),
        ]
    )
    db_session.flush()
    db_session.add_all(
        [
            LanguageAffix(
                iso_639_3=iso,
                form="mine-",
                position="prefix",
                gloss="goes",
                target_version_id=version_id,
            ),
            LanguageAffix(
                iso_639_3=other_iso,
                form="-stays",
                position="suffix",
                gloss="other-iso-legacy",
                target_version_id=None,
            ),
        ]
    )
    db_session.commit()

    resp = client.delete(
        f"/{prefix}/affixes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["total_deleted"] == 1

    db_session.expire_all()
    assert (
        db_session.query(LanguageAffix)
        .filter(LanguageAffix.iso_639_3 == other_iso)
        .count()
        == 1
    ), "Legacy rows of an unrelated ISO must not be deleted"

    db_session.query(LanguageAffix).filter(
        LanguageAffix.iso_639_3 == other_iso
    ).delete()
    db_session.query(LanguageProfile).filter(
        LanguageProfile.iso_639_3 == other_iso
    ).delete()
    db_session.commit()
    _cleanup(db_session, version_id, iso)


def test_delete_affixes_by_version_403_when_user_lacks_version_access(
    client, regular_token2, test_revision_id, db_session
):
    version_id = _resolve_version_id(db_session, test_revision_id)
    resp = client.delete(
        f"/{prefix}/affixes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token2}"},
    )
    assert resp.status_code == 403, resp.text


def test_delete_affixes_by_version_403_when_version_unknown(
    client, regular_token1, db_session
):
    resp = client.delete(
        f"/{prefix}/affixes-by-version/999999999",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 403, resp.text


def test_delete_affixes_by_version_admin_gets_404_for_unknown_version(
    client, admin_token, db_session
):
    resp = client.delete(
        f"/{prefix}/affixes-by-version/999999999",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 404, resp.text


def test_delete_affixes_by_version_without_token(client, test_revision_id, db_session):
    version_id = _resolve_version_id(db_session, test_revision_id)
    resp = client.delete(f"/{prefix}/affixes-by-version/{version_id}")
    assert resp.status_code == 401


def test_delete_affixes_by_version_latest_alias(
    client, regular_token1, test_revision_id, db_session
):
    """The affix router is mounted under both /v3 and /latest — the
    rebuild path in aqua-assessments calls /latest, so pin the alias."""
    version_id = _resolve_version_id(db_session, test_revision_id)
    iso = _resolve_iso(db_session, version_id)
    _cleanup(db_session, version_id, iso)

    db_session.add(LanguageProfile(iso_639_3=iso, name="English"))
    db_session.flush()
    db_session.add(
        LanguageAffix(
            iso_639_3=iso,
            form="alias-",
            position="prefix",
            gloss="via-latest",
            target_version_id=version_id,
        )
    )
    db_session.commit()

    resp = client.delete(
        f"/latest/affixes-by-version/{version_id}",
        headers={"Authorization": f"Bearer {regular_token1}"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "target_version_id": version_id,
        "iso_639_3": iso,
        "version_stamped_deleted": 1,
        "iso_keyed_deleted": 0,
        "total_deleted": 1,
    }

    _cleanup(db_session, version_id, iso)

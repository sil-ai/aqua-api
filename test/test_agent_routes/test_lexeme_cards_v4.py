"""Tests for the ``GET /v4/lexeme-cards`` reads (issue #896, epic #842).

The read half of the lexeme-card slice: the list that ``aqua-django-app`` calls twice
over, once per word and once in bulk, and the by-id read beside it. One module for both
because they are one resource — the by-id classes at the bottom lean on the same fixture
helpers, and ``TestByIdMatchesTheListRow`` asserts against the list read directly, which
it could not do from another file.

Two things here are new rather than ported, and carry most of the weight below.
**Authorization**, because v3 has none on this family — any authenticated caller reads any
card for any version pair — so every rule these tests pin is a rule that did not exist
before. And the **language overlay**, where v4 reports a missing translation by nulling
``source_language_iso`` rather than by v3's separate boolean.

Rows are inserted directly rather than through v3's ``POST /agent/lexeme-card``. Same
reason the sibling v4 slices give: the write path normalizes and validates, and these
tests need shapes it will not produce on request — a null confidence, a ``senses`` array
holding a bare string, a ``surface_forms`` column holding an object rather than an array.
The fixture helpers are near-copies of ``test_agent_result_reads_v4.py``'s, copied rather
than imported so a failure in one module cannot look like a failure in the other.
"""

import itertools
from datetime import datetime

from api_v4.pagination import RESULT_DEFAULT_LIMIT, RESULT_MAX_LIMIT
from database.models import (
    AgentLexemeCard,
    AgentLexemeCardExample,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    CardTranslation,
    CardTranslationExample,
    Group,
)
from database.models import UserDB as UserModel

PREFIX = "/v4"
PATH = f"{PREFIX}/lexeme-cards"

_names = itertools.count()


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


def _group_id(db_session, name):
    group = db_session.query(Group).filter_by(name=name).first()
    assert group is not None, f"expected group {name} in fixtures"
    return group.id


def _user_id(db_session, username):
    user = db_session.query(UserModel).filter_by(username=username).first()
    assert user is not None
    return user.id


def _make_version(db_session, group_name, *, iso_language="eng", deleted=False):
    """Insert a version reachable only through ``group_name``."""
    n = next(_names)
    version = BibleVersion(
        name=f"V4LC Version {n}",
        iso_language=iso_language,
        iso_script="Latn",
        abbreviation=f"V4L{n}",
        owner_id=_user_id(db_session, "testuser1"),
        machine_translation=False,
        is_reference=False,
        deleted=deleted,
    )
    db_session.add(version)
    db_session.commit()
    db_session.refresh(version)
    db_session.add(
        BibleVersionAccess(
            bible_version_id=version.id, group_id=_group_id(db_session, group_name)
        )
    )
    db_session.commit()
    return version.id


def _grant(db_session, version_id, group_name):
    db_session.add(
        BibleVersionAccess(
            bible_version_id=version_id, group_id=_group_id(db_session, group_name)
        )
    )
    db_session.commit()
    return version_id


def _make_revision(db_session, version_id):
    revision = BibleRevision(
        bible_version_id=version_id,
        name=f"V4LC Revision {next(_names)}",
        date=datetime.now(),
        published=False,
        machine_translation=False,
        deleted=False,
    )
    db_session.add(revision)
    db_session.commit()
    db_session.refresh(revision)
    return revision.id


def _make_card(
    db_session,
    source_version_id,
    target_version_id,
    *,
    target_lemma=None,
    source_lemma="grace",
    source_language_iso="eng",
    surface_forms=None,
    source_surface_forms=None,
    senses=None,
    confidence=0.5,
    pos=None,
    model=None,
    alignment_scores=None,
    english_lemma=None,
    last_user_edit=None,
):
    card = AgentLexemeCard(
        source_lemma=source_lemma,
        target_lemma=target_lemma or f"lemma{next(_names)}",
        source_version_id=source_version_id,
        target_version_id=target_version_id,
        source_language_iso=source_language_iso,
        surface_forms=surface_forms,
        source_surface_forms=source_surface_forms,
        senses=senses,
        confidence=confidence,
        pos=pos,
        model=model,
        alignment_scores=alignment_scores,
        english_lemma=english_lemma,
        last_user_edit=last_user_edit,
    )
    db_session.add(card)
    db_session.commit()
    db_session.refresh(card)
    return card.id


def _make_example(db_session, card_id, revision_id, source_text, target_text):
    example = AgentLexemeCardExample(
        lexeme_card_id=card_id,
        revision_id=revision_id,
        source_text=source_text,
        target_text=target_text,
    )
    db_session.add(example)
    db_session.commit()
    db_session.refresh(example)
    return example.id


def _make_overlay(
    db_session,
    card_id,
    language_iso,
    *,
    source_lemma=None,
    source_surface_forms=None,
    senses=None,
    last_user_edit=None,
    example_translations=None,
):
    overlay = CardTranslation(
        card_id=card_id,
        language_iso=language_iso,
        source_lemma=source_lemma,
        source_surface_forms=source_surface_forms,
        senses=senses,
        last_user_edit=last_user_edit,
    )
    db_session.add(overlay)
    db_session.commit()
    db_session.refresh(overlay)
    for example_id, text in (example_translations or {}).items():
        db_session.add(
            CardTranslationExample(
                card_translation_id=overlay.id,
                example_id=example_id,
                source_text=text,
            )
        )
    db_session.commit()
    return overlay.id


def _get(client, token, **params):
    return client.get(PATH, params=params, headers=_auth(token))


def _get_by_id(client, token, card_id, **params):
    return client.get(f"{PATH}/{card_id}", params=params, headers=_auth(token))


def _error(response):
    return response.json()["error"]


def _ids(payload):
    return [item["id"] for item in payload["items"]]


class TestLexemeCardAuthorization:
    """The rules v3 does not have. Every assertion here is new behaviour."""

    def test_reads_cards_of_a_target_version_the_caller_reaches(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target)

        response = _get(client, regular_token1, target_version_id=target)

        assert response.status_code == 200
        assert _ids(response.json()) == [card_id]

    def test_target_version_outside_the_callers_groups_is_404(
        self, client, db_session, regular_token2
    ):
        """Not an empty page: ``target_version_id`` names the scope of the read.

        The distinction ``/v4/revisions`` draws between a parent filter and a narrowing
        one. A caller who cannot reach the version has not asked a question about an
        empty set, they have asked about something that is not theirs.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target)

        response = _get(client, regular_token2, target_version_id=target)

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "VERSION_NOT_FOUND"

    def test_source_access_alone_does_not_grant_the_cards(
        self, client, db_session, regular_token1, regular_token2
    ):
        """The rule that matters: cards follow the local-language translation, only.

        Running an agent assessment needs access to both sides. Reading the cards it
        produced needs access to the **target** — the local-language translation the
        cards describe. This pins the inverse, which is the case worth being sure about:
        a caller who holds the majority-language source and nothing else gets nothing,
        even though the card names their version.
        """
        source = _make_version(db_session, "Group2")  # testuser2 only
        target = _make_version(db_session, "Group1")  # testuser1 only
        card_id = _make_card(db_session, source, target)

        source_only = _get(client, regular_token2, target_version_id=target)
        target_only = _get(client, regular_token1, target_version_id=target)

        assert source_only.status_code == 404
        assert source_only.json()["error"]["code"] == "VERSION_NOT_FOUND"

        assert target_only.status_code == 200
        assert _ids(target_only.json()) == [card_id]

    def test_unknown_target_version_is_the_same_404(
        self, client, db_session, regular_token1
    ):
        """Same code and shape as the forbidden case, so ids cannot be probed."""
        forbidden_target = _make_version(db_session, "Group2")
        missing = _get(client, regular_token1, target_version_id=99_999_999)
        forbidden = _get(client, regular_token1, target_version_id=forbidden_target)

        assert missing.status_code == forbidden.status_code == 404
        assert (
            missing.json()["error"]["code"]
            == forbidden.json()["error"]["code"]
            == "VERSION_NOT_FOUND"
        )

    def test_soft_deleted_target_version_is_404(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target)
        version = db_session.query(BibleVersion).filter_by(id=target).first()
        version.deleted = True
        db_session.commit()

        response = _get(client, regular_token1, target_version_id=target)

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "VERSION_NOT_FOUND"

    def test_admin_reads_a_version_no_group_grants(
        self, client, db_session, admin_token
    ):
        source = _make_version(db_session, "Group2")
        target = _make_version(db_session, "Group2")
        card_id = _make_card(db_session, source, target)

        response = _get(client, admin_token, target_version_id=target)

        assert response.status_code == 200
        assert _ids(response.json()) == [card_id]

    def test_source_version_filter_is_authorized_too(
        self, client, db_session, regular_token1
    ):
        """A filter that names a version is checked even though it only narrows.

        Otherwise it would be an oracle: pass an id, and whether the page is empty or
        full tells you something about a version you cannot read.
        """
        target = _make_version(db_session, "Group1")
        forbidden_source = _make_version(db_session, "Group2")

        response = _get(
            client,
            regular_token1,
            target_version_id=target,
            source_version_id=forbidden_source,
        )

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "VERSION_NOT_FOUND"
        assert response.json()["error"]["details"] == {"version_id": forbidden_source}

    def test_source_version_id_is_served_even_when_unreachable(
        self, client, db_session, regular_token2
    ):
        """Pinned as intended, because it is the one id this read does expose.

        Gating on the target version alone means a card can name a
        ``source_version_id`` the caller has no grant on, and the response reports it.
        That is deliberate and is what makes the field useful — it is how a caller learns
        which value to send back to ``?source_version_id=``, and pivot Bibles are shared
        precisely so that cards built against them can be read by projects that do not
        own them.

        What does **not** leak is anything about that version beyond its id: no name, no
        language, no source-side content unless an overlay for the caller's own language
        holds it, and no example text — those stay behind the per-revision filter. Asking
        ``?source_version_id=`` about it still answers 404, so the id cannot be turned
        into access.
        """
        hidden_source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group2")
        card_id = _make_card(db_session, hidden_source, target)

        listed = _get(client, regular_token2, target_version_id=target)

        assert listed.status_code == 200
        (card,) = listed.json()["items"]
        assert card["id"] == card_id
        assert card["source_version_id"] == hidden_source

        # The id is visible; the version behind it is still not reachable.
        probed = _get(
            client,
            regular_token2,
            target_version_id=target,
            source_version_id=hidden_source,
        )
        assert probed.status_code == 404
        assert probed.json()["error"]["code"] == "VERSION_NOT_FOUND"

    def test_unauthenticated_is_401(self, client, db_session):
        target = _make_version(db_session, "Group1")
        response = client.get(PATH, params={"target_version_id": target})
        assert response.status_code == 401


class TestExampleVisibility:
    """v3's one real access rule, carried across unchanged."""

    def test_examples_come_from_either_side_of_the_pair(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        source_revision = _make_revision(db_session, source)
        target_revision = _make_revision(db_session, target)
        card_id = _make_card(db_session, source, target)
        _make_example(db_session, card_id, source_revision, "from source", "chanzo")
        _make_example(db_session, card_id, target_revision, "from target", "lengo")

        response = _get(client, regular_token1, target_version_id=target)

        assert response.status_code == 200
        (card,) = response.json()["items"]
        assert [e["source"] for e in card["examples"]] == ["from source", "from target"]

    def test_example_from_a_revision_the_caller_cannot_reach_is_hidden(
        self, client, db_session, regular_token2
    ):
        """The card is still returned — only the example is withheld.

        Both callers can reach the target version here, so both see the card; they differ
        only in whether they can reach the *source* version the example was drawn from.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _grant(db_session, target, "Group2")
        source_revision = _make_revision(db_session, source)
        target_revision = _make_revision(db_session, target)
        card_id = _make_card(db_session, source, target)
        _make_example(db_session, card_id, source_revision, "hidden", "siri")
        _make_example(db_session, card_id, target_revision, "shown", "onekana")

        response = _get(client, regular_token2, target_version_id=target)

        assert response.status_code == 200
        (card,) = response.json()["items"]
        assert card["id"] == card_id
        assert [e["source"] for e in card["examples"]] == ["shown"]

    def test_admin_sees_every_example(self, client, db_session, admin_token):
        source = _make_version(db_session, "Group2")
        target = _make_version(db_session, "Group2")
        card_id = _make_card(db_session, source, target)
        _make_example(db_session, card_id, _make_revision(db_session, source), "a", "x")
        _make_example(db_session, card_id, _make_revision(db_session, target), "b", "y")

        response = _get(client, admin_token, target_version_id=target)

        (card,) = response.json()["items"]
        assert [e["source"] for e in card["examples"]] == ["a", "b"]

    def test_example_scope_is_per_card_not_a_union_across_the_page(
        self, client, db_session, regular_token2
    ):
        """Where v3's bulk read differs from its by-id read, and is wrong.

        v3 authorizes the page's examples against the union of every returned card's
        source version plus the target. So an example of card A passes because the caller
        reaches card B's source. Two cards on one target version, built from different
        sources, only one of which the caller can reach: the union would leak A's example.
        """
        reachable_source = _make_version(db_session, "Group2")
        hidden_source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group2")
        hidden_revision = _make_revision(db_session, hidden_source)

        leaky_card = _make_card(db_session, hidden_source, target, confidence=0.9)
        _make_example(
            db_session, leaky_card, hidden_revision, "must not leak", "hapana"
        )
        other_card = _make_card(db_session, reachable_source, target, confidence=0.1)

        response = _get(client, regular_token2, target_version_id=target)

        assert response.status_code == 200
        cards = {c["id"]: c for c in response.json()["items"]}
        assert set(cards) == {leaky_card, other_card}
        assert cards[leaky_card]["examples"] == []

    def test_example_from_a_third_version_is_hidden_from_non_admins(
        self, client, db_session, regular_token1, admin_token
    ):
        """The in-pair invariant is a write-path rule, not a database constraint.

        v3's ``POST`` refuses an example whose revision belongs to neither the card's
        source nor its target version, and the read filter leans on that. Nothing at the
        database enforces it, so a direct write can produce such a row. Pinned here so the
        behaviour is known rather than discovered: a non-admin does not see it, because
        the filter asks about the card's own two versions and this revision is in neither.
        An admin does, because admins bypass the filter entirely.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        third = _make_version(db_session, "Group1")
        third_revision = _make_revision(db_session, third)
        card_id = _make_card(db_session, source, target)
        _make_example(db_session, card_id, third_revision, "out of pair", "nje")

        as_user = _get(client, regular_token1, target_version_id=target)
        as_admin = _get(client, admin_token, target_version_id=target)

        assert as_user.status_code == 200
        (user_card,) = as_user.json()["items"]
        assert user_card["examples"] == []

        (admin_card,) = as_admin.json()["items"]
        assert [e["source"] for e in admin_card["examples"]] == ["out of pair"]

    def test_admin_examples_stay_with_their_own_card_across_a_page(
        self, client, db_session, admin_token
    ):
        """The admin path skips the auth filter but keeps the card join."""
        source = _make_version(db_session, "Group2")
        target = _make_version(db_session, "Group2")
        revision = _make_revision(db_session, target)
        first = _make_card(db_session, source, target, confidence=0.9)
        second = _make_card(db_session, source, target, confidence=0.1)
        _make_example(db_session, first, revision, "first card", "kwanza")
        _make_example(db_session, second, revision, "second card", "pili")

        response = _get(client, admin_token, target_version_id=target)

        cards = {c["id"]: c for c in response.json()["items"]}
        assert [e["source"] for e in cards[first]["examples"]] == ["first card"]
        assert [e["source"] for e in cards[second]["examples"]] == ["second card"]

    def test_example_reports_its_revision(self, client, db_session, regular_token1):
        """v3 does not serve this, which left its client guessing on a round trip."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        revision = _make_revision(db_session, target)
        card_id = _make_card(db_session, source, target)
        _make_example(db_session, card_id, revision, "src", "tgt")

        response = _get(client, regular_token1, target_version_id=target)

        (card,) = response.json()["items"]
        assert card["examples"][0]["revision_id"] == revision


class TestLanguageOverlay:
    """``?source_language_iso=``, and the nullable field that replaced v3's boolean."""

    def test_canonical_card_when_no_language_is_requested(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(
            db_session,
            source,
            target,
            source_lemma="grace",
            source_language_iso="eng",
            senses=[{"definition": "unearned favour", "examples": ["by grace"]}],
        )

        response = _get(client, regular_token1, target_version_id=target)

        (card,) = response.json()["items"]
        assert card["source_language_iso"] == "eng"
        assert card["source_lemma"] == "grace"
        assert card["senses"] == [
            {"definition": "unearned favour", "examples": ["by grace"]}
        ]

    def test_requesting_the_cards_own_language_returns_it_unchanged(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target, source_lemma="grace")

        response = _get(
            client, regular_token1, target_version_id=target, source_language_iso="eng"
        )

        (card,) = response.json()["items"]
        assert card["source_language_iso"] == "eng"
        assert card["source_lemma"] == "grace"

    def test_overlay_replaces_the_source_side_only(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        revision = _make_revision(db_session, target)
        card_id = _make_card(
            db_session,
            source,
            target,
            target_lemma="neema",
            source_lemma="grace",
            source_surface_forms=["grace", "graces"],
            surface_forms=["neema", "neema-"],
            senses=[{"definition": "unearned favour", "examples": []}],
        )
        example_id = _make_example(
            db_session, card_id, revision, "by grace", "kwa neema"
        )
        _make_overlay(
            db_session,
            card_id,
            "swh",
            source_lemma="rehema",
            source_surface_forms=["rehema"],
            senses=[{"definition": "fadhili", "examples": []}],
            example_translations={example_id: "kwa rehema"},
        )

        response = _get(
            client, regular_token1, target_version_id=target, source_language_iso="swh"
        )

        (card,) = response.json()["items"]
        assert card["source_language_iso"] == "swh"
        assert card["source_lemma"] == "rehema"
        assert card["source_surface_forms"] == ["rehema"]
        assert card["senses"] == [{"definition": "fadhili", "examples": []}]
        assert card["examples"][0]["source"] == "kwa rehema"
        # Target side is shared across every language view.
        assert card["target_lemma"] == "neema"
        assert card["surface_forms"] == ["neema", "neema-"]
        assert card["examples"][0]["target"] == "kwa neema"

    def test_missing_overlay_nulls_the_source_side_including_the_language(
        self, client, db_session, regular_token1
    ):
        """v3's by-id read answers 404 here. v4 reports the state instead."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        revision = _make_revision(db_session, target)
        card_id = _make_card(
            db_session,
            source,
            target,
            source_lemma="grace",
            source_surface_forms=["grace"],
            senses=[{"definition": "unearned favour", "examples": []}],
        )
        _make_example(db_session, card_id, revision, "by grace", "kwa neema")

        response = _get(
            client, regular_token1, target_version_id=target, source_language_iso="swh"
        )

        assert response.status_code == 200
        (card,) = response.json()["items"]
        assert card["source_language_iso"] is None
        assert card["source_lemma"] is None
        assert card["source_surface_forms"] is None
        assert card["senses"] is None
        assert card["examples"][0]["source"] is None
        assert card["examples"][0]["target"] == "kwa neema"

    def test_untranslated_example_falls_back_to_the_canonical_text(
        self, client, db_session, regular_token1
    ):
        """An overlay that translated one example of two does not blank the other."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        revision = _make_revision(db_session, target)
        card_id = _make_card(db_session, source, target)
        first = _make_example(db_session, card_id, revision, "translated", "moja")
        _make_example(db_session, card_id, revision, "untranslated", "mbili")
        _make_overlay(
            db_session,
            card_id,
            "swh",
            source_lemma="rehema",
            example_translations={first: "imetafsiriwa"},
        )

        response = _get(
            client, regular_token1, target_version_id=target, source_language_iso="swh"
        )

        (card,) = response.json()["items"]
        assert [e["source"] for e in card["examples"]] == [
            "imetafsiriwa",
            "untranslated",
        ]

    def test_last_user_edit_is_the_later_of_canonical_and_overlay(
        self, client, db_session, regular_token1
    ):
        """Neither v3 read does this, so a source-only edit was invisible on both.

        v3's merge lives in ``_build_lexeme_card_out_for_lang``, which only its patch
        handlers call — both of its *reads* report the canonical row's timestamp alone.
        So v3 answers one edit time when you write a card and another when you read it
        back. v4 applies the merge on both reads instead; ``TestByIdMatchesTheListRow``
        pins that the two agree.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(
            db_session,
            source,
            target,
            last_user_edit=datetime(2026, 1, 1, 12, 0, 0),
        )
        _make_overlay(
            db_session,
            card_id,
            "swh",
            source_lemma="rehema",
            last_user_edit=datetime(2026, 6, 1, 12, 0, 0),
        )

        response = _get(
            client, regular_token1, target_version_id=target, source_language_iso="swh"
        )

        (card,) = response.json()["items"]
        assert card["last_user_edit"].startswith("2026-06-01")

    def test_an_overlay_in_a_different_language_is_not_served(
        self, client, db_session, regular_token1
    ):
        """Having *an* overlay is not having *the* overlay.

        A card with a Swahili translation, asked for in French, must come back with the
        source side null — not with the Swahili text. This is the case that would catch a
        lookup matching any overlay rather than the requested language.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target, source_lemma="grace")
        _make_overlay(
            db_session,
            card_id,
            "swh",
            source_lemma="rehema",
            source_surface_forms=["rehema"],
        )

        response = _get(
            client, regular_token1, target_version_id=target, source_language_iso="fra"
        )

        assert response.status_code == 200
        (card,) = response.json()["items"]
        assert card["source_language_iso"] is None
        assert card["source_lemma"] is None
        assert card["source_surface_forms"] is None

    def test_overlays_resolve_on_a_later_page(self, client, db_session, regular_token1):
        """Overlays are loaded per page, so page 2 must resolve its own.

        The shape of the real bulk call: many words, many cards, paginated, read in the
        translator's own language.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        # Descending confidence, so the order across pages is deterministic.
        for index, confidence in enumerate([0.9, 0.7, 0.5, 0.3]):
            card_id = _make_card(
                db_session,
                source,
                target,
                target_lemma=f"paged{index}",
                confidence=confidence,
            )
            _make_overlay(db_session, card_id, "swh", source_lemma=f"rehema{index}")

        second = _get(
            client,
            regular_token1,
            target_version_id=target,
            source_language_iso="swh",
            limit=2,
            offset=2,
        )

        assert second.status_code == 200
        cards = second.json()["items"]
        assert [c["target_lemma"] for c in cards] == ["paged2", "paged3"]
        assert [c["source_lemma"] for c in cards] == ["rehema2", "rehema3"]
        assert all(c["source_language_iso"] == "swh" for c in cards)

    def test_language_code_must_be_three_characters(
        self, client, db_session, regular_token1
    ):
        target = _make_version(db_session, "Group1")
        response = _get(
            client, regular_token1, target_version_id=target, source_language_iso="en"
        )
        assert response.status_code == 422


class TestFilters:
    def test_target_word_matches_lemma_or_surface_form(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        by_lemma = _make_card(db_session, source, target, target_lemma="neema")
        by_form = _make_card(
            db_session, source, target, target_lemma="rehema", surface_forms=["neema"]
        )
        _make_card(db_session, source, target, target_lemma="amani")

        response = _get(
            client, regular_token1, target_version_id=target, target_word="neema"
        )

        assert sorted(_ids(response.json())) == sorted([by_lemma, by_form])

    def test_target_word_repeats_for_a_set(self, client, db_session, regular_token1):
        """One repeated parameter replaces v3's ``target_word``/``target_words`` pair."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        first = _make_card(db_session, source, target, target_lemma="neema")
        second = _make_card(db_session, source, target, target_lemma="amani")
        _make_card(db_session, source, target, target_lemma="upendo")

        response = client.get(
            PATH,
            params=[
                ("target_version_id", target),
                ("target_word", "neema"),
                ("target_word", "amani"),
            ],
            headers=_auth(regular_token1),
        )

        assert response.status_code == 200
        assert sorted(_ids(response.json())) == sorted([first, second])

    def test_word_match_is_case_insensitive_and_nfc_normalized(
        self, client, db_session, regular_token1
    ):
        """A decomposed query finds a composed row, which is v3 issue #779's fix.

        Codepoints spelled out rather than pasted: the two spellings are
        indistinguishable in a source file, and a test whose stored and queried strings
        are secretly the same normalization passes without testing anything.
        ``LexemeCardIn`` composes on the way in, so composed is what storage holds.
        """
        composed = "pes\u0107a"  # c-with-acute as one codepoint
        decomposed = "pesc\u0301a"  # c + combining acute
        assert composed != decomposed

        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target, target_lemma=composed)

        response = _get(
            client,
            regular_token1,
            target_version_id=target,
            target_word=decomposed.upper(),
        )

        assert _ids(response.json()) == [card_id]

    def test_source_word_matches_the_canonical_when_no_language_is_asked_for(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(
            db_session, source, target, source_lemma="grace", target_lemma="neema"
        )
        _make_card(
            db_session, source, target, source_lemma="peace", target_lemma="amani"
        )

        response = _get(
            client, regular_token1, target_version_id=target, source_word="grace"
        )

        assert _ids(response.json()) == [card_id]

    def test_source_word_searches_the_requested_language_not_the_canonical(
        self, client, db_session, regular_token1
    ):
        """The v3 behaviour this fixes: filter and projection must agree.

        Two cards. One's canonical source says ``grace`` and its Swahili overlay says
        ``rehema``; the other is the reverse. Asking for ``grace`` in the Swahili view
        must return the card whose *Swahili* text is ``grace`` — because that is the text
        the response will contain — and not the one whose English canonical happens to
        match but whose Swahili the caller is actually shown.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")

        english_grace = _make_card(
            db_session, source, target, source_lemma="grace", target_lemma="neema"
        )
        _make_overlay(db_session, english_grace, "swh", source_lemma="rehema")

        swahili_grace = _make_card(
            db_session, source, target, source_lemma="peace", target_lemma="amani"
        )
        _make_overlay(db_session, swahili_grace, "swh", source_lemma="grace")

        response = _get(
            client,
            regular_token1,
            target_version_id=target,
            source_language_iso="swh",
            source_word="grace",
        )

        assert _ids(response.json()) == [swahili_grace]

    def test_source_word_with_the_cards_own_language_requested(
        self, client, db_session, regular_token1
    ):
        """The canonical branch of the source-word clause, which nothing else covers.

        ``source_language_iso`` equal to the card's own language plus a ``source_word``
        takes the first leg of the ``or_()``, where the same bound word array is reused.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        wanted = _make_card(
            db_session, source, target, source_lemma="grace", target_lemma="neema"
        )
        _make_card(
            db_session, source, target, source_lemma="peace", target_lemma="amani"
        )

        response = _get(
            client,
            regular_token1,
            target_version_id=target,
            source_language_iso="eng",
            source_word="grace",
        )

        assert response.status_code == 200
        assert _ids(response.json()) == [wanted]

    def test_source_version_id_is_matched_exactly(
        self, client, db_session, regular_token1
    ):
        """No pivot rewriting: the filter you send is the filter that runs."""
        source_a = _make_version(db_session, "Group1")
        source_b = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        from_a = _make_card(db_session, source_a, target)
        _make_card(db_session, source_b, target)

        response = _get(
            client,
            regular_token1,
            target_version_id=target,
            source_version_id=source_a,
        )

        assert _ids(response.json()) == [from_a]

    def test_pos_filter(self, client, db_session, regular_token1):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        noun = _make_card(db_session, source, target, pos="noun")
        _make_card(db_session, source, target, pos="verb")

        response = _get(client, regular_token1, target_version_id=target, pos="noun")

        assert _ids(response.json()) == [noun]

    def test_pos_accepts_a_value_outside_the_builders_enum(
        self, client, db_session, regular_token1
    ):
        """Function-word seeding writes these, so declaring an enum would 500 the read."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target, pos="tam_marker")

        response = _get(
            client, regular_token1, target_version_id=target, pos="tam_marker"
        )

        assert response.status_code == 200
        assert _ids(response.json()) == [card_id]

    def test_model_filter_excludes_unstamped_cards(
        self, client, db_session, regular_token1
    ):
        """Most cards are unstamped; asking for a model is not asking for those."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        stamped = _make_card(db_session, source, target, model="anthropic.claude-x")
        _make_card(db_session, source, target, model=None)

        response = _get(
            client,
            regular_token1,
            target_version_id=target,
            model="anthropic.claude-x",
        )

        assert _ids(response.json()) == [stamped]

    def test_a_filter_matching_nothing_is_an_empty_page_not_an_error(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target)

        response = _get(
            client, regular_token1, target_version_id=target, pos="nonexistent"
        )

        assert response.status_code == 200
        payload = response.json()
        # The whole envelope, not just items: an empty page must still be a page.
        assert payload == {
            "items": [],
            "total": 0,
            "limit": RESULT_DEFAULT_LIMIT,
            "offset": 0,
            "next_updated_since": None,
        }

    def test_too_many_words_is_422(self, client, db_session, regular_token1):
        target = _make_version(db_session, "Group1")
        params = [("target_version_id", target)] + [
            ("target_word", f"w{i}") for i in range(201)
        ]
        response = client.get(PATH, params=params, headers=_auth(regular_token1))
        assert response.status_code == 422

    def test_a_blank_word_filter_is_422_not_the_whole_collection(
        self, client, db_session, regular_token1
    ):
        """Failing wide is the wrong direction.

        ``?target_word=`` asks for some cards. Dropping the filter would answer with every
        card, and nothing in the response would say the filter was ignored.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target)

        response = _get(
            client, regular_token1, target_version_id=target, target_word="   "
        )

        assert response.status_code == 422
        assert response.json()["error"]["code"] == "INVALID_WORD_FILTER"
        assert response.json()["error"]["details"] == {"parameter": "target_word"}

    def test_a_blank_alongside_a_real_word_is_dropped(
        self, client, db_session, regular_token1
    ):
        """Only an entirely blank filter is refused; one blank among words is not."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        wanted = _make_card(db_session, source, target, target_lemma="neema")
        _make_card(db_session, source, target, target_lemma="amani")

        response = client.get(
            PATH,
            params=[
                ("target_version_id", target),
                ("target_word", "neema"),
                ("target_word", "  "),
            ],
            headers=_auth(regular_token1),
        )

        assert response.status_code == 200
        assert _ids(response.json()) == [wanted]

    def test_source_word_and_target_word_together(
        self, client, db_session, regular_token1
    ):
        """Both filters in one request, which is where a bind-parameter collision would show.

        The two word lists are bound as separate arrays. If they ever shared a name, one
        would silently overwrite the other and this would return the wrong card.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        # Distinct target lemmas: ix_agent_lexeme_cards_unique_v5 allows only one card
        # per (lower(target_lemma), source_language_iso, target_version_id).
        both = _make_card(
            db_session, source, target, source_lemma="grace", target_lemma="neema"
        )
        _make_card(
            db_session, source, target, source_lemma="grace", target_lemma="amani"
        )
        _make_card(
            db_session, source, target, source_lemma="peace", target_lemma="upendo"
        )

        response = _get(
            client,
            regular_token1,
            target_version_id=target,
            source_word="grace",
            target_word="neema",
        )

        assert response.status_code == 200
        assert _ids(response.json()) == [both]

    def test_v3_lang_is_refused_not_ignored(self, client, db_session, regular_token1):
        """The worst silent failure of the three, so it gets its own test.

        A translator's client sends ``?lang=swh`` asking for Swahili. FastAPI ignores an
        unrecognized query parameter, so without this guard the response is a 200 full of
        English — the canonical source side — and nothing anywhere says the request was
        not honoured.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target)

        response = _get(client, regular_token1, target_version_id=target, lang="swh")

        assert response.status_code == 422
        error = response.json()["error"]
        assert error["code"] == "WITHDRAWN_QUERY_PARAMETER"
        assert error["details"]["parameters"] == {"lang": "source_language_iso"}

    def test_v3_target_words_is_refused_not_ignored(
        self, client, db_session, regular_token1
    ):
        """The bulk read's parameter. Ignored, it would page the whole collection."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target)

        response = _get(
            client,
            regular_token1,
            target_version_id=target,
            target_words="neema,amani",
        )

        assert response.status_code == 422
        assert response.json()["error"]["code"] == "WITHDRAWN_QUERY_PARAMETER"

    def test_every_withdrawn_parameter_is_named_at_once(
        self, client, db_session, regular_token1
    ):
        """A client mid-migration should learn all of its renames in one round trip."""
        target = _make_version(db_session, "Group1")

        response = _get(
            client,
            regular_token1,
            target_version_id=target,
            lang="swh",
            target_words="neema",
            source_words="grace",
        )

        assert response.status_code == 422
        assert response.json()["error"]["details"]["parameters"] == {
            "lang": "source_language_iso",
            "target_words": "target_word",
            "source_words": "source_word",
        }

    def test_an_unrelated_unknown_parameter_is_still_ignored(
        self, client, db_session, regular_token1
    ):
        """Only the three renames are refused; this is not a closed query string.

        Closing the whole query string is a surface-wide decision, not this slice's to
        take, and the shipped v4 reads do not do it.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target)

        response = _get(
            client, regular_token1, target_version_id=target, not_a_parameter="x"
        )

        assert response.status_code == 200
        assert _ids(response.json()) == [card_id]

    def test_target_version_id_is_required(self, client, regular_token1):
        response = client.get(PATH, headers=_auth(regular_token1))
        assert response.status_code == 422


class TestOrderingAndPagination:
    def test_ordered_by_confidence_descending_with_nulls_last(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        low = _make_card(db_session, source, target, confidence=0.1)
        high = _make_card(db_session, source, target, confidence=0.9)
        unscored = _make_card(db_session, source, target, confidence=None)
        middle = _make_card(db_session, source, target, confidence=0.5)

        response = _get(client, regular_token1, target_version_id=target)

        assert _ids(response.json()) == [high, middle, low, unscored]

    def test_id_breaks_a_confidence_tie(self, client, db_session, regular_token1):
        """Without it, offset paging can show one row twice and miss another."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        first = _make_card(db_session, source, target, confidence=0.5)
        second = _make_card(db_session, source, target, confidence=0.5)
        third = _make_card(db_session, source, target, confidence=0.5)

        page_one = _get(client, regular_token1, target_version_id=target, limit=2)
        page_two = _get(
            client, regular_token1, target_version_id=target, limit=2, offset=2
        )

        assert _ids(page_one.json()) == [first, second]
        assert _ids(page_two.json()) == [third]

    def test_total_counts_every_match_not_the_page(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        for _ in range(3):
            _make_card(db_session, source, target)

        response = _get(client, regular_token1, target_version_id=target, limit=1)

        payload = response.json()
        assert len(payload["items"]) == 1
        assert payload["total"] == 3
        assert payload["limit"] == 1
        assert payload["offset"] == 0

    def test_uses_the_result_family_bounds(self, client, db_session, regular_token1):
        target = _make_version(db_session, "Group1")

        default = _get(client, regular_token1, target_version_id=target)
        over = _get(
            client,
            regular_token1,
            target_version_id=target,
            limit=RESULT_MAX_LIMIT + 1,
        )

        assert default.json()["limit"] == RESULT_DEFAULT_LIMIT
        assert over.status_code == 422

    def test_publishes_no_delta_watermark(self, client, db_session, regular_token1):
        """The key is present and null: adding delta later is not a shape change."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target)

        payload = _get(client, regular_token1, target_version_id=target).json()

        assert "next_updated_since" in payload
        assert payload["next_updated_since"] is None

    def test_updated_since_is_not_a_parameter(self, client, db_session, regular_token1):
        """An unknown query parameter is ignored, so this pins the schema instead."""
        from api_v4.app import create_v4_app

        spec = create_v4_app(configure_cors=lambda app: None).openapi()
        names = {p["name"] for p in spec["paths"]["/lexeme-cards"]["get"]["parameters"]}
        assert "updated_since" not in names


class TestMalformedJsonbIsRepaired:
    """A legacy row must not turn a read into a 500."""

    def test_senses_holding_bare_strings(self, client, db_session, regular_token1):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target, senses=["just a definition"])

        response = _get(client, regular_token1, target_version_id=target)

        assert response.status_code == 200
        (card,) = response.json()["items"]
        assert card["senses"] == [{"definition": "just a definition", "examples": []}]

    def test_senses_holding_an_object_instead_of_an_array(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target, senses={"definition": "not an array"})

        response = _get(client, regular_token1, target_version_id=target)

        assert response.status_code == 200
        assert response.json()["items"][0]["senses"] is None

    def test_sense_missing_its_definition(self, client, db_session, regular_token1):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target, senses=[{"examples": ["a", "b"]}])

        response = _get(client, regular_token1, target_version_id=target)

        assert response.status_code == 200
        assert response.json()["items"][0]["senses"] == [
            {"definition": "", "examples": ["a", "b"]}
        ]

    def test_surface_forms_holding_a_mixed_array(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target, surface_forms=["ok", 7, None])

        response = _get(client, regular_token1, target_version_id=target)

        assert response.status_code == 200
        assert response.json()["items"][0]["surface_forms"] == ["ok"]

    def test_a_malformed_surface_forms_column_does_not_break_a_word_filter(
        self, client, db_session, regular_token1
    ):
        """``jsonb_typeof`` guards the unnest, so the row is skipped rather than raising."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(
            db_session, source, target, target_lemma="broken", surface_forms={"a": 1}
        )
        wanted = _make_card(db_session, source, target, target_lemma="neema")

        response = _get(
            client, regular_token1, target_version_id=target, target_word="neema"
        )

        assert response.status_code == 200
        assert _ids(response.json()) == [wanted]

    def test_nan_confidence_does_not_break_the_response_body(
        self, client, db_session, regular_token1
    ):
        """PostgreSQL ``numeric`` accepts NaN; JSON does not.

        Served raw, it reaches the wire as the bare literal ``NaN``. That is not valid
        JSON, so a strict parser rejects the entire body while the status line still says
        200 — the client sees a broken response, not a broken field.
        """
        from decimal import Decimal

        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target, confidence=Decimal("NaN"))

        response = _get(client, regular_token1, target_version_id=target)

        assert response.status_code == 200
        # .json() is what proves it: a NaN literal in the body raises here.
        (card,) = response.json()["items"]
        assert card["id"] == card_id
        assert card["confidence"] is None

    def test_an_oversized_alignment_score_does_not_500(
        self, client, db_session, regular_token1
    ):
        """``jsonb`` numbers are arbitrary precision; a Python float is not.

        A stored integer too large to convert raises ``OverflowError``, which would be a
        500 on a read that can otherwise serve the row perfectly well.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(
            db_session,
            source,
            target,
            alignment_scores={"huge": 10**400, "god": 0.9},
        )

        response = _get(client, regular_token1, target_version_id=target)

        assert response.status_code == 200
        assert response.json()["items"][0]["alignment_scores"] == {"god": 0.9}

    def test_alignment_scores_drops_non_numeric_values(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(
            db_session,
            source,
            target,
            alignment_scores={"god": 1.23, "bad": "high", "flag": True},
        )

        response = _get(client, regular_token1, target_version_id=target)

        assert response.status_code == 200
        assert response.json()["items"][0]["alignment_scores"] == {"god": 1.23}


class TestSourceLanguageTrigger:
    def test_the_database_fills_source_language_iso_when_a_writer_omits_it(
        self, client, db_session, regular_token1
    ):
        """``trg_fill_lexeme_card_source_language_iso``, installed by an after_create DDL.

        Pinned here because a local database created before the trigger existed silently
        lacks it, and the symptom is a confusing NOT NULL violation rather than a missing
        trigger. See CLAUDE.md on recreating the volume.
        """
        source = _make_version(db_session, "Group1", iso_language="swh")
        target = _make_version(db_session, "Group1")
        card = AgentLexemeCard(
            target_lemma=f"trigger{next(_names)}",
            source_version_id=source,
            target_version_id=target,
            source_language_iso=None,
            confidence=0.5,
        )
        db_session.add(card)
        db_session.commit()
        db_session.refresh(card)

        assert card.source_language_iso == "swh"

        response = _get(client, regular_token1, target_version_id=target)
        (served,) = [item for item in response.json()["items"] if item["id"] == card.id]
        assert served["source_language_iso"] == "swh"


class TestRowShape:
    def test_serves_every_documented_field(self, client, db_session, regular_token1):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        revision = _make_revision(db_session, target)
        card_id = _make_card(
            db_session,
            source,
            target,
            target_lemma="neema",
            source_lemma="grace",
            surface_forms=["neema"],
            source_surface_forms=["grace"],
            senses=[{"definition": "favour", "examples": []}],
            confidence=0.75,
            pos="noun",
            model="anthropic.claude-x",
            alignment_scores={"grace": 0.9},
            english_lemma="grace",
        )
        example_id = _make_example(
            db_session, card_id, revision, "by grace", "kwa neema"
        )

        (card,) = _get(client, regular_token1, target_version_id=target).json()["items"]

        # created_at/last_updated are server-stamped, so they are checked for shape
        # rather than value — but checked, not compared against themselves.
        for stamp in ("created_at", "last_updated"):
            assert isinstance(card[stamp], str), f"{stamp} should be an ISO timestamp"
            datetime.fromisoformat(card[stamp])

        assert card == {
            "id": card_id,
            "target_lemma": "neema",
            "source_lemma": "grace",
            "source_version_id": source,
            "target_version_id": target,
            "source_language_iso": "eng",
            "pos": "noun",
            "surface_forms": ["neema"],
            "source_surface_forms": ["grace"],
            "senses": [{"definition": "favour", "examples": []}],
            "examples": [
                {
                    "id": example_id,
                    "revision_id": revision,
                    "source": "by grace",
                    "target": "kwa neema",
                }
            ],
            "confidence": 0.75,
            "english_lemma": "grace",
            "alignment_scores": {"grace": 0.9},
            "build_version": None,
            "model": "anthropic.claude-x",
            "created_at": card["created_at"],
            "last_updated": card["last_updated"],
            "last_user_edit": None,
        }

    def test_has_translation_overlay_is_not_on_the_wire(
        self, client, db_session, regular_token1
    ):
        """v3's boolean, withdrawn under guide §10's bare-boolean rule."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        _make_card(db_session, source, target)

        (card,) = _get(client, regular_token1, target_version_id=target).json()["items"]

        assert "has_translation_overlay" not in card


class TestByIdAuthorization:
    """The target-version rule, reached from the card rather than from the version."""

    def test_reads_a_card_whose_target_version_the_caller_reaches(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target)

        response = _get_by_id(client, regular_token1, card_id)

        assert response.status_code == 200
        assert response.json()["id"] == card_id

    def test_source_access_alone_does_not_grant_the_card(
        self, client, db_session, regular_token1, regular_token2
    ):
        """The list read's central rule, pinned again on the path that bypasses it.

        ``test_source_access_alone_does_not_grant_the_cards`` proves the list cannot be
        used to read another project's dictionary. That proof does not carry to this
        route: the list is authorized on a ``target_version_id`` the caller typed, while
        here the version is one the caller never mentioned and the handler had to go and
        find. A by-id read that skipped the lookup would be a hole in the same wall.
        """
        source = _make_version(db_session, "Group2")  # testuser2 only
        target = _make_version(db_session, "Group1")  # testuser1 only
        card_id = _make_card(db_session, source, target)

        source_only = _get_by_id(client, regular_token2, card_id)
        target_only = _get_by_id(client, regular_token1, card_id)

        assert source_only.status_code == 404
        assert _error(source_only)["code"] == "LEXEME_CARD_NOT_FOUND"

        assert target_only.status_code == 200
        assert target_only.json()["id"] == card_id

    def test_a_hidden_card_is_indistinguishable_from_one_that_does_not_exist(
        self, client, db_session, regular_token1
    ):
        """The whole point of the shared code: card ids are enumerable.

        ``target_version_id`` is supplied by the caller, so the list read can name it in
        a refusal. A card id is not — it is a bare sequential integer — so a refusal that
        distinguished "not yours" from "no such row" would let anyone walk the space and
        count another project's dictionary. Message and ``details`` are compared too, not
        just the code: either one differing would be the same oracle.
        """
        hidden_target = _make_version(db_session, "Group2")
        hidden_card = _make_card(
            db_session, hidden_target, hidden_target, target_lemma="hiddenlemma"
        )
        missing_card = 99_999_999

        hidden = _get_by_id(client, regular_token1, hidden_card)
        missing = _get_by_id(client, regular_token1, missing_card)

        assert hidden.status_code == missing.status_code == 404
        assert (
            _error(hidden)["code"] == _error(missing)["code"] == "LEXEME_CARD_NOT_FOUND"
        )
        assert _error(hidden)["details"] == {"card_id": hidden_card}
        assert _error(missing)["details"] == {"card_id": missing_card}
        assert _error(hidden)["message"].replace(str(hidden_card), "N") == _error(
            missing
        )["message"].replace(str(missing_card), "N")

    def test_the_refusal_never_says_version_not_found(
        self, client, db_session, regular_token1
    ):
        """The specific leak the service catches and re-raises.

        Letting the version signal out would answer ``VERSION_NOT_FOUND`` for a card that
        exists and ``LEXEME_CARD_NOT_FOUND`` for one that does not — the probe restated
        in a different field.
        """
        hidden_target = _make_version(db_session, "Group2")
        card_id = _make_card(
            db_session, hidden_target, hidden_target, target_lemma="othergroupslemma"
        )

        body = _get_by_id(client, regular_token1, card_id).json()

        assert body["error"]["code"] != "VERSION_NOT_FOUND"
        assert str(hidden_target) not in body["error"]["message"]
        assert "version_id" not in body["error"]["details"]

    def test_soft_deleted_target_version_is_404(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target)
        version = db_session.query(BibleVersion).filter_by(id=target).first()
        version.deleted = True
        db_session.commit()

        response = _get_by_id(client, regular_token1, card_id)

        assert response.status_code == 404
        assert _error(response)["code"] == "LEXEME_CARD_NOT_FOUND"

    def test_admin_reads_a_card_no_group_grants(self, client, db_session, admin_token):
        source = _make_version(db_session, "Group2")
        target = _make_version(db_session, "Group2")
        card_id = _make_card(db_session, source, target)

        response = _get_by_id(client, admin_token, card_id)

        assert response.status_code == 200
        assert response.json()["id"] == card_id

    def test_admin_does_not_bypass_a_soft_deleted_target_version(
        self, client, db_session, admin_token
    ):
        """Admin reaches past a missing grant, not past a deletion.

        ``version_service.get_version`` passes ``include_deleted=False`` for everyone, so
        soft-deleting a translation withdraws its cards from administrators too. Worth
        pinning separately from the two halves either side of it: an admin bypass added
        to that helper later would break no other test here.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target)
        version = db_session.query(BibleVersion).filter_by(id=target).first()
        version.deleted = True
        db_session.commit()

        response = _get_by_id(client, admin_token, card_id)

        assert response.status_code == 404
        assert _error(response)["code"] == "LEXEME_CARD_NOT_FOUND"

    def test_an_unreachable_source_version_does_not_hide_the_card(
        self, client, db_session, regular_token2
    ):
        """The counterpart of ``test_source_version_id_is_served_even_when_unreachable``.

        Cards are pivot-routed, so ``source_version_id`` routinely names a shared Bible
        the reading project has no grant on. Checking it here would make most cards
        unreadable by id while the same rows list fine — the two reads must agree.
        """
        hidden_source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group2")
        card_id = _make_card(db_session, hidden_source, target)

        response = _get_by_id(client, regular_token2, card_id)

        assert response.status_code == 200
        assert response.json()["source_version_id"] == hidden_source

    def test_unauthenticated_is_401(self, client, db_session):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target)

        assert client.get(f"{PATH}/{card_id}").status_code == 401

    def test_a_non_integer_card_id_is_a_validation_error(
        self, client, db_session, regular_token1
    ):
        response = _get_by_id(client, regular_token1, "not-a-number")

        assert response.status_code == 422
        assert _error(response)["code"] == "VALIDATION_ERROR"

    def test_an_id_too_large_for_the_column_is_a_404_not_a_500(
        self, client, db_session, regular_token1
    ):
        """``agent_lexeme_cards.id`` is a 32-bit integer; the path parameter is not.

        Without the range guard, asyncpg refuses to encode the bind parameter and the
        route answers 500 for an id that provably names no card — breaking the rule the
        rest of this class pins, that every id you cannot have is the same 404. Both
        signs, because FastAPI parses a leading minus into an ``int`` just as happily.
        """
        for card_id in (2**31, 2**64, -(2**31) - 1):
            response = _get_by_id(client, regular_token1, card_id)

            assert response.status_code == 404, f"{card_id}: {response.text}"
            assert _error(response)["code"] == "LEXEME_CARD_NOT_FOUND"
            assert _error(response)["details"] == {"card_id": card_id}

    def test_the_largest_storable_id_is_still_served(
        self, client, db_session, regular_token1
    ):
        """The bound is inclusive, so the guard must not swallow a real id.

        A card is inserted *at* the maximum rather than merely asked for, because a 404
        cannot tell "looked up and absent" from "refused before the lookup" — the
        off-by-one this is here to catch would pass a test that only asserted 404. Every
        other fixture card gets a small sequential id, so nothing else exercises the
        boundary.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card = AgentLexemeCard(
            id=2**31 - 1,
            source_lemma="grace",
            target_lemma="maxidlemma",
            source_version_id=source,
            target_version_id=target,
            source_language_iso="eng",
        )
        db_session.add(card)
        db_session.commit()

        response = _get_by_id(client, regular_token1, 2**31 - 1)

        assert response.status_code == 200, response.text
        assert response.json()["id"] == 2**31 - 1


class TestByIdExampleVisibility:
    """v3's per-revision example filter, unchanged and applied by the shared loader."""

    def test_example_from_a_revision_the_caller_cannot_reach_is_hidden(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        unreachable = _make_version(db_session, "Group2")
        readable_revision = _make_revision(db_session, target)
        hidden_revision = _make_revision(db_session, unreachable)
        card_id = _make_card(db_session, source, target)
        readable = _make_example(
            db_session, card_id, readable_revision, "by grace", "kwa neema"
        )
        _make_example(db_session, card_id, hidden_revision, "hidden", "siri")

        card = _get_by_id(client, regular_token1, card_id).json()

        assert [example["id"] for example in card["examples"]] == [readable]

    def test_admin_sees_every_example(self, client, db_session, admin_token):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        unreachable = _make_version(db_session, "Group2")
        card_id = _make_card(db_session, source, target)
        first = _make_example(
            db_session, card_id, _make_revision(db_session, target), "a", "b"
        )
        second = _make_example(
            db_session, card_id, _make_revision(db_session, unreachable), "c", "d"
        )

        card = _get_by_id(client, admin_token, card_id).json()

        assert [example["id"] for example in card["examples"]] == [first, second]

    def test_only_this_cards_examples_are_served(
        self, client, db_session, regular_token1
    ):
        """The loader is given a one-card list; nothing else on the version may leak in."""
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        revision = _make_revision(db_session, target)
        wanted = _make_card(db_session, source, target, target_lemma="wantedlemma")
        other = _make_card(db_session, source, target, target_lemma="otherlemma")
        mine = _make_example(db_session, wanted, revision, "mine", "yangu")
        _make_example(db_session, other, revision, "theirs", "yao")

        card = _get_by_id(client, regular_token1, wanted).json()

        assert [example["id"] for example in card["examples"]] == [mine]


class TestByIdLanguageOverlay:
    """Where v3's by-id read and v4's part company."""

    def test_a_missing_overlay_is_served_rather_than_404(
        self, client, db_session, regular_token1
    ):
        """The single biggest behavioural difference from v3's by-id read.

        v3 answers ``404`` for a language the card has no translation into, so that the
        caller triggers a derivation pipeline. v4 reports the state: the card comes back
        with its whole source side null, ``source_language_iso`` included, and the target
        side intact — which is what the list read already does, and what a client needs
        in order to render the target while the translation is missing.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        revision = _make_revision(db_session, target)
        card_id = _make_card(
            db_session,
            source,
            target,
            target_lemma="neemamissing",
            source_lemma="grace",
            source_surface_forms=["grace", "graces"],
            senses=[{"definition": "unearned favour", "examples": []}],
        )
        _make_example(db_session, card_id, revision, "by grace", "kwa neema")

        response = _get_by_id(
            client, regular_token1, card_id, source_language_iso="swh"
        )

        assert response.status_code == 200
        card = response.json()
        assert card["source_language_iso"] is None
        assert card["source_lemma"] is None
        assert card["source_surface_forms"] is None
        assert card["senses"] is None
        assert [example["source"] for example in card["examples"]] == [None]
        # The target side is untouched — that is the point of serving the row at all.
        assert card["target_lemma"] == "neemamissing"
        assert [example["target"] for example in card["examples"]] == ["kwa neema"]

    def test_overlay_replaces_the_source_side_only(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        revision = _make_revision(db_session, target)
        card_id = _make_card(
            db_session,
            source,
            target,
            target_lemma="neema",
            source_lemma="grace",
            surface_forms=["neema"],
            source_surface_forms=["grace"],
            senses=[{"definition": "unearned favour", "examples": []}],
        )
        example_id = _make_example(
            db_session, card_id, revision, "by grace", "kwa neema"
        )
        _make_overlay(
            db_session,
            card_id,
            "swh",
            source_lemma="rehema",
            source_surface_forms=["rehema"],
            senses=[{"definition": "fadhili", "examples": []}],
            example_translations={example_id: "kwa rehema"},
        )

        card = _get_by_id(
            client, regular_token1, card_id, source_language_iso="swh"
        ).json()

        assert card["source_language_iso"] == "swh"
        assert card["source_lemma"] == "rehema"
        assert card["source_surface_forms"] == ["rehema"]
        assert card["senses"] == [{"definition": "fadhili", "examples": []}]
        assert card["examples"] == [
            {
                "id": example_id,
                "revision_id": revision,
                "source": "kwa rehema",
                "target": "kwa neema",
            }
        ]
        # One target column, projected by every language view.
        assert card["target_lemma"] == "neema"
        assert card["surface_forms"] == ["neema"]

    def test_untranslated_example_falls_back_to_the_canonical_text(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        revision = _make_revision(db_session, target)
        card_id = _make_card(db_session, source, target)
        translated = _make_example(
            db_session, card_id, revision, "by grace", "kwa neema"
        )
        untranslated = _make_example(
            db_session, card_id, revision, "of grace", "ya neema"
        )
        _make_overlay(
            db_session,
            card_id,
            "swh",
            source_lemma="rehema",
            example_translations={translated: "kwa rehema"},
        )

        card = _get_by_id(
            client, regular_token1, card_id, source_language_iso="swh"
        ).json()

        assert {e["id"]: e["source"] for e in card["examples"]} == {
            translated: "kwa rehema",
            untranslated: "of grace",
        }

    def test_requesting_the_cards_own_language_returns_it_unchanged(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(
            db_session, source, target, source_lemma="grace", source_language_iso="eng"
        )
        _make_overlay(db_session, card_id, "swh", source_lemma="rehema")

        card = _get_by_id(
            client, regular_token1, card_id, source_language_iso="eng"
        ).json()

        assert card["source_language_iso"] == "eng"
        assert card["source_lemma"] == "grace"

    def test_an_overlay_in_a_different_language_is_not_served(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target, source_lemma="grace")
        _make_overlay(db_session, card_id, "swh", source_lemma="rehema")

        card = _get_by_id(
            client, regular_token1, card_id, source_language_iso="fra"
        ).json()

        assert card["source_language_iso"] is None
        assert card["source_lemma"] is None

    def test_the_language_code_is_matched_case_insensitively(
        self, client, db_session, regular_token1
    ):
        """Overlays are stored lowercase; the caller's casing must not decide the answer.

        Nothing else in this module sends anything but a lowercase code, so the
        ``.lower()`` both reads depend on has no other test standing over it — and its
        failure mode is a silent one: an uppercase request would report the card as
        having no translation rather than erroring.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target, source_lemma="grace")
        _make_overlay(db_session, card_id, "swh", source_lemma="rehema")

        card = _get_by_id(
            client, regular_token1, card_id, source_language_iso="SWH"
        ).json()

        assert card["source_language_iso"] == "swh"
        assert card["source_lemma"] == "rehema"

    def test_language_code_must_be_three_characters(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target)

        response = _get_by_id(client, regular_token1, card_id, source_language_iso="en")

        assert response.status_code == 422


class TestByIdWithdrawnParameters:
    def test_v3_lang_is_refused_not_ignored(self, client, db_session, regular_token1):
        """The one rename that reaches this route, and the reason the guard is here.

        ``?lang=swh`` ignored would serve the English canonical to a translator who asked
        for Swahili — a wrong answer wearing a 200, which is the failure the guard exists
        to convert into an error.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target)

        response = _get_by_id(client, regular_token1, card_id, lang="swh")

        assert response.status_code == 422
        assert _error(response)["code"] == "WITHDRAWN_QUERY_PARAMETER"
        assert _error(response)["details"] == {
            "parameters": {"lang": "source_language_iso"}
        }

    def test_a_word_filter_is_not_refused_here(
        self, client, db_session, regular_token1
    ):
        """The by-id guard is a subset, deliberately.

        v3's by-id read never took ``target_words`` or ``source_words``, so no client can
        arrive here still sending them, and this route has no word filter for one to
        silently drop. On this path they are unrecognized keys like any other and are
        ignored — the same treatment
        ``test_an_unrelated_unknown_parameter_is_still_ignored`` pins on the list.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target)

        response = _get_by_id(
            client, regular_token1, card_id, target_words="neema", source_words="grace"
        )

        assert response.status_code == 200
        assert response.json()["id"] == card_id


class TestByIdMalformedJsonbIsRepaired:
    """The router's conversion helpers are shared, so this is a wiring check."""

    def test_senses_holding_a_bare_string(self, client, db_session, regular_token1):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target, senses=["a bare string"])

        card = _get_by_id(client, regular_token1, card_id).json()

        assert card["senses"] == [{"definition": "a bare string", "examples": []}]

    def test_nan_confidence_does_not_break_the_response_body(
        self, client, db_session, regular_token1
    ):
        from decimal import Decimal

        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(db_session, source, target, confidence=Decimal("NaN"))

        response = _get_by_id(client, regular_token1, card_id)

        assert response.status_code == 200
        # Against the raw text, not the parsed body: Python's json.loads accepts a bare
        # ``NaN`` literal, so ``.json()`` alone would not catch one reaching the wire.
        assert "NaN" not in response.text
        assert response.json()["confidence"] is None


class TestByIdMatchesTheListRow:
    """The decision this PR turns on: by-id serves one row of the list, not more.

    Both reads resolve a card through ``_views_for`` and convert it through
    ``_to_lexeme_card_out``, so the two bodies are built by the same code. These compare
    them anyway — a divergence would be someone adding a by-id-only field later, which is
    exactly what the shared resolver exists to prevent.
    """

    def _both(self, client, token, target, card_id, **params):
        listed = _get(client, token, target_version_id=target, **params)
        assert listed.status_code == 200, listed.text
        (row,) = [item for item in listed.json()["items"] if item["id"] == card_id]

        single = _get_by_id(client, token, card_id, **params)
        assert single.status_code == 200, single.text
        return row, single.json()

    def test_canonical_read(self, client, db_session, regular_token1):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        revision = _make_revision(db_session, target)
        card_id = _make_card(
            db_session,
            source,
            target,
            target_lemma="neema",
            source_lemma="grace",
            surface_forms=["neema"],
            source_surface_forms=["grace"],
            senses=[{"definition": "favour", "examples": ["by grace"]}],
            confidence=0.75,
            pos="noun",
            model="anthropic.claude-x",
            alignment_scores={"grace": 0.9},
            english_lemma="grace",
            last_user_edit=datetime(2026, 1, 1, 12, 0, 0),
        )
        _make_example(db_session, card_id, revision, "by grace", "kwa neema")

        row, single = self._both(client, regular_token1, target, card_id)

        assert single == row

    def test_overlaid_read(self, client, db_session, regular_token1):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        revision = _make_revision(db_session, target)
        card_id = _make_card(
            db_session, source, target, target_lemma="neema2", source_lemma="grace"
        )
        example_id = _make_example(
            db_session, card_id, revision, "by grace", "kwa neema"
        )
        _make_overlay(
            db_session,
            card_id,
            "swh",
            source_lemma="rehema",
            senses=[{"definition": "fadhili", "examples": []}],
            example_translations={example_id: "kwa rehema"},
        )

        row, single = self._both(
            client, regular_token1, target, card_id, source_language_iso="swh"
        )

        assert single == row
        assert single["source_lemma"] == "rehema"

    def test_missing_overlay_read(self, client, db_session, regular_token1):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(
            db_session, source, target, target_lemma="neema3", source_lemma="grace"
        )

        row, single = self._both(
            client, regular_token1, target, card_id, source_language_iso="fra"
        )

        assert single == row
        assert single["source_language_iso"] is None

    def test_last_user_edit_agrees_when_the_overlay_is_the_later_edit(
        self, client, db_session, regular_token1
    ):
        """The one field v3 reports differently depending on how you reached the card.

        v3's merge of canonical and overlay ``last_user_edit`` lives in the helper its
        patch handlers use, so a v3 client sees one timestamp on a write and an older one
        on either read. v4 merges on both reads; this pins that the two agree.
        """
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        card_id = _make_card(
            db_session,
            source,
            target,
            target_lemma="neema4",
            last_user_edit=datetime(2026, 1, 1, 12, 0, 0),
        )
        _make_overlay(
            db_session,
            card_id,
            "swh",
            source_lemma="rehema",
            last_user_edit=datetime(2026, 6, 1, 12, 0, 0),
        )

        row, single = self._both(
            client, regular_token1, target, card_id, source_language_iso="swh"
        )

        assert single == row
        assert single["last_user_edit"].startswith("2026-06-01")

    def test_examples_hidden_from_the_list_are_hidden_here_too(
        self, client, db_session, regular_token1
    ):
        source = _make_version(db_session, "Group1")
        target = _make_version(db_session, "Group1")
        unreachable = _make_version(db_session, "Group2")
        card_id = _make_card(db_session, source, target, target_lemma="neema5")
        visible = _make_example(
            db_session, card_id, _make_revision(db_session, target), "seen", "ona"
        )
        _make_example(
            db_session, card_id, _make_revision(db_session, unreachable), "unseen", "x"
        )

        row, single = self._both(client, regular_token1, target, card_id)

        assert single == row
        assert [example["id"] for example in single["examples"]] == [visible]

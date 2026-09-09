"""Tests for ``GET /v4/lexeme-cards`` (issue #896, epic #842).

The first half of the lexeme-card slice: the list read that ``aqua-django-app`` calls
twice over, once per word and once in bulk.

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
        """v3 does this on its by-id read only, so the two reads disagreed."""
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
        assert response.json()["items"] == []
        assert response.json()["total"] == 0

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
        _make_example(db_session, card_id, revision, "by grace", "kwa neema")

        (card,) = _get(client, regular_token1, target_version_id=target).json()["items"]

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
                    "id": card["examples"][0]["id"],
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

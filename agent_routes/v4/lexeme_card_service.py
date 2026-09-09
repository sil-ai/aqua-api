"""Data access for the v4 lexeme-card reads (issue #896, epic #842).

The last of the agent family, and the half of it that is *not* assessment output.
:mod:`agent_routes.v4.agent_routes` serves critique issues and agent translations, which
hang off a run; a lexeme card is a dictionary entry keyed on a version pair, outliving
any run, so guide §15.7 promotes it to a top-level collection with no assessment to nest
under. That difference is the whole reason this is a separate module: the two families
share a package and share nothing else, least of all their authorization.

:mod:`agent_routes.v4.lexeme_card_routes` owns the HTTP half; this module owns queries
and the language-overlay resolution, and nothing else.


Authorization, which v3 does not have
-------------------------------------

**v3 gates lexeme cards on nothing.** ``GET /agent/lexeme-card`` and
``GET /agent/lexeme-card/{card_id}`` take a ``current_user`` and never consult it about
the card: any authenticated caller reads any card for any version pair. The only check
either performs is on *examples*, which are filtered to revisions the caller can reach.
That is not a shape v4 can copy, and it is not a v3 bug this slice fixes either — v3 is
frozen, and the fix belongs to the version a client can migrate onto.

So v4 requires the caller to reach the **target version**, resolved through
:func:`bible_routes.v4.version_service.get_version` rather than re-derived here, which is
the same delegation :func:`bible_routes.v4.revision_service._require_visible_version`
makes and for the same reason: "a version this caller may see" has one implementation on
the v4 surface. An unreachable ``target_version_id`` is a ``404``, not an empty page,
because it names the scope this collection is read within rather than narrowing an
already-authorized set — the distinction ``/v4/revisions``' ``version_id`` filter draws.
``source_version_id`` is checked on the same terms, but only when the caller sends it.

**The by-id read applies the same rule from the other end.** The list read is handed a
``target_version_id`` and returns the cards under it; the by-id read is handed a card and
must reach the version *it* names. One helper serves both, so the two cannot come apart.
What differs is only what may be said about a refusal: the list read names the version
back to the caller who supplied it, while the by-id read reports nothing but the card id
it was given — see :class:`LexemeCardNotFound`.

**Why the target version alone, and not both.** The strictest reading would demand access
to source *and* target, the way ``is_user_authorized_for_assessment`` demands the
revision's version and the reference's. It was rejected on what the data actually looks
like: cards are pivot-routed, so ``source_version_id`` usually names a shared pivot Bible
that a project's own group has no grant on. Requiring it would empty the card list for
callers who can plainly see the translation the cards describe, which is a regression
dressed as a tightening. The source side is not left unguarded — the source-language
material a caller can read is exactly what the overlay for their language holds, and
example text stays behind v3's per-revision filter, kept here unchanged.

Examples keep that filter verbatim: a non-admin sees an example only when its revision
belongs to a version — source **or** target — one of their groups reaches. Admins see
every example. Two callers can therefore hold the same card with different examples,
which the response documents rather than hides.


Pivot routing is not carried
----------------------------

v3 rewrites ``source_version_id`` through ``_effective_source_version_expr``: the value
you send is replaced by the pivot Bible registered for the target's language, if there is
one, so the pivot stays invisible. v4 matches ``source_version_id`` exactly instead.

Three reasons. Guide §15.7 rules the pivot endpoints v3-only, so a v4 read silently
resolving through ``language_pivot`` / ``pivot_candidate`` would import a subsystem v4
does not otherwise acknowledge. The rewrite is a *hidden* rewrite — the filter you send
is not the filter that runs, and nothing in the response says so. And it has no v4
caller: the one client that sends ``source_version_id`` at all is the runner, which stays
on v3. A v4 caller that wants a specific source pair reads ``source_version_id`` off the
rows and sends that back, which is reproducible in a way the rewrite is not.


No delta feed
-------------

No ``updated_since``, and the reason is the ``agent_critique_issue`` reason plus two
more. ``agent_lexeme_cards.last_updated`` is **nullable with no database default and no
trigger** — its default is applied in Python, so a row written outside the ORM has none,
and such a row would be silently absent from every window rather than merely late. The
overlay's ``card_translations.last_updated`` is ``NOT NULL`` with a ``now()`` server
default but has *no* ``onupdate`` and no trigger, so it is stamped once at insert and
never moves again on its own; v3's patch handlers do set it by hand, which means the
column tracks writes made through v3 and nothing else. And a card can be hard-deleted —
v3 has two DELETE endpoints the runner calls on every rebuild — with no tombstone, so no
watermark can carry the removal. A feed that looked like it worked while dropping
unstamped rows and every deletion is the failure the delta contract exists to refuse.
"""

__version__ = "v4"

import unicodedata
from dataclasses import dataclass, field
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy import and_, bindparam, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from bible_routes.v4 import version_service
from database.models import (
    AgentLexemeCard,
    AgentLexemeCardExample,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    CardTranslation,
    CardTranslationExample,
    UserDB,
    UserGroup,
)

#: Ceiling on the repeated ``?source_word=`` / ``?target_word=`` filters. Taken from the
#: one known client, whose bulk card lookup caps its own request at 200 words before
#: calling: matching that means the cap can only be reached by a caller doing something
#: the existing UI does not, and a 422 naming the bound is a better answer for that caller
#: than a query the planner turns into a sequential scan.
MAX_WORD_FILTER = 200

#: Bounds of ``agent_lexeme_cards.id``, which is a PostgreSQL ``integer`` — checked
#: against the column rather than read off the model, which types it as a bare
#: ``Integer`` that says nothing about width.
#:
#: A path id outside them reaches asyncpg as a bind parameter it cannot encode, which
#: raises before the statement runs and leaves the route answering ``500`` for an id that
#: provably names no card. :func:`get_lexeme_card` refuses those up front instead, so
#: "every id that does not name a card you may read is the same 404" holds for every
#: value FastAPI will parse into an ``int`` rather than only the ones that fit.
#:
#: The same gap is open across the rest of the v4 surface — ``/v4/versions/{id}``,
#: ``/v4/revisions/{id}`` and ``/v4/assessments/{id}`` all raise on an out-of-range id —
#: and closing it there means a shared bounded path-id type, not a copy of this constant
#: per module. This guard is deliberately local: it covers the one parameter this route
#: owns and claims nothing about the others.
_CARD_ID_MIN = -(2**31)
_CARD_ID_MAX = 2**31 - 1


class LexemeCardServiceError(Exception):
    """Base class for the signals this module raises."""


class VersionNotVisible(LexemeCardServiceError):
    """A ``version_id`` naming this collection's scope is not visible.

    Covers "no such version", "not accessible to this caller" and "soft-deleted" with one
    signal, for the reason :class:`bible_routes.v4.revision_service.VersionNotVisible`
    gives: reporting them apart would tell a caller which version ids exist.
    """

    def __init__(self, version_id: int) -> None:
        self.version_id = version_id
        super().__init__(f"Version {version_id} does not exist.")


class LexemeCardNotFound(LexemeCardServiceError):
    """No card with this id that this caller may read.

    Three cases behind one signal: no row with that id at all, a row whose target
    version the caller has no grant on, and a row whose target version is soft-deleted.
    The by-id read collapses them deliberately. A card id is a bare sequential integer,
    so the whole space is enumerable, and any status code that told "not yours" apart
    from "no such card" would be an oracle for which cards exist on translations the
    caller cannot see — including how many a project has.

    Distinct from :class:`VersionNotVisible`, which the list read raises for the same
    underlying refusal, because the two name different resources. There the caller
    supplied the ``target_version_id`` themselves and it names the scope of the whole
    read, so reporting it back is not a disclosure; here the version is one the caller
    never mentioned and learned nothing about, so the answer is about the card.

    **The collapse is in the response, not in the cost.** A missing id is refused after
    one indexed lookup; a card that exists but is not yours is refused after a second,
    joined one, so the two are in principle separable by timing. Closing that would mean
    scoping the card lookup by visibility in a single statement, the way
    :func:`assessment_routes.v4.assessment_service.get_assessment` does — and the only
    way to do it without a second copy of the version predicate here, which
    :func:`_require_visible_version` exists to avoid, is a public visibility subquery on
    :mod:`bible_routes.v4.version_service` that does not exist yet. Left as is: what the
    channel yields is bare existence with no owner attached, at a cost that network
    jitter dominates, and a duplicated access predicate that could drift from
    ``/v4/versions`` is the worse risk of the two.
    """

    def __init__(self, card_id: int) -> None:
        self.card_id = card_id
        super().__init__(f"Lexeme card {card_id} does not exist.")


class InvalidWordFilter(LexemeCardServiceError):
    """A word filter was sent, but nothing in it is a word.

    ``?target_word=`` or ``?target_word=%20`` reaches the handler as a list holding an
    empty or blank string. Treating that as "no filter" would answer a request that asked
    for *some* cards with *every* card, which is the wrong direction to fail in: the
    caller gets more than they asked for and nothing says the filter was dropped. v3 makes
    the same call on the half of this parameter that can express it, answering 400 when
    ``target_words`` parses to no words.
    """

    def __init__(self, parameter: str) -> None:
        self.parameter = parameter
        super().__init__(
            f"{parameter} was sent with no usable words; each value must contain at "
            "least one non-whitespace character."
        )


@dataclass
class ExampleView:
    """One example as the caller will read it, after the overlay is applied."""

    id: int
    revision_id: int
    source: str | None
    target: str | None


@dataclass
class LexemeCardView:
    """A card resolved into the language the caller asked for.

    The canonical row plus the source side that actually applies, so the router maps
    fields rather than re-deciding which of the two sources won. ``source_language_iso``
    is ``None`` exactly when an overlay was requested and none existed, which is the state
    v3 published as ``has_translation_overlay: false`` and answered ``404`` for on its
    by-id read.
    """

    card: AgentLexemeCard
    source_language_iso: str | None
    source_lemma: str | None
    source_surface_forms: object | None
    senses: object | None
    last_user_edit: datetime | None
    examples: list[ExampleView] = field(default_factory=list)


def _normalize_words(words: list[str] | None, parameter: str) -> list[str] | None:
    """NFC-normalize and lowercase a repeated word filter, dropping blanks.

    Matches what ``LexemeCardIn`` applies on the way in, so an NFD-decomposed query finds
    an NFC-stored row (v3 issue #779).

    ``None`` in, ``None`` out — the filter was not sent. A filter that *was* sent but
    holds nothing usable raises :class:`InvalidWordFilter` rather than degrading to "no
    filter", which would widen the result set instead of narrowing it. Individual blanks
    among real words are still dropped: ``?target_word=grace&target_word=`` is a request
    for ``grace``.
    """
    if words is None:
        return None
    cleaned = [
        unicodedata.normalize("NFC", w.strip()).lower()
        for w in words
        if w and w.strip()
    ]
    if not cleaned:
        raise InvalidWordFilter(parameter)
    return cleaned


def _words_param(name: str, words: list[str]):
    """Bind a word list as a single ``text[]`` so the predicate is one ``= ANY(...)``.

    One array parameter rather than N ``OR``-ed equalities: the statement's shape stops
    depending on how many words were sent, so the planner sees one cached plan instead of
    one per arity.
    """
    return bindparam(name, value=words, type_=sa.ARRAY(sa.Text))


def _lemma_or_surface_match(lemma_column, forms_column, words_param):
    """``lemma`` matches, or any element of the ``jsonb`` forms array does.

    ``jsonb_typeof(...) = 'array'`` guards the unnest: the column has no constraint, so a
    row holding an object or a scalar is possible and must not raise. Case-insensitive on
    both halves, against the already-lowercased array.
    """
    element = func.jsonb_array_elements_text(forms_column).column_valued("form")
    return or_(
        func.lower(lemma_column) == sa.any_(words_param),
        and_(
            func.jsonb_typeof(forms_column) == "array",
            select(sa.literal(1))
            .where(func.lower(element) == sa.any_(words_param))
            .exists(),
        ),
    )


def _overlay_source_match(language_iso: str, words_param):
    """The same match, against the overlay row for ``language_iso``.

    Correlated ``EXISTS`` on ``card_translations`` rather than a join, so a card with no
    overlay simply fails the predicate instead of vanishing from or duplicating the
    result set.
    """
    return (
        select(sa.literal(1))
        .select_from(CardTranslation)
        .where(
            CardTranslation.card_id == AgentLexemeCard.id,
            CardTranslation.language_iso == language_iso,
            _lemma_or_surface_match(
                CardTranslation.source_lemma,
                CardTranslation.source_surface_forms,
                words_param,
            ),
        )
        .exists()
    )


def _source_word_clause(words: list[str], requested_language_iso: str | None):
    """Filter on the source side **the caller will be served**, not the stored one.

    v3 always matches the canonical ``source_lemma`` / ``source_surface_forms`` columns,
    whatever ``?lang=`` says. So a translator searching their own language searches text
    they were never shown: the request asks about the Spanish view and the filter runs
    against the English canonical, and the two disagree silently.

    v4 makes the filter and the projection agree. With no language requested, or one that
    is the card's own, the canonical columns are the source side and are what is matched.
    Otherwise the overlay for that language is, so a card whose overlay matches is
    returned and one whose canonical happens to match is not — which is the only reading
    under which the rows that come back contain the word that was asked for.
    """
    param = _words_param("source_words", words)
    canonical = _lemma_or_surface_match(
        AgentLexemeCard.source_lemma, AgentLexemeCard.source_surface_forms, param
    )
    if requested_language_iso is None:
        return canonical
    return or_(
        and_(
            AgentLexemeCard.source_language_iso == requested_language_iso,
            canonical,
        ),
        and_(
            AgentLexemeCard.source_language_iso != requested_language_iso,
            _overlay_source_match(requested_language_iso, param),
        ),
    )


async def _require_visible_version(
    db: AsyncSession, user: UserDB, version_id: int
) -> BibleVersion:
    """Resolve a version the caller may see, or raise :class:`VersionNotVisible`.

    Delegates to :func:`bible_routes.v4.version_service.get_version` rather than
    re-deriving the predicate, so this collection cannot drift from what ``/v4/versions``
    considers visible. Its ``VersionNotFound`` is re-signalled as this module's exception
    so the router maps one family.
    """
    try:
        return await version_service.get_version(db, user, version_id)
    except version_service.VersionNotFound as exc:
        raise VersionNotVisible(version_id) from exc


def _example_is_authorized(user: UserDB):
    """``EXISTS``: the example's revision belongs to a version its own card reaches.

    v3's example filter, kept clause for clause — a non-admin sees an example only when
    its revision belongs to the card's source **or** target version and one of their
    groups grants that version. It is the one access rule this family already enforces,
    and narrowing it would hide examples a caller reads today.

    Correlated on **both** sides: on ``AgentLexemeCardExample.revision_id`` for the
    example, and on ``AgentLexemeCard``'s two version columns for the card, so the caller
    must join the card in. That correlation is what makes the scope per card. The
    alternative — collecting the page's version ids into a Python list and testing every
    example against the union — is what v3's bulk read does, and it lets an example of
    card A pass because the caller reaches card B's source version. Invisible while every
    card on a page shares one pivot, wrong as soon as they do not.

    One clause regardless of page size, rather than one ``OR`` branch per card: at the
    result family's 1000-row maximum the per-card form compiles a thousand correlated
    subqueries into a single statement.
    """
    return (
        select(sa.literal(1))
        .select_from(BibleRevision)
        .join(BibleVersion, BibleVersion.id == BibleRevision.bible_version_id)
        .join(
            BibleVersionAccess,
            BibleVersionAccess.bible_version_id == BibleVersion.id,
        )
        .join(UserGroup, UserGroup.group_id == BibleVersionAccess.group_id)
        .where(
            BibleRevision.id == AgentLexemeCardExample.revision_id,
            UserGroup.user_id == user.id,
            BibleVersion.id.in_(
                [
                    AgentLexemeCard.source_version_id,
                    AgentLexemeCard.target_version_id,
                ]
            ),
        )
        .exists()
    )


def _filtered_cards(
    *,
    target_version_id: int,
    source_version_id: int | None,
    source_language_iso: str | None,
    source_words: list[str] | None,
    target_words: list[str] | None,
    pos: str | None,
    model: str | None,
):
    """``SELECT AgentLexemeCard`` narrowed by every filter the caller sent.

    Each filter is applied only when present, so an absent one adds no clause rather than
    a tautological one and the planner sees the narrowest predicate the request implies.
    ``target_version_id`` is always pinned: it is required, and it is what the caller was
    authorized against.

    ``model`` is an exact match, which excludes cards whose ``model`` is null — SQL's
    ``=`` semantics, and the right answer, since a caller asking for cards built by a
    named model is not asking for the unstamped ones. Most cards are unstamped: only the
    agentic builder records it.
    """
    clauses = [AgentLexemeCard.target_version_id == target_version_id]
    if source_version_id is not None:
        clauses.append(AgentLexemeCard.source_version_id == source_version_id)
    if pos is not None:
        clauses.append(AgentLexemeCard.pos == pos)
    if model is not None:
        clauses.append(AgentLexemeCard.model == model)
    if target_words:
        clauses.append(
            _lemma_or_surface_match(
                AgentLexemeCard.target_lemma,
                AgentLexemeCard.surface_forms,
                _words_param("target_words", target_words),
            )
        )
    if source_words:
        clauses.append(_source_word_clause(source_words, source_language_iso))
    return select(AgentLexemeCard).where(*clauses)


async def _load_examples(
    db: AsyncSession, user: UserDB, cards: list[AgentLexemeCard]
) -> dict[int, list[ExampleView]]:
    """Batch-load the examples of every card on the page, filtered by revision access.

    One query for the whole page rather than one per card, and one authorization clause
    rather than one per card — see :func:`_example_is_authorized` for the correlation that
    keeps the scope per card anyway, and for what goes wrong when it is not.

    The card is joined in for two reasons: the ``EXISTS`` correlates on its version
    columns, and the join is what confines the result to cards on this page even for an
    admin.
    """
    if not cards:
        return {}

    by_card: dict[int, list[ExampleView]] = {card.id: [] for card in cards}
    stmt = (
        select(
            AgentLexemeCardExample.id,
            AgentLexemeCardExample.lexeme_card_id,
            AgentLexemeCardExample.revision_id,
            AgentLexemeCardExample.source_text,
            AgentLexemeCardExample.target_text,
        )
        .join(
            AgentLexemeCard, AgentLexemeCard.id == AgentLexemeCardExample.lexeme_card_id
        )
        .where(AgentLexemeCardExample.lexeme_card_id.in_(list(by_card)))
    )
    if not user.is_admin:
        stmt = stmt.where(_example_is_authorized(user))

    rows = (
        await db.execute(
            stmt.order_by(
                AgentLexemeCardExample.lexeme_card_id, AgentLexemeCardExample.id
            )
        )
    ).all()
    for row in rows:
        by_card[row.lexeme_card_id].append(
            ExampleView(
                id=row.id,
                revision_id=row.revision_id,
                source=row.source_text,
                target=row.target_text,
            )
        )
    return by_card


async def _load_overlays(
    db: AsyncSession, card_ids: list[int], language_iso: str
) -> tuple[dict[int, CardTranslation], dict[int, dict[int, str]]]:
    """Load the ``card_translations`` rows for ``language_iso`` and their example text.

    Returns the overlay per card and, per card, the translated source text keyed by the
    canonical example id it replaces. Two queries for the whole page; a card with no
    overlay is simply absent from both maps.
    """
    if not card_ids:
        return {}, {}

    overlays = {
        overlay.card_id: overlay
        for overlay in (
            await db.execute(
                select(CardTranslation).where(
                    CardTranslation.card_id.in_(card_ids),
                    CardTranslation.language_iso == language_iso,
                )
            )
        )
        .scalars()
        .all()
    }
    if not overlays:
        return {}, {}

    by_overlay_id = {overlay.id: card_id for card_id, overlay in overlays.items()}
    translated: dict[int, dict[int, str]] = {card_id: {} for card_id in overlays}
    rows = (
        await db.execute(
            select(
                CardTranslationExample.card_translation_id,
                CardTranslationExample.example_id,
                CardTranslationExample.source_text,
            ).where(CardTranslationExample.card_translation_id.in_(list(by_overlay_id)))
        )
    ).all()
    for row in rows:
        translated[by_overlay_id[row.card_translation_id]][
            row.example_id
        ] = row.source_text
    return overlays, translated


def _build_view(
    card: AgentLexemeCard,
    examples: list[ExampleView],
    *,
    requested_language_iso: str | None,
    overlay: CardTranslation | None,
    translated_examples: dict[int, str],
) -> LexemeCardView:
    """Resolve one card into the language the caller asked for.

    Three cases, and the target side is identical in all of them because there is one
    target column and every language view projects it.

    Canonical — no language requested, or the card's own — is the stored row verbatim.
    Overlaid replaces the four source-side values with the overlay's, falling back to the
    canonical example text for any example the overlay did not translate. Missing nulls
    the source side entirely, ``source_language_iso`` included, which is how the caller
    learns the overlay is absent.

    ``last_user_edit`` is the later of the canonical row's and the overlay's, so a
    source-only edit still moves it. v3 does this on its by-id read and not on its bulk
    read, which left the same card reporting two different edit times depending on which
    endpoint you asked; one rule here for both.
    """
    if (
        requested_language_iso is None
        or requested_language_iso == card.source_language_iso
    ):
        return LexemeCardView(
            card=card,
            source_language_iso=card.source_language_iso,
            source_lemma=card.source_lemma,
            source_surface_forms=card.source_surface_forms,
            senses=card.senses,
            last_user_edit=card.last_user_edit,
            examples=examples,
        )

    if overlay is None:
        return LexemeCardView(
            card=card,
            source_language_iso=None,
            source_lemma=None,
            source_surface_forms=None,
            senses=None,
            last_user_edit=card.last_user_edit,
            examples=[
                ExampleView(
                    id=example.id,
                    revision_id=example.revision_id,
                    source=None,
                    target=example.target,
                )
                for example in examples
            ],
        )

    last_user_edit = card.last_user_edit
    if overlay.last_user_edit is not None and (
        last_user_edit is None or overlay.last_user_edit > last_user_edit
    ):
        last_user_edit = overlay.last_user_edit

    return LexemeCardView(
        card=card,
        source_language_iso=requested_language_iso,
        source_lemma=overlay.source_lemma,
        source_surface_forms=overlay.source_surface_forms,
        senses=overlay.senses,
        last_user_edit=last_user_edit,
        examples=[
            ExampleView(
                id=example.id,
                revision_id=example.revision_id,
                source=translated_examples.get(example.id, example.source),
                target=example.target,
            )
            for example in examples
        ],
    )


async def _views_for(
    db: AsyncSession,
    user: UserDB,
    cards: list[AgentLexemeCard],
    requested_language_iso: str | None,
) -> list[LexemeCardView]:
    """Attach examples and the requested language's source side to a page of cards."""
    examples_by_card = await _load_examples(db, user, cards)

    overlays: dict[int, CardTranslation] = {}
    translated: dict[int, dict[int, str]] = {}
    if requested_language_iso is not None:
        needs_overlay = [
            card.id
            for card in cards
            if card.source_language_iso != requested_language_iso
        ]
        overlays, translated = await _load_overlays(
            db, needs_overlay, requested_language_iso
        )

    return [
        _build_view(
            card,
            examples_by_card.get(card.id, []),
            requested_language_iso=requested_language_iso,
            overlay=overlays.get(card.id),
            translated_examples=translated.get(card.id, {}),
        )
        for card in cards
    ]


async def list_lexeme_cards(
    db: AsyncSession,
    user: UserDB,
    *,
    target_version_id: int,
    source_version_id: int | None = None,
    source_language_iso: str | None = None,
    source_words: list[str] | None = None,
    target_words: list[str] | None = None,
    pos: str | None = None,
    model: str | None = None,
    limit: int,
    offset: int,
) -> tuple[list[LexemeCardView], int]:
    """One page of a target version's lexeme cards, and the total match count.

    Raises :class:`VersionNotVisible` for a ``target_version_id`` — or a supplied
    ``source_version_id`` — the caller cannot reach, rather than returning an empty page:
    both name the scope of the read.

    **Ordered by confidence descending with nulls last, then by id.** v3 orders by
    confidence alone, which is genuinely the useful order — a client showing one card per
    word wants the best one — but is not a total order, so under ``offset`` pagination two
    cards tying on confidence can swap between pages and a client walking the collection
    can see one twice and miss another. The trailing ``id`` fixes that. ``nulls_last`` is
    load-bearing for the same reason it is on the critique read: PostgreSQL sorts nulls
    *first* under ``DESC``, so without it every page would open with the cards nobody
    scored.
    """
    await _require_visible_version(db, user, target_version_id)
    if source_version_id is not None:
        await _require_visible_version(db, user, source_version_id)

    requested_language_iso = (
        source_language_iso.lower() if source_language_iso else None
    )
    stmt = _filtered_cards(
        target_version_id=target_version_id,
        source_version_id=source_version_id,
        source_language_iso=requested_language_iso,
        source_words=_normalize_words(source_words, "source_word"),
        target_words=_normalize_words(target_words, "target_word"),
        pos=pos,
        model=model,
    )

    total = await db.scalar(select(func.count()).select_from(stmt.subquery()))
    cards = (
        (
            await db.execute(
                stmt.order_by(
                    desc(AgentLexemeCard.confidence).nulls_last(),
                    AgentLexemeCard.id,
                )
                .limit(limit)
                .offset(offset)
            )
        )
        .scalars()
        .all()
    )

    views = await _views_for(db, user, list(cards), requested_language_iso)
    return views, total or 0


async def get_lexeme_card(
    db: AsyncSession,
    user: UserDB,
    card_id: int,
    *,
    source_language_iso: str | None = None,
) -> LexemeCardView:
    """One lexeme card by id, resolved into the language the caller asked for.

    **Authorized on the card's target version and nothing else**, which is the list
    read's rule reached from the other direction: there the caller names the version and
    the cards follow, here the card names the version and the caller must reach it. Same
    helper, so the two cannot drift. The card's ``source_version_id`` is *not* checked —
    a card can name a pivot Bible the caller has no grant on and still be theirs to read,
    and the list read serves that id on exactly the same terms.

    Raises :class:`LexemeCardNotFound` for a card that does not exist, for one whose
    target version the caller cannot reach, and for an id outside the column's range
    (see :data:`_CARD_ID_MAX`), with no way to tell any of them apart. The
    version-level signal is caught and re-raised here rather than propagating: letting
    ``VersionNotVisible`` out would answer ``VERSION_NOT_FOUND`` for a card that exists
    and ``LEXEME_CARD_NOT_FOUND`` for one that does not, which is the probe the single
    signal exists to close.

    **The row is built by** :func:`_views_for`, **the list read's own resolver, given a
    one-card list.** That is why it takes a list: examples, the language overlay and the
    ``last_user_edit`` merge are one implementation, so by-id returns field for field what
    the same card returns as a list row. Do not grow a by-id-only field here — the shape
    is shared on purpose, and :class:`api_v4.schemas.agent.LexemeCardOut` documents one
    body for both reads.

    That parity is a small change from v3 in one place. v3's ``last_user_edit`` on
    *both* its reads is the canonical row's alone; the later-of-canonical-and-overlay
    merge lives in ``_build_lexeme_card_out_for_lang``, which only v3's patch handlers
    call, so v3 reports one edit time when you read a card and another when you write to
    it. v4 applies the merge everywhere, which makes a source-only overlay edit visible
    to a client rendering an "edited recently" marker on either read.

    The other change from v3 is the larger one and belongs to the whole slice: a
    ``source_language_iso`` this card has no overlay for is **served with the source side
    null**, where v3's by-id read answers ``404`` to trigger a derivation pipeline. See
    :func:`_build_view`, and the routes module for why v4 does not carry the side effect.
    """
    if not _CARD_ID_MIN <= card_id <= _CARD_ID_MAX:
        raise LexemeCardNotFound(card_id)

    card = await db.scalar(select(AgentLexemeCard).where(AgentLexemeCard.id == card_id))
    if card is None:
        raise LexemeCardNotFound(card_id)

    try:
        await _require_visible_version(db, user, card.target_version_id)
    except VersionNotVisible as exc:
        raise LexemeCardNotFound(card_id) from exc

    requested_language_iso = (
        source_language_iso.lower() if source_language_iso else None
    )
    views = await _views_for(db, user, [card], requested_language_iso)
    return views[0]

"""v4 lexeme-cards router (issue #896, epic #842).

``GET /v4/lexeme-cards`` — the dictionary a translation team is building, one entry per
target-language word: its inflected forms, what it means, and verses where it is used.

The last family in guide §15.7's three-way split of the agent surface, and the one that
is not assessment output. Critique issues and agent translations hang off a run and so
became sub-resources of ``/v4/assessments/{id}`` (see
:mod:`agent_routes.v4.agent_routes`); a lexeme card is keyed on a version pair and
outlives every run, so it promotes to a top-level collection. **There is no
``/v4/agent/…`` namespace**: "agent" names the process that produced a row rather than
the thing the row is.

Its own router rather than more routes on :mod:`agent_routes.v4.agent_routes` because the
prefix differs — ``/lexeme-cards`` against that module's ``/assessments`` — so it could
not share one ``APIRouter`` even if the two families' authorization were not entirely
different, which it is.


What a v3 caller will notice
----------------------------

**Cards are now authorized.** v3 gates them on nothing: any authenticated caller reads any
card for any version pair. v4 requires the caller to reach the card's **target version**,
and an unreachable one is a ``404 VERSION_NOT_FOUND`` rather than an empty page. The
per-revision filter v3 already applies to *examples* is kept exactly as it was.

**One repeated ``?target_word=`` replaces v3's ``target_word`` / ``target_words`` pair.**
v3 has two parameters for one idea and answers ``400`` if you send both. v4 has one that
repeats — ``?target_word=grace&target_word=mercy`` — which is how every other v4 list
takes a set (``?id=``, ``?against=``). ``?source_word=`` is the same. Sending one word is
the same request as v3's singular form.

**``?lang=`` is ``?source_language_iso=``**, matching the response field of that name so
the two round-trip, and because ``lang`` is ambiguous on a resource with a language on
each side.

**``has_translation_overlay`` is gone**, replaced by a nullable ``source_language_iso``.
Guide §10 makes a boolean bare and closes the ``is_``-prefixed list at two, and once the
language itself is on the wire a boolean beside it says nothing extra. Null means the card
has no translation into the language you asked for, and the whole source side is null with
it. See :class:`api_v4.schemas.agent.LexemeCardOut`.

**A source-word search now searches the language you asked for.** v3 matches the stored
canonical columns whatever ``?lang=`` says, so a translator searching in Spanish is
matched against English text they were never shown. v4 filters the source side it is about
to serve.

**``source_version_id`` is matched exactly, not pivot-routed.** v3 silently rewrites it to
the pivot Bible registered for the target's language. The pivot endpoints are v3-only
(§15.7), the rewrite is invisible in the response, and no v4 caller sends the parameter,
so v4 filters on the value you send. :mod:`agent_routes.v4.lexeme_card_service` argues it.

**The list paginates**, where v3 returns the whole filtered set with no page parameters at
all. It takes the result family's bounds — 100 by default, 1000 at most — rather than the
catalog bounds, because the bulk word lookup is the one call that exists to avoid a
round trip per word and a 100-row ceiling would put the round trips back.
"""

__version__ = "v4"

import math
from decimal import Decimal
from typing import List, Optional

import fastapi
from fastapi import Depends, Query, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from agent_routes.v4 import lexeme_card_service
from agent_routes.v4.lexeme_card_service import MAX_WORD_FILTER, LexemeCardView
from api_v4.errors import V4APIError
from api_v4.pagination import ResultPaginationParams, V4Page
from api_v4.schemas.agent import LexemeCardExampleOut, LexemeCardOut, SenseOut
from database.dependencies import get_db
from database.models import UserDB as UserModel
from security_routes.v4.dependencies import get_current_user_v4

router = fastapi.APIRouter(prefix="/lexeme-cards", tags=["Lexeme cards"])

#: v3 query parameters this endpoint renamed, mapped to what replaced them.
#:
#: Sending one is a ``422`` rather than the silent ignore FastAPI would otherwise give an
#: unrecognized query parameter, and the reason is that **all three are live in the one
#: client today**. ``aqua-django-app`` sends ``lang`` on its per-word read and
#: ``target_words`` on its bulk read. Pointed at v4 unchanged, neither would error: the
#: bulk read would drop its word filter and page through the whole collection, and the
#: per-word read would quietly serve the canonical source side — English text — to a
#: translator who asked for their own language. Both are wrong answers wearing a ``200``.
#:
#: This is the query-string counterpart of the closed request bodies in guide §10: an
#: unrecognized key is rejected, never dropped, because a request reporting success while
#: doing something other than what it asked is the failure mode worth spending a check on.
#: Scoped to this endpoint rather than the whole surface because this is where a rename
#: collided with a live caller; a general rule is a bigger decision than this slice.
WITHDRAWN_QUERY_PARAMS = {
    "lang": "source_language_iso",
    "target_words": "target_word",
    "source_words": "source_word",
}


def _withdrawn_parameter_error(sent: list[str]) -> V4APIError:
    """Name every withdrawn parameter in the request, and what to send instead."""
    replacements = {name: WITHDRAWN_QUERY_PARAMS[name] for name in sent}
    told = ", ".join(f"{old} is now {new}" for old, new in replacements.items())
    return V4APIError(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        code="WITHDRAWN_QUERY_PARAMETER",
        message=(
            f"This endpoint renamed {len(sent)} query parameter(s) from v3: {told}. "
            "They are refused rather than ignored, so a request cannot succeed while "
            "silently dropping a filter."
        ),
        details={"parameters": replacements},
    )


def _version_not_visible_error(
    exc: lexeme_card_service.VersionNotVisible, version_id: int
) -> V4APIError:
    """Map the service's one version signal onto its ``V4APIError``.

    The code is ``VERSION_NOT_FOUND`` — the same code ``/v4/versions`` and ``/v4/revisions``
    use, because it is the same fact about the same resource — and it covers unknown,
    inaccessible and soft-deleted alike. ``details`` names the id that was refused rather
    than echoing the whole request, so a caller who sent both version parameters can tell
    which one was the problem.
    """
    return V4APIError(
        status_code=status.HTTP_404_NOT_FOUND,
        code="VERSION_NOT_FOUND",
        message=str(exc),
        details={"version_id": version_id},
    )


def _string_list(value: object) -> list[str] | None:
    """A ``jsonb`` array of strings, or null if the column does not hold one.

    ``surface_forms``, ``source_surface_forms`` and a sense's ``examples`` are ``jsonb``
    with no database constraint, written by a v3 model that types them as a bare ``list``.
    A row holding an object, a scalar or a mixed array is therefore possible even though
    no current writer produces one, and a strictly validated response model would answer
    ``500`` on it — refusing to show the caller a card because one of its columns is
    malformed. Non-array becomes null and a non-string element is dropped, which is what
    the runner's own normalizer does before writing.
    """
    if not isinstance(value, list):
        return None
    return [item for item in value if isinstance(item, str)]


def _to_senses(value: object) -> list[SenseOut] | None:
    """Fold a stored ``senses`` array into :class:`SenseOut`, repairing what it can.

    The writer types this as ``Sense(definition: str, examples: list[str])`` and enforces
    it three ways over, so the declared shape is the real one — the tolerance here is for
    rows that predate it. A bare string is read as a definition with no examples, which is
    the same salvage the runner applies to loose model output; an entry that is neither a
    string nor an object is dropped, because there is nothing in it to serve.
    """
    if not isinstance(value, list):
        return None
    senses: list[SenseOut] = []
    for item in value:
        if isinstance(item, str):
            senses.append(SenseOut(definition=item, examples=[]))
        elif isinstance(item, dict):
            definition = item.get("definition")
            senses.append(
                SenseOut(
                    definition=definition if isinstance(definition, str) else "",
                    examples=_string_list(item.get("examples")) or [],
                )
            )
    return senses


def _finite_float(value: object) -> float | None:
    """A stored number as a JSON-safe float, or ``None`` if it cannot be one.

    Three stored values are numbers to the database and not numbers to JSON, and each of
    them turns a read into a broken response rather than an error anyone would notice:

    * PostgreSQL ``numeric`` accepts ``NaN``, and ``float(Decimal("NaN"))`` is ``nan``.
      ``json.dumps`` writes that as the bare literal ``NaN``, which is not valid JSON — so
      the caller's parser rejects the **whole body**, not just this field, and the
      response still carries a ``200``.
    * ``jsonb`` numbers are arbitrary precision. A stored integer larger than a float can
      hold raises ``OverflowError`` on conversion, which is a ``500`` on a read that could
      otherwise have served the row.
    * Infinities serialize exactly the way ``NaN`` does.

    None of the three is reachable through the v3 write path, which types both columns as
    ``float``. All three are reachable by a direct SQL write, which is how these tables
    are corrected today. Same judgement as the JSONB repair above: serve the row without
    the unrepresentable value rather than refuse the row.

    **One consequence is left alone deliberately.** PostgreSQL sorts ``numeric`` ``NaN``
    as *larger* than every number, so a NaN-scored card sorts first under
    ``confidence DESC`` while arriving on the wire as ``confidence: null`` — which
    otherwise means a card that sorts last. Making the two agree needs
    ``nullif(confidence, 'NaN')`` in the ``ORDER BY``, and wrapping the column in a
    function stops ``ix_agent_lexeme_cards_version_confidence`` from serving the sort.
    Paying that on every request to tidy a row only a direct SQL write can create is the
    wrong trade, so the quirk is recorded here instead.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def _to_alignment_scores(value: object) -> dict[str, float] | None:
    """A ``{word: number}`` map, or null if the column does not hold one.

    ``bool`` is excluded by :func:`_finite_float`: it is a subclass of ``int`` in Python,
    so a stored ``true`` would otherwise be served as ``1.0`` — a number the caller would
    rank on. A key whose value is not a representable number is dropped rather than
    served as null, because the field is a ranking and a null has no place in one.
    """
    if not isinstance(value, dict):
        return None
    scores: dict[str, float] = {}
    for key, score in value.items():
        if not isinstance(key, str):
            continue
        number = _finite_float(score)
        if number is not None:
            scores[key] = number
    return scores


def _to_lexeme_card_out(view: LexemeCardView) -> LexemeCardOut:
    """Build one card row from a resolved view.

    Field by field rather than by ``model_validate`` on the ORM object, for the reason
    :func:`agent_routes.v4.agent_routes._to_critique_issue_out` gives and one more: four of
    these values do not come from the card row at all. ``source_lemma``,
    ``source_surface_forms``, ``senses`` and ``source_language_iso`` come from whichever of
    the canonical row and the language overlay applies, which the service has already
    decided — spelling the mapping out here is what keeps that decision in one place while
    keeping a column rename from silently changing the wire.

    ``confidence`` goes through :func:`_finite_float` rather than a bare ``float()``. The
    column is ``numeric``, which returns ``Decimal`` and accepts ``NaN`` — and ``float()``
    does not repair that, it propagates it into a response body no strict JSON parser will
    accept.
    """
    card = view.card
    return LexemeCardOut(
        id=card.id,
        target_lemma=card.target_lemma,
        source_lemma=view.source_lemma,
        source_version_id=card.source_version_id,
        target_version_id=card.target_version_id,
        source_language_iso=view.source_language_iso,
        pos=card.pos,
        surface_forms=_string_list(card.surface_forms),
        source_surface_forms=_string_list(view.source_surface_forms),
        senses=_to_senses(view.senses),
        examples=[
            LexemeCardExampleOut(
                id=example.id,
                revision_id=example.revision_id,
                source=example.source,
                target=example.target,
            )
            for example in view.examples
        ],
        confidence=_finite_float(card.confidence),
        english_lemma=card.english_lemma,
        alignment_scores=_to_alignment_scores(card.alignment_scores),
        build_version=card.build_version,
        model=card.model,
        created_at=card.created_at,
        last_updated=card.last_updated,
        last_user_edit=view.last_user_edit,
    )


@router.get(
    "",
    response_model=V4Page[LexemeCardOut],
)
async def list_lexeme_cards(
    request: Request,
    target_version_id: int = Query(
        ...,
        description=(
            "The version whose cards to read — the translation being worked on. "
            "**Required**, and the only parameter that is: it is what the request is "
            "authorized against, and without it this collection has no bound. A version "
            "you cannot reach is a `404 VERSION_NOT_FOUND`, the same answer as one that "
            "does not exist, so the response cannot be used to learn which version ids "
            "are real."
        ),
    ),
    source_version_id: Optional[int] = Query(
        None,
        description=(
            "Restrict to cards built from this version. **Matched exactly** — unlike v3, "
            "which silently rewrites it to the pivot Bible registered for the target's "
            "language. Pass a `source_version_id` you read off a row here. Also "
            "authorized: one you cannot reach is a `404`, not an empty page. Most callers "
            "should omit it — a target version is normally built against a single source, "
            "so pinning it changes nothing."
        ),
    ),
    source_language_iso: Optional[str] = Query(
        None,
        min_length=3,
        max_length=3,
        description=(
            "ISO 639-3 code of the language you want the **source side** of each card in. "
            "Omit it for the card as stored. A card whose canonical source is already "
            "this language is returned unchanged; one with a stored translation into it "
            "is returned with `source_lemma`, `source_surface_forms`, `senses` and each "
            "example's `source` replaced by that translation; one with neither is "
            "returned with the whole source side null and `source_language_iso` null to "
            "say so. The target side never changes — there is one target column and every "
            "language view projects it. v3 calls this `lang`."
        ),
    ),
    source_words: Optional[List[str]] = Query(
        None,
        alias="source_word",
        max_length=MAX_WORD_FILTER,
        description=(
            "Return cards whose source-side lemma or surface forms include any of these "
            "words. Repeat the parameter for more than one "
            "(`?source_word=grace&source_word=mercy`). Case-insensitive and "
            "NFC-normalized, so a decomposed spelling finds a composed one, but otherwise "
            "an exact word match rather than a prefix or substring search. **Matches the "
            "source side you asked for**: with `source_language_iso` set, it searches that "
            "language's translation rather than the stored canonical, which is what v3 "
            "does and why v3 can return cards that do not contain the word you searched."
        ),
    ),
    target_words: Optional[List[str]] = Query(
        None,
        alias="target_word",
        max_length=MAX_WORD_FILTER,
        description=(
            "Return cards whose target lemma or surface forms include any of these words, "
            "on the same terms as `source_word`. Repeating it is v3's `target_words`; "
            "sending one is v3's `target_word`. The two are one parameter here, so the "
            "`400` v3 answers for sending both cannot arise."
        ),
    ),
    pos: Optional[str] = Query(
        None,
        description=(
            "Restrict to one part of speech. An exact match against the stored value, "
            "which is free text rather than an enum — see `LexemeCardOut.pos` for why. A "
            "value matching nothing yields an empty page, not a `422`."
        ),
    ),
    model: Optional[str] = Query(
        None,
        description=(
            "Restrict to cards built by one model, e.g. `anthropic.claude-sonnet-4-6`. "
            "Exact match. **This excludes cards with no model recorded**, which is most of "
            "them — only the agentic card builder stamps it, while translation-discovered "
            "lemmas and function-word seeds do not. That follows from SQL comparison "
            "against null and is the intended reading: a caller harvesting cards from a "
            "trusted model is not asking for the unattributed ones."
        ),
    ),
    page: ResultPaginationParams = Depends(),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> V4Page[LexemeCardOut]:
    """Read a translation's lexeme cards, best-scored first.

    **A card is a dictionary entry for one target-language word**, built against a source
    text: the lemma, the forms it inflects into, what it means, and verses where it is
    used. Cards are reference data rather than assessment output — they are keyed on a
    version pair and outlive any single run — which is why they are a top-level collection
    with no assessment id in the path.

    **Ordered by `confidence` descending, nulls last, then by `id`.** The confidence leg is
    v3's and is kept because it is the useful order: a client showing one card per word
    wants the best one first. The `id` is what v3 lacks — without it two cards tying on
    confidence can swap between pages, so walking the collection with `offset` could show
    one twice and miss another.

    **Examples are filtered to what you may read.** A card is visible when you can reach
    its target version, but an individual example is shown only when you can reach the
    revision it was drawn from — its own version, source or target. So two callers can
    hold the same card with different examples, and an empty `examples` list means "none
    you may read" rather than "none stored". Administrators see them all. This is v3's
    rule, unchanged.

    **No `updated_since`.** This collection publishes no delta feed and `next_updated_since`
    is always null. `last_updated` looks like a watermark and is not one: the column is
    nullable with no database default, so a row written outside the ORM has none and would
    be missing from every window rather than merely late; and cards are hard-deleted on
    every pipeline rebuild, with no tombstone a watermark could carry. A feed that dropped
    those silently would be worse than no feed, which is the judgement the agent-result
    reads made for the same reason.

    Status codes beyond the shared set: `404 VERSION_NOT_FOUND` for a `target_version_id`
    or `source_version_id` that does not exist or is not yours, and
    `422 INVALID_WORD_FILTER` for a `source_word` or `target_word` sent with nothing but
    blanks in it — which is refused rather than dropped, since dropping it would answer a
    narrowed request with the whole collection.

    Sending v3's `lang`, `target_words` or `source_words` is
    `422 WITHDRAWN_QUERY_PARAMETER`, naming what replaced each. They are refused rather
    than ignored so that a client mid-migration cannot get a `200` carrying a filter it
    thinks it applied.
    """
    withdrawn = [
        name for name in WITHDRAWN_QUERY_PARAMS if name in request.query_params
    ]
    if withdrawn:
        raise _withdrawn_parameter_error(withdrawn)

    try:
        views, total = await lexeme_card_service.list_lexeme_cards(
            db,
            current_user,
            target_version_id=target_version_id,
            source_version_id=source_version_id,
            source_language_iso=source_language_iso,
            source_words=source_words,
            target_words=target_words,
            pos=pos,
            model=model,
            limit=page.limit,
            offset=page.offset,
        )
    except lexeme_card_service.VersionNotVisible as exc:
        raise _version_not_visible_error(exc, exc.version_id) from exc
    except lexeme_card_service.InvalidWordFilter as exc:
        raise V4APIError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="INVALID_WORD_FILTER",
            message=str(exc),
            details={"parameter": exc.parameter},
        ) from exc

    # No next_updated_since, for the reason the docstring gives: the table's only
    # modification timestamp is nullable with no default and cards are hard-deleted, so
    # there is no watermark that could be trusted. The key stays present and null, per the
    # envelope's contract that adding delta support later is not a response-shape change.
    return V4Page[LexemeCardOut].create(
        items=[_to_lexeme_card_out(view) for view in views],
        total=total,
        pagination=page,
    )

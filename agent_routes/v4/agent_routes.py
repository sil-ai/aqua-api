"""v4 agent-results router (issue #896, epic #842).

The slice that closes the one place v4 was **broken** rather than merely incomplete.
Everywhere else v4 lacks something, frozen v3 still does that thing correctly; here v4
half-did it — ``POST /v4/assessments {"options": {"type": "agent-critique", ...}}`` could
start a run and nothing in v4 could read what came back. Two reads and one write:

* ``GET   /v4/assessments/{id}/critique-issues`` — the problems the agent found,
  paginated, in canonical Bible order, worst-first within a verse.
* ``GET   /v4/assessments/{id}/translations``    — the text it produced for each verse,
  with the back-translations explaining it.
* ``PATCH /v4/assessments/{id}/critique-issues/{issue_id}`` — mark an issue resolved or
  reopen it, replacing v3's ``/resolve`` + ``/unresolve`` pair with one endpoint taking
  ``{"resolved": true|false}``.

They are also what unblocks ``aqua-django-app`` finishing its move to v4: while agent
results were v3-only it had to straddle both versions indefinitely.

**The write is why neither read offers a delta feed.** Resolving mutates a row without
touching ``created_at``, and the table has no ``updated_at``, so an ``updated_since``
built on the only timestamp available would look like it worked while missing every
resolution — see :func:`agent_routes.v4.agent_service.get_critique_issues`.

**The write is also the one v4 write that never answers 403**, because it authorizes by
read access rather than ownership. That is a deliberate departure from the rest of the
write surface and :func:`agent_routes.v4.agent_service.resolve_critique_issue` argues it.


Why this is its own router on the ``/assessments`` prefix
--------------------------------------------------------

The paths hang off ``/v4/assessments/{id}`` because guide §15.7 rules that critique
issues and agent translations *are* assessment results — both tables carry a non-null
``assessment_id``, and the run that produces them is an assessment. But the router is this
module rather than eight hundred more lines on
:mod:`assessment_routes.v4.assessment_routes`, which is already ~1,800 lines over a
~3,500-line service.

That is the pattern :mod:`bible_routes.v4.verse_routes` established and documents: it
shares the ``/revisions`` prefix with the Revisions router while declaring only
sub-paths, and neither can shadow the other because ``/revisions/{id}`` and
``/revisions/{id}/verses`` are distinct path patterns. The same holds here for
``/assessments/{id}`` and ``/assessments/{id}/critique-issues``, so registration order in
:func:`api_v4.app.create_v4_app` is not load-bearing. It also gives these two their own
tag in ``/v4/openapi.json``, and gives the rest of the agent family — ``/v4/lexeme-cards``
and ``/v4/agent-word-alignments``, which are version-keyed reference data with no
assessment to nest under — a package to land in.

**There is no ``/v4/agent/…`` namespace and no ``/v4/critiques`` collection**, which §15.7
settled on 31 August 2026: "agent" names the process that produced a row rather than the
thing the row is. Guide §5's table showed the withdrawn
``PATCH /v4/agent/critiques/{id}`` for longer than that ruling stood; it is corrected to
the nested path alongside this module.


What a v3 caller will notice
----------------------------

**Books come back in Bible order.** v3's ``GET /agent/critique`` sorts on
``AgentCritiqueIssue.book``, a text column, so it returns books alphabetically. This is
the same fix the other typed reads made, and it is the change most likely to be visible.

**The alternative subject lookup is gone.** v3 accepts ``revision_id`` +
``reference_id`` + ``all_assessments`` instead of an ``assessment_id`` and picks a run
for you, ``ORDER BY end_time DESC`` for the non-``all`` case. v4 does not resolve "the
latest assessment for this pair" on any read — the ruling ``/score-comparison`` records —
so name the assessment and let ``GET /v4/assessments`` find it. The same request is two
calls instead of one, and the second one is reproducible.

**``is_resolved`` is now ``resolved``**, and ``agent_translations.version`` is now
``attempt``. Both are guide §10, and :mod:`api_v4.schemas.agent` carries the arguments.

**Re-resolving an already-resolved issue is a ``200``, not v3's ``400``.** Refusing an
assertion the row already satisfies leaves a client whose response was lost unable to
retry safely, which is the opposite of what a ``PATCH`` should offer.

**Both reads paginate**, where v3 returned the whole filtered set and declared no page
parameters at all. In practice this changes little: an ``agent-critique`` run is capped at
one chapter by the runner repository, and the measured median run holds about 13 rows of
each kind, so the ordinary result set is smaller than one default page.
"""

__version__ = "v4"

from typing import Optional

import fastapi
from fastapi import Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from agent_routes.v4 import agent_service
from api_v4.errors import V4APIError, error_responses
from api_v4.pagination import ResultPaginationParams, V4Page
from api_v4.schemas.agent import (
    MAX_SEVERITY,
    MIN_SEVERITY,
    AgentTranslationOut,
    CritiqueIssueOut,
    CritiqueIssueResolution,
)
from assessment_routes.v4 import assessment_service
from assessment_routes.v4.assessment_routes import VerseScopeParams
from database.dependencies import get_db
from database.models import UserDB as UserModel
from security_routes.v4.dependencies import get_current_user_v4

#: Shares the ``/assessments`` prefix with the Assessments router; see the module
#: docstring for why that is safe and why these two reads live here. The tag is their own
#: so ``/v4/docs`` groups them together with the rest of the agent family as it arrives,
#: rather than burying them among the twelve assessment operations.
router = fastapi.APIRouter(prefix="/assessments", tags=["Agent results"])


def _not_found_error(exc: Exception, assessment_id: int) -> V4APIError:
    """Map the family's one visibility signal onto its ``V4APIError``.

    Deliberately identical to the assessments router's own helper, including the code and
    the ``details`` key. Both reads here refuse for five distinct reasons — no such id,
    outside your groups, soft-deleted, a training row, an assessment of some other type —
    and all five must be indistinguishable from the outside, or the status code becomes a
    probe for which assessment ids exist.
    """
    return V4APIError(
        status_code=fastapi.status.HTTP_404_NOT_FOUND,
        code="ASSESSMENT_NOT_FOUND",
        message=str(exc),
        details={"assessment_id": assessment_id},
    )


def _to_critique_issue_out(row, continuations: dict) -> CritiqueIssueOut:
    """Build one critique-issue row, deriving ``vref`` and ``vrefs`` from the triple.

    **``vref`` is formatted from ``book``/``chapter``/``verse``, not read from the stored
    ``vref`` column**, and that is the rule
    :func:`assessment_routes.v4.assessment_routes._to_result_out` states and
    :func:`assessment_routes.v4.assessment_service.get_alignment_scores` follows: on a
    table that stores *both*, the triple is the authority and the ``vref`` string is the
    redundant copy, so formatting from the triple is what stops the two disagreeing.

    Contrast :func:`_to_agent_translation_out`, which correctly does the opposite:
    ``agent_translations`` stores **only** ``vref``, its triple is derived by a join, and
    the inner join to ``verse_reference`` is what guarantees the stored value is a literal
    canonical vref. Reading the stored column there is safe for exactly the reason it is
    not safe here — this read deliberately does *not* join ``verse_reference`` (only
    ``book_reference``, for the sort ordinal), so nothing validates the stored string.

    The disagreement is reachable, not theoretical. v3's push parses the vref it copies
    from the translation with ``re.match(r"([A-Z1-3]{3})\\s+(\\d+):(\\d+)", vref)`` —
    ``re.match`` with no end anchor — so ``"MAT 9:20-21"``, ``"MAT  9:20"`` and
    ``"MAT 9:20a"`` all yield the correct triple ``(MAT, 9, 20)`` while the stored string
    keeps its extra characters. Serving that string would emit a value that is not a
    verse as ``vref`` and as ``vrefs[0]``, in a field documented as verses in canonical
    order, and it would not join against ``vref.txt``. Formatting from the triple also
    keeps ``vrefs[0]`` consistent with the continuations, which are keyed on that same
    triple. Found in review of #944.

    Constructed field by field rather than by ``model_validate`` on the ORM object,
    because two names differ from their columns: ``is_resolved`` is served as ``resolved``
    (guide §10's bare-boolean rule), and the projected ``book_number`` the query sorts on
    is not part of the contract. Spelling the mapping out here is what keeps a column
    rename from silently changing the wire.

    **``resolved`` is passed through, not coerced with ``bool()``**, and the difference
    from :func:`assessment_routes.v4.assessment_routes._to_result_out` is deliberate.
    That function coerces ``flag`` and ``hide`` because those columns are genuinely
    nullable — a row written outside ``push_results`` can hold NULL, which is the shape
    that once 500'd v3's ``/alignmentscores``. ``agent_critique_issue.is_resolved`` is
    ``NOT NULL`` **in the database**, checked rather than assumed from the model, so a
    ``bool()`` here would be guarding a row that cannot exist. ``TestCritiqueIssuesRows``
    pins the constraint instead, so relaxing the column would fail a test rather than
    quietly start serving nulls through a required field.
    """
    vref = f"{row.book} {row.chapter}:{row.verse}"
    return CritiqueIssueOut(
        id=row.id,
        assessment_id=row.assessment_id,
        agent_translation_id=row.agent_translation_id,
        vref=vref,
        vrefs=[vref, *continuations.get((row.book, row.chapter, row.verse), ())],
        book=row.book,
        chapter=row.chapter,
        verse=row.verse,
        dimension=row.dimension,
        subtype=row.subtype,
        detector=row.detector,
        source_text=row.source_text,
        draft_text=row.draft_text,
        comments=row.comments,
        severity=row.severity,
        evidence=row.evidence,
        suggestions=row.suggestions,
        resolved=row.is_resolved,
        resolved_by_id=row.resolved_by_id,
        resolved_at=row.resolved_at,
        resolution_notes=row.resolution_notes,
        created_at=row.created_at,
    )


def _to_agent_translation_out(row, continuations: dict) -> AgentTranslationOut:
    """Build one agent-translation row, deriving its ``vrefs`` from the span map.

    Field by field for the same reason as :func:`_to_critique_issue_out`, and here two
    names differ: the ``script`` column is served as ``iso_script`` (v4's spelling of that
    concept, as on versions) and ``version`` is served as ``attempt`` (it is an attempt
    ordinal, not a Bible version). The three location columns the query derives from
    ``verse_reference`` to sort on are not part of the contract — ``vref`` is the stored
    value and the only location this row publishes.
    """
    return AgentTranslationOut(
        id=row.id,
        assessment_id=row.assessment_id,
        revision_id=row.revision_id,
        reference_version_id=row.reference_version_id,
        iso_script=row.script,
        vref=row.vref,
        vrefs=[row.vref, *continuations.get((row.book, row.chapter, row.verse), ())],
        attempt=row.version,
        draft_text=row.draft_text,
        hyper_literal_translation=row.hyper_literal_translation,
        literal_translation=row.literal_translation,
        english_translation=row.english_translation,
        alternatives=row.alternatives,
        created_at=row.created_at,
    )


@router.get(
    "/{assessment_id}/critique-issues",
    response_model=V4Page[CritiqueIssueOut],
)
async def get_assessment_critique_issues(
    assessment_id: int,
    page: ResultPaginationParams = Depends(),
    scope: VerseScopeParams = Depends(),
    dimension: Optional[str] = Query(
        None,
        description=(
            "Restrict to one MQM dimension. An **exact, case-sensitive** match against "
            "the stored value, which is what makes this round-trip against the "
            "`dimension` the read emits. The agent writes `accuracy`, `terminology` or "
            "`linguistic_conventions` — note the underscore, which is the stored "
            "spelling and is served unchanged. A value matching nothing yields an empty "
            "page, not a 422: the column has no database constraint, so this API is in "
            "no position to say which values are real."
        ),
    ),
    subtype: Optional[str] = Query(
        None,
        description=(
            "Restrict to one MQM leaf classification, e.g. "
            "`mistranslation/hallucination-numbers`. Exact and case-sensitive, on the "
            "same terms as `dimension`."
        ),
    ),
    min_severity: Optional[int] = Query(
        None,
        ge=MIN_SEVERITY,
        le=MAX_SEVERITY,
        description=(
            f"Return only issues the agent graded **at or above** this severity, "
            f"{MIN_SEVERITY} to {MAX_SEVERITY}. **This excludes issues with no severity "
            "at all**, which is v3's behaviour and follows from SQL comparison against "
            "null — so omit this filter to see every issue, including the ungraded ones. "
            "Out of range is a 422 naming the bound (v3 answers 400)."
        ),
    ),
    resolved: Optional[bool] = Query(
        None,
        description=(
            "Restrict to resolved (`true`) or unresolved (`false`) issues. Omit it for "
            "both. v3's `is_resolved`, renamed to match the response field."
        ),
    ),
    agent_translation_id: Optional[int] = Query(
        None,
        description=(
            "Restrict to the issues raised against one translation — the `id` of a row "
            "on `GET /v4/assessments/{id}/translations`. A translation id from a "
            "different assessment yields an empty page rather than a 404: it narrows an "
            "already-authorized set instead of naming this collection's parent, the same "
            "rule the other filters on this surface follow."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> V4Page[CritiqueIssueOut]:
    """Read the problems an agent critique found, in canonical Bible order.

    Serves `type = agent-critique` only. An assessment of any other type reports
    `404 ASSESSMENT_NOT_FOUND` — the same answer as one that does not exist, is outside
    your groups, is soft-deleted, or is a training run. The other types keep their results
    in their own tables and have their own sub-resources.

    **A row is one problem in one verse, not one verse.** A verse contributes as many rows
    as the agent raised issues against it, and a verse it was happy with contributes
    none. So the number of rows is not a coverage figure and a verse absent from the
    result set means "nothing flagged", not "not assessed" — the opposite reading from
    `/results`, where one row per verse is the invariant. To learn which verses were
    critiqued at all, read `/translations`: the agent writes a translation row per verse
    it processed, whether or not it found fault.

    **Each issue names the draft text it is about.** `source_text` and `draft_text` are
    the *spans* the agent is contrasting, not whole verses; `agent_translation_id` points
    at the translation row holding the full verse, and is the field to filter on to pull
    one verse-attempt's issues together.

    **Ordering is canonical Bible order, then severity descending with ungraded issues
    last, then row id.** The severity leg is v3's and worth keeping — the worst problem in
    a verse is the one to read first. The Bible-order leg is a **behaviour change** rather
    than only a nicer guarantee: v3 sorts on the `book` text column, so it returns `ACT`
    before `GEN`. The trailing row id is what makes `offset` pagination stable, which v3
    could not promise.

    **`severity` may be null, and that is meaningful.** It records that the agent declined
    to grade the issue, which is a different fact from grading it 1. It is never coerced
    to a number, and `min_severity` filters such rows out — see that parameter.

    **No `next_updated_since`, and it cannot honestly be added.** The table has a
    `created_at` and no `updated_at`, and resolving an issue mutates the row without
    touching `created_at` — so a delta feed keyed on it would appear to work while missing
    every resolution. The envelope's key is still present and null, per its contract that
    gaining delta support later is not a response-shape change.

    **Filters v3 has that this does not.** `vref` is expressible as `book` + `chapter` +
    `verse`, which is the spelling the rest of the family uses (and, as it happens, the
    one the existing client already sends — v3 declares only `book`, so its `chapter` and
    `verse` have been silently discarded). `revision_id`, `reference_id` and
    `all_assessments` are gone with the alternative subject lookup: v4 does not pick an
    assessment for you on any read.
    """
    try:
        rows, total, continuations = await agent_service.get_critique_issues(
            db,
            current_user,
            assessment_id,
            scope=scope.scope,
            dimension=dimension,
            subtype=subtype,
            min_severity=min_severity,
            resolved=resolved,
            agent_translation_id=agent_translation_id,
            limit=page.limit,
            offset=page.offset,
        )
    except assessment_service.AssessmentNotFound as exc:
        raise _not_found_error(exc, assessment_id) from exc
    # No next_updated_since, and it cannot honestly be added: the table has no
    # updated_at, and the resolution PATCH mutates rows without touching created_at, so
    # a delta feed keyed on it would appear to work while missing every resolution. The
    # key stays present and null, per the envelope's contract that gaining delta support
    # later is not a response-shape change.
    return V4Page[CritiqueIssueOut].create(
        items=[_to_critique_issue_out(row, continuations) for row in rows],
        total=total,
        pagination=page,
    )


@router.get(
    "/{assessment_id}/translations",
    response_model=V4Page[AgentTranslationOut],
)
async def get_assessment_translations(
    assessment_id: int,
    page: ResultPaginationParams = Depends(),
    scope: VerseScopeParams = Depends(),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> V4Page[AgentTranslationOut]:
    """Read the text an agent critique produced for each verse, in canonical Bible order.

    Serves `type = agent-critique` only, refusing every other case as
    `404 ASSESSMENT_NOT_FOUND` — identical to `/critique-issues`, deliberately, since both
    reads are governed by the same assessment's visibility.

    **A row is one verse as the agent rendered it**, plus up to three back-translations
    explaining that rendering: `hyper_literal_translation` follows the draft's own
    morphology word for word, `literal_translation` is literal but grammatical, and
    `english_translation` is natural English for a reviewer who does not read the target
    language. Any of them can be null where the agent produced none.

    **This is the read that tells you what was critiqued.** The agent writes a translation
    row for every verse it processed, so the verses here are the assessed set —
    `/critique-issues` covers only the subset it found fault with.

    **Every attempt is returned.** v3 collapses to the latest `version` per verse unless
    you ask for `all_versions`; this returns every stored row and labels it with
    `attempt`. Within one assessment that is usually the same set — a single bulk push
    gives every verse one ordinal — but where two attempts exist, both come back rather
    than the older one being hidden from a client with no way to learn it existed. Rows
    for one verse arrive in `attempt` order.

    **`attempt` is scoped to `(revision_id, reference_version_id, iso_script)`, not to
    this assessment**, so numbering continues across runs over the same text and a gap
    means another run pushed, not that a row is missing. Read it as an ordinal, not a
    count; the schema has the detail, including why the two v3 writers increment it
    differently.

    **Not carried from v3:** `revision_id` + `reference_version_id` + `script` as an
    alternative subject (that mode also skipped authorization entirely, and it resolves
    "the latest translation across every assessment" — the resolution v4 declines);
    `version` and `all_versions`, which the always-return-everything shape removes; and
    `first_vref` / `last_vref`, which a one-chapter cap and `?book=` / `?chapter=` leave
    with no request they make possible.

    **No `next_updated_since`:** the table carries no modification timestamp, so there is
    no honest watermark to publish. The key is present and null.
    """
    try:
        rows, total, continuations = await agent_service.get_translations(
            db,
            current_user,
            assessment_id,
            scope=scope.scope,
            limit=page.limit,
            offset=page.offset,
        )
    except assessment_service.AssessmentNotFound as exc:
        raise _not_found_error(exc, assessment_id) from exc
    # No next_updated_since: agent_translations carries no modification timestamp, so
    # there is no watermark to publish. Same reason /results and /text-lengths give.
    return V4Page[AgentTranslationOut].create(
        items=[_to_agent_translation_out(row, continuations) for row in rows],
        total=total,
        pagination=page,
    )


@router.patch(
    "/{assessment_id}/critique-issues/{issue_id}",
    response_model=CritiqueIssueOut,
    # No 403, which makes this the one v4 write that declares none. It authorizes by
    # read access rather than ownership (see the service for why), so a caller who
    # cannot write also cannot see the resource and gets the family's 404 instead.
    # Declaring a 403 here would put dead error-handling in every generated client.
    responses=error_responses(fastapi.status.HTTP_404_NOT_FOUND),
)
async def resolve_critique_issue(
    assessment_id: int,
    issue_id: int,
    data: CritiqueIssueResolution,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> CritiqueIssueOut:
    """Mark a critique issue resolved, or reopen it.

    **One endpoint replaces v3's `PATCH /agent/critique/{id}/resolve` and
    `/unresolve`**, taking `{"resolved": true}` or `{"resolved": false}`. The verb belongs
    to the method, not the path (guide §5).

    **The body is the resolution you are asserting.** `resolved: true` records you as the
    resolver at the current time, with the `resolution_notes` you sent — and with none if
    you sent none, so preserving existing notes means re-sending them. `resolved: false`
    clears the flag, the resolver, the timestamp and the notes together, which keeps the
    notes describing the resolution currently in force rather than a past one.

    **`resolution_notes` is only accepted with `resolved: true`.** Sent alongside
    `resolved: false` it is a `422` naming the field, rather than a value quietly dropped
    because unresolving would have cleared it anyway.

    **`resolved_by_id` and `resolved_at` are stamped by the server** and cannot be set:
    they are absent from the request model, so sending either is a `422`. A client able
    to set them could attribute a resolution to another user.

    **Re-asserting what is already stored is a `200` that writes nothing** — no `UPDATE`,
    so `resolved_at` does not move and a retried request cannot re-date a resolution.
    This is a **deliberate change from v3**, which answers `400` for "already resolved"
    and "not currently resolved"; refusing an assertion the row already satisfies leaves a
    client whose response was lost unable to retry safely. One nuance: "already stored"
    includes who stored it, so a *different* user asserting the same resolution does write
    and takes over `resolved_by_id` — the field says who currently stands behind it.

    **Anyone who can read the assessment can resolve its issues.** This is the one v4
    write not gated on ownership, and so the one that never answers `403`. Resolving is
    shared review work, and the row carries a `resolved_by_id` precisely because more
    than one person can do it. v3 authorizes this write the same way.

    An assessment you cannot reach — or one that is not an `agent-critique` run — is
    `404 ASSESSMENT_NOT_FOUND`, exactly as on the two reads. An issue id that is not on
    *this* assessment is `404 CRITIQUE_ISSUE_NOT_FOUND`, whether or not it exists
    elsewhere, so the endpoint cannot be used to probe another assessment's issue ids.

    The response is the full updated issue, in the same shape
    `GET /v4/assessments/{id}/critique-issues` returns.
    """
    try:
        issue, continuations = await agent_service.resolve_critique_issue(
            db,
            current_user,
            assessment_id,
            issue_id,
            resolved=data.resolved,
            resolution_notes=data.resolution_notes,
        )
    except assessment_service.AssessmentNotFound as exc:
        raise _not_found_error(exc, assessment_id) from exc
    except agent_service.CritiqueIssueNotFound as exc:
        raise V4APIError(
            status_code=fastapi.status.HTTP_404_NOT_FOUND,
            code="CRITIQUE_ISSUE_NOT_FOUND",
            message=str(exc),
            details={"assessment_id": assessment_id, "issue_id": issue_id},
        ) from exc
    return _to_critique_issue_out(issue, continuations)

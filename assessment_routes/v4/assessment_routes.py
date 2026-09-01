"""v4 Assessments router (issues #826/#827/#828/#865/#893, epic #842).

The first real consumer of :mod:`api_v4.jobs`, and the first v4 endpoint whose work
happens somewhere else. Eleven endpoints:

* ``POST   /v4/assessments``      — submit a run; ``202 Accepted`` with ``Location``
  and ``Retry-After``.
* ``GET    /v4/assessments/{id}`` — poll it. ``202`` while ``PENDING``, ``200``
  otherwise, and the body is the assessment resource merged with the job envelope.
* ``GET    /v4/assessments``      — paginated list (``V4Page[AssessmentOut]``), v3's
  filters, and ``updated_since`` delta sync.
* ``DELETE /v4/assessments/{id}`` — soft-delete (``204``); ``404``/``403`` as
  appropriate.
* ``GET    /v4/assessments/{id}/results`` — the generic per-verse scores, paginated,
  in canonical vref order, with v3's scoping filters and its ``aggregate`` rollups.
* ``GET    /v4/assessments/{id}/ngrams`` — the ``ngrams`` type's n-grams, paginated,
  each with the verses it occurs in. The one result read whose rows are not verses.
* ``GET    /v4/assessments/{id}/similar-verses`` — the ``tfidf`` type's nearest-neighbour
  search. Not a listing and not paginated; see its own contract note below.
* ``GET    /v4/assessments/{id}/alignment-scores`` — the ``word-alignment`` type's
  word-level pairings, paginated. Absorbs v3's ``GET /alignmentmatches``.
* ``GET    /v4/assessments/{id}/missing-words`` — the same type's poorly-aligned words,
  weighed against peer assessments named by ``against``.
* ``GET    /v4/assessments/{id}/text-lengths`` — the ``text-lengths`` type's per-verse
  word and character counts and their z-scores, paginated, with the same scoping and
  ``aggregate`` rollups as ``/results``.
* ``GET    /v4/assessments/{id}/score-comparison`` — the same rows as ``/results``, with
  each score placed against a distribution built from the peer assessments named by
  ``against``. The only read whose envelope is not a bare ``V4Page``.

The one remaining typed result sub-resource (the ``POST`` form of ``/similar-verses``) is
the rest of #893 and lands in a follow-up PR.

The shape of the change from v3, which is the whole point of the slice:

===============  =========================================  ===============================
                 v3 ``POST /assessment``                    v4 ``POST /v4/assessments``
===============  =========================================  ===============================
Input            ``AssessmentIn = Depends()`` + 6 query      one JSON body
                 flags + stringified ``extra_kwargs``
Options          free-form dict, validated for shape only    discriminated union on ``type``
Reference        optional everywhere, checked at runtime     required by the union member
Authorization    existence only (#865)                       caller must see both revisions
Response         ``200`` + ``List[AssessmentOut]``           ``202`` + ``{"job_id": ...}``
===============  =========================================  ===============================

This module owns HTTP concerns only: :mod:`assessment_routes.v4.assessment_service`
does the data access, authorization and dispatch, and each of its domain signals is
mapped here onto a :class:`~api_v4.errors.V4APIError` with a stable ``code``. Auth is
applied at the router level in :func:`api_v4.app.create_v4_app` (#831), so the
handler re-declares ``current_user`` only because it needs the user.


Contract decisions worth reading before changing anything here
--------------------------------------------------------------

**A caller who may not see a revision gets a 404, not a 403.** The service authorizes
through the Revisions slice's own visibility predicate, which returns one signal for
"no such revision" and "not yours" — deliberately, so a caller cannot probe which
revision ids exist by watching the status code change. Reporting a 403 would either
undo that (403 for yours-but-forbidden, 404 for missing) or attach a "forbidden" code
to ids that do not exist. The denial is what #865 is about; the status code is chosen
to match the rest of the v4 surface, where ``get_version`` and ``get_revision``
already hide existence behind a 404.

**``Idempotency-Key`` is deliberately not implemented here.** #827 lists it as
optional and it needs its own persistence table, so it is a follow-up. It is not
merely skipped: the duplicate-enqueue class of bug it exists to prevent is already
covered on this endpoint by the per-quadruple advisory lock plus exact-``kwargs``
dedup, which serialize concurrent submits — including a v4 submit racing a v3 one.
What remains uncovered is a *client* retry after a dropped ``202``, which will enqueue
a second run; per #827 the header must not be documented as supported until the
table lands, so this endpoint does not read it.

**The poll body is the assessment resource merged with the job envelope**, not the bare
four-key envelope (#893's 2026-08-26 decision 1). The reasoning and the resulting field
set live with the schemas (:mod:`api_v4.schemas.assessment`); what belongs here is the
HTTP half. A ``FAILED`` job is an HTTP **200** — reading the job succeeded, the job did
not — so the failure travels in the body's ``error`` field rather than through the #828
exception handler, and every one of the envelope's four keys is emitted on every poll,
``"error": null`` included. **Do not add ``response_model_exclude_none=True`` to the
poll route**; a polling client reads ``body["error"]`` unconditionally on every tick.

**A ``FAILED`` poll reports the generic ``JOB_FAILED`` code.** :mod:`api_v4.jobs` invites
a slice that can classify its failures to pass a more specific one, and this slice
cannot: the only failure signal on the row is ``status_detail``, which is free prose
written by the runner. Branching on a code derived from parsing prose would be worse
than a generic code honestly labelled — so the classification waits for the runner to
report something structured.

**``/similar-verses`` is the one read here that is not a list, and it is named after the
operation rather than the algorithm.** ``GET /tfidf_result`` takes a required ``vref``,
loads that verse's vector and ranks every other verse in the assessment against it — a
nearest-neighbour search, not a result listing. So it takes no pagination, returns no
``total`` and has no ``offset`` (v3 never had one: its client sends a ``page`` parameter
the v3 route does not declare, and FastAPI has always discarded it), and its body is its
own small envelope naming the query point. The path is ``/similar-verses`` rather than
``/tfidf`` because the question it answers is "which verses are most like this one";
naming the resource after TF-IDF would describe how the answer is computed. A deliberate
departure from guide §15.3, which plans ``/tfidf``.

**The two alignment reads are one handler pair, not four endpoints.** v3 spreads this
table across ``GET /alignmentscores``, ``GET /alignmentmatches`` and
``GET /missingwords``. Here ``/alignmentmatches`` is *gone*, folded into
``/alignment-scores`` as ``source`` + ``min_score``, because it returns the same rows
narrowed — the Q2/Q3 test for "filter, not resource". ``/missing-words`` keeps its own
endpoint by the same test, since it adds fields derived from *other* assessments, which
is what also kept ``/score-comparison`` off ``/results``.

The fold is what closes **#858** (``/alignmentmatches`` is unauthenticated) — not by
adding a check, but by leaving no separate endpoint on which one could be forgotten.
**#860** (``/missingwords`` authenticates but never authorizes) closes the same way: the
read has no authorization code of its own, only :func:`assessment_service.get_assessment`.
Both are properties of the design rather than arguments about scheduling — the v3
exposure lasts until v3 is retired regardless of when this ships.

**``missing-words`` is not an assessment type, and the endpoint says so.** Both reads
take a ``word-alignment`` assessment id. "Pass the id of the missing-words assessment"
is the natural guess and there is no such thing to pass — no enum value, no table, no
runner.

**``against`` peers are authorized by the same predicate as the subject, then checked
for comparability.** An unreachable peer is the family's ordinary ``404`` naming *that*
id; a readable but incomparable one — different reference, or a revision sharing a Bible
version with the subject's revision or reference — is a ``422``
``INCOMPATIBLE_BASELINE_ASSESSMENT`` naming it. v3 dropped such peers silently, which
was defensible when the caller handed over a list of revisions to be resolved and is not
now that they name assessments explicitly. Both reads that take ``against`` share one
service helper for all of it, so the guarantees cannot drift apart between them; what
differs is the type filter, which ``/score-comparison`` has to pin to the subject's own
type because it serves three of them.

That sharing is what closes **#862** (``/compareresults`` does not authorize baselines),
the last of this slice's five security issues, and it closes it the way #858 and #860
were closed — by leaving no place for a check to be forgotten rather than by adding one.
As with those, a property of the design and **not** an argument about scheduling: the v3
exposure lasts until v3 is retired regardless.

**``/score-comparison`` is the one read here whose response envelope is not a plain
``V4Page``.** Q2 §4 requires both sides of a comparison to be named, and the path names
only the subject, so :class:`~api_v4.schemas.assessment.ScoreComparisonPage` subclasses
the shared envelope to add ``against_assessment_ids``. The shared envelope itself is
untouched — a peer-ids field means nothing on the eleven other lists that use it — and
the subclass rather than a standalone model is argued on that class.

**The list's filters cannot 404.** They are applied after the visibility predicate and
only ever narrow what the caller could already see, so a ``revision_id`` outside the
caller's groups yields an empty page. That differs on purpose from
``GET /v4/revisions``, whose ``version_id`` names the collection's parent and is
therefore validated; see :func:`assessment_service.list_assessments`.

**``/text-lengths`` is the only read that needs a location triple its table does not
store, and that is invisible from outside.** Precisely: three of the reads here filter and
sort on stored ``book``/``chapter``/``verse`` columns (``/results`` and the two alignment
reads, whose tables carry them); ``/ngrams`` and ``/similar-verses`` read vref-only tables
but never need the triple, because one is not verse-keyed and the other ranks by
similarity with no scope filters. This read is the intersection — it needs the triple and
``text_lengths_table`` has none of it — so it reaches ``verse_reference`` →
``chapter_reference`` → ``book_reference`` for the canonical order, for the scope filters,
and for the key the ``<range>`` span map is keyed on. The
wire shape is deliberately unchanged by that — it is the ``/results`` shape with four
measures in place of one score — because a client should not be able to tell which of its
result reads happens to have a denormalized table behind it. What it *does* have to know is
that a rolled-up z-score is a mean of per-verse z-scores; the endpoint and the field
descriptions both say so.

**The result reads declare their own pagination and their own scope, and neither is
re-validated in the handler.** ``ResultPaginationParams`` (100/1000) rather than the
shared catalog params (20/100), because a results consumer wants bulk —
:mod:`api_v4.pagination` asks a heavy list to define its own dependency rather than raise
the shared cap. The same bounds apply to both alignment reads: the bound follows the
row's weight, and these are small fixed-width rows.
:class:`VerseScopeParams` and :class:`ResultScopeParams` are thin adapters over
:class:`~api_v4.schemas.assessment.VerseScope` and its ``aggregate``-carrying subclass,
whose ``model_validator`` methods hold the parameters' invariants, so an inconsistent
combination cannot reach the service at all. That is #486's principle: v4 satisfies
these by construction instead of by a runtime guard like v3's ``validate_parameters``.
The word-keyed reads take the ``VerseScope`` half only — a row there is a word, so there
is no per-verse set for an ``aggregate`` to roll up.
"""

__version__ = "v4"

from datetime import datetime
from typing import List, Optional

import fastapi
from fastapi import Depends, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.delta import next_watermark, updated_since_description
from api_v4.errors import V4APIError
from api_v4.jobs import (
    JobEnvelope,
    JobState,
    JobSubmitAccepted,
    job_accepted_response,
    set_poll_headers,
    state_for_assessment_status,
)
from api_v4.pagination import PaginationParams, ResultPaginationParams, V4Page
from api_v4.schemas.assessment import (
    BOOK_ABBREVIATION_LENGTH,
    VREF_MAX_LENGTH,
    AlignmentScoreOut,
    AlignmentScoreType,
    AssessmentCreate,
    AssessmentJob,
    AssessmentOut,
    AssessmentResultAggregateOut,
    AssessmentResultOut,
    AssessmentResultRow,
    MissingWordOut,
    NgramResultOut,
    ResultAggregate,
    ResultScope,
    ScoreComparisonAggregateOut,
    ScoreComparisonOut,
    ScoreComparisonPage,
    SimilarVerseOut,
    SimilarVersesOut,
    TextLengthsAggregateOut,
    TextLengthsOut,
    TextLengthsRow,
    VerseScope,
)
from assessment_routes.v4 import assessment_service
from config import settings
from database.dependencies import get_db
from database.models import Assessment
from database.models import UserDB as UserModel
from schemas.assessment import AssessmentType
from security_routes.auth_routes import get_current_user

router = fastapi.APIRouter(prefix="/assessments", tags=["Assessments"])

#: Default and maximum neighbours for ``GET /v4/assessments/{id}/similar-verses``.
#: 10 is v3's default, kept because nothing suggests it is wrong. The ceiling is new: v3
#: declares ``limit: int = 10`` with no bounds at all, so a caller can ask one request to
#: rank and serialize an entire assessment's 41,899 verses *with their text*. 100 is a
#: ranking; past that a caller is listing, and this endpoint does not list.
#:
#: Not :data:`~api_v4.pagination.MAX_LIMIT`, despite sharing its value: that constant is
#: the catalog *page* ceiling and moving it should not move this, since the two bound
#: different things — a page of a list versus the depth of a ranking.
SIMILAR_VERSES_DEFAULT_LIMIT = 10
SIMILAR_VERSES_MAX_LIMIT = 100

#: Bound on the ``source`` filter of ``GET /v4/assessments/{id}/alignment-scores``. The
#: column is unbounded ``Text``, but the values are single tokenized words, so a value
#: this long matches nothing and is a client bug worth reporting as a 422 rather than
#: turning into a full scan for a string no index entry can hold. v3 declares ``word: str``
#: with no bound at all.
ALIGNMENT_WORD_MAX_LENGTH = 200

#: Most peer assessments ``GET /v4/assessments/{id}/missing-words`` and
#: ``GET /v4/assessments/{id}/score-comparison`` will weigh against at once — one bound,
#: because ``against`` means the same thing on both. ``against`` is caller-controlled and
#: repeated, and the service authorizes each *distinct* peer through
#: :func:`assessment_service.get_assessment` — one small indexed query apiece — so an
#: unbounded list turns one cheap request into arbitrarily many.
#:
#: 1000 rather than something tighter, and :data:`~api_v4.schemas.bible.MAX_VREFS`'
#: reasoning is why: **the limit is not the point, the error is.** It is set above any
#: legitimate use so that hitting it means a client bug, and it answers with a 422 naming
#: the limit and the number received instead of degrading quietly. Measured against
#: production, the largest pool of finished word-alignment assessments sharing any single
#: reference is 598 and the mean is 3.4, so this cannot bite a real caller.
MAX_AGAINST_ASSESSMENTS = 1000

#: Cadence advertised on the 202, in seconds. Required rather than inherited — there
#: is no v4-wide default, precisely so a slice cannot pick up a cadence tuned for
#: something else (:mod:`api_v4.jobs`, divergence 3). Assessments span roughly a
#: minute (``sentence-length`` on one revision) to forty (``agent-critique`` over a
#: long range on a busy Modal environment), so this is set for the slow end: 30s costs
#: a fast job one extra poll, while v3's 10s predict cadence would cost a long one
#: ~240 wasted polls an hour. :func:`get_assessment` re-advertises the same value on
#: every non-terminal poll, so a client never has to invent a cadence.
ASSESSMENT_RETRY_AFTER_S = 30


def _poll_url(request: Request, assessment_id: int) -> str:
    """The URL the client should poll for ``assessment_id``.

    Root-relative (``/v4/assessments/42``), which RFC 9110 §10.2.2 allows for
    ``Location`` and :func:`api_v4.jobs.job_accepted_response` passes through
    unchanged.

    Derived from the poll route itself via ``url_for``, as :mod:`api_v4.jobs`
    recommends, so the header cannot drift from the route it names: renaming
    :func:`get_assessment` now makes this raise ``NoMatchFound`` on the next submit
    rather than quietly emitting a ``Location`` that 404s.

    Only the ``.path`` is taken. ``url_for`` returns an absolute URL built from the
    request's own scheme and host, which behind a TLS-terminating proxy that does not
    set forwarded headers would hand the client ``http://`` for a request it made over
    ``https``. A root-relative path cannot be wrong about the scheme, and the produced
    value is byte-identical to what the hand-built form emitted, so the pinned test
    value is unchanged by the swap.
    """
    return request.url_for("get_assessment", assessment_id=assessment_id).path


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={202: {"model": JobSubmitAccepted}},
)
async def create_assessment(
    request: Request,
    data: AssessmentCreate,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> JSONResponse:
    """Submit an assessment run.

    Returns ``202 Accepted`` with ``Location`` pointing at the assessment — which
    *is* the job (#842's 2026-08-25 decision 2: there is no separate job resource) —
    and ``Retry-After`` advertising the polling cadence. The body carries only
    ``job_id``; the poll URL travels in the header so a generic HTTP client can
    follow it without parsing the response.

    The caller must be able to see both the revision and, where the type needs one,
    the reference (#865). Submitting work equivalent to an assessment that already
    finished is a ``409`` that ``force`` overrides; equivalent work still in flight is
    a ``409`` that it does not.

    Note that a successful ``202`` means the run was accepted *and dispatched*: the
    row is already ``running`` by the time this returns. A failure to reach the runner
    is a ``503`` and leaves the row marked ``failed`` rather than queued forever.
    """
    try:
        assessment = await assessment_service.create_assessment(db, current_user, data)
    except assessment_service.RevisionNotVisible as exc:
        raise V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="REVISION_NOT_FOUND",
            message=str(exc),
            details={"revision_id": exc.revision_id},
        ) from exc
    except assessment_service.ReferenceNotVisible as exc:
        raise V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="REFERENCE_NOT_FOUND",
            message=str(exc),
            details={"reference_id": exc.reference_id},
        ) from exc
    except assessment_service.AssessmentAlreadyCompleted as exc:
        raise V4APIError(
            status_code=status.HTTP_409_CONFLICT,
            code="ASSESSMENT_ALREADY_COMPLETED",
            message=str(exc),
            details={"existing_assessment_id": exc.existing_id},
        ) from exc
    except assessment_service.AssessmentAlreadyInProgress as exc:
        raise V4APIError(
            status_code=status.HTTP_409_CONFLICT,
            code="ASSESSMENT_ALREADY_IN_PROGRESS",
            message=str(exc),
            details={"existing_assessment_id": exc.existing_id},
        ) from exc
    except assessment_service.AssessmentAlreadyDispatched as exc:
        raise V4APIError(
            status_code=status.HTTP_409_CONFLICT,
            code="ASSESSMENT_ALREADY_DISPATCHED",
            message=str(exc),
            details={
                "assessment_id": exc.assessment_id,
                "status": exc.current_status,
            },
        ) from exc
    except assessment_service.AssessmentDispatchFailed as exc:
        raise V4APIError(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="ASSESSMENT_DISPATCH_FAILED",
            message=str(exc),
            details={"assessment_id": exc.assessment_id},
        ) from exc

    return job_accepted_response(
        job_id=str(assessment.id),
        poll_url=_poll_url(request, assessment.id),
        retry_after_s=ASSESSMENT_RETRY_AFTER_S,
    )


def _to_out(assessment: Assessment) -> AssessmentOut:
    """Build the ``AssessmentOut`` resource body from an ORM row.

    From **named columns**, never from ``assessment.__dict__`` — the rule the Revisions
    slice states (#891) and the reason the three v3 fields v4 drops (``status``,
    ``is_training``, ``attempt_count``) cannot reappear by accident. This is also the
    one place the wire names are bridged to the ORM spellings: ``kwargs`` -> ``options``,
    and ``status`` -> the public ``state``.

    ``state_for_assessment_status`` raises ``ValueError`` on a status outside the four
    internal values, which reaches the #828 catch-all as a 500. That is
    :mod:`api_v4.jobs`' documented intent and it is left alone deliberately: the honest
    signal for a row whose ``status`` the server cannot read is an error, not a state
    invented for it. ``Assessment.status`` is an unconstrained ``Text`` column, so this
    is reachable in principle — by a bad manual UPDATE, not by any code path.

    ``deleted`` is coerced with ``bool(...)`` because the column is nullable and legacy
    rows may hold NULL (mirroring v3's null-to-false coercion). ``percent_complete`` and
    ``updated_at`` are *not* coerced: NULL is meaningful on both — "never reported
    progress" and "predates the column" — and both are optional on the wire.
    """
    return AssessmentOut(
        id=assessment.id,
        revision_id=assessment.revision_id,
        reference_id=assessment.reference_id,
        type=assessment.type,
        state=state_for_assessment_status(assessment.status),
        status_detail=assessment.status_detail,
        percent_complete=assessment.percent_complete,
        requested_time=assessment.requested_time,
        start_time=assessment.start_time,
        end_time=assessment.end_time,
        owner_id=assessment.owner_id,
        options=assessment.kwargs,
        deleted=bool(assessment.deleted),
        updated_at=assessment.updated_at,
    )


def _to_job(assessment: Assessment) -> AssessmentJob:
    """Build the poll body: the resource plus ``job_id`` / ``result`` / ``error``.

    The ``error`` is built by :meth:`api_v4.jobs.JobEnvelope.failed` and then lifted off
    the envelope it returns, rather than constructed here. That is not indirection for
    its own sake: ``failed()`` owns the fallback message for a row that reached
    ``failed`` with a null ``status_detail`` — which happens — and a slice that wrote its
    own ``or "..."`` guard and forgot it would trip the envelope's validator and turn a
    legitimately failed job into a 500. ``failed()`` cannot be used as a constructor for
    :class:`AssessmentJob` itself, since it supplies only the four envelope keys and the
    resource fields are required.

    ``result`` is left at its ``None`` default in every state, which the envelope
    explicitly permits for a ``SUCCEEDED`` job. See :mod:`api_v4.schemas.assessment` for
    what may eventually go there.
    """
    out = _to_out(assessment)
    job_id = str(assessment.id)
    error = None
    if out.state is JobState.FAILED:
        error = JobEnvelope.failed(
            job_id=job_id, message=assessment.status_detail
        ).error
    return AssessmentJob(**out.model_dump(), job_id=job_id, error=error)


def _not_found_error(exc: Exception, assessment_id: int) -> V4APIError:
    """The 404 for an assessment the caller cannot read or reach.

    Shared by the poll, the delete and every typed result read, so all of them report an
    unreachable id identically. One code covers "no such id", "outside your groups",
    "soft-deleted", "its revision was soft-deleted" and "it is a training row" — the
    service resolves all five in one scoped query and must not separate them (see its
    module docstring). On a result read the same code also covers a sixth: an assessment
    whose *type* has no rows in that read's result table. That is one clause on the same
    statement, so it cannot be told apart from the other five either, and a caller learns
    nothing about an assessment they may not see by asking for its results. The prose
    comes from the signal rather than being re-written here, so the two cannot drift.

    Every read in the family reuses this one helper deliberately: authorization defined
    per endpoint is what produced four of this slice's five security issues.
    """
    return V4APIError(
        status_code=status.HTTP_404_NOT_FOUND,
        code="ASSESSMENT_NOT_FOUND",
        message=str(exc),
        details={"assessment_id": assessment_id},
    )


def _incompatible_peer_error(
    exc: "assessment_service.IncompatiblePeerAssessment",
) -> V4APIError:
    """The 422 for an ``against`` peer that is readable but cannot serve as a baseline.

    Shared by the two reads that take ``against``, for the same reason
    :func:`_not_found_error` is shared: the comparability rules live in one service
    helper, so the answer they produce should be built in one place too. A 422 rather
    than a 404 because the id is real and the caller has already been shown they may read
    it — the peer went through the same visibility predicate first — so naming it in
    ``details`` discloses nothing new and is the only way the caller can tell which of
    several peers was refused.
    """
    return V4APIError(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        code="INCOMPATIBLE_BASELINE_ASSESSMENT",
        message=str(exc),
        details={"assessment_id": exc.assessment_id, "reason": exc.reason},
    )


@router.get(
    "",
    response_model=V4Page[AssessmentOut],
)
async def list_assessments(
    page: PaginationParams = Depends(),
    # Optional[...] rather than `X | None` only because black splits the union across
    # lines when it carries a Query() default; identical semantics. Same below.
    ids: Optional[List[int]] = Query(
        None,
        alias="id",
        description=(
            "Filter by one or more assessment ids (repeated parameter, e.g. "
            "`?id=1&id=2`). Ids that do not exist or are not visible to the caller are "
            "silently omitted — a partial result is not an error, matching v3."
        ),
    ),
    revision_id: Optional[int] = Query(
        None,
        description=(
            "Return only assessments of this revision. A revision the caller cannot "
            "see yields an empty page rather than a 404: this narrows an already "
            "authorized set, it does not name a parent resource."
        ),
    ),
    reference_id: Optional[int] = Query(
        None,
        description=(
            "Return only assessments that compared against this reference revision. "
            "Never matches the four assessment types that take no reference."
        ),
    ),
    assessment_type: Optional[AssessmentType] = Query(
        None,
        alias="type",
        description=(
            "Return only assessments of this type. Validated against the closed set, "
            "so a misspelled type is a 422 rather than an empty page."
        ),
    ),
    include_deleted: bool = Query(
        False,
        description=(
            "Admins only (ignored for other callers): also return soft-deleted "
            "assessments, and assessments whose revision, reference or either of their "
            "versions is soft-deleted."
        ),
    ),
    updated_since: Optional[datetime] = Query(
        None,
        description=updated_since_description(
            "assessments",
            cannot_carry=(
                "No watermark can carry a hard-deleted row (it never enters any "
                "window), nor an assessment that left your scope because its revision, "
                "its reference or one of their versions was soft-deleted or had a "
                "group's access revoked — each of those moves that *other* resource's "
                "watermark, not the assessment's."
            ),
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> V4Page[AssessmentOut]:
    """List assessments the caller may access, lowest id first, paginated.

    An assessment is visible when the caller's groups reach the version of its revision
    **and** — where it has one — the version of its reference, and when nothing in that
    chain is soft-deleted. Admins see every assessment.

    Two deliberate differences from v3's ``GET /assessment``, both of which mean a
    client comparing the two lists sees a different set rather than a regression:

    * **Training jobs are excluded.** ``train_routes`` stores its jobs as rows in the
      same table with ``is_training=true`` and v3 does not filter them, so v3 returns
      training runs mixed in with assessments. They become their own v4 resource in
      #895; a client will see **fewer** rows here than on v3.
    * **An assessment whose revision (or reference, or either version) was soft-deleted
      is hidden**, matching ``GET /v4/revisions``. v3 checks only group access and so
      keeps listing assessments of deleted revisions.

    Ordered by id rather than v3's newest-requested-first: offset pagination needs a
    total order on a column that cannot tie or move, and ``requested_time`` is nullable.

    ``updated_since`` turns the same list into a delta feed, and every response carries
    ``next_updated_since`` — the caller's next watermark, lapped server-side by
    :func:`api_v4.delta.next_watermark` (#899). Deliberately the same contract as
    ``GET /v4/versions`` and ``GET /v4/revisions``, so a mirror walks all three the same
    way. Note the second cause of a row silently leaving scope named above: a mirror
    learns about an assessment's *own* soft-delete from the feed, but not about its
    revision's, which is one of the things the required periodic full reconcile is for.
    """
    assessments, total, max_updated_at = await assessment_service.list_assessments(
        db,
        current_user,
        limit=page.limit,
        offset=page.offset,
        ids=ids,
        revision_id=revision_id,
        reference_id=reference_id,
        # .value: Assessment.type is a Text column holding the enum's wire spelling.
        assessment_type=assessment_type.value if assessment_type else None,
        include_deleted=include_deleted,
        updated_since=updated_since,
    )
    return V4Page[AssessmentOut].create(
        items=[_to_out(a) for a in assessments],
        total=total,
        pagination=page,
        next_updated_since=next_watermark(max_updated_at),
    )


@router.get(
    "/{assessment_id}",
    response_model=AssessmentJob,
    # The 202 is a real, documented outcome of this GET, not an error path, so it needs
    # its own entry — FastAPI documents only the declared status_code otherwise.
    responses={
        202: {"model": AssessmentJob, "description": "Accepted, not yet started"}
    },
)
async def get_assessment(
    assessment_id: int,
    response: Response,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> AssessmentJob:
    """Poll one assessment: its own fields, merged with the job envelope.

    ``202`` while the run is ``PENDING``, ``200`` in every other state, and
    ``Retry-After`` on every non-terminal poll — :func:`api_v4.jobs.set_poll_headers`
    applies all three, so this endpoint answers a poll the same way training and predict
    will. A ``FAILED`` run is a ``200`` whose body carries ``state: "FAILED"`` and an
    ``error``: reading the job succeeded, the job did not.

    The body is the assessment resource *and* the envelope, at every state — so a client
    polling a run that has not finished still learns its type, its revision, its
    timestamps and how far along it is (``percent_complete``, ``status_detail``), rather
    than only that it is running. ``result`` is null in every state for now; a
    ``SUCCEEDED`` job with no result is explicitly legal, and the typed result reads are
    a follow-up on #893.

    Note ``job_id`` is the string form of the integer ``id`` in the same body. That is
    deliberate — the envelope stringifies so a client parses one type across
    assessments, training and predict (:mod:`api_v4.jobs`).

    New in v4: v3 had no single-assessment read, so a client polled by listing. An
    assessment the caller cannot see, one whose revision was soft-deleted, and a
    training row all report the same ``404 ASSESSMENT_NOT_FOUND`` as a missing id.
    """
    try:
        assessment = await assessment_service.get_assessment(
            db, current_user, assessment_id
        )
    except assessment_service.AssessmentNotFound as exc:
        raise _not_found_error(exc, assessment_id) from exc

    job = _to_job(assessment)
    set_poll_headers(response, state=job.state, retry_after_s=ASSESSMENT_RETRY_AFTER_S)
    return job


def _scope_description(field: str) -> str:
    """The ``Query`` description for one scope parameter, taken from ``ResultScope``.

    Read off the schema rather than written twice, so the parameter documented in
    ``/v4/openapi.json`` and the field that actually validates it cannot drift apart.
    """
    return ResultScope.model_fields[field].description


def _book_query():
    """A fresh ``Query`` for ``book``, built per use rather than shared.

    Two dependency classes declare these three parameters, and a ``Query`` object is a
    mutable ``FieldInfo`` that FastAPI fills in (``alias``, among others) while building
    each route. Handing the same instance to two signatures would let one route's build
    write into the other's declaration, so each call makes its own. The *description*
    still comes from :func:`_scope_description`, which is the drift that actually
    matters.
    """
    return Query(
        None,
        min_length=BOOK_ABBREVIATION_LENGTH,
        max_length=BOOK_ABBREVIATION_LENGTH,
        description=_scope_description("book"),
    )


def _chapter_query():
    """A fresh ``Query`` for ``chapter``; see :func:`_book_query`."""
    return Query(None, ge=1, description=_scope_description("chapter"))


def _verse_query():
    """A fresh ``Query`` for ``verse``; see :func:`_book_query`."""
    return Query(None, ge=1, description=_scope_description("verse"))


def _validated_scope(model, **values):
    """Build a scope model, re-raising its ``ValidationError`` as a request error.

    A Pydantic error escaping a dependency would reach the #828 catch-all as a **500**,
    turning a malformed request into a server fault. Re-raised, it lands on the handler
    that already shapes FastAPI's own validation failures, so an inconsistent scope is
    the same ``422 VALIDATION_ERROR`` envelope as a misspelled ``aggregate`` — one error
    shape for the whole endpoint. ``loc`` is prefixed with ``"query"`` because that is
    where the values came from; Pydantic reports a model-level error with an empty
    ``loc``.
    """
    try:
        return model(**values)
    except ValidationError as exc:
        raise RequestValidationError(
            [{**error, "loc": ("query", *error["loc"])} for error in exc.errors()]
        ) from exc


class VerseScopeParams:
    """Adapter turning ``book``/``chapter``/``verse`` into one validated ``VerseScope``.

    Consumed as ``scope: VerseScopeParams = Depends()``; the handler then passes
    ``scope.scope`` to the service. Used by the reads that narrow to verses but do not
    roll up — ``/alignment-scores`` and ``/missing-words``, whose rows are *words*, so
    there is no per-verse set for an ``aggregate`` to summarize.

    Why an adapter rather than the model itself. FastAPI 0.115 can take a Pydantic model
    as a query-parameter container, but this version renders it in OpenAPI as a *single*
    parameter whose schema is a ``$ref`` — so a generated client would send one object
    instead of three query parameters. Declaring the parameters here keeps the documented
    surface flat while :class:`~api_v4.schemas.assessment.VerseScope` stays the one place
    the invariants live.

    The ``Query`` bounds duplicate the model's own bounds so OpenAPI advertises them
    (``BOOK_ABBREVIATION_LENGTH``, ``ge=1``). They are the same numbers from the same
    constant, not a second rule: whichever layer rejects first, the answer is the same
    422, and the model remains the authority that nothing can bypass.
    """

    def __init__(
        self,
        book: Optional[str] = _book_query(),
        chapter: Optional[int] = _chapter_query(),
        verse: Optional[int] = _verse_query(),
    ) -> None:
        self.scope: VerseScope = _validated_scope(
            VerseScope, book=book, chapter=chapter, verse=verse
        )


class ResultScopeParams:
    """:class:`VerseScopeParams` plus ``aggregate``, for the reads that roll up.

    A sibling rather than a subclass: FastAPI reads the ``__init__`` signature it is
    given, so a subclass would have to redeclare all four parameters anyway, and the two
    classes would then differ only in a line that is easy to miss. The shared half is
    :class:`~api_v4.schemas.assessment.ResultScope` inheriting ``VerseScope``, where the
    rules actually live.
    """

    def __init__(
        self,
        book: Optional[str] = _book_query(),
        chapter: Optional[int] = _chapter_query(),
        verse: Optional[int] = _verse_query(),
        aggregate: Optional[ResultAggregate] = Query(
            None, description=_scope_description("aggregate")
        ),
    ) -> None:
        self.scope: ResultScope = _validated_scope(
            ResultScope, book=book, chapter=chapter, verse=verse, aggregate=aggregate
        )


def _to_result_out(row, continuations: dict) -> AssessmentResultOut:
    """Build one verse-level result row, deriving its ``vrefs`` from the span map.

    ``vref`` is formatted from the row's ``book``/``chapter``/``verse`` rather than read
    from the stored ``vref`` column — which is what v3 does, and which means the label
    cannot disagree with the triple the row was deduplicated and ordered on, nor be null on
    a legacy row.

    ``vrefs`` is that label followed by whatever the revision merged into this verse.
    ``continuations`` holds an entry only for verses that absorbed something, so the common
    case is a one-element list built with no lookup cost. ``flag`` and ``hide`` are coerced
    because both columns are nullable and carry only a Python-side default, so a row
    written outside ``push_results`` can hold NULL — the same coercion ``_to_out`` applies
    to ``deleted``.
    """
    vref = f"{row.book} {row.chapter}:{row.verse}"
    return AssessmentResultOut(
        id=row.id,
        assessment_id=row.assessment_id,
        vref=vref,
        vrefs=[vref, *continuations.get((row.book, row.chapter, row.verse), ())],
        score=row.score,
        flag=bool(row.flag),
        hide=bool(row.hide),
        note=row.note,
    )


def _to_result_aggregate(row) -> AssessmentResultAggregateOut:
    """Build one rolled-up result row.

    ``book`` and ``chapter`` are read with a default because the aggregate query projects
    *different columns per level* — both at ``aggregate=chapter``, only ``book`` at
    ``aggregate=book``, neither at ``aggregate=text``, where the single row summarizes
    everything and so has no location. That varying projection is v3's, and it is the
    reason an aggregate row is its own response type rather than a verse row with fields
    nulled: ``vrefs`` is *absent* here, not empty.
    """
    return AssessmentResultAggregateOut(
        assessment_id=row.assessment_id,
        book=getattr(row, "book", None),
        chapter=getattr(row, "chapter", None),
        score=row.score,
        flag=bool(row.flag),
        hide=bool(row.hide),
    )


@router.get(
    "/{assessment_id}/results",
    response_model=V4Page[AssessmentResultRow],
)
async def get_assessment_results(
    assessment_id: int,
    page: ResultPaginationParams = Depends(),
    scope: ResultScopeParams = Depends(),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> V4Page[AssessmentResultRow]:
    """Read one assessment's per-verse scores, in canonical Bible order.

    Serves the three assessment types whose scores are stored per verse —
    `word-alignment`, `semantic-similarity` and `sentence-length`. An assessment of any
    other type reports `404 ASSESSMENT_NOT_FOUND`, the same answer as an assessment that
    does not exist, that is outside your groups, or that is a training run: the other
    types keep their scores in their own tables and have their own sub-resources.

    **A result set is not one row per verse, and you cannot derive coverage by
    subtracting.** Two different things cause a verse to be missing from this list. It may
    be *covered* by the row above it, because the revision merges it into the preceding
    verse (published as `MAT 9:20-21`, stored once under `MAT 9:20`); or it may never have
    been scored at all, which happens for real reasons — `MAT 23:14` on one measured
    revision has 355 characters of text and no score, because the reference lacks the
    verse and there was nothing to compare against. The two mean opposite things: "look at
    the row above" versus "no data exists".

    `vrefs` is what tells them apart. Each row carries `vref`, the verse it is stored
    under, **and** `vrefs`, every verse it covers. The union of `vrefs` across a result set
    is exactly the assessed set, so a verse outside that union was genuinely never scored.

    **`vref` is the span's first verse, never a range label.** A row covering
    `MAT 9:20-21` reports `vref: "MAT 9:20"` and `vrefs: ["MAT 9:20", "MAT 9:21"]`. `vref`
    is always a literal canonical verse reference, so it joins directly against a
    `vref.txt`-style fixture and against the verses read — a label like `"MAT 9:20-21"`
    matches no canonical line and would be silently dropped by a client that joins on it.

    **Ordering is canonical vref order** — Bible order, then chapter, then verse — not
    v3's insertion order, so a client no longer has to re-sort every page. Exactly one row
    per verse, which is what makes `offset` pagination stable across pages.

    **Scoping and rollups.** `book`, `chapter` and `verse` narrow progressively; each
    needs the one above it. `aggregate` rolls the scores up instead, to `chapter`, `book`
    or the whole `text`, and cannot be combined with a scope narrower than itself. An
    inconsistent combination is a `422`, not a silently ignored parameter.

    Aggregated rows are a **different shape** — they carry the location they summarize
    (`book`/`chapter`, and neither at `aggregate=text`, which is a single row), and they
    have no `vref`, `vrefs`, `note` or `id`. The rollup is not symmetric across fields:

    * `score` is the **mean** of the verses in scope.
    * `flag` and `hide` are **any** — one flagged verse flags its whole chapter.

    `source` and `target` are not returned at any level. They are populated only for
    missing-words-shaped assessments, whose per-word rows this read does not serve.
    """
    try:
        rows, total, continuations = await assessment_service.get_results(
            db,
            current_user,
            assessment_id,
            scope=scope.scope,
            limit=page.limit,
            offset=page.offset,
        )
    except assessment_service.AssessmentNotFound as exc:
        raise _not_found_error(exc, assessment_id) from exc

    if scope.scope.aggregate is None:
        items = [_to_result_out(row, continuations) for row in rows]
    else:
        items = [_to_result_aggregate(row) for row in rows]
    # No next_updated_since: assessment_result carries no modification timestamp, so this
    # list has no delta feed. The key is still present and null, per the envelope's
    # contract that adding delta support later is not a response-shape change.
    return V4Page[AssessmentResultRow].create(items=items, total=total, pagination=page)


@router.get(
    "/{assessment_id}/ngrams",
    response_model=V4Page[NgramResultOut],
)
async def get_assessment_ngrams(
    assessment_id: int,
    page: PaginationParams = Depends(),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> V4Page[NgramResultOut]:
    """Read one assessment's n-grams, each with the verses it occurs in.

    Serves `type = ngrams` only. An assessment of any other type reports
    `404 ASSESSMENT_NOT_FOUND` — the same answer as one that does not exist, is outside
    your groups, or is a training run — because the other types keep their results in
    their own tables and have their own sub-resources.

    **A row here is an n-gram, not a verse.** This is the one result read on this parent
    that is not keyed by verse, and it is worth reading the difference before wiring a
    client to it. `/results` returns one row per verse; this returns one row per n-gram,
    and each row's `occurrences` lists the verses that n-gram was found in. There is
    therefore **no `book`, `chapter`, `verse` or `aggregate`** — there is no per-verse
    axis to narrow and no per-verse set to roll up. Those parameters are not merely
    unsupported, they are absent: v4 ignores unrecognised query parameters, so sending
    `?book=MAT` here returns the unfiltered page rather than an error.

    **`occurrences` is not `/results`' `vrefs`, despite the family resemblance.** On
    `/results`, `vrefs` is the verses a single merged span *covers* — a range-merge
    concept, almost always one entry, whose purpose is joining a score to the text it
    scored. Here the list is every verse in which the n-gram *occurs*: an occurrence
    list, sometimes hundreds of entries, with no range-merge meaning at all. v3 called
    this field `vrefs`; v4 renames it precisely because the two would otherwise collide
    silently — nothing errors when a client treats one as the other.

    **An n-gram with no verse references is returned with an empty `occurrences`, not
    omitted**, and it is counted in `total`. v3 deliberately made these visible after an
    earlier `INNER JOIN` dropped them from the page while still counting them; the two
    agree here for the same reason.

    **Ordering is by the stored row id**, which is neither alphabetical nor by frequency.
    An n-gram is a token sequence, not a location, so it has no canonical order the way a
    verse does — and offset pagination needs a total order on a column that cannot tie or
    move, which `ngram` (nullable, non-unique, unindexed) is not. Sorting a page yourself
    is safe; sorting *across* pages requires fetching them all.

    `total` counts every n-gram in the assessment, including vrefless ones, and ignores
    `limit`/`offset`.
    """
    try:
        rows, total = await assessment_service.get_ngrams(
            db, current_user, assessment_id, limit=page.limit, offset=page.offset
        )
    except assessment_service.AssessmentNotFound as exc:
        raise _not_found_error(exc, assessment_id) from exc
    # No next_updated_since, for the reason /results gives: neither ngrams_table nor
    # ngram_vref_table carries a modification timestamp, so there is no watermark to
    # publish. Present and null rather than absent, so gaining a delta feed later would
    # not change the response shape.
    return V4Page[NgramResultOut].create(
        items=[NgramResultOut(**row) for row in rows], total=total, pagination=page
    )


@router.get(
    "/{assessment_id}/similar-verses",
    response_model=SimilarVersesOut,
)
async def get_assessment_similar_verses(
    assessment_id: int,
    vref: str = Query(
        min_length=1,
        max_length=VREF_MAX_LENGTH,
        description=(
            "**Required.** The verse to find neighbours for, as a canonical vref "
            "(`MAT 9:20`). This is the query point, not a filter: the ranking is "
            "computed against this verse's vector, so there is no meaningful response "
            "without it. Omitting it is a 422 naming this parameter rather than a "
            "default — a request with no verse in mind has no answer."
        ),
    ),
    limit: int = Query(
        SIMILAR_VERSES_DEFAULT_LIMIT,
        ge=1,
        le=SIMILAR_VERSES_MAX_LIMIT,
        description=(
            f"How many neighbours to return. Defaults to "
            f"{SIMILAR_VERSES_DEFAULT_LIMIT}; must be between 1 and "
            f"{SIMILAR_VERSES_MAX_LIMIT} (out-of-range values are rejected with 422, "
            f"not clamped)."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> SimilarVersesOut:
    """Find the verses most similar to a given verse, within one `tfidf` assessment.

    Serves `type = tfidf` only. An assessment of any other type reports
    `404 ASSESSMENT_NOT_FOUND` — the same answer as one that does not exist, is outside
    your groups, or is a training run.

    **This is a ranking, not a page**, and it is the one read on this parent that is not
    a list. The rows do not exist in a table: each is computed by comparing one verse to
    the verse you asked about. So there is **no `total`** — there is no population to
    count, and a total equal to `limit` would be a number that is present, defensible and
    misleading — and **no `offset`**, because paging a similarity ranking is not a thing.
    Ask for more neighbours with a larger `limit`.

    **`vref` is the query, not a filter.** It names the verse everything is ranked
    against, so a request without it has no answer and is a `422` naming the parameter.
    A `vref` this assessment holds no vector for is `404 VREF_NOT_FOUND` — a *different*
    code from an unreachable assessment, so a typo is distinguishable from a permission
    boundary. By that point you have already established you may read the assessment, so
    the distinction discloses nothing.

    **The queried verse is excluded from its own results.** It would otherwise be the
    first hit, every time, at maximum similarity.

    **Verse text comes back with each hit**, so a ranked list is renderable without a
    request per row. `text` is the assessed revision's; `reference_text` is the
    assessment's own reference, and is null for every hit when the assessment has no
    reference — the normal case for this type, and not an error. v3's `reference_id`
    parameter is **gone**: letting a caller name any revision made the response depend on
    a display preference rather than on the assessment, and a caller who wants arbitrary
    verse text has `GET /v4/revisions/{id}/verses`. Passing `reference_id` here is
    ignored, not honoured.

    **`similarity` ranks, it does not calibrate.** It is the inner product of the two
    verses' 300-dimensional PCA-reduced TF-IDF vectors: higher is closer, the ordering is
    meaningful, and the absolute value is not. Assessments are vectorized independently,
    so comparing a number from one against a number from another is meaningless. Ties
    break on `vref`, so repeating a request returns the same order.

    The search is exact and scoped to this assessment — at most 41,899 vectors behind an
    index — rather than approximate. Two verses ranked adjacent today will still be
    ranked adjacent tomorrow for the same stored vectors.
    """
    try:
        hits = await assessment_service.get_similar_verses(
            db, current_user, assessment_id, vref=vref, limit=limit
        )
    except assessment_service.SimilarityVrefNotFound as exc:
        raise V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="VREF_NOT_FOUND",
            message=str(exc),
            details={"assessment_id": assessment_id, "vref": vref},
        ) from exc
    except assessment_service.AssessmentNotFound as exc:
        raise _not_found_error(exc, assessment_id) from exc

    return SimilarVersesOut(
        query_vref=vref,
        limit=limit,
        items=[SimilarVerseOut(**hit) for hit in hits],
    )


@router.get(
    "/{assessment_id}/alignment-scores",
    response_model=V4Page[AlignmentScoreOut],
)
async def get_assessment_alignment_scores(
    assessment_id: int,
    page: ResultPaginationParams = Depends(),
    scope: VerseScopeParams = Depends(),
    score_type: AlignmentScoreType = Query(
        AlignmentScoreType.top,
        description=(
            "Which of the two stored score sets to read. `top` (the default) is one row "
            "per source word per verse — its single best-scoring target. `threshold` is "
            "every target that scored above the runner's cutoff, so one source word can "
            "appear several times in a verse with different targets. **There is no "
            "fallback between them**: an assessment whose runner wrote only one set "
            "answers an empty page for the other, which is the honest answer. Silently "
            "serving the other table would return different rows under the same request "
            "with nothing in the response saying so."
        ),
    ),
    source: Optional[str] = Query(
        None,
        min_length=1,
        max_length=ALIGNMENT_WORD_MAX_LENGTH,
        description=(
            "Return only alignments of this source-side word. **Case-insensitive** — "
            "stored source words are lower-cased, and the value you send is lowered to "
            "match. Together with `min_score` this is v3's `GET /alignmentmatches`, "
            "which no longer exists as its own endpoint."
        ),
    ),
    min_score: Optional[float] = Query(
        None,
        description=(
            "Return only alignments scoring **at or above** this value. v3's `threshold` "
            "on `/alignmentmatches`, renamed to say which way it cuts; v3's default of "
            f"{settings.alignment_threshold} is not carried over, because here the "
            "parameter is an optional filter rather than half of the endpoint's meaning."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> V4Page[AlignmentScoreOut]:
    """Read one assessment's word-level alignment scores, in canonical Bible order.

    Serves `type = word-alignment` only. An assessment of any other type reports
    `404 ASSESSMENT_NOT_FOUND` — the same answer as one that does not exist, is outside
    your groups, or is a training run.

    **A row here is a word pairing, not a verse.** One source word aligned to one target
    word in one verse, so a single verse contributes as many rows as it has source words.
    The per-verse score for the same assessment is a different read: `/results`.

    **This endpoint absorbs v3's `GET /alignmentmatches`.** That endpoint was this read
    filtered to one `source` above one `min_score`, and it does not get a v4 equivalent
    of its own — which is also how its missing authentication (#858) stops existing
    rather than being fixed: there is no separate handler left to forget the check.
    Everything it returned is here, verse text included.

    **Filter before you page.** An unfiltered word-alignment assessment holds on the
    order of 242,000 rows — roughly 2,400 pages at the maximum page size. `source`,
    `min_score` and the `book`/`chapter`/`verse` scope are how this read is meant to be
    used, not conveniences bolted on.

    **Ordering is canonical vref order, then `source`, then the stored row id** — Bible
    order, not `vref`'s lexical order and not v3's, which declared no ordering at all and
    so could return the same row on two pages while skipping another. The `source` and
    `id` keys are what make the order total: a verse holds many source words, and on
    `score_type=threshold` a single `(verse, source)` legitimately holds several rows.

    **Verse text always comes back.** `text` is the assessed revision's stored text for
    the verse and `reference_text` is the reference's, fetched once per page rather than
    once per row. For a row whose `vrefs` lists more than one verse this is the whole
    merged span's text, which is what the alignment actually ran over.

    **There is no `aggregate`.** Rolling word rows up to a chapter mean would produce a
    number that looks like the one `/results` gives for the same assessment and is not
    it. Sending `aggregate` here is ignored, not an error.
    """
    try:
        rows, total = await assessment_service.get_alignment_scores(
            db,
            current_user,
            assessment_id,
            scope=scope.scope,
            score_type=score_type,
            source=source,
            min_score=min_score,
            limit=page.limit,
            offset=page.offset,
        )
    except assessment_service.AssessmentNotFound as exc:
        raise _not_found_error(exc, exc.assessment_id) from exc
    # No next_updated_since, for the reason /results gives: neither alignment table
    # carries a modification timestamp, so there is no watermark to publish.
    return V4Page[AlignmentScoreOut].create(
        items=[AlignmentScoreOut(**row) for row in rows],
        total=total,
        pagination=page,
    )


@router.get(
    "/{assessment_id}/missing-words",
    response_model=V4Page[MissingWordOut],
)
async def get_assessment_missing_words(
    assessment_id: int,
    page: ResultPaginationParams = Depends(),
    scope: VerseScopeParams = Depends(),
    max_score: float = Query(
        settings.missing_words_missing_threshold,
        description=(
            "Return only words this assessment aligned **strictly below** this score — "
            "the definition of "
            f'"missing" here. Defaults to {settings.missing_words_missing_threshold}. '
            "v3's `threshold`, renamed to say which way it cuts (note the opposite "
            "direction from `/alignment-scores`' inclusive `min_score`)."
        ),
    ),
    against: Optional[List[int]] = Query(
        None,
        max_length=MAX_AGAINST_ASSESSMENTS,
        description=(
            "Peer **assessment** ids to weigh this assessment's alignments against "
            "(repeated parameter, e.g. `?against=1&against=2`). Each must be a "
            "word-alignment assessment you can read, aligned against the **same "
            "reference** as this one, and on a **different Bible version** from this "
            "assessment's revision and reference — a sibling revision is not an "
            "independent witness. A peer failing either rule is a `422` naming it, not a "
            "silently dropped baseline as in v3. A peer named twice counts once — it is "
            f"still one witness. **At most {MAX_AGAINST_ASSESSMENTS} per request**; more "
            "is a 422 naming the limit and the number received. These are assessment "
            "ids, not revision ids: v3's `baseline_ids` named revisions and let the "
            "server pick a run."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> V4Page[MissingWordOut]:
    """Read words a translation appears to have dropped, judged against peer translations.

    **Pass the id of a `word-alignment` assessment.** `missing-words` is not an
    assessment type — there is no such value in the type enum, no such table and no such
    runner. It is a *reading* of a word-alignment assessment's low-scoring rows, so the
    id in the path is the alignment run whose translation you are examining. An
    assessment of any other type reports `404 ASSESSMENT_NOT_FOUND`, the same answer as
    one that does not exist or is outside your groups. (v3 authenticated this endpoint
    but never authorized it, #860; here it goes through the family's one predicate like
    every other read on this parent.)

    **A row is a source word this assessment aligned poorly** — below `max_score` — in
    one verse. That alone is weak evidence: a low score can mean the word is genuinely
    untranslated, or merely that the aligner did badly on it.

    **`against` is what turns a low score into evidence.** Name peer assessments of the
    same reference, and each row gains a `targets` list saying what each peer made of the
    same word, plus a `flag` that is true when the peers aligned it well (mean above
    0.35) *and* far better than this assessment did (more than five times its score).
    Without `against`, `targets` is empty and `flag` is always false — the read is then
    just `/alignment-scores` filtered to low scores.

    **Every peer appears in every row's `targets`, including peers that had nothing.**
    A peer with no alignment for the word is reported with `target: null` rather than
    omitted, because its silence is part of the evidence. `target` is also null when the
    peer aligned the word too weakly to count as a translation; the two causes are not
    distinguished, which is v3's behaviour preserved.

    **This read is paginated, which v3's was not.** v3 returned the whole filtered set
    and declared no page parameters at all, so a client that sent them had them silently
    discarded. `limit` is capped at the same 1000 the other result reads use. In practice
    the filtered set is small — measured at a few hundred rows for a whole book — so a
    book-scoped request is usually one page, but a whole-Bible request is not.

    Rows are in canonical Bible order, then by source word, then by stored row id.
    """
    try:
        rows, total = await assessment_service.get_missing_words(
            db,
            current_user,
            assessment_id,
            scope=scope.scope,
            max_score=max_score,
            against=against or [],
            limit=page.limit,
            offset=page.offset,
        )
    except assessment_service.AssessmentNotFound as exc:
        # exc.assessment_id, not the path id: an `against` peer the caller cannot read
        # is refused in the same shape as an unreachable subject, and the details must
        # name the id that was actually refused or the caller cannot tell which.
        raise _not_found_error(exc, exc.assessment_id) from exc
    except assessment_service.IncompatiblePeerAssessment as exc:
        raise _incompatible_peer_error(exc) from exc
    # No next_updated_since, for the reason /results gives.
    return V4Page[MissingWordOut].create(
        items=[MissingWordOut(**row) for row in rows], total=total, pagination=page
    )


def _to_text_lengths_out(row, continuations: dict) -> TextLengthsOut:
    """Build one verse-level text-lengths row, deriving its ``vrefs`` from the span map.

    ``vref`` is the **stored** column here, not a string rebuilt from the location triple
    the way :func:`_to_result_out` builds it. That inversion is deliberate and follows from
    which value is the key: on ``assessment_result`` the triple is stored and ``vref`` is
    the redundant copy, so formatting from the triple is what stops the two disagreeing;
    on ``text_lengths_table`` ``vref`` is the only stored location and the triple is
    derived from it by the join, so the stored value is the authority and rebuilding it
    would be the redundant step. The inner join through ``verse_reference`` is what
    guarantees it is a literal canonical vref and not null.

    ``continuations`` is keyed on ``(book, chapter, verse)``, which is why the service
    projects the derived triple even though the wire shape does not carry it.
    """
    return TextLengthsOut(
        id=row.id,
        assessment_id=row.assessment_id,
        vref=row.vref,
        vrefs=[row.vref, *continuations.get((row.book, row.chapter, row.verse), ())],
        word_lengths=row.word_lengths,
        char_lengths=row.char_lengths,
        word_lengths_z=row.word_lengths_z,
        char_lengths_z=row.char_lengths_z,
    )


def _to_text_lengths_aggregate(row) -> TextLengthsAggregateOut:
    """Build one rolled-up text-lengths row.

    ``book`` and ``chapter`` are read with a default for the reason
    :func:`_to_result_aggregate` gives: the aggregate query projects different columns per
    level — both at ``aggregate=chapter``, only ``book`` at ``aggregate=book``, neither at
    ``aggregate=text``, where the single row summarizes everything and so has no location.
    """
    return TextLengthsAggregateOut(
        assessment_id=row.assessment_id,
        book=getattr(row, "book", None),
        chapter=getattr(row, "chapter", None),
        word_lengths=row.word_lengths,
        char_lengths=row.char_lengths,
        word_lengths_z=row.word_lengths_z,
        char_lengths_z=row.char_lengths_z,
    )


@router.get(
    "/{assessment_id}/text-lengths",
    response_model=V4Page[TextLengthsRow],
)
async def get_assessment_text_lengths(
    assessment_id: int,
    page: ResultPaginationParams = Depends(),
    scope: ResultScopeParams = Depends(),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> V4Page[TextLengthsRow]:
    """Read how long each verse of a translation is, in canonical Bible order.

    Serves `type = text-lengths` only. An assessment of any other type reports
    `404 ASSESSMENT_NOT_FOUND`, the same answer as one that does not exist, is outside
    your groups, or is a training run — the other types keep their results in their own
    tables and have their own sub-resources.

    **A row is one verse, with four measures of it.** `word_lengths` and `char_lengths`
    are counts; `word_lengths_z` and `char_lengths_z` are those counts standardized, so
    they say how unusually long or short a verse is for this translation. All four are
    computed by the runner and stored as they arrive — this read never recomputes them, and
    the population a z-score was standardized over is the runner's choice rather than
    something this API defines.

    **A result set is not one row per verse, and you cannot derive coverage by
    subtracting** — the same caveat as `/results`, and `vrefs` is the same answer to it. A
    verse can be missing because the revision merges it into the verse above (published as
    `MAT 9:20-21`, measured once under `MAT 9:20`, and the measurements are the whole
    span's), or because it was never measured at all. Each row carries `vref`, the verse it
    is stored under, and `vrefs`, every verse it covers, so the union of `vrefs` across a
    set is exactly the measured set.

    **Ordering is canonical vref order** — Bible order, then chapter, then verse — not
    v3's insertion order, so a client no longer has to re-sort every page against a
    `vref.txt` fixture. Exactly one row per verse, which is what makes `offset` pagination
    stable across pages. Note this is a real behaviour change and not only a nicer
    guarantee: v3 orders these rows by `min(id)` at every level, so its page order is the
    order the runner happened to push in. And do not substitute a sort on `vref` for this
    one — lexical vref order puts `GEN 10:1` before `GEN 2:1` and the books in alphabetical
    order, which is neither Bible order nor v3's.

    **Scoping and rollups.** `book`, `chapter` and `verse` narrow progressively; each needs
    the one above it. `aggregate` rolls the measures up instead, to `chapter`, `book` or
    the whole `text`, and cannot be combined with a scope narrower than itself. An
    inconsistent combination is a `422`, not a silently ignored parameter.

    Aggregated rows are a **different shape** — they carry the location they summarize
    (`book`/`chapter`, and neither at `aggregate=text`, which is a single row) and have no
    `id`, `vref` or `vrefs`. All four measures roll up as the plain mean.

    **Read this before using an aggregated z-score.** `word_lengths_z` on a chapter row is
    *the mean of that chapter's verses' z-scores*. It is **not** the chapter's own z-score
    against a distribution of chapters — nothing in this system computes such a
    distribution. A rolled-up value near zero therefore says "these verses are each
    typical for the revision", not "this chapter is typical among chapters", which is a
    stronger claim and the one the field name invites. v3 returns this same number and says
    nothing about it.

    `include_text` is not accepted. v3's endpoint does not declare it either — a client
    that sends it has always had it discarded — and there is no text field on these rows
    for it to fill. Verse text comes from the verses read.
    """
    try:
        rows, total, continuations = await assessment_service.get_text_lengths(
            db,
            current_user,
            assessment_id,
            scope=scope.scope,
            limit=page.limit,
            offset=page.offset,
        )
    except assessment_service.AssessmentNotFound as exc:
        raise _not_found_error(exc, assessment_id) from exc

    if scope.scope.aggregate is None:
        items = [_to_text_lengths_out(row, continuations) for row in rows]
    else:
        items = [_to_text_lengths_aggregate(row) for row in rows]
    # No next_updated_since, for the reason /results gives: text_lengths_table carries no
    # modification timestamp, so there is no watermark to publish. The key stays present
    # and null, per the envelope's contract that adding delta support later is not a
    # response-shape change.
    return V4Page[TextLengthsRow].create(items=items, total=total, pagination=page)


@router.get(
    "/{assessment_id}/score-comparison",
    response_model=ScoreComparisonPage,
)
async def get_assessment_score_comparison(
    assessment_id: int,
    page: ResultPaginationParams = Depends(),
    scope: ResultScopeParams = Depends(),
    against: List[int] = Query(
        ...,
        min_length=1,
        max_length=MAX_AGAINST_ASSESSMENTS,
        description=(
            "Peer **assessment** ids to build the baseline distribution from (repeated "
            "parameter, e.g. `?against=1&against=2`). **Required**: without peers there "
            "is no distribution, and the read would be `/results` with three null "
            "columns. Each must be an assessment you can read, of the **same type** as "
            "this one, against the **same reference**, and on a **different Bible "
            "version** from this assessment's revision and reference — a sibling "
            "revision is not an independent witness. A peer failing any of those is a "
            "`422` naming it, not a silently dropped baseline as in v3. A peer named "
            f"twice counts once. **At most {MAX_AGAINST_ASSESSMENTS} per request**; more "
            "is a 422 naming the limit and the number received. These are assessment "
            "ids, not revision ids: v3's `baseline_ids` named revisions and let the "
            "server pick a run."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> ScoreComparisonPage:
    """Read how unusual a translation's scores are, against comparable translations.

    **A row is one verse of this assessment, plus the shape of the peers' scores at the
    same verse.** `score` is this assessment's own — the same number `/results` returns,
    from the same query. `mean_score` and `stdev_score` describe the peers named by
    `against`, and `z_score` says how many standard deviations this assessment sits from
    them. Near zero means it scores about like its peers here; a large negative one is the
    case this read exists to find.

    **Serves `word-alignment`, `semantic-similarity` and `sentence-length`.** v3's
    `/compareresults` is word-alignment only, which is an artifact of how it finds its
    subject — by `(revision_id, reference_id, type='word-alignment')` — rather than a fact
    about the data: all three types' rows live in the same table with the same shape. An
    assessment of any other type reports `404 ASSESSMENT_NOT_FOUND`, the same answer as
    one that does not exist, is outside your groups, or is a training run.

    **Every `against` peer is authorized exactly as the subject is** (#862). v3 never
    checked baselines at all, so any authenticated caller could name any assessment id and
    read its aggregate scores. Here a peer you cannot see is the ordinary 404 naming
    *that* id, indistinguishable from one that does not exist.

    **A peer must be the same type, on the same reference, and on a different Bible
    version** from this assessment's revision and its reference. The first two keep the
    scores on one scale — a mean across references is a number that means nothing — and
    the third is what makes a peer an independent witness rather than a sibling of the
    text under assessment. It also rules out naming this assessment as its own peer. Any
    of them failing is a `422 INCOMPATIBLE_BASELINE_ASSESSMENT` naming the peer; v3
    dropped it silently, which was defensible when you handed over revision ids for the
    server to resolve and is not now that you name the assessment.

    **Read `baseline_count` on every row.** It says how many peers actually contributed.
    v3 reports a mean with no way to tell five peers from one, and a peer with no row at a
    verse drops out of it silently. `0` means the row is uncompared, not that the peers
    agreed.

    **One baseline never produces a `z_score`.** `stdev_score` is the sample standard
    deviation, undefined for a single observation, so both it and `z_score` come back
    null. Expected rather than an error, and v3 answers the same way without saying so.

    **A score is never combined across a merged verse span, so where two revisions merge
    differently the peer is dropped rather than averaged in.** If this revision publishes
    `MAT 9:20-21` as one span and a peer's revision does not — or merges a different span
    — that peer contributes nothing at that verse: it leaves `mean_score`, `stdev_score`
    and `baseline_count` together, while this assessment's own `score` still comes back.
    The reason is that a span's score is a property of the span's *text*: the similarity
    of two verses concatenated is not any function of their two similarities, and a mean
    would weight a three-word verse like a forty-word one. Nothing here holds a model to
    recompute with, so comparability is a precondition, not something to compute around.

    **So `vrefs` here is the *comparable* population, not the assessed one.** On
    `/results` a verse in no row's `vrefs` was never scored; here it may be scored on both
    sides and simply not comparable. Do not derive coverage from this read.

    **Scoping and rollups** work as on `/results`. `book`, `chapter` and `verse` narrow
    progressively; `aggregate` rolls up to `chapter`, `book` or the whole `text` and
    cannot be combined with a scope narrower than itself. Aggregated rows are a different
    shape — they carry the location they summarize (`book`/`chapter`, neither at
    `aggregate=text`) and have no `id`, `vref` or `vrefs`. Each peer is rolled up the same
    way this assessment is before the distribution is taken, so a peer is one observation
    whether it contributed one verse or a whole book. **The span test does not apply under
    a rollup** — there is no per-verse row left to refuse — so a rollup can average across
    a span disagreement the verse level would have excluded. That is v3's behaviour at
    every level, and small next to the rollup itself, but it is real.

    **This read will not find "the latest assessment" for you.** v3 resolves its subject
    and each baseline independently with `ORDER BY end_time DESC LIMIT 1`, so two
    identical requests can compare different assessments, and the subject can land on a
    different runner than its own baselines.
    `GET /v4/assessments?revision_id=&reference_id=&type=` answers that question
    explicitly and paginated. `use_eflomal` is gone with it: naming the assessment already
    determines the runner, so there is nothing left to select.

    Rows are in canonical Bible order, one per verse, which is what makes `offset`
    pagination stable across pages.
    """
    try:
        items, total, against_ids = await assessment_service.get_score_comparison(
            db,
            current_user,
            assessment_id,
            scope=scope.scope,
            against=against,
            limit=page.limit,
            offset=page.offset,
        )
    except assessment_service.AssessmentNotFound as exc:
        # exc.assessment_id, not the path id: an `against` peer the caller cannot read is
        # refused in the same shape as an unreachable subject, so the details must name
        # the id that was actually refused or the caller cannot tell which.
        raise _not_found_error(exc, exc.assessment_id) from exc
    except assessment_service.IncompatiblePeerAssessment as exc:
        raise _incompatible_peer_error(exc) from exc

    row_model = (
        ScoreComparisonOut
        if scope.scope.aggregate is None
        else ScoreComparisonAggregateOut
    )
    # No next_updated_since, for the reason /results gives: assessment_result carries no
    # modification timestamp, so there is no watermark to publish. The key stays present
    # and null, per the envelope's contract that adding delta support later is not a
    # response-shape change.
    return ScoreComparisonPage.create(
        items=[row_model(**item) for item in items],
        total=total,
        pagination=page,
        against_assessment_ids=against_ids,
    )


@router.delete("/{assessment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_assessment(
    assessment_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
) -> Response:
    """Soft-delete an assessment (its owner, or an admin).

    ``204`` with no body, where v3 returned ``200`` and a prose ``{"detail": ...}``.
    The soft-delete itself is unchanged: the row and its result rows stay, ``deleted``
    flips. Idempotent — re-deleting an already-deleted assessment is another ``204``.

    Three things about this endpoint are worth knowing before calling it:

    * **``404`` and ``403`` mean different things here than on v3.** An assessment the
      caller cannot reach is a ``404``, and ``403 ASSESSMENT_ACCESS_FORBIDDEN`` is
      returned only for one they *can* reach but do not own. v3 looked the row up with
      no permission filter and answered ``403``, so its status code told an
      unauthorized caller whether an id existed.
    * **Assessments created before ``owner_id`` existed have no owner**, so no
      non-admin can delete them — nobody is their owner. That is a property of the
      data, not an authorization failure; ask an admin.
    * **Deleting a queued or running assessment does not stop the Modal run.** It keeps
      going, keeps costing GPU time, and its results still push into the now-hidden
      row. Refusing while in flight was considered and rejected: it would block the
      most likely reason to call this — cancelling an expensive run started by mistake —
      without actually stopping anything, because v4 holds no Modal handle to cancel
      with. Deleting then resubmitting the same work is supported: dedup ignores
      soft-deleted rows, so an identical resubmit is not a spurious ``409``.
    """
    try:
        await assessment_service.soft_delete_assessment(db, current_user, assessment_id)
    except assessment_service.AssessmentNotFound as exc:
        raise _not_found_error(exc, assessment_id) from exc
    except assessment_service.AssessmentAccessForbidden as exc:
        raise V4APIError(
            status_code=status.HTTP_403_FORBIDDEN,
            code="ASSESSMENT_ACCESS_FORBIDDEN",
            message=str(exc),
            details={"assessment_id": assessment_id},
        ) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)

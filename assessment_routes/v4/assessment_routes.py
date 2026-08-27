"""v4 Assessments router (issues #826/#827/#828/#865/#893, epic #842).

The first real consumer of :mod:`api_v4.jobs`, and the first v4 endpoint whose work
happens somewhere else. Five endpoints:

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

The remaining typed result sub-resources (``/ngrams``, ``/text-lengths``,
``/alignment-scores``, ``/tfidf``, ``/missing-words``) are the rest of #893 and land in
follow-up PRs, as does the comparisons family.

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

**The list's filters cannot 404.** They are applied after the visibility predicate and
only ever narrow what the caller could already see, so a ``revision_id`` outside the
caller's groups yields an empty page. That differs on purpose from
``GET /v4/revisions``, whose ``version_id`` names the collection's parent and is
therefore validated; see :func:`assessment_service.list_assessments`.

**The results read declares its own pagination and its own scope, and neither is
re-validated in the handler.** ``ResultPaginationParams`` (100/1000) rather than the
shared catalog params (20/100), because a results consumer wants bulk —
:mod:`api_v4.pagination` asks a heavy list to define its own dependency rather than raise
the shared cap. :class:`ResultScopeParams` is a thin adapter over
:class:`~api_v4.schemas.assessment.ResultScope`, whose ``model_validator`` holds the four
parameters' invariants, so an inconsistent combination cannot reach the service at all.
That is #486's principle: v4 satisfies these by construction instead of by a runtime
guard like v3's ``validate_parameters``.
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
    AssessmentCreate,
    AssessmentJob,
    AssessmentOut,
    AssessmentResultAggregateOut,
    AssessmentResultOut,
    AssessmentResultRow,
    ResultAggregate,
    ResultScope,
)
from assessment_routes.v4 import assessment_service
from database.dependencies import get_db
from database.models import Assessment
from database.models import UserDB as UserModel
from schemas.assessment import AssessmentType
from security_routes.auth_routes import get_current_user

router = fastapi.APIRouter(prefix="/assessments", tags=["Assessments"])

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

    Shared by the poll, the delete and the results read so all three report an
    unreachable id identically. One code covers "no such id", "outside your groups",
    "soft-deleted", "its revision was soft-deleted" and "it is a training row" — the
    service resolves all five in one scoped query and must not separate them (see its
    module docstring). On the results read the same code also covers a sixth: an
    assessment whose *type* has no rows in this result table. That is one clause on the
    same statement, so it cannot be told apart from the other five either, and a caller
    learns nothing about an assessment they may not see by asking for its results. The
    prose comes from the signal rather than being re-written here, so the two cannot
    drift.
    """
    return V4APIError(
        status_code=status.HTTP_404_NOT_FOUND,
        code="ASSESSMENT_NOT_FOUND",
        message=str(exc),
        details={"assessment_id": assessment_id},
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


class ResultScopeParams:
    """Adapter turning the four scope query parameters into one validated ``ResultScope``.

    Consumed as ``scope: ResultScopeParams = Depends()``; the handler then passes
    ``scope.scope`` to the service.

    Why an adapter rather than the model itself. FastAPI 0.115 can take a Pydantic model
    as a query-parameter container, but this version renders it in OpenAPI as a *single*
    parameter whose schema is a ``$ref`` — so a generated client would send one object
    instead of four query parameters. Declaring the parameters here keeps the documented
    surface flat while :class:`~api_v4.schemas.assessment.ResultScope` stays the one place
    the invariants live.

    Why the ``ValidationError`` is re-raised as a ``RequestValidationError``: a Pydantic
    error escaping a dependency would reach the #828 catch-all as a **500**, turning a
    malformed request into a server fault. Re-raised, it lands on the handler that already
    shapes FastAPI's own validation failures, so an inconsistent scope is the same
    ``422 VALIDATION_ERROR`` envelope as a misspelled ``aggregate`` — one error shape for
    the whole endpoint. ``loc`` is prefixed with ``"query"`` because that is where the
    values came from; Pydantic reports a model-level error with an empty ``loc``.

    The ``Query`` bounds duplicate the model's own bounds so OpenAPI advertises them
    (``BOOK_ABBREVIATION_LENGTH``, ``ge=1``). They are the same numbers from the same
    constant, not a second rule: whichever layer rejects first, the answer is the same
    422, and the model remains the authority that nothing can bypass.
    """

    def __init__(
        self,
        book: Optional[str] = Query(
            None,
            min_length=BOOK_ABBREVIATION_LENGTH,
            max_length=BOOK_ABBREVIATION_LENGTH,
            description=_scope_description("book"),
        ),
        chapter: Optional[int] = Query(
            None, ge=1, description=_scope_description("chapter")
        ),
        verse: Optional[int] = Query(
            None, ge=1, description=_scope_description("verse")
        ),
        aggregate: Optional[ResultAggregate] = Query(
            None, description=_scope_description("aggregate")
        ),
    ) -> None:
        try:
            self.scope: ResultScope = ResultScope(
                book=book, chapter=chapter, verse=verse, aggregate=aggregate
            )
        except ValidationError as exc:
            raise RequestValidationError(
                [{**error, "loc": ("query", *error["loc"])} for error in exc.errors()]
            ) from exc


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

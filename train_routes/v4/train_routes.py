"""v4 Training router (issue #895, epic #842).

Six endpoints across two collections, because a training submit fans out and the thing
it creates is not the thing it creates *one* of:

* ``POST   /v4/training-sessions``                    — submit; ``202`` with ``Location``
  and ``Retry-After``.
* ``GET    /v4/training-sessions/{session_id}``       — the session: an aggregate state,
  every job in it, and which analyses the pair can now run.
* ``GET    /v4/training-sessions/{session_id}/results`` — the trained output, one
  paginated row per verse.
* ``GET    /v4/training-jobs``                        — paginated list, v3's filters.
* ``GET    /v4/training-jobs/{job_id}``               — one job, as a job envelope.
* ``DELETE /v4/training-jobs/{job_id}``               — soft-delete; terminal jobs only.

``GET /train/{job_id}/data`` is **not** here. It is the runner's data-pull, ruled off the
client contract by #842, and it stays on v3.

This module owns HTTP concerns only; :mod:`train_routes.v4.train_service` does the
authorization, the dispatch and the query work, and each of its domain signals is mapped
here onto a :class:`~api_v4.errors.V4APIError` with a stable ``code``. Auth is applied at
the router level in :func:`api_v4.app.create_v4_app` (#831), so each handler re-declares
``current_user`` only because it needs the user.


Contract decisions worth reading before changing anything here
--------------------------------------------------------------

**The submit is a ``202``, unlike predict's ``200``.** Predict answers ``200`` because
its fast results come back inline and the slow leg is one optional part of them; training
has no inline half at all — every app is a GPU run measured in minutes — so the whole
operation is asynchronous and takes the standard
:func:`~api_v4.jobs.job_accepted_response` shape.

**The ``202``'s ``job_id`` is the session key, not a job id**, and ``Location`` points at
the session. That follows :mod:`api_v4.jobs`' actual rule — ``job_id`` is the id of the
resource served at the poll URL — rather than the word "job": what the caller submitted
is a session, and the session is what they poll. #895's scope table describes the ``202``
as returning "the session (job ids + ``inference_readiness``)"; it does not, because the
submit response is the one shape every v4 submit shares, and both of those arrive on the
first poll the client makes anyway.

**Two collections, not a nested one.** Jobs are addressed at ``/v4/training-jobs/{id}``
rather than under their session, because a job's session is metadata on the job rather
than its parent — ``training_job.session_id`` is a nullable text column, so a job can
have none — and because the list is naturally cross-session. The session's own reads live
under ``/v4/training-sessions`` and reach jobs by the key.

**A selector the caller cannot see is a ``404``, not v3's ``403``.** v3's version branch
answers ``403`` (its revision branch already answers ``404``, so v3 is not even
self-consistent); #842 answers reachability with a ``404`` everywhere on this surface so
that ids cannot be probed by watching a status code change. The consequence is that the
**only** operation here that can answer ``403`` is the delete, where it means "visible,
but not yours" — so it is the one training operation in ``FORBIDDEN_OPERATIONS``.

**A job with no linked assessment is answered three different ways, on purpose.**
``training_job.assessment_id`` is ``ON DELETE SET NULL`` and ``TrainingJob`` carries no
state of its own, so a row can exist with no state carrier. The list reports it — a row
with ``state: null`` and an ``error`` naming the fault — because a list is the only view
that can surface a data-integrity problem, and silently dropping the row would hide it.
The single-job read cannot: its body *is* a :class:`~api_v4.jobs.JobEnvelope`, whose
validator requires a state and forbids an error on anything but ``FAILED``, so it raises
``TRAINING_JOB_STATE_UNAVAILABLE`` as a named ``500`` rather than letting
``state_for_assessment_status`` reach the catch-all as a bare ``INTERNAL_ERROR``. The
delete refuses with the same code as a ``409``, which is v3's own answer on that path
(``train_routes/v3/train_routes.py:1816``) and the right class there: the row is real, the
refusal is conditional, and an administrator can resolve it.

**A ``FAILED`` job reports the generic ``JOB_FAILED`` code**, as on assessments and
predict. The only failure signal on the row is the assessment's ``status_detail``, which
is free prose written by the runner; a code derived from parsing prose is worse than a
generic code honestly labelled. The prose still reaches the client as the error's
``message``.

**The results read takes the *catalog* pagination bounds (20/100), not the result reads'
(100/1000).** :mod:`api_v4.pagination` invites a heavy list to define its own dependency
rather than widen a shared cap; this is the first list that wants a *narrower* one, and
the shared catalog params already are it. A row here is the heaviest on the v4 surface —
up to ``tfidf_top_k`` neighbours per side, each carrying two verse texts, plus every
n-gram firing on the verse with its full cross-corpus occurrence list, each of *those*
carrying two verse texts as well. A thousand of them is not a page.
"""

__version__ = "v4"

from typing import Optional

import fastapi
from fastapi import Depends, Query, Request, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.errors import (
    V4_FORBIDDEN_RESPONSE,
    V4APIError,
    V4ErrorDetail,
    error_responses,
)
from api_v4.jobs import (
    JOB_ACCEPTED_HEADERS,
    JOB_POLL_HEADERS,
    JOB_POLL_PENDING_HEADERS,
    JobEnvelope,
    JobState,
    JobSubmitAccepted,
    job_accepted_response,
    set_poll_headers,
)
from api_v4.pagination import PaginationParams, V4Page
from api_v4.schemas.training import (
    TFIDF_TOP_K_DEFAULT,
    TFIDF_TOP_K_MAX,
    TrainingJobDetail,
    TrainingJobOut,
    TrainingResultRow,
    TrainingSessionCreate,
    TrainingSessionOut,
)
from assessment_routes.v4.assessment_routes import VerseScopeParams
from database.dependencies import get_db
from database.models import TrainingJob
from database.models import UserDB as UserModel
from schemas.training import TrainingType
from security_routes.v4.dependencies import get_current_user_v4
from train_routes.v4 import train_service

session_router = fastapi.APIRouter(prefix="/training-sessions", tags=["Training"])
job_router = fastapi.APIRouter(prefix="/training-jobs", tags=["Training"])

#: The one code covering "this job has no linked assessment". Shared by all three
#: operations that meet it, which report it at three different statuses — see the module
#: docstring — because the code is the stable thing a client branches on and the fault is
#: one fault however it is met.
STATE_UNAVAILABLE_CODE = "TRAINING_JOB_STATE_UNAVAILABLE"

#: ``SelectorNotVisible.field`` -> the error ``code`` the client branches on. A stated
#: mapping rather than a code built by upper-casing the field name, so the published
#: codes cannot be renamed by renaming a request field. The two version spellings match
#: the predict slice's, which refuses the same two ids under the same names.
_SELECTOR_ERROR_CODES = {
    "source_version_id": "SOURCE_VERSION_NOT_FOUND",
    "target_version_id": "TARGET_VERSION_NOT_FOUND",
    "source_revision_id": "SOURCE_REVISION_NOT_FOUND",
    "target_revision_id": "TARGET_REVISION_NOT_FOUND",
}


def _selector_error(exc: train_service.SelectorNotVisible) -> V4APIError:
    """The 404 for a pair selector the caller cannot reach.

    ``details`` names the field as well as the id, because a request carries two of them
    and "404, version 12 does not exist" is not actionable when 12 was sent as both
    sides.
    """
    return V4APIError(
        status_code=status.HTTP_404_NOT_FOUND,
        code=_SELECTOR_ERROR_CODES[exc.field],
        message=str(exc),
        details={"field": exc.field, "resource_id": exc.resource_id},
    )


def _session_url(request: Request, session_id: str) -> str:
    """The URL a client should poll for ``session_id``.

    Root-relative, and derived from the session route itself via ``url_for`` rather than
    formatted by hand — so renaming :func:`get_training_session` raises ``NoMatchFound``
    on the next submit instead of quietly publishing a ``Location`` that 404s. Only
    ``.path`` is taken: ``url_for`` builds an absolute URL from the request's own scheme,
    which behind a TLS-terminating proxy that sets no forwarded headers would hand back
    ``http://`` for a request made over ``https``.
    """
    return request.url_for("get_training_session", session_id=session_id).path


def _to_job_out(job: TrainingJob) -> TrainingJobOut:
    """Build the job resource from an ORM row and its linked assessment.

    From **named columns**, never by splatting the row (#891), so the field set stays
    closed. This is also the one place the two rows are bridged: the job carries its own
    identity and pair, and every status/timing field comes from ``job.assessment`` — which
    may be absent, which is why each of those reads is guarded rather than assumed.

    ``error`` is built here for both of its causes, and neither is built by hand:
    :meth:`~api_v4.jobs.JobEnvelope.failed` owns the fallback message for an assessment
    that reached ``failed`` with a null ``status_detail`` (which happens), and the
    state-unavailable prose comes from the service so the list and the two operations that
    *raise* it cannot come to word the same fault differently.
    """
    assessment = job.assessment
    state = train_service.state_for_training_job(job)

    error = None
    if state is None:
        error = V4ErrorDetail(
            code=STATE_UNAVAILABLE_CODE,
            message=train_service.state_unavailable_message(job.id),
            details={"training_job_id": job.id},
        )
    elif state is JobState.FAILED:
        error = JobEnvelope.failed(
            job_id=str(job.id), message=assessment.status_detail
        ).error

    return TrainingJobOut(
        id=job.id,
        session_id=job.session_id,
        type=job.type,
        state=state,
        error=error,
        status_detail=assessment.status_detail if assessment is not None else None,
        percent_complete=(
            assessment.percent_complete if assessment is not None else None
        ),
        source_revision_id=job.source_revision_id,
        target_revision_id=job.target_revision_id,
        source_version_id=job.source_version_id,
        target_version_id=job.target_version_id,
        options=job.options,
        assessment_id=job.assessment_id,
        requested_at=job.requested_time,
        started_at=assessment.start_time if assessment is not None else None,
        ended_at=assessment.end_time if assessment is not None else None,
        owner_id=job.owner_id,
    )


def _job_not_found(exc: Exception, job_id: int) -> V4APIError:
    """The 404 for a job the caller cannot read or reach.

    Shared by the read and the delete so both report an unreachable id identically. One
    code covers "no such id", "outside your groups and not yours", "soft-deleted" and "its
    revision or version was soft-deleted" — the service resolves all four in one scoped
    query and must not separate them.
    """
    return V4APIError(
        status_code=status.HTTP_404_NOT_FOUND,
        code="TRAINING_JOB_NOT_FOUND",
        message=str(exc),
        details={"training_job_id": job_id},
    )


def _session_not_found(exc: Exception, session_id: str) -> V4APIError:
    """The 404 for a session key with no visible jobs.

    Shared by the session read and its results read. A session is a column value rather
    than a row, so "no jobs" and "never existed" are the same state — see
    :class:`~train_routes.v4.train_service.TrainingSessionNotFound`.
    """
    return V4APIError(
        status_code=status.HTTP_404_NOT_FOUND,
        code="TRAINING_SESSION_NOT_FOUND",
        message=str(exc),
        details={"session_id": session_id},
    )


@session_router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        202: {"model": JobSubmitAccepted, "headers": JOB_ACCEPTED_HEADERS},
        # The one status only this operation can answer, so it is declared here rather
        # than in the shared set: every requested app already had an active job, so
        # nothing was created and there is no session to poll.
        **error_responses(status.HTTP_409_CONFLICT),
    },
)
async def create_training_session(
    request: Request,
    data: TrainingSessionCreate,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> JSONResponse:
    """Submit a training run for a revision pair.

    Returns `202 Accepted` with `Location` pointing at the new **session** and
    `Retry-After` advertising the polling cadence. The body carries only `job_id`, which
    here is the session key: what you submitted is a session, and the session is what you
    poll. The per-app job ids and `inference_readiness` are in the first poll's body.

    Each selected app becomes its own job, dispatched independently — so one app failing
    to start does not stop the others, and that job reports the reason on the session
    read. Omitting `apps` trains all of them.

    **An app that already has an active job for this pair with these options is skipped,
    not duplicated.** The session then holds only the jobs that were created, and a
    submit where *every* app was skipped is a `409` naming the existing job ids. A
    *finished* run is not a duplicate: retraining a pair is how new verse text is picked
    up.

    Each side of the pair is named by version (its latest non-deleted revision is
    resolved) or by revision, and the caller must be able to see it — an id outside their
    groups reports exactly as a non-existent one, naming the field it came from. A
    visible version with no revisions is a `422` rather than a `404`, because the version
    is real and uploading a revision is what fixes it.
    """
    try:
        session_id, _ = await train_service.create_session(db, current_user, data)
    except train_service.SelectorNotVisible as exc:
        raise _selector_error(exc) from exc
    except train_service.VersionHasNoRevisions as exc:
        raise V4APIError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="VERSION_HAS_NO_REVISIONS",
            message=str(exc),
            details={"field": exc.field, "version_id": exc.version_id},
        ) from exc
    except train_service.TrainingJobsAlreadyActive as exc:
        raise V4APIError(
            status_code=status.HTTP_409_CONFLICT,
            code="TRAINING_JOBS_ALREADY_ACTIVE",
            message=str(exc),
            details={"existing_job_ids": exc.existing_job_ids, "apps": exc.apps},
        ) from exc

    return job_accepted_response(
        job_id=session_id,
        poll_url=_session_url(request, session_id),
        retry_after_s=train_service.TRAINING_RETRY_AFTER_S,
    )


@session_router.get(
    "/{session_id}",
    response_model=TrainingSessionOut,
    # The 200 entry declares no model: FastAPI generates that half from
    # ``response_model`` and deep-merges this dict into it, so the entry exists purely to
    # hang ``Retry-After`` on the status a still-running session returns. No 202 entry —
    # the aggregate state is never PENDING, so this read cannot produce one.
    responses={200: {"headers": JOB_POLL_HEADERS}},
)
async def get_training_session(
    session_id: str,
    response: Response,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> TrainingSessionOut:
    """Poll one training session: its aggregate state, its jobs, and what it has unlocked.

    `state` is one thing to branch on — `FAILED` if any job failed, else `RUNNING` if any
    is still going, else `SUCCEEDED` — and `jobs` is where the detail lives. **It is never
    `PENDING`**: a session that has been accepted but not started reports `RUNNING`, and
    the per-job `state` is what distinguishes queued from running. It is `null` only when
    a job's state cannot be read at all, which that job's own `error` explains.

    `Retry-After` rides every non-terminal response and is removed once the session is
    terminal, so a polling loop stops being invited back. A session whose jobs all failed
    is still an HTTP `200`: reading the session succeeded, the training did not.

    `inference_readiness` says which analyses can be run against this revision pair now.
    It is computed over every finished training job for the pair rather than only this
    session's, so it can report an analysis ready that this session never ran.

    A session key with no jobs the caller can see is a `404` — there is no session row, so
    "you cannot see its jobs", "its jobs were deleted" and "that key was never issued" are
    one answer.
    """
    try:
        jobs = await train_service.get_session_jobs(db, current_user, session_id)
    except train_service.TrainingSessionNotFound as exc:
        raise _session_not_found(exc, session_id) from exc

    items = [_to_job_out(job) for job in jobs]
    state = train_service.session_state([item.state for item in items])
    readiness = await train_service.inference_readiness(
        db, jobs[0].source_revision_id, jobs[0].target_revision_id
    )

    if state is None:
        # Nothing to advertise: polling cannot resolve a missing assessment row, so the
        # cadence hint would invite a loop that can never finish. Deleted rather than
        # left unset, for the same post-condition ``set_poll_headers`` enforces.
        del response.headers["Retry-After"]
    else:
        set_poll_headers(
            response, state=state, retry_after_s=train_service.TRAINING_RETRY_AFTER_S
        )
    return TrainingSessionOut(
        session_id=session_id,
        state=state,
        jobs=items,
        inference_readiness=readiness,
    )


@session_router.get(
    "/{session_id}/results",
    response_model=V4Page[TrainingResultRow],
)
async def get_training_session_results(
    session_id: str,
    page: PaginationParams = Depends(),
    scope: VerseScopeParams = Depends(),
    tfidf_top_k: int = Query(
        TFIDF_TOP_K_DEFAULT,
        ge=1,
        le=TFIDF_TOP_K_MAX,
        description=(
            "How many TF-IDF neighbours to return per side, per verse. This multiplies "
            "the size of every row on the page, so it is capped well below the "
            "assessment similarity read's own limit, which bounds a single ranking "
            "rather than one per row."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> V4Page[TrainingResultRow]:
    """Read a training session's output, one row per verse, in canonical Bible order.

    Each row interleaves every finished job's result for that verse: the
    `semantic-similarity` score, the `word-alignment` pairings and their verse-level
    score, the `tfidf` nearest neighbours per side, and the `ngrams` corpus hits per side.
    A type whose job has **not finished** contributes nothing — its field is null or
    empty — so a row of nulls means "nothing has finished here yet", not "no data
    exists". The session read is where a job's state is visible.

    **The verses are the union across those types, not a fixed per-verse grid.** A verse
    appears when at least one finished type has something for it, which is what makes one
    ordered, offset-paginated sequence out of four differently-keyed tables. `total`
    counts that union.

    Both sides of `tfidf` and `ngrams` are populated where they exist. The source side
    comes from a separate session that trained on this session's *source* revision; when
    there is none, the source field is `null` rather than an empty list, so "no
    source-side corpus" is distinguishable from "a corpus with nothing for this verse".

    `book`, `chapter` and `verse` narrow progressively, each needing the one above it. A
    well-formed book abbreviation naming no book yields an empty page rather than v3's
    `400`: it narrows an already-authorized set instead of naming a parent resource,
    which is how every other v4 filter behaves.

    Pagination is mandatory, where v3 returned every verse when `page` was omitted.
    """
    try:
        jobs = await train_service.get_session_jobs(db, current_user, session_id)
    except train_service.TrainingSessionNotFound as exc:
        raise _session_not_found(exc, session_id) from exc

    rows, total = await train_service.session_results(
        db,
        jobs,
        scope=scope.scope,
        limit=page.limit,
        offset=page.offset,
        tfidf_top_k=tfidf_top_k,
    )
    # No next_updated_since: none of the result tables carries a modification timestamp,
    # so this list has no delta feed. The key is still present and null, per the
    # envelope's contract that adding delta support later is not a shape change.
    return V4Page[TrainingResultRow].create(items=rows, total=total, pagination=page)


@job_router.get(
    "",
    response_model=V4Page[TrainingJobOut],
)
async def list_training_jobs(
    page: PaginationParams = Depends(),
    state: Optional[JobState] = Query(
        None,
        description=(
            "Return only jobs in this state. Takes the public state vocabulary; v3's "
            "equivalent filter matches the raw internal status of the linked assessment. "
            "Filtering by any state excludes jobs that have no linked assessment, since "
            "such a job has no state to match."
        ),
    ),
    training_type: Optional[TrainingType] = Query(
        None,
        alias="type",
        description=(
            "Return only jobs training this analysis. Validated against the closed set, "
            "so a misspelled type is a 422 rather than an empty page."
        ),
    ),
    source_version_id: Optional[int] = Query(
        None,
        description=(
            "Return only jobs whose source side is this version. A version the caller "
            "cannot see yields an empty page rather than a 404: this narrows an already "
            "authorized set, it does not name a parent resource."
        ),
    ),
    target_version_id: Optional[int] = Query(
        None,
        description="Return only jobs whose target side is this version.",
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> V4Page[TrainingJobOut]:
    """List training jobs the caller may access, lowest id first, paginated.

    A job is visible to its owner, to an admin, and to anyone whose groups reach the
    versions of **both** sides of its pair — one rule, where v3 scopes its list by group
    access alone and its single-job read by owner *or* group access, so a v3 caller can
    read a job by id that never appears in their own list.

    Two further differences from v3's `GET /train`, both of which mean a client comparing
    the two sees a different set rather than a regression:

    * **Paginated, and bounded.** v3 returns every matching job in one unbounded list.
    * **A job whose revision or version was soft-deleted is hidden**, matching
      `GET /v4/assessments`. v3 checks neither.

    `state` is read from each job's linked assessment, because `training_job` stores no
    state of its own. **A job whose assessment has been deleted reports `state: null` and
    an `error`** rather than being dropped from the page: that is a data-integrity fault,
    and a list is the only view that can surface it. It is rare, and a client that ignores
    it will treat such a job as neither running nor finished, which is the truth.

    No `updated_since` delta sync, unlike `/versions`, `/revisions` and `/assessments`:
    `training_job` has no `updated_at` column, so there is nothing to filter or watermark
    on. Adding one needs a migration, which this slice does not write.

    Ordered by id rather than v3's insertion order, because offset pagination needs a
    total order on a column that cannot tie or move.
    """
    jobs, total = await train_service.list_jobs(
        db,
        current_user,
        limit=page.limit,
        offset=page.offset,
        state=state,
        training_type=training_type,
        source_version_id=source_version_id,
        target_version_id=target_version_id,
    )
    return V4Page[TrainingJobOut].create(
        items=[_to_job_out(job) for job in jobs], total=total, pagination=page
    )


@job_router.get(
    "/{job_id}",
    response_model=TrainingJobDetail,
    responses={
        200: {"headers": JOB_POLL_HEADERS},
        202: {
            "model": TrainingJobDetail,
            "description": "Accepted, not yet started",
            "headers": JOB_POLL_PENDING_HEADERS,
        },
    },
)
async def get_training_job(
    job_id: int,
    response: Response,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> TrainingJobDetail:
    """Poll one training job: its own fields, merged with the job envelope.

    `202` while the job is `PENDING`, `200` in every other state, and `Retry-After` on
    every non-terminal poll. A `FAILED` job is a `200` whose body carries
    `state: "FAILED"` and an `error`: reading the job succeeded, the job did not.

    `job_id` is the string form of the integer `id` in the same body — the envelope
    stringifies so a client parses one type across assessments, training and predict.

    **A training run is observable under two ids.** This one, and the `assessment_id` in
    the body, which is the row its state, timing and results are actually stored under.
    They are one job seen twice, not two jobs, and both report the same state. Only the
    training-job id is addressable here: no v4 training endpoint accepts an assessment id,
    and `GET /v4/assessments` does not serve training rows.

    A job whose assessment has been deleted has no state to report, and this body has
    nowhere to say so — it is a job envelope, and the envelope has no state for "unknown".
    So it answers `500 TRAINING_JOB_STATE_UNAVAILABLE`, named rather than generic. The
    same job is still listable: `GET /v4/training-jobs` reports it with `state: null`.
    """
    try:
        job = await train_service.get_job(db, current_user, job_id)
    except train_service.TrainingJobNotFound as exc:
        raise _job_not_found(exc, job_id) from exc

    out = _to_job_out(job)
    if out.state is None:
        raise V4APIError(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            code=STATE_UNAVAILABLE_CODE,
            message=train_service.state_unavailable_message(job_id),
            details={"training_job_id": job_id},
        )

    set_poll_headers(
        response, state=out.state, retry_after_s=train_service.TRAINING_RETRY_AFTER_S
    )
    return TrainingJobDetail(**out.model_dump(), job_id=str(job.id))


@job_router.delete(
    "/{job_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        # 403 is declared per write rather than shared: v4 answers 404 for a resource the
        # caller cannot see, so 403 only ever means "visible, but not yours". This is the
        # only training operation that can answer one.
        **V4_FORBIDDEN_RESPONSE,
        **error_responses(status.HTTP_409_CONFLICT),
    },
)
async def delete_training_job(
    job_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> Response:
    """Soft-delete a finished training job (its owner, or an admin).

    `204` with no body, where v3 returned `200` and a prose `{"detail": ...}`. The
    soft-delete itself is unchanged: the row stays, its results stay, `deleted` flips.
    Idempotent — re-deleting an already-deleted job is another `204`, where v3 answers
    `404`.

    **Only a terminal job can be deleted.** A queued or running one is a `409`: v4 holds
    no Modal handle, so deleting would hide the row while the GPU run kept going and kept
    costing. Wait for it to finish, or fail, and then delete. A job whose linked
    assessment is gone cannot be *verified* terminal and is refused with the same `409`
    the state-unavailable fault gets everywhere else.

    `403` means the job is visible to you but belongs to someone else. Jobs created before
    `owner_id` existed have no owner, so no non-admin can delete those — a property of the
    data rather than an authorization failure.

    Deleting a job does not delete the assessment row behind it, and does not remove the
    trained artifacts: the next submit for the same pair and options will still see a
    *running* job as a duplicate, because duplicate detection reads the assessment.
    """
    try:
        await train_service.soft_delete_job(db, current_user, job_id)
    except train_service.TrainingJobNotFound as exc:
        raise _job_not_found(exc, job_id) from exc
    except train_service.TrainingJobAccessForbidden as exc:
        raise V4APIError(
            status_code=status.HTTP_403_FORBIDDEN,
            code="TRAINING_JOB_ACCESS_FORBIDDEN",
            message=str(exc),
            details={"training_job_id": job_id},
        ) from exc
    except train_service.TrainingJobStateUnavailable as exc:
        raise V4APIError(
            status_code=status.HTTP_409_CONFLICT,
            code=STATE_UNAVAILABLE_CODE,
            message=str(exc),
            details={"training_job_id": job_id},
        ) from exc
    except train_service.TrainingJobNotTerminal as exc:
        raise V4APIError(
            status_code=status.HTTP_409_CONFLICT,
            code="TRAINING_JOB_NOT_TERMINAL",
            message=str(exc),
            details={"training_job_id": job_id, "state": exc.state.value},
        ) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)

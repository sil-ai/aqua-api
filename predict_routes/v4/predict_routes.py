"""v4 Predict router (issue #894, epic #842).

Four endpoints, three of them ``POST``:

* ``POST /v4/predictions`` — fan a set of text pairs out to the selected analysis apps
  and return every result inline. Always ``200``; the slow agent leg, when one was
  started, rides as a nullable ``job`` in the same body.
* ``GET  /v4/predictions/{job_id}`` — poll that slow leg. The job envelope merged with
  the per-pair results, and ``Retry-After`` while it runs.
* ``POST /v4/predictions/semantic-similarity`` — score one pair against the model
  fine-tuned for a version pair.
* ``POST /v4/predictions/length-comparison`` — word- and character-count differences
  between two texts.

This module owns HTTP concerns only; :mod:`predict_routes.v4.predict_service` does the
authorization, the Modal dispatch and the job bookkeeping, and each of its domain
signals is mapped here onto a :class:`~api_v4.errors.V4APIError` with a stable ``code``.
Auth is applied at the router level in :func:`api_v4.app.create_v4_app` (#831), so each
handler re-declares ``current_user`` only because it needs the user.


Contract decisions worth reading before changing anything here
--------------------------------------------------------------

**Three of the four are ``POST``, and only one of them creates anything.** The
assessments slice already broke the "a POST that names a resource creates one" reading
with its similarity search; predict leans on the same ground. ``/semantic-similarity``
and ``/length-comparison`` are computations over text the caller supplies in the body
rather than over a stored resource, and they are ``POST`` because verse-sized text does
not belong in a query string (#826) — v3's ``GET /predict/text-lengths`` took two
10,000-character query parameters. ``POST /v4/predictions`` does create a row, but only
sometimes: only a request that asks for the agent's slow pass writes a ``predict_jobs``
row, and it is not what the response is *about*.

**``POST /v4/predictions`` is always ``200``, never ``202``.** #894's decision,
reaffirming v3's. The slow leg is one optional part of a response that otherwise
carries five apps' results, so making the whole path answer ``202`` would charge every
caller for a shape they mostly do not use. ``job`` is always present as a key — null
when no slow leg was started — which is #842's envelope rule; v3 dropped the key
entirely when unset, and a v4 client reads one shape.

**A selector the caller cannot see is a ``404``, not v3's ``403``.** These are
reachability checks against group membership, and #842 answers those with a 404
everywhere on this surface so that ids cannot be probed. The consequence worth stating
is that **no operation in this module can answer a ``403``** — so none of the four goes
into ``FORBIDDEN_OPERATIONS`` in ``test_v4_openapi.py``, and a client generated from
the schema carries no forbidden-handling for predict. The service's module docstring
has the reasoning.

**The poll never answers ``202``.** ``poll_status_code`` maps ``PENDING`` to 202, and
``predict_jobs`` has no pending state: the row is written after the Modal spawn has
already been attempted, so it is born ``running`` or ``failed``. The 202 is therefore
not declared on the poll route — publishing a status this endpoint cannot produce would
put dead branches in every generated client.

**A ``FAILED`` poll reports the generic ``JOB_FAILED`` code.** :mod:`api_v4.jobs`
invites a slice that can classify its failures to pass something more specific, and
this slice can classify them only by parsing the prose stored in ``predict_jobs.error``
— which is exactly what the assessments slice refused to do over ``status_detail``, for
the same reason: a code derived from prose is worse than a generic code honestly
labelled. The prose still reaches the client as the error's ``message``.
"""

import asyncio
import contextlib

import fastapi
from fastapi import Depends, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.errors import V4APIError, error_responses
from api_v4.jobs import JOB_POLL_HEADERS, JobEnvelope, JobState, set_poll_headers
from api_v4.schemas.predict import (
    LengthComparisonOut,
    LengthComparisonRequest,
    PredictApp,
    PredictJob,
    PredictJobHandle,
    PredictOut,
    PredictRequest,
    SimilarityOut,
    SimilarityRequest,
)
from config import settings
from database.dependencies import get_db
from database.models import UserDB as UserModel
from predict_routes.v4 import predict_service
from security_routes.v4.dependencies import get_current_user_v4

router = fastapi.APIRouter(prefix="/predictions", tags=["Predictions"])

#: ``SelectorNotVisible.field`` -> the error ``code`` the client branches on. One
#: mapping rather than a code built by string-mangling the field name, so the published
#: codes are stated rather than derived — and so renaming a request field cannot
#: silently rename a wire contract. The two revision roles follow the assessments
#: slice's spellings (``REVISION_NOT_FOUND`` / ``REFERENCE_NOT_FOUND``) because they
#: mean the same thing there.
_SELECTOR_ERROR_CODES = {
    "revision_id": "REVISION_NOT_FOUND",
    "reference_id": "REFERENCE_NOT_FOUND",
    "assessment_id": "ASSESSMENT_NOT_FOUND",
    "source_version_id": "SOURCE_VERSION_NOT_FOUND",
    "target_version_id": "TARGET_VERSION_NOT_FOUND",
}


def _selector_error(exc: predict_service.SelectorNotVisible) -> V4APIError:
    """The 404 for a selector id the caller cannot reach.

    ``details`` names the field rather than only the id, because a request may carry
    five of them and "404, revision 12 does not exist" is not actionable when the
    caller sent 12 as two different selectors.
    """
    return V4APIError(
        status_code=status.HTTP_404_NOT_FOUND,
        code=_SELECTOR_ERROR_CODES[exc.field],
        message=str(exc),
        details={"field": exc.field, "resource_id": exc.resource_id},
    )


def _poll_url(request: Request, job_id: str) -> str:
    """The URL a client should poll for ``job_id``.

    Root-relative (``/v4/predictions/prj_...``), and derived from the poll route itself
    via ``url_for`` rather than formatted by hand — so renaming :func:`get_prediction`
    raises ``NoMatchFound`` on the next fan-out instead of quietly publishing a URL that
    404s. Only ``.path`` is taken: ``url_for`` builds an absolute URL from the request's
    own scheme, which behind a TLS-terminating proxy that sets no forwarded headers
    would hand back ``http://`` for a request made over ``https``.
    """
    return request.url_for("get_prediction", job_id=job_id).path


@router.post(
    "",
    response_model=PredictOut,
)
async def create_prediction(
    request: Request,
    data: PredictRequest,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> PredictOut:
    """Run the selected analyses over a set of text pairs.

    Every selected app is called in parallel and gets its own entry in ``results``.
    Per-app failure is isolated: an app that times out, fails, or has never been trained
    for this revision reports that in its own entry and never suppresses the others — so
    a ``200`` here means "the fan-out ran", not "everything succeeded". Check each
    entry's ``status`` before reading its ``data``.

    Asking for the agent's translation or critique starts a second, background agent
    call, because those passes are LLM work that can outlast any sane request timeout.
    The inline ``agent-critique`` entry then holds the fast slice only, and ``job``
    carries the handle to poll for the slow one. Every other request returns
    ``"job": null``.

    Any of the five selector ids may be omitted; each app reads the ones it needs. An id
    that is present must name a resource the caller can see, or the request is a ``404``
    naming the offending field — including ``source_version_id`` and
    ``target_version_id``, which v3 accepted unchecked.
    """
    try:
        await predict_service.authorize_selectors(db, current_user, data)
    except predict_service.SelectorNotVisible as exc:
        raise _selector_error(exc) from exc

    apps = data.apps if data.apps is not None else list(PredictApp)
    modal_env = settings.modal_env
    payload = predict_service.runner_payload(data)

    # Translation and critique are the agent's only slow legs. When either is asked
    # for, the synchronous fan-out runs with both flags off — so every app, agent
    # included, returns its fast slice — and the full payload goes to the spawned call
    # instead. Without this the agent's inline leg would do the same LLM work twice.
    spawn_slow_agent = PredictApp.agent_critique in apps and (
        data.include_translation or data.include_critique
    )
    sync_payload = payload
    if spawn_slow_agent:
        sync_payload = {
            **payload,
            "include_translation": False,
            "include_critique": False,
        }

    fanout = predict_service.run_fanout(sync_payload, apps, modal_env)
    if not spawn_slow_agent:
        return PredictOut(pairs=data.pairs, results=await fanout, job=None)

    # Overlapped rather than sequential: the spawn is its own round trip to Modal and
    # there is no reason for it to wait on six inference calls. Running them together is
    # safe despite the shared session, because only the spawn touches the database.
    #
    # Not ``asyncio.gather``, though, and the difference matters. ``spawn_slow_agent``
    # handles a Modal spawn that throws, but its own commit can still fail (a dropped
    # connection, an exhausted pool). Under ``gather`` that exception propagates while
    # the fan-out task keeps running detached — the client gets a 500 and every
    # inference call already in flight is paid for and thrown away. Cancelling it
    # explicitly means a failed submit stops the work it can no longer report.
    fanout_task = asyncio.create_task(fanout)
    try:
        job = await predict_service.spawn_slow_agent(
            db, current_user, data, payload, modal_env
        )
    except BaseException:
        fanout_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await fanout_task
        raise
    results = await fanout_task
    return PredictOut(
        pairs=data.pairs,
        results=results,
        job=PredictJobHandle(
            job_id=job.id,
            state=predict_service.state_for_predict_status(job.status),
            includes=predict_service.includes_for(
                job.include_translation, job.include_critique
            ),
            poll_url=_poll_url(request, job.id),
            retry_after_s=predict_service.PREDICT_RETRY_AFTER_S,
        ),
    )


@router.post(
    "/semantic-similarity",
    response_model=SimilarityOut,
    # The one status only this operation can answer, so it is declared here rather than
    # in the shared set: the inference app was unreachable, the request was valid, and
    # the call is worth retrying. Distinct from the 422 this endpoint also returns for
    # a version pair that has no model, which retrying will not fix.
    responses=error_responses(status.HTTP_503_SERVICE_UNAVAILABLE),
)
async def create_semantic_similarity(
    data: SimilarityRequest,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> SimilarityOut:
    """Score how close two texts are in meaning.

    The two version ids select the fine-tuned model, and the caller must be able to see
    both — the authorization v3 did not perform on this endpoint (#861), which made it
    the one predict operation that took resource ids from an authenticated caller
    without checking they were theirs.

    Two failures to tell apart, and the ``code`` is what tells them apart: a ``422``
    means this version pair has no usable model and training is what fixes it, while a
    ``503`` means the inference service could not be reached and retrying might.
    """
    try:
        await predict_service.authorize_version_pair(
            db, current_user, data.source_version_id, data.target_version_id
        )
    except predict_service.SelectorNotVisible as exc:
        raise _selector_error(exc) from exc

    try:
        score = await predict_service.semantic_similarity(data, settings.modal_env)
    except predict_service.SimilarityModelUnavailable as exc:
        raise V4APIError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="SIMILARITY_MODEL_UNAVAILABLE",
            message=str(exc),
            details={
                "reason": exc.reason,
                "source_version_id": exc.source_version_id,
                "target_version_id": exc.target_version_id,
            },
        ) from exc
    except predict_service.InferenceUnavailable as exc:
        raise V4APIError(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="INFERENCE_UNAVAILABLE",
            message=str(exc),
            details={"app": exc.app},
        ) from exc

    return SimilarityOut(score=score)


@router.post(
    "/length-comparison",
    response_model=LengthComparisonOut,
)
async def create_length_comparison(
    data: LengthComparisonRequest,
    current_user: UserModel = Depends(get_current_user_v4),
) -> LengthComparisonOut:
    """Compare two texts by word and character count.

    Both differences are source minus target, so a positive value means the source is
    longer. A pure string comparison: it reaches no database and no inference service,
    which is why this is the one handler here that takes no session.

    Renamed from v3's ``GET /predict/text-lengths`` because
    ``GET /v4/assessments/{id}/text-lengths`` already holds that name and a path segment
    names one operation across the surface (§13.3). The two are genuinely different
    operations: that one returns stored per-verse counts for a whole revision, this one
    compares two strings the caller just supplied.
    """
    source_words = len(data.source_text.split()) if data.source_text.strip() else 0
    target_words = len(data.target_text.split()) if data.target_text.strip() else 0
    return LengthComparisonOut(
        word_count_difference=source_words - target_words,
        char_count_difference=len(data.source_text) - len(data.target_text),
    )


@router.get(
    "/{job_id}",
    response_model=PredictJob,
    # The 200 entry declares no model: FastAPI generates that half from
    # ``response_model`` and deep-merges this dict into it, so the entry exists purely
    # to hang ``Retry-After`` on the status a RUNNING poll returns. No 202 entry —
    # see the module docstring for why this poll cannot produce one.
    responses={200: {"headers": JOB_POLL_HEADERS}},
)
async def get_prediction(
    job_id: str,
    response: Response,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> PredictJob:
    """Poll the slow agent leg started by ``POST /v4/predictions``.

    ``pairs`` is present in every state, echoing what was submitted, so a client polling
    a running job still sees its own request; ``translation`` and ``critique`` fill in
    once the job succeeds. Read ``includes`` to know which of the two to expect —
    a null ``critique`` on a succeeded job means it was never asked for.

    ``Retry-After`` rides every non-terminal response and is removed once the job is
    terminal, so a polling loop stops being invited back. A ``FAILED`` job is still an
    HTTP ``200``: reading the job succeeded, the job did not, and the reason travels in
    the body's ``error`` rather than as a transport error.

    Polling is also what *advances* the job — ``Function.spawn`` has no callback, so
    nothing else can — but concurrent polls cannot corrupt it: the first one to see a
    finished Modal call records the outcome and the rest read what it wrote.

    A job belonging to another caller reports the same ``404`` as one that never
    existed.
    """
    try:
        job = await predict_service.get_job(db, current_user, job_id)
    except predict_service.PredictJobNotFound as exc:
        raise V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="PREDICTION_JOB_NOT_FOUND",
            message=str(exc),
            details={"job_id": job_id},
        ) from exc

    job = await predict_service.advance_job(db, job)
    state = predict_service.state_for_predict_status(job.status)

    # Built by ``JobEnvelope.failed`` and lifted off the envelope it returns rather than
    # constructed here: that classmethod owns the fallback message for a row that
    # reached ``failed`` with a null ``error``, which happens, and a slice writing its
    # own ``or "..."`` guard and forgetting it would trip the envelope's validator and
    # turn a legitimately failed job into a 500.
    error = None
    if state is JobState.FAILED:
        error = JobEnvelope.failed(job_id=job.id, message=job.error).error

    body = PredictJob(
        job_id=job.id,
        state=state,
        error=error,
        includes=predict_service.includes_for(
            job.include_translation, job.include_critique
        ),
        pairs=predict_service.job_pairs(job),
    )
    set_poll_headers(
        response, state=state, retry_after_s=predict_service.PREDICT_RETRY_AFTER_S
    )
    return body

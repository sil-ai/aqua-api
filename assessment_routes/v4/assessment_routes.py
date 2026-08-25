"""v4 Assessments router (issues #826/#827/#828/#865/#893, epic #842).

The first real consumer of :mod:`api_v4.jobs`, and the first v4 endpoint whose work
happens somewhere else. One endpoint ships here:

* ``POST /v4/assessments`` — submit an assessment run; ``202 Accepted`` with
  ``Location`` and ``Retry-After``.

List, poll (``GET /v4/assessments/{id}``), delete and the typed result sub-resources
are the remaining pieces of #893 and land in follow-up PRs. **Until the poll endpoint
lands, the ``Location`` this returns is a valid URL that 404s** — the header names
where the job *will* be readable, and pointing it anywhere else would mean changing
the contract twice.

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


Two contract decisions worth reading before changing anything here
------------------------------------------------------------------

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
"""

__version__ = "v4"

import fastapi
from fastapi import Depends, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.errors import V4APIError
from api_v4.jobs import JobSubmitAccepted, job_accepted_response
from api_v4.schemas.assessment import AssessmentCreate
from assessment_routes.v4 import assessment_service
from database.dependencies import get_db
from database.models import UserDB as UserModel
from security_routes.auth_routes import get_current_user

router = fastapi.APIRouter(prefix="/assessments", tags=["Assessments"])

#: Cadence advertised on the 202, in seconds. Required rather than inherited — there
#: is no v4-wide default, precisely so a slice cannot pick up a cadence tuned for
#: something else (:mod:`api_v4.jobs`, divergence 3). Assessments span roughly a
#: minute (``sentence-length`` on one revision) to forty (``agent-critique`` over a
#: long range on a busy Modal environment), so this is set for the slow end: 30s costs
#: a fast job one extra poll, while v3's 10s predict cadence would cost a long one
#: ~240 wasted polls an hour. The poll endpoint will re-advertise the same value on
#: every non-terminal poll.
ASSESSMENT_RETRY_AFTER_S = 30


def _poll_url(request: Request, assessment_id: int) -> str:
    """The URL the client should poll for ``assessment_id``.

    Root-relative (``/v4/assessments/42``), which RFC 9110 §10.2.2 allows for
    ``Location`` and :func:`api_v4.jobs.job_accepted_response` passes through
    unchanged.

    Built from the sub-app's ``root_path`` (Starlette sets it to the mount prefix,
    ``/v4``) plus this router's own prefix, rather than from a hard-coded literal, so
    the header cannot drift if either changes. It cannot use the
    ``str(request.url_for("get_assessment", ...))`` form :mod:`api_v4.jobs`
    recommends, because that route does not exist yet — ``url_for`` raises
    ``NoMatchFound`` for an unregistered name. **Switch to ``url_for`` in the PR that
    adds the poll endpoint**; a test pins the produced value so the swap is checkable.
    """
    return f"{request.scope.get('root_path', '')}{router.prefix}/{assessment_id}"


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

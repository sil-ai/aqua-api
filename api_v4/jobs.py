"""Reusable async-job contract for every /v4 long-running operation (issue #827,
epic #842).

Assessments, training runs, and predict's slow path are all "submit now, answer
later" operations that v3 exposes three different ways. v4 gives them **one**
envelope, **one** submit response, and **one** poll shape (migration guide §7,
mirroring AERO's task pattern)::

    POST /v4/assessments        -> 202 Accepted
                                   Location: /v4/assessments/42
                                   Retry-After: 30
                                   {"job_id": "42"}

    GET  /v4/assessments/42     -> 200 (or 202 while PENDING)
                                   {"job_id": "42", "state": "SUCCEEDED",
                                    "result": {...}, "error": null}

This module ships four pieces:

* :class:`JobState` — the closed, public state vocabulary (``PENDING`` /
  ``RUNNING`` / ``SUCCEEDED`` / ``FAILED``) plus
  :data:`ASSESSMENT_STATE_MAP` / :func:`state_for_assessment_status`, the
  translation from the internal ``queued/running/finished/failed`` vocabulary.
* :class:`JobEnvelope` — the ``{job_id, state, result, error}`` poll body.
* :func:`job_accepted_response` — the 202 submit response (``Location`` +
  ``Retry-After``, body ``{"job_id": ...}``).
* :func:`poll_status_code` / :func:`set_poll_headers` — the poll-side half of the
  same contract, so every slice answers a poll identically.
* :data:`JOB_ACCEPTED_HEADERS` / :data:`JOB_POLL_PENDING_HEADERS` /
  :data:`JOB_POLL_HEADERS` — the ``responses={...: {"headers": ...}}`` declarations
  that publish those two headers to ``/v4/openapi.json`` (#928). The functions above
  set the headers; these document them, and FastAPI never checks one against the
  other, so a change to either wants the same change to its pair.

Scope: this PR defines the reusable *contract only*. It deliberately wires
nothing into assessments, training, or predict — those are separate vertical
slices that will consume this module the way the Versions slice (#883) consumed
:class:`~api_v4.pagination.V4Page` and :class:`~api_v4.errors.V4APIError`. It
also ships **no idempotency-key handling** (see "Not in this module" below).


Deliberate contract decisions
-----------------------------

**The state vocabulary is closed, uppercase, and public.** Four states, and a
client may branch exhaustively on them. ``CANCELED`` / ``CANCELLING`` are
deliberately **not** shipped: nothing in the system can cancel a Modal run today,
so a fifth state would only make every client's exhaustive switch wrong for a
capability that does not exist. Adding a state later is a breaking change for
exhaustive clients, which is precisely why the set stays minimal until something
can actually produce the new value. The internal vocabulary keeps its own
lowercase spellings (``schemas.assessment.AssessmentStatus``,
``predict_jobs.status``) — v4 does not rename database values, it translates them
at the edge.

**The adapter seam is a per-slice mapping function, not a protocol and not a
``from_model`` constructor.** The three future consumers do not share a shape:

===============  ====================================  ============================
Model            Status source                         Vocabulary
===============  ====================================  ============================
``Assessment``   ``assessment.status`` (``Text``)       queued/running/finished/failed
``TrainingJob``  ``training_job.assessment.status``     (borrowed — see below)
``PredictJob``   ``predict_jobs.status`` (CHECK'd)      running/complete/failed
===============  ====================================  ============================

So each slice writes its own total function ``(row) -> JobState`` and builds the
envelope itself; **this module imports nothing from** :mod:`database.models`, and
the dependency direction is always slice -> infrastructure. Rejected
alternatives, and why:

* *A* ``Protocol`` *with a* ``status: str`` *attribute.* Structurally all three
  would satisfy it and it would guarantee nothing — the whole difficulty is that
  the three ``status`` values mean different things. Worse, ``TrainingJob`` has no
  ``status`` column at all (it is metadata only; status/timing/progress live on
  the linked ``Assessment`` row — see aqua-api#584/#593), so the one model that
  most needs the seam would fail the protocol.
* *A* ``JobEnvelope.from_model(row)`` *classmethod.* That is exactly the import
  this module must not have: shared v4 infrastructure would then depend on three
  ORM models and grow an ``isinstance`` ladder that every new job-bearing
  resource has to edit.
* *A registry slices register adapters into.* Runtime indirection with exactly
  one implementation per slice, plus a half-populated-registry failure mode that
  is invisible until a request arrives.

**...but the assessment mapping ships here, and predict's does not.** That
asymmetry is intentional, not an oversight. ``AssessmentStatus`` is the *shared*
internal status vocabulary of **two** of the three consumers — the assessments
slice reads it directly, and the training slice reads it through
``TrainingJob.assessment`` — and the guide's §7 state table is written in it. A
vocabulary two slices must agree on belongs in the shared module; predict's
vocabulary is private to its own table (enforced by
``ck_predict_jobs_status``) and belongs in the predict slice. Note this imports
:mod:`schemas.assessment` (a Pydantic enum module with no DB dependency), not an
ORM model — importing the enum rather than restating its four strings is what
keeps the mapping from silently going stale if the internal vocabulary changes.

**What** ``job_id`` **refers to: the id of the resource served at the poll URL.**
``job_id`` and the ``{id}`` segment of the ``Location`` header are always the same
value. There is no separate job registry and ``job_id`` is *not* defined as "a row
in a jobs table". Checked against all three consumers:

* **Assessments** — poll ``GET /v4/assessments/{id}``, ``job_id = str(assessment.id)``.
  The job *is* the resource, as the guide specifies.
* **Predict** — poll ``GET /v4/predict/jobs/{id}``, ``job_id = predict_job.id``.
  The client never submitted a "predict job", but the ``predict_jobs`` row is
  nonetheless the resource at the poll URL, so the rule holds unchanged.
* **Training** — poll ``GET /v4/training-jobs/{id}``, ``job_id = str(training_job.id)``.

Training is where the rule earns its wording: a ``TrainingJob`` stores no state,
so ``job_id`` identifies the pollable resource while the state is read from a
*different* row (``training_job.assessment``). The rule holds; the adapter seam
absorbs the indirection. Two consequences the training slice must handle rather
than inherit silently:

1. A training run is observable under **two** ids — its training-job id and its
   underlying assessment id — and both envelopes report the same state. They are
   one job seen twice, not two jobs. Do not treat ``assessment_id`` as a job id
   on the training surface.
2. ``training_job.assessment_id`` is nullable (``ondelete="SET NULL"``), so a
   training row can exist with **no state carrier at all**. That is a
   data-integrity fault, not a job state; this module offers no ``UNKNOWN`` state
   to launder it into.

**Wire type is** ``str`` **, uniformly.** ``Assessment.id`` and ``TrainingJob.id``
are integer primary keys and ``PredictJob.id`` is an opaque ``prj_…`` string. The
envelope stringifies, so a client parses one type across the whole surface
instead of switching per endpoint. The cost is real and worth reviewing: for
assessments the envelope reports ``"job_id": "42"`` while the resource body for
the same row reports ``"id": 42``. The rejected alternative — an ``int | str``
union — pushes that same inconsistency onto every client as a runtime type check.

**A failed job reuses** :class:`~api_v4.errors.V4ErrorDetail` **rather than
inventing a second error shape.** Per the guide's table a ``FAILED`` poll is an
HTTP **200** — the request to read job state succeeded; it is the *job* that
failed. So the failure cannot travel through the #828 exception handler, and
raising :class:`~api_v4.errors.V4APIError` for it would be wrong twice over: it
would turn a successful poll into a 4xx/5xx, and it would discard ``job_id`` and
``state``. But the client should not have to parse two different error objects
depending on *where* the failure happened, so ``JobEnvelope.error`` is literally
``V4ErrorDetail`` — the same ``{code, message, details}`` object, with the same
"``code`` is the stable thing you branch on, ``details`` is machine-readable
context, prose lives in ``message``" rules. Only the surrounding envelope differs:
a transport error is ``{"error": {...}}`` at the top level, a job failure is the
``error`` *field* of the job envelope, alongside ``job_id`` and ``state``.

**All four envelope keys are always present, including** ``"error": null``. This
diverges from the error envelope, which uses ``exclude_none`` to drop an absent
``details``. Here a fixed four-key shape lets a client read ``body["error"]`` and
``body["result"]`` unconditionally, which is what a polling loop actually does on
every tick; it also matches the envelope printed in #827 verbatim. So do **not**
add ``response_model_exclude_none=True`` to a poll route.

**Illegal state/payload combinations are rejected at construction.**
:class:`JobEnvelope` enforces that ``FAILED`` carries an ``error`` and no other
state does, and that ``result`` is present only on ``SUCCEEDED``. Encoding the
invariants in the model (rather than trusting each slice) means a client can rely
on "``state == "SUCCEEDED"`` implies ``result`` is usable" without defensive
checks. The ``result``-only-on-``SUCCEEDED`` half is the stricter, more arguable
one: it means partial or in-progress output cannot be published through
``result``. That is deliberate — ``result`` is the job's *outcome*, and progress
(``Assessment.percent_complete``, ``status_detail``) is a different concept that
should arrive as its own field if and when a slice needs it, not as a
half-populated ``result`` that clients cannot distinguish from a finished one.


Where v4 deliberately diverges from v3's predict jobs
-----------------------------------------------------

v3 already implements a version of this at ``predict_routes/v3/predict_routes.py``
(``_JOB_POLL_INTERVAL_S``, ``PredictJobHandle.poll_url``, and the ``Retry-After``
set at line 372). v4 keeps its instincts and changes four things:

1. **202 + ``Location``, not 200 + a URL in the body.** v3 returns ``200`` and
   puts the poll URL in the payload (``PredictJobHandle.poll_url``), so a client
   must know the response shape to find out where to poll. v4 returns
   ``202 Accepted`` with the poll URL in the ``Location`` header, which any
   generic HTTP client can follow without parsing the body.
2. **``Retry-After`` on the submit, not only on the poll.** v3 sets it only on a
   still-running *poll* response, so a client's very first poll happens at a
   cadence it invented. v4 advertises the cadence with the 202 itself, then keeps
   re-advertising it on every non-terminal poll (:func:`set_poll_headers`).
3. **The cadence is a required argument, with no shared default.** v3's
   ``_JOB_POLL_INTERVAL_S = 10`` is module-private to one route file, tuned for
   translation jobs that finish in 30–120s. Exporting that as a v4-wide default
   would silently apply a 10-second cadence to an assessment that runs for forty
   minutes — 240 pointless polls per hour per job, which is the exact load
   ``Retry-After`` exists to prevent. So :func:`job_accepted_response` requires
   ``retry_after_s`` and each slice states its own cadence out loud.
4. **``Location`` points at the concrete ``/v4`` path, not the floating
   ``/latest`` alias.** v3 builds ``/latest/predict/jobs/{id}``, so a client that
   deliberately pinned v3 gets handed a URL that will one day mean v4. v4 hands
   back the version the caller actually called. Build it with
   ``str(request.url_for("route_name", ...))`` so the path cannot drift from the
   route it names.

One v3 behavior is *not* carried over as a divergence but as a warning: v3's poll
handler advances job state as a side effect of being polled (it calls Modal,
flips ``status``, and commits — lines 344-387). That is a pragmatic design for a
``Function.spawn`` with no callback, and this module neither requires nor forbids
it. But a slice that does it must ensure the transition it writes is legal for
its own vocabulary; ``JobEnvelope`` validates the shape of what you report, never
the legality of a transition (``ASSESSMENT_VALID_TRANSITIONS`` in
``schemas/assessment.py`` owns that for assessments).


Not in this module
------------------

**Idempotency keys (#827, mitigating #722) are not implemented.** Honoring
``Idempotency-Key`` needs a persistence table, so it lands as a follow-up PR with
its own migration. Until then ``job_accepted_response`` does not read the header,
and a client that retries after a dropped 202 **will** enqueue a second Modal run.
Do not document the header as supported on any v4 endpoint before that PR lands.

**The** ``results_push_*`` **callback surface is untouched.** It stays an
explicitly internal, separately authenticated surface (#827); nothing here is
meant for it.
"""

from __future__ import annotations

from enum import Enum
from typing import Generic, TypeVar

from fastapi import Response, status
from fastapi.responses import JSONResponse
from pydantic import Field, model_validator

from api_v4.errors import V4ErrorDetail
from api_v4.schemas.base import V4BaseModel
from schemas.assessment import AssessmentStatus


class JobState(str, Enum):
    """The public, closed state vocabulary for every v4 async job.

    Uppercase on the wire (``"SUCCEEDED"``), deliberately distinct from the
    lowercase internal vocabularies it is translated from. See the module
    docstring for why the set is exactly these four and why ``CANCELED`` is not
    among them.
    """

    #: Accepted and queued; no work has started yet. Polls as HTTP 202.
    PENDING = "PENDING"
    #: Work is under way.
    RUNNING = "RUNNING"
    #: Terminal success; ``result`` carries the outcome.
    SUCCEEDED = "SUCCEEDED"
    #: Terminal failure; ``error`` carries the reason.
    FAILED = "FAILED"

    @property
    def is_terminal(self) -> bool:
        """Whether no further state change is possible.

        A polling client stops on a terminal state, which is also why
        :func:`set_poll_headers` emits ``Retry-After`` only when this is false.
        """
        return self in TERMINAL_JOB_STATES


#: The states from which nothing further happens. Kept as data (not just the
#: property) so a slice can use it in a query filter or an ``in`` check.
TERMINAL_JOB_STATES = frozenset({JobState.SUCCEEDED, JobState.FAILED})


#: Internal assessment vocabulary -> public state (migration guide §7). Total
#: over :class:`~schemas.assessment.AssessmentStatus` — a test pins that, so
#: adding an internal status fails loudly here instead of silently falling
#: through at runtime. Shared by the assessments and training slices (training
#: reads status off its linked ``Assessment``); see the module docstring for why
#: this one mapping lives in the shared module and predict's does not.
ASSESSMENT_STATE_MAP: dict[AssessmentStatus, JobState] = {
    AssessmentStatus.queued: JobState.PENDING,
    AssessmentStatus.running: JobState.RUNNING,
    AssessmentStatus.finished: JobState.SUCCEEDED,
    AssessmentStatus.failed: JobState.FAILED,
}


def state_for_assessment_status(status: AssessmentStatus | str | None) -> JobState:
    """Translate an internal assessment status into its public :class:`JobState`.

    Accepts either the enum or the raw string, because ``Assessment.status`` is an
    unconstrained ``Text`` column and the ORM hands back a plain ``str``.

    Raises :class:`ValueError` on anything outside the four internal statuses
    (including ``None``). That is deliberate and it is the caller's decision what
    to do with it — this module will not invent a state for data it does not
    understand. Mapping an unrecognized value to ``FAILED`` would tell a client a
    job failed when the truth is that the server cannot read its own row, and
    mapping it to ``PENDING`` would make a dead job look alive forever. Letting it
    surface as an ``INTERNAL_ERROR`` 500 is the honest signal that a row is
    inconsistent; a slice that would rather degrade gracefully must catch this
    explicitly and say so at its own call site.
    """
    try:
        return ASSESSMENT_STATE_MAP[AssessmentStatus(status)]
    except ValueError as exc:
        raise ValueError(
            f"{status!r} is not a known internal assessment status "
            f"({', '.join(s.value for s in AssessmentStatus)})."
        ) from exc


ResultT = TypeVar("ResultT")


class JobEnvelope(V4BaseModel, Generic[ResultT]):
    """The poll body for every v4 async job: ``{job_id, state, result, error}``.

    Declare it as a poll route's ``response_model=JobEnvelope[SomeResult]`` so the
    result payload is typed in OpenAPI (FastAPI renders the parametrized model
    under a generated name like ``JobEnvelope_AssessmentSummary_``; clients read
    the ``$ref``, not the name). The bare ``JobEnvelope`` leaves ``result``
    untyped, which is fine for a slice whose result shape is genuinely open.

    All four keys are always emitted, including ``"error": null`` — do not add
    ``response_model_exclude_none=True`` to a poll route (module docstring).

    Construction is plain for the three non-failure states::

        JobEnvelope(job_id="42", state=JobState.RUNNING)
        JobEnvelope[Summary](job_id="42", state=JobState.SUCCEEDED, result=summary)

    and goes through :meth:`failed` for the fourth, which is the only case that
    has to build a :class:`~api_v4.errors.V4ErrorDetail`.
    """

    job_id: str = Field(
        description=(
            "Identifier of the job, equal to the resource id in the poll URL "
            "(the Location header returned by the submit). Always a string, even "
            "where the underlying primary key is an integer."
        ),
    )
    state: JobState = Field(
        description="The job's current public state; branch on this.",
    )
    result: ResultT | None = Field(
        default=None,
        description=(
            "The job's outcome. Non-null only when state is SUCCEEDED; null in "
            "every other state (progress is not reported here)."
        ),
    )
    error: V4ErrorDetail | None = Field(
        default=None,
        description=(
            "Why the job failed — the same {code, message, details} object the v4 "
            "error envelope uses. Non-null only when state is FAILED. Note that a "
            "FAILED poll is still HTTP 200: reading the job succeeded, the job "
            "did not."
        ),
    )

    @model_validator(mode="after")
    def _check_payload_matches_state(self) -> JobEnvelope[ResultT]:
        """Reject envelopes whose payload contradicts their state.

        Two invariants, both of which clients are then allowed to rely on without
        defensive checks (see the module docstring for the rationale and for why
        the ``result`` half is the stricter of the two):

        * ``error`` is non-null if and only if ``state`` is ``FAILED``;
        * ``result`` is null unless ``state`` is ``SUCCEEDED``.

        A ``SUCCEEDED`` job with no result is *permitted* — plenty of jobs finish
        with nothing to return, and a required-result rule would force slices to
        invent filler payloads.
        """
        if self.state is JobState.FAILED and self.error is None:
            raise ValueError(
                "a FAILED job must carry an error; build it with "
                "JobEnvelope.failed(...), which supplies a default message when "
                "the source row records none"
            )
        if self.state is not JobState.FAILED and self.error is not None:
            raise ValueError(f"state {self.state.value} must not carry an error")
        if self.result is not None and self.state is not JobState.SUCCEEDED:
            raise ValueError(
                f"state {self.state.value} must not carry a result; result is the "
                "job's outcome, not its progress"
            )
        return self

    @classmethod
    def failed(
        cls,
        *,
        job_id: str,
        message: str | None = None,
        code: str = "JOB_FAILED",
        details: dict | None = None,
    ) -> JobEnvelope[ResultT]:
        """Build the ``FAILED`` envelope.

        ``message`` is optional and falls back to a generic sentence, because the
        stored reason is routinely absent: ``predict_jobs.error`` is nullable, and
        an assessment can reach ``failed`` with a null ``status_detail``. Without
        the fallback every slice would have to write the same ``or "…"`` guard, and
        forgetting it would trip the model validator and turn a legitimate failed
        job into a 500.

        ``code`` defaults to the generic ``JOB_FAILED``; a slice that can classify
        its failures (a timeout, a missing model, a Modal container OOM) should
        pass a more specific stable code, since ``code`` is what clients branch on.
        """
        return cls(
            job_id=job_id,
            state=JobState.FAILED,
            error=V4ErrorDetail(
                code=code,
                message=message or "The job failed.",
                details=details,
            ),
        )


class JobSubmitAccepted(V4BaseModel):
    """The 202 submit body: just ``{"job_id": "..."}``.

    Deliberately minimal — the poll URL travels in the ``Location`` header, not in
    here (see divergence 1 in the module docstring). Declare it on a submit route
    as ``status_code=status.HTTP_202_ACCEPTED, responses={202: {"model":
    JobSubmitAccepted}}`` so the 202 is documented; the body itself is built by
    :func:`job_accepted_response`.
    """

    job_id: str = Field(
        description=(
            "Identifier of the newly accepted job. Poll it at the URL in the "
            "Location header."
        ),
    )


def job_accepted_response(
    *,
    job_id: str,
    poll_url: str,
    retry_after_s: int,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    """Build the ``202 Accepted`` response every v4 submit endpoint returns.

    Sets ``Location`` to ``poll_url`` and ``Retry-After`` to ``retry_after_s``,
    with body ``{"job_id": job_id}``. The body is dumped from
    :class:`JobSubmitAccepted` rather than hand-built, so the wire shape cannot
    drift from the declared schema (same reasoning as
    :func:`api_v4.errors._error_response`).

    ``poll_url`` — the URL the client should poll, whose id segment must equal
    ``job_id``. Pass ``str(request.url_for("route_name", …))`` so it cannot drift
    from the route; a root-relative path (``/v4/assessments/42``) is equally valid
    per RFC 9110 §10.2.2, so this helper passes through whatever it is given
    without rewriting it.

    ``retry_after_s`` — required on purpose; there is no v4-wide default cadence.
    See divergence 3 in the module docstring.

    ``headers`` — optional extras for the same response. The two contract headers
    are **assigned** afterwards, so a caller cannot shadow them at any casing. They
    must not be merged into the same dict: HTTP header names are case-insensitive
    but Python dict keys are not, so ``{"location": ..., "Location": ...}`` keeps
    both entries and Starlette emits *two* ``Location`` headers — of which lookups
    return the caller's, because it was added first. Assigning through
    ``response.headers`` instead goes via ``MutableHeaders.__setitem__``, which
    matches case-insensitively and overwrites rather than appends.

    Because this returns a ``Response`` directly, FastAPI does not validate it
    against the route's ``response_model``; declare the route as::

        @router.post("", status_code=status.HTTP_202_ACCEPTED,
                     responses={202: {"model": JobSubmitAccepted}})

    so the 202 still shows up in ``/v4/openapi.json``.
    """
    if retry_after_s < 1:
        # A zero or negative delta-seconds tells a client "poll immediately,
        # forever". Fail at the call site rather than shipping a header that
        # invites a hot loop.
        raise ValueError(f"retry_after_s must be >= 1, got {retry_after_s}")
    if not poll_url:
        raise ValueError("poll_url is required: the 202 must say where to poll")

    response = JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=JobSubmitAccepted(job_id=job_id).model_dump(mode="json"),
        headers=headers,
    )
    # Assigned, not merged — see the docstring. This overwrites any casing of the
    # same name the caller supplied instead of appending a duplicate.
    response.headers["Location"] = poll_url
    response.headers["Retry-After"] = str(retry_after_s)
    return response


def poll_status_code(state: JobState) -> int:
    """The HTTP status a poll returns for ``state`` (migration guide §7 table).

    ``PENDING`` -> ``202 Accepted``, everything else -> ``200 OK``. A 202 on a
    ``GET`` is unusual, but it is what the guide specifies and it is useful: it
    lets a client distinguish "accepted, not started" from "running" without
    reading the body. An unknown job id is a ``404`` and never reaches here — it is
    a transport error raised as :class:`~api_v4.errors.V4APIError`, so it gets the
    ``{"error": {...}}`` envelope rather than a job envelope with a made-up state.
    """
    if state is JobState.PENDING:
        return status.HTTP_202_ACCEPTED
    return status.HTTP_200_OK


def set_poll_headers(
    response: Response,
    *,
    state: JobState,
    retry_after_s: int,
) -> None:
    """Apply the poll-side contract to ``response``: status code and cadence hint.

    Sets ``response.status_code`` from :func:`poll_status_code`, and sets
    ``Retry-After`` only while ``state`` is non-terminal — a terminal job must not
    invite another poll, matching v3's predict poll, which omits the header once
    the job is done. Kept as a helper for the same reason
    :meth:`api_v4.pagination.V4Page.create` exists: so the three job-bearing
    slices cannot each drift on the details.

    On a terminal state the header is *removed*, not merely left unset, so the
    post-condition ("a terminal poll carries no ``Retry-After``") holds no matter
    what state the response arrived in — e.g. a handler that set the header itself
    before deciding the job was done. This function enforces the contract rather
    than assuming a clean response. (It cannot police middleware, which runs after
    the handler returns and mutates the finished response.)

    Usage in a poll handler that declares ``response: Response`` and
    ``response_model=JobEnvelope[...]``::

        envelope = JobEnvelope(job_id=str(row.id), state=state)
        set_poll_headers(response, state=state, retry_after_s=30)
        return envelope

    ``retry_after_s`` is validated even when the state is terminal (and the header
    consequently unused), so a bad constant is caught on the first poll rather
    than on whichever poll happens to catch a running job.
    """
    if retry_after_s < 1:
        raise ValueError(f"retry_after_s must be >= 1, got {retry_after_s}")

    response.status_code = poll_status_code(state)
    if state.is_terminal:
        # MutableHeaders.__delitem__ matches case-insensitively and is a no-op
        # when the header is absent, so this needs no membership guard.
        del response.headers["Retry-After"]
    else:
        response.headers["Retry-After"] = str(retry_after_s)


def _retry_after_header(*, required: bool, when: str) -> dict:
    """One OpenAPI header object for ``Retry-After``.

    ``when`` completes the sentence "Sent ..." so each response can say on what
    condition the header appears; ``required`` is what OpenAPI has instead of that
    prose, and it is only true where the status code alone guarantees the header.
    """
    return {
        "description": (
            "Seconds to wait before polling again. Always delta-seconds, never an "
            f"HTTP-date. Sent {when}."
        ),
        "required": required,
        "schema": {"type": "integer", "minimum": 1},
    }


#: OpenAPI ``headers`` for a submit's ``202`` — what :func:`job_accepted_response`
#: actually sets (#928). Declared as data here, rather than inline on the one route
#: that has a submit today, for the same reason the helpers above are shared: training
#: and predict answer the same 202 and must document it the same way.
#:
#: Both are ``required``: :func:`job_accepted_response` sets them unconditionally and
#: refuses to build a response without a ``poll_url`` or with a sub-1 cadence.
JOB_ACCEPTED_HEADERS: dict[str, dict] = {
    "Location": {
        "description": (
            "The poll URL for the job just accepted — a root-relative path, since the "
            "assessment *is* the job. Follow this rather than building the URL."
        ),
        "required": True,
        "schema": {"type": "string", "format": "uri-reference"},
    },
    "Retry-After": _retry_after_header(required=True, when="on every 202"),
}

#: OpenAPI ``headers`` for a poll's ``202``. No ``Location``: a poll answers *at* the
#: poll URL, and :func:`set_poll_headers` does not set one — declaring it would invent
#: a header the endpoint never sends.
#:
#: ``required``, because a ``202`` means ``PENDING``, which is never terminal.
JOB_POLL_PENDING_HEADERS: dict[str, dict] = {
    "Retry-After": _retry_after_header(required=True, when="on every 202"),
}

#: OpenAPI ``headers`` for a poll's ``200`` — the response a polling loop sees most,
#: and the reason this is declared at all: ``RUNNING`` is non-terminal but answers
#: ``200`` (:func:`poll_status_code`), so the cadence hint rides on the 200 too.
#:
#: Not ``required``: the same 200 covers the terminal states, where
#: :func:`set_poll_headers` deliberately *removes* the header so a finished job cannot
#: invite another poll.
JOB_POLL_HEADERS: dict[str, dict] = {
    "Retry-After": _retry_after_header(
        required=False, when="while the job is RUNNING, and omitted once it is terminal"
    ),
}


# Re-exported so a slice needs one import line for the whole contract.
__all__ = [
    "ASSESSMENT_STATE_MAP",
    "JOB_ACCEPTED_HEADERS",
    "JOB_POLL_HEADERS",
    "JOB_POLL_PENDING_HEADERS",
    "TERMINAL_JOB_STATES",
    "JobEnvelope",
    "JobState",
    "JobSubmitAccepted",
    "job_accepted_response",
    "poll_status_code",
    "set_poll_headers",
    "state_for_assessment_status",
]

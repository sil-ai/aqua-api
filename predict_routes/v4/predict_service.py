"""Data access, authorization and Modal dispatch for the v4 Predict slice (#894).

The router owns HTTP; this module owns everything else, mirroring the split
:mod:`assessment_routes.v4.assessment_service` established. Each domain signal it
raises is mapped to a :class:`~api_v4.errors.V4APIError` with a stable ``code`` in
exactly one place, :mod:`predict_routes.v4.predict_routes`.


Authorization: five selectors, all checked, all through v4's own predicates
---------------------------------------------------------------------------

:func:`authorize_selectors` refuses any selector id the caller cannot see. Two things
about it differ from v3, both deliberate.

**It checks five ids where v3 checked three.** v3's ``POST /predict`` authorized
``revision_id``, ``reference_id`` and ``assessment_id`` and left ``source_version_id``
and ``target_version_id`` unchecked — yet those two are the *primary* selectors for
four of the six apps (the runner's ``predict()`` docstrings resolve revisions and
trained artifacts from them), so a caller could name a version outside their groups and
have an app read its artifacts. That gap is not what #861 names, but it is the same
class of hole on the same endpoint, and closing it is the point of porting the check at
all.

**It refuses with a 404, not v3's 403.** These are reachability checks — can this
caller's groups see the row — not ownership checks on a row they can already see. #842
answers the first with a ``404`` everywhere else on the surface (``get_version``,
``get_revision``, ``assessment_service.get_assessment`` all hide existence behind one),
precisely so a caller cannot enumerate ids by watching a status code change from 404 to
403. A 403 here would undo that for five id spaces at once. This is a deliberate,
documented divergence from v3, of the same kind Revisions made for soft-deleted
versions.

The predicates are v4's own — :func:`bible_routes.v4.version_service.get_version`,
:func:`bible_routes.v4.revision_service.get_revision`,
:func:`assessment_routes.v4.assessment_service.get_assessment` — rather than
``security_routes/utilities.py``'s ``is_user_authorized_for_*``. They answer a stricter
question (v4's predicates also exclude soft-deleted rows and their soft-deleted
parents, which the v3 helpers do not), and reusing them means predict cannot come to
disagree with ``GET /v4/revisions`` about which revisions exist.

**It checks every id the caller sent, not only the ids the selected apps will read.**
That is stricter than necessary and it is a decision, not an oversight. Which selectors
an app consults is the *runner's* rule — its cascades resolve ``revision_id`` before
``source_version_id``, and ``text-lengths`` reads none of them — and those rules live in
another repository and change without this one. Scoping the check to them would mean
modelling that cascade here, and any drift in the model would skip a check the runner
then performs: a silent authorization bypass, which is the failure mode this function
exists to prevent. The cost of being strict is the opposite and much smaller — a caller
who sends a context id that nothing would have read is told they cannot see it.

The checks run in a fixed order — revision, reference, assessment, source version,
target version — so a request naming two unreachable ids always reports the same one.
Sequentially, not gathered: they share one ``AsyncSession``, which is not safe for
concurrent statements, and five small indexed lookups are not the cost of this request.


Dispatch: the app name is the enum value
-----------------------------------------

v3 carried ``PREDICT_APPS``, a dict translating its own app keys into Modal app names.
v4 needs no such table: :class:`~api_v4.schemas.predict.PredictApp`'s values *are* the
Modal app names (see that enum's module docstring for why, and
``test_predict_routes_v4.py`` for the test that pins it against v3's dict in both
directions). So dispatch is ``modal.Function.from_name(app.value, "predict", ...)``,
and there is no mapping that can drift from the runner.

The ``Function`` cache is this module's own rather than imported from frozen v3. It is
eight lines, and reaching into a frozen module's privates to save them would tie v4's
dispatch to a file nobody may edit.


The slow leg, and what happens when it cannot be spawned
---------------------------------------------------------

``POST /v4/predictions`` runs the fan-out and, when the request asked for translation
or critique *and* selected the agent, spawns a second agent call in the background and
persists a ``predict_jobs`` row for the client to poll. That is v3's design and v4
keeps it: the translation pass can take minutes, which no synchronous request should
hold open.

**When the spawn itself throws, v4 persists a ``failed`` row; v3 did not.** v3
synthesized a job handle whose id had never been written, so a client that polled it
got a bare ``404`` indistinguishable from "someone else's job". Persisting costs one
insert and makes the contract total: every ``job_id`` v4 hands out is pollable, and a
polling client learns *why* it failed instead of being told the job never existed.

The wrinkle, stated because it is a real cost: ``predict_jobs.modal_call_id`` is
``NOT NULL`` and a failed spawn has no Modal call id, so such a row stores the empty
string. It is never dereferenced — :func:`advance_job` returns immediately for any row
that is not ``running``, and these rows are born ``failed`` — but it is a placeholder in
a column whose name promises a call id. Making the column nullable is the clean fix and
wants a migration, which this slice does not write.


Polling advances the job, and closes v3's race while doing it
--------------------------------------------------------------

``Function.spawn`` has no callback, so nothing moves a row off ``running`` except a
poll: :func:`advance_job` asks Modal for the result with ``timeout=0`` and writes the
terminal state when there is one. :mod:`api_v4.jobs` neither requires nor forbids this,
but it does require a slice that does it to own transition legality — which is where v3
and v4 differ.

v3 mutates the ORM row and commits, so two polls arriving together can both read
``running``, both call Modal and both write. v4 writes through a conditional
``UPDATE ... WHERE id = :id AND status = 'running'`` and then re-reads the row, so the
first writer wins and the second silently observes the winner's result instead of
overwriting it. ``running`` is the only non-terminal status, so this single guard is
the whole transition rule: nothing can leave ``complete`` or ``failed``, and nothing can
reach either twice.

The semantics that buys are **first commit wins, not best answer wins**, and the
difference is worth stating because it is a real if narrow exposure. Two polls racing
the same job ask Modal independently; if one hits a transient transport error while the
other reads the true result, whichever commits first decides the job permanently, and
there is no reconciliation pass. A terminal state is terminal. This is accepted rather
than solved: the alternative — letting a later success overwrite a recorded failure —
would mean no state is ever really terminal, which costs every polling client the
guarantee it actually relies on. ``TestPollAdvanceRace`` pins the behaviour in both
directions so it stays a decision.
"""

from __future__ import annotations

import asyncio
import secrets
import socket
import time
from datetime import datetime, timezone
from typing import Any

import modal
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.jobs import JobState
from api_v4.schemas.predict import (
    PredictApp,
    PredictAppResult,
    PredictAppStatus,
    PredictInclude,
    PredictJobPair,
    PredictRequest,
    SimilarityRequest,
)
from assessment_routes.v4 import assessment_service
from bible_routes.v4 import revision_service, version_service
from config import settings
from database.models import PredictJob as PredictJobRow
from database.models import UserDB
from utils.logging_config import setup_logger

logger = setup_logger(__name__, container_id=socket.gethostname())

#: The Modal entry point every assessment app exposes for real-time inference.
#: ``aqua-assessments`` renamed these from ``inference`` to ``predict`` on 2026-04-21
#: (commit ``6e132af``, "Rename realtime inference entry points to predict() for
#: train/predict symmetry") — one name across all six apps, which is what lets dispatch
#: be a single line.
PREDICT_ENTRYPOINT = "predict"

#: Per-app wall-clock ceiling, in seconds, for one leg of the fan-out. The agent's is
#: raised well above the shared default because its fast slice still provisions a
#: TF-IDF corpus on a cold container; v3 measured 60s as too tight and produced
#: spurious timeouts at that value.
DEFAULT_PER_APP_TIMEOUT_S = settings.predict_per_app_timeout_s
PER_APP_TIMEOUT_S: dict[PredictApp, float] = {PredictApp.agent_critique: 300.0}

#: Polling cadence advertised for a predict job, in seconds. Required rather than
#: inherited: :mod:`api_v4.jobs` has no v4-wide default precisely so a slice cannot pick
#: up a cadence tuned for something else. Translation alone for a chapter typically
#: lands in 30-120s and critique adds a similar amount, so 10s keeps a client
#: responsive without hammering the API — a tenth of the assessments slice's 30s, which
#: is set for runs that can take forty minutes.
PREDICT_RETRY_AFTER_S = 10

#: Internal ``predict_jobs.status`` vocabulary -> public :class:`~api_v4.jobs.JobState`.
#: Lives here rather than in :mod:`api_v4.jobs` on that module's own instruction: this
#: vocabulary is private to one table (enforced by ``ck_predict_jobs_status``), unlike
#: the assessment statuses two slices must agree on. Note there is no ``PENDING``
#: — the constraint admits no "queued" value, because a row is only ever inserted after
#: the spawn has already been attempted.
PREDICT_STATE_MAP: dict[str, JobState] = {
    "running": JobState.RUNNING,
    "complete": JobState.SUCCEEDED,
    "failed": JobState.FAILED,
}

#: What a spawn-failure row stores in ``modal_call_id``. See the module docstring.
NO_MODAL_CALL = ""

_fn_cache: dict[tuple[str, str], modal.Function] = {}


class PredictServiceError(Exception):
    """Base for predict-service domain signals the router maps to V4APIError."""


class SelectorNotVisible(PredictServiceError):
    """A selector id the caller cannot see (or that does not exist).

    Carries the request field it came from rather than being split into five exception
    classes, because all five mean one thing — "you cannot reach this row" — and the
    router derives the error ``code`` from :attr:`field` through one mapping. Five
    classes would be five chances for the codes to drift apart.
    """

    def __init__(self, field: str, resource_id: int) -> None:
        self.field = field
        self.resource_id = resource_id
        super().__init__(f"No {field} {resource_id} is visible to this caller.")


class PredictJobNotFound(PredictServiceError):
    """No predict job with this id belongs to the caller, or none exists.

    One signal for both, matching v3's own posture on this endpoint and v4's
    everywhere: a caller must not be able to discover that a job id exists by watching
    the status code change.
    """

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"Prediction job {job_id} does not exist.")


class InferenceUnavailable(PredictServiceError):
    """The inference app could not be reached, or failed in a way we cannot classify.

    A transport condition, not a statement about the request: the caller's input may be
    perfectly valid and the same call may succeed on retry.
    """

    def __init__(self, app: str, reason: str) -> None:
        self.app = app
        self.reason = reason
        super().__init__(f"The {app} inference service is unavailable.")


class SimilarityModelUnavailable(PredictServiceError):
    """The semantic-similarity app refused the request's version pair.

    Distinct from :class:`InferenceUnavailable` because it is the opposite kind of
    failure: the app was reached and answered, and its answer is that this pair has no
    usable model (no fine-tune published, a missing vocab tag). Retrying changes
    nothing; training does. v3 collapsed the two into a 503 and a 422 with no codes to
    tell them apart.
    """

    def __init__(self, reason: str, source_version_id: int, target_version_id: int):
        self.reason = reason
        self.source_version_id = source_version_id
        self.target_version_id = target_version_id
        super().__init__(
            "No semantic-similarity model is available for this version pair."
        )


def state_for_predict_status(status: str | None) -> JobState:
    """Translate a ``predict_jobs.status`` value into its public :class:`JobState`.

    This slice's own ``(row) -> JobState`` adapter, written here rather than added to
    :mod:`api_v4.jobs` on that module's instruction — see :data:`PREDICT_STATE_MAP`.

    Raises :class:`ValueError` on anything else, which reaches the #828 catch-all as a
    500. That is :mod:`api_v4.jobs`' documented intent and is left alone deliberately:
    a row whose status the server cannot read is a data fault, and inventing a state
    for it would tell the client a job failed (or is still alive) when the truth is
    that we cannot say. ``ck_predict_jobs_status`` makes it unreachable short of a hand
    UPDATE.
    """
    try:
        return PREDICT_STATE_MAP[status]
    except KeyError as exc:
        raise ValueError(
            f"{status!r} is not a known predict job status "
            f"({', '.join(PREDICT_STATE_MAP)})."
        ) from exc


def new_job_id() -> str:
    """A fresh opaque predict-job id.

    v3's format, unchanged, because both versions write the same table: a ``prj_``
    prefix and 24 hex characters of ``secrets`` entropy. The prefix is what makes a
    stray id self-describing in a log; the entropy is what stops one being guessed,
    which matters because the id is the only thing standing between a caller and
    another caller's translation — the poll checks ownership, but an unguessable id
    means a leaked one cannot be walked to its neighbours.
    """
    return f"prj_{secrets.token_hex(12)}"


def includes_for(include_translation: bool, include_critique: bool):
    """The ``includes`` list for a job, in a fixed order.

    Translation first, because critique cannot be requested without it.
    """
    return [
        name
        for name, on in (
            (PredictInclude.translation, include_translation),
            (PredictInclude.critique, include_critique),
        )
        if on
    ]


async def authorize_selectors(
    db: AsyncSession, user: UserDB, request: PredictRequest
) -> None:
    """Refuse any selector id ``user`` cannot see.

    Raises :class:`SelectorNotVisible` naming the first unreachable field, in the fixed
    order documented on the module. Ids the request omitted are not checked — every
    selector is optional and which ones matter depends on the apps selected.
    """
    for field, resource_id, resolve in (
        ("revision_id", request.revision_id, _require_revision),
        ("reference_id", request.reference_id, _require_revision),
        ("assessment_id", request.assessment_id, _require_assessment),
        ("source_version_id", request.source_version_id, _require_version),
        ("target_version_id", request.target_version_id, _require_version),
    ):
        if resource_id is not None:
            await resolve(db, user, field, resource_id)


async def authorize_version_pair(
    db: AsyncSession, user: UserDB, source_version_id: int, target_version_id: int
) -> None:
    """Refuse a version pair ``user`` cannot see, source first.

    The standalone semantic-similarity endpoint's half of
    :func:`authorize_selectors` — it takes the same two ids under the same names and
    must refuse them the same way, so the two share one resolver and one error signal
    rather than growing separate checks that could come to disagree. This is what #861
    asked for: v3's ``POST /predict/semantic-similarity`` opened no database session at
    all and so checked nothing.
    """
    for field, version_id in (
        ("source_version_id", source_version_id),
        ("target_version_id", target_version_id),
    ):
        await _require_version(db, user, field, version_id)


async def _require_revision(
    db: AsyncSession, user: UserDB, field: str, revision_id: int
) -> None:
    try:
        await revision_service.get_revision(db, user, revision_id)
    except revision_service.RevisionNotFound as exc:
        raise SelectorNotVisible(field, revision_id) from exc


async def _require_version(
    db: AsyncSession, user: UserDB, field: str, version_id: int
) -> None:
    try:
        await version_service.get_version(db, user, version_id)
    except version_service.VersionNotFound as exc:
        raise SelectorNotVisible(field, version_id) from exc


async def _require_assessment(
    db: AsyncSession, user: UserDB, field: str, assessment_id: int
) -> None:
    try:
        await assessment_service.get_assessment(db, user, assessment_id)
    except assessment_service.AssessmentNotFound as exc:
        raise SelectorNotVisible(field, assessment_id) from exc


def _predict_fn(app_name: str, modal_env: str) -> modal.Function:
    """The cached Modal ``predict`` handle for one app in one environment.

    ``Function.from_name`` is a network lookup; the fan-out would otherwise repeat it
    for every app on every request.
    """
    key = (app_name, modal_env)
    fn = _fn_cache.get(key)
    if fn is None:
        fn = modal.Function.from_name(
            app_name, PREDICT_ENTRYPOINT, environment_name=modal_env
        )
        _fn_cache[key] = fn
    return fn


def runner_payload(request: PredictRequest) -> dict[str, Any]:
    """The ``PredictInput`` body the runner's apps expect.

    ``apps`` is dropped: it selects *which* apps to call and is meaningless to the app
    being called. Everything else passes through under the same names the runner's
    ``shared/predict_input.py`` declares — the two models are kept in step by hand, and
    the runner ignores fields it does not know, so a field added on either side degrades
    to being unread rather than to an error.
    """
    return request.model_dump(exclude={"apps"}, mode="json")


async def run_fanout(
    payload: dict[str, Any], apps: list[PredictApp], modal_env: str
) -> dict[PredictApp, PredictAppResult]:
    """Call every app in ``apps`` in parallel and collect one result each.

    Per-app failure is isolated — a slow or failing app never blocks or suppresses the
    others — which is the whole reason the fan-out exists rather than the client making
    six calls.
    """

    async def call_one(app: PredictApp) -> tuple[PredictApp, PredictAppResult]:
        started = time.perf_counter()
        timeout_s = timeout_for(app)
        try:
            fn = _predict_fn(app.value, modal_env)
            data = await asyncio.wait_for(fn.remote.aio(payload), timeout=timeout_s)
        except asyncio.TimeoutError:
            duration_ms = _elapsed_ms(started)
            logger.warning(f"predict app {app.value} timed out after {duration_ms}ms")
            return app, PredictAppResult(
                status=PredictAppStatus.error,
                error=f"timeout after {timeout_s}s",
                duration_ms=duration_ms,
            )
        except Exception as exc:
            duration_ms = _elapsed_ms(started)
            logger.warning(
                f"predict app {app.value} failed: {type(exc).__name__}", exc_info=True
            )
            return app, PredictAppResult(
                status=_status_for(exc),
                error=_error_text(exc),
                duration_ms=duration_ms,
            )
        return app, PredictAppResult(
            status=PredictAppStatus.ok, data=data, duration_ms=_elapsed_ms(started)
        )

    return dict(await asyncio.gather(*(call_one(app) for app in apps)))


def timeout_for(app: PredictApp) -> float:
    """The wall-clock ceiling for one call to ``app``.

    Shared by the fan-out and the standalone semantic-similarity endpoint so the same
    app cannot be given two different ceilings depending on which door it was called
    through.
    """
    return PER_APP_TIMEOUT_S.get(app, DEFAULT_PER_APP_TIMEOUT_S)


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def _status_for(exc: Exception) -> PredictAppStatus:
    """Classify a per-app exception into a :class:`PredictAppStatus`.

    "Training hasn't run yet" is an expected, actionable state rather than an error, so
    it gets its own status. Matched by *class name* rather than ``isinstance``, as v3
    does, so the check survives even if pickle resolution collapses
    ``TrainingNotAvailableError`` to a bare class with the same name but a different
    module path (#743). It must be decided before any ``ValueError`` handling, since
    ``TrainingNotAvailableError`` subclasses ``ValueError``.
    """
    if type(exc).__name__ == "TrainingNotAvailableError":
        return PredictAppStatus.not_trained
    return PredictAppStatus.error


def _error_text(exc: Exception) -> str:
    """What to report as an app's ``error``.

    ``ValueError`` messages are surfaced because per-app input validation raises them
    and they are caller-actionable ("agent.predict requires vref and source_text on
    every pair"). Everything else reports only its type name: an arbitrary exception
    string from a container we do not control is the kind of thing that leaks paths and
    connection strings.
    """
    return str(exc) if isinstance(exc, ValueError) else type(exc).__name__


async def spawn_slow_agent(
    db: AsyncSession,
    user: UserDB,
    request: PredictRequest,
    payload: dict[str, Any],
    modal_env: str,
) -> PredictJobRow:
    """Spawn the agent's slow pass and persist the row that tracks it.

    Returns a ``running`` row on success and a ``failed`` one when the spawn could not
    be placed — never nothing, so the caller always has a pollable job id (see the
    module docstring).
    """
    job_id = new_job_id()
    modal_call_id = NO_MODAL_CALL
    status = "failed"
    error = None
    completed_at = None
    try:
        agent_fn = _predict_fn(PredictApp.agent_critique.value, modal_env)
        function_call = await agent_fn.spawn.aio(payload)
        modal_call_id = function_call.object_id
        status = "running"
    except Exception as exc:
        logger.error(
            f"failed to spawn slow agent path: {type(exc).__name__}: {exc}",
            exc_info=True,
        )
        error = _error_text(exc)
        completed_at = datetime.now(timezone.utc)

    job = PredictJobRow(
        id=job_id,
        modal_call_id=modal_call_id,
        modal_environment=modal_env,
        status=status,
        include_translation=request.include_translation,
        include_critique=request.include_critique,
        pairs_input=[pair.model_dump(mode="json") for pair in request.pairs],
        error=error,
        completed_at=completed_at,
        owner_id=user.id,
    )
    try:
        db.add(job)
        await db.commit()
        await db.refresh(job)
    except Exception:
        # The same guard every v4 write path uses: never leave the shared session in an
        # aborted-transaction state. It matters more here than most, because the very
        # next statement is a ``refresh`` — without the rollback a failed commit
        # surfaces as ``PendingRollbackError`` from that line and buries whatever
        # actually went wrong.
        await db.rollback()
        raise
    return job


async def get_job(db: AsyncSession, user: UserDB, job_id: str) -> PredictJobRow:
    """Return a predict job the caller may read, or raise :class:`PredictJobNotFound`.

    Scoped to the caller's own jobs, with admins exempt. There is no group dimension:
    a predict job belongs to whoever submitted it, and ``predict_jobs.owner_id`` is
    ``NOT NULL``, so unlike assessments there are no unowned legacy rows to decide
    about.
    """
    stmt = select(PredictJobRow).where(PredictJobRow.id == job_id)
    if not user.is_admin:
        stmt = stmt.where(PredictJobRow.owner_id == user.id)
    job = (await db.execute(stmt)).scalars().first()
    if job is None:
        raise PredictJobNotFound(job_id)
    return job


async def advance_job(db: AsyncSession, job: PredictJobRow) -> PredictJobRow:
    """Ask Modal whether a ``running`` job has finished, and record it if so.

    A no-op for a job already in a terminal state. Returns the row as it now stands —
    which, on a lost race, is the state the *other* poll wrote (see the module
    docstring).
    """
    if job.status != "running":
        return job

    try:
        function_call = modal.FunctionCall.from_id(job.modal_call_id)
        data = await function_call.get.aio(timeout=0)
    except (
        modal.exception.FunctionTimeoutError,
        modal.exception.OutputExpiredError,
    ) as exc:
        # The Modal container hit its own timeout, or the result expired before we
        # polled. Both subclass ``modal.exception.TimeoutError``, so they must be
        # caught BEFORE the bare-timeout block below — which catches that class too and
        # would silently read them as "still running", leaving the row ``running``
        # forever. This ordering is load-bearing; it is v3's, and it is not re-derived.
        logger.warning(
            f"predict job {job.id} timed out on Modal: {type(exc).__name__}: {exc}",
            exc_info=True,
        )
        return await _finish(db, job, status="failed", error=f"{type(exc).__name__}")
    except (TimeoutError, modal.exception.TimeoutError):
        # ``modal._functions.poll_function`` raises the *builtin* ``TimeoutError`` (not
        # ``modal.exception.TimeoutError``, which does not subclass it) when
        # ``timeout=0`` finds no result yet. Catching both means the installed modal
        # version does not decide whether a running job reads as running.
        return job
    except Exception as exc:
        logger.warning(
            f"predict job {job.id} failed: {type(exc).__name__}: {exc}", exc_info=True
        )
        return await _finish(db, job, status="failed", error=_error_text(exc))

    return await _finish(db, job, status="complete", result=data)


async def _finish(
    db: AsyncSession,
    job: PredictJobRow,
    *,
    status: str,
    result: Any | None = None,
    error: str | None = None,
) -> PredictJobRow:
    """Write a terminal state, but only if nobody else got there first.

    The ``status == 'running'`` clause is the whole transition rule — see the module
    docstring. The unconditional re-read afterwards is what makes losing the race
    harmless rather than invisible: the loser returns the winner's row, so two
    concurrent polls answer identically instead of one reporting a result it then
    failed to store.
    """
    try:
        await db.execute(
            update(PredictJobRow)
            .where(PredictJobRow.id == job.id, PredictJobRow.status == "running")
            .values(
                status=status,
                result=result,
                error=error,
                completed_at=datetime.now(timezone.utc),
            )
        )
        await db.commit()
        await db.refresh(job)
    except Exception:
        # See ``spawn_slow_agent`` for why the rollback is not optional next to a
        # ``refresh``.
        await db.rollback()
        raise
    return job


def job_pairs(job: PredictJobRow) -> list[PredictJobPair]:
    """Reconstitute the per-pair slow-path payload, ordered as submitted.

    The agent preserves input order in its reply, so ``translation`` and ``critique``
    are taken positionally from ``result["pairs"][idx]``. The ``vref`` /
    ``source_text`` / ``target_text`` echo always comes from the stored
    ``pairs_input`` — never from the agent's reply — for two reasons: a caller that
    omitted ``vref`` can still match by index, and a runner-side bug that mangled the
    echo cannot propagate into it.

    A running job has no ``result`` yet, so every pair comes back with its echo and two
    nulls; that is the shape a client polls against and the reason ``pairs`` is present
    in every state rather than appearing at the end.
    """
    agent_pairs = (job.result or {}).get("pairs") or []
    pairs = []
    for index, submitted in enumerate(job.pairs_input or []):
        agent_pair = agent_pairs[index] if index < len(agent_pairs) else {}
        pairs.append(
            PredictJobPair(
                vref=submitted.get("vref"),
                source_text=submitted.get("source_text"),
                target_text=submitted.get("target_text", ""),
                translation=agent_pair.get("translation"),
                critique=agent_pair.get("critique"),
            )
        )
    return pairs


async def semantic_similarity(request: SimilarityRequest, modal_env: str) -> float:
    """Score one pair against the model fine-tuned for its version pair.

    Calls the ``semantic-similarity`` app's ``predict`` with a one-pair payload. v3
    called an entry point named ``inference``, which the runner renamed to ``predict``
    on 2026-04-21 (see :data:`PREDICT_ENTRYPOINT`) — so v4 calls the name the runner
    actually defines, and the two standalone endpoints dispatch the same way the
    fan-out does.

    Raises :class:`SimilarityModelUnavailable` when the app answers with an ``error``
    key (its documented way of reporting a request-level config failure) and
    :class:`InferenceUnavailable` when it cannot be reached at all. A reply that is
    neither — no ``error`` and no readable score — is left to raise, reaching the #828
    catch-all as a 500: our own inference app breaking its response contract is a
    server fault, and it is not worth retrying, which is what a 503 would advertise.
    """
    payload = {
        "pairs": [
            {
                "source_text": request.source_text,
                "target_text": request.target_text,
            }
        ],
        "source_version_id": request.source_version_id,
        "target_version_id": request.target_version_id,
    }
    logger.info(
        "semantic similarity inference request",
        extra={
            "source_version_id": request.source_version_id,
            "target_version_id": request.target_version_id,
            "modal_env": modal_env,
        },
    )
    timeout_s = timeout_for(PredictApp.semantic_similarity)
    try:
        fn = _predict_fn(PredictApp.semantic_similarity.value, modal_env)
        result = await asyncio.wait_for(fn.remote.aio(payload), timeout=timeout_s)
    except asyncio.TimeoutError as exc:
        # Bounded here rather than left to Modal's own ceiling. The app declares no
        # function timeout, so without this the wait falls back to Modal's default of
        # 300s — five minutes of a held worker on a request a client is sitting on
        # synchronously, and five times what the same app gets through the fan-out.
        logger.warning(f"semantic similarity timed out after {timeout_s}s")
        raise InferenceUnavailable(
            PredictApp.semantic_similarity.value, f"timeout after {timeout_s}s"
        ) from exc
    except Exception as exc:
        logger.error(
            f"semantic similarity inference failed: {type(exc).__name__}: {exc}",
            exc_info=True,
        )
        raise InferenceUnavailable(
            PredictApp.semantic_similarity.value, _error_text(exc)
        ) from exc

    if isinstance(result, dict) and result.get("error"):
        raise SimilarityModelUnavailable(
            str(result["error"]),
            request.source_version_id,
            request.target_version_id,
        )
    return float(result["pairs"][0]["score"])

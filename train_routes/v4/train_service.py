"""Data access, authorization and Modal dispatch for the v4 Training slice (#895).

The router owns HTTP; this module owns everything else, mirroring the split
:mod:`assessment_routes.v4.assessment_service` established and
:mod:`predict_routes.v4.predict_service` followed. Each domain signal it raises is
mapped to a :class:`~api_v4.errors.V4APIError` with a stable ``code`` in exactly one
place, :mod:`train_routes.v4.train_routes`.


What is imported from frozen v3, and why each one has to be
------------------------------------------------------------

Three helpers are imported from ``train_routes/v3/train_routes.py`` rather than
reimplemented. v3 is frozen, so importing is allowed and editing is not; in each case a
second implementation would be a second definition of something that must have exactly
one:

* :func:`~train_routes.v3.train_routes._acquire_training_job_dup_lock` (and the key
  helper behind it, ``_training_job_dup_lock_key``). **Both surfaces are live at once**,
  so a v4 submit racing a *v3* submit on the same ``(source, target, type)`` triple is a
  real path. A v4-local key would hash to a different lock, leave that race completely
  unprotected, and look correct in every v4-only test. This is the same reasoning — and
  the same shape — as the assessments slice reusing v3's ``_acquire_assess_dup_lock``.
* :func:`~train_routes.v3.train_routes._build_runner_train_config`. The runner is a
  separate repository reading a fixed set of config keys; a second builder here could
  drift from what the runner reads, and the failure would be a GPU job that silently
  trains the wrong thing.
* :func:`~train_routes.v3.train_routes._training_options_for_type`, which is what forces
  ``finetune=True`` onto ``semantic-similarity``. It has to be the same function on both
  surfaces because it is applied *twice*: once to what is stored, and once to an
  existing job's stored options during the duplicate check. Two implementations that
  disagreed would make a v4 submit fail to recognise a v3 job as its duplicate and
  enqueue a second run of it.

Note #722 — "the training-job duplicate check is not atomic" — is **already fixed in
v3** (the advisory lock at ``train_routes/v3/train_routes.py:187`` and ``:211``, taken
per triple at ``:817``); the issue is merely still open. This module neither re-fixes it
nor skips it: it takes the same lock with the same key.


Visibility: one predicate, and it is wider than v3's list
----------------------------------------------------------

:func:`_visible_jobs_query` is the single read predicate behind the list, the two
session reads, the single-job read and the delete gate. Two deliberate differences from
v3, both of which mean a v4 client sees a different set rather than a regression:

* **The owner can always see their own job.** v3's ``GET /train`` scopes purely by group
  access to both revisions, while ``GET /train/{job_id}`` accepts admin, owner *or*
  group access — so on v3 a caller who submitted a job and then lost access to a version
  can still read it by id but will never find it in their own list. v4 uses one rule
  everywhere: admin, owner, or group access to both sides.
* **A job whose revision or version was soft-deleted is hidden**, matching
  ``GET /v4/assessments`` and ``GET /v4/revisions``. v3 checks neither, and so keeps
  listing jobs that train rows a user has chosen to remove. ``include_deleted=True`` is
  passed on the write path only, for the same reason the assessments slice passes it:
  an already-deleted row, and a row whose parent was deleted, must stay *writable* so a
  delete is idempotent rather than a 404.

``is_not(True)`` rather than ``is_(False)`` on every ``deleted`` column throughout: they
are nullable and legacy rows may hold NULL, which must stay visible.


State is read from another row, and sometimes there is no other row
--------------------------------------------------------------------

``TrainingJob`` has no status column: status, timing and progress live on the linked
``Assessment`` (aqua-api#584/#593). Every read here therefore eager-loads
``TrainingJob.assessment`` with ``selectinload`` — without it the list is N+1 across the
page, which is the one performance trap this slice can walk into by accident.

``training_job.assessment_id`` is nullable (``ondelete="SET NULL"``), so the load can
come back with nothing. This module does not decide what that means on the wire — see
:mod:`api_v4.schemas.training` for the three answers and
:func:`state_for_training_job`, which returns ``None`` rather than raising so the list
can report the fault instead of failing the page.
"""

from __future__ import annotations

import asyncio
import socket
import uuid
from datetime import datetime
from typing import Any, Sequence

import modal
from sqlalchemy import Integer, and_, bindparam, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased, selectinload

from api_v4.jobs import ASSESSMENT_STATE_MAP, JobState, state_for_assessment_status
from api_v4.schemas.training import (
    InferenceReadinessOut,
    TrainingNeighbour,
    TrainingNgramMatch,
    TrainingNgramOccurrence,
    TrainingNgrams,
    TrainingResultRow,
    TrainingTfidfNeighbours,
    TrainingVerseScore,
    TrainingWordAlignment,
)
from bible_routes.v4 import revision_service, version_service
from config import settings
from database.models import (
    AlignmentTopSourceScores,
    Assessment,
    AssessmentResult,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    BookReference,
    NgramsTable,
    NgramVrefTable,
    TfidfPcaVector,
    TrainingJob,
    UserDB,
    UserGroup,
    VerseReference,
    VerseText,
)
from schemas.assessment import AssessmentStatus
from schemas.training import TrainingType
from train_routes.v3.train_routes import (
    ASSESSMENT_FINISHED_VALUE,
    ASSESSMENT_TERMINAL_VALUES,
    INFERENCE_DEPENDENCIES,
    TRAINABLE_ASSESSMENT_TYPES,
    _acquire_training_job_dup_lock,
    _build_runner_train_config,
    _training_options_for_type,
)
from utils.logging_config import setup_logger

logger = setup_logger(__name__, container_id=socket.gethostname())

#: Polling cadence advertised for a training session, in seconds. Required rather than
#: inherited: :mod:`api_v4.jobs` has no v4-wide default precisely so a slice cannot pick
#: up a cadence tuned for something else. A training run is an assessment run with
#: ``is_training`` set — same runner, same apps, same order of magnitude — so this is the
#: assessments slice's 30s rather than predict's 10s, which is set for translation jobs
#: that finish in a minute or two.
TRAINING_RETRY_AFTER_S = 30

#: Public state -> internal assessment status, for the list's ``state`` filter.
#:
#: :data:`~api_v4.jobs.ASSESSMENT_STATE_MAP` is one-way and the filter needs the other
#: direction: the caller sends the public spelling and the column stores the internal
#: one. Inverted from that map rather than written out, so the two cannot come to
#: disagree; the inversion is lossless only while the map is injective, which a test
#: pins by comparing the two lengths.
INTERNAL_STATUS_FOR_STATE: dict[JobState, AssessmentStatus] = {
    state: status for status, state in ASSESSMENT_STATE_MAP.items()
}

#: The Modal app and entry point every training dispatch goes through. One runner
#: function for every type — it dispatches to the right app itself, from ``config.type``
#: and ``config["is_training"]``.
RUNNER_APP = "runner"
RUNNER_ENTRYPOINT = "run_assessment_runner"


class TrainServiceError(Exception):
    """Base for train-service domain signals the router maps to V4APIError."""


class SelectorNotVisible(TrainServiceError):
    """A version or revision id the caller cannot see (or that does not exist).

    Carries the request field it came from rather than being split into four exception
    classes, because all four mean one thing — "you cannot reach this row" — and the
    router derives the error ``code`` from :attr:`field` through one mapping, the way
    the predict slice does for its five selectors.
    """

    def __init__(self, field: str, resource_id: int) -> None:
        self.field = field
        self.resource_id = resource_id
        super().__init__(f"No {field} {resource_id} is visible to this caller.")


class VersionHasNoRevisions(TrainServiceError):
    """A visible version that has no non-deleted revision to train.

    Distinct from :class:`SelectorNotVisible` because it is the opposite kind of
    failure: the caller can see the version and named it correctly, and what is missing
    is something they can fix by uploading a revision. v3 reports this as a 404 on the
    version, which says the version does not exist when it does.
    """

    def __init__(self, field: str, version_id: int) -> None:
        self.field = field
        self.version_id = version_id
        super().__init__(
            f"Version {version_id} has no revisions to train on. Upload a revision "
            "first, or name a revision id directly."
        )


class TrainingJobsAlreadyActive(TrainServiceError):
    """Every selected app already has an active job for this pair and these options.

    Only raised when *nothing* could be created — a submit that creates some jobs and
    skips others succeeds, and the session simply holds the jobs that were created.
    """

    def __init__(self, existing_job_ids: list[int], apps: list[str]) -> None:
        self.existing_job_ids = existing_job_ids
        self.apps = apps
        super().__init__(
            f"Active training jobs already exist for every requested app ({', '.join(apps)})."
        )


class TrainingJobNotFound(TrainServiceError):
    """No training job with this id is visible to the caller, or none exists.

    One signal for both, as everywhere on the v4 surface: a caller must not be able to
    discover that an id exists by watching the status code change.
    """

    def __init__(self, job_id: int) -> None:
        self.job_id = job_id
        super().__init__(f"Training job {job_id} does not exist.")


class TrainingSessionNotFound(TrainServiceError):
    """No visible training job carries this session key.

    A session is a column value, not a row, so "the session has no jobs" and "there was
    never such a session" are the same state and cannot be told apart — which is exactly
    why this is a 404 rather than a 200 with an empty list.
    """

    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        super().__init__(f"Training session {session_id} does not exist.")


def state_unavailable_message(job_id: int) -> str:
    """The one wording for "this job has no linked assessment".

    A function rather than an f-string at three call sites because the fault is reported
    in two different ways — raised by the single-job read and the delete, and *returned*
    as a field by the list, which has nothing to catch. Both paths take the sentence from
    here so the same fault cannot be described two ways depending on which door it came
    through.
    """
    return (
        f"Training job {job_id} has no linked assessment, so its state cannot be read."
    )


class TrainingJobStateUnavailable(TrainServiceError):
    """The job has no linked assessment, so its state cannot be read.

    A data-integrity fault rather than a job state (``assessment_id`` is
    ``ON DELETE SET NULL``). Raised only where the shape has no way to report it in the
    body: the single-job read, whose response *is* a job envelope, and the delete, which
    cannot verify that the job is terminal. The list reports it as ``state: null`` plus
    this error instead — see :mod:`api_v4.schemas.training`.
    """

    def __init__(self, job_id: int) -> None:
        self.job_id = job_id
        super().__init__(state_unavailable_message(job_id))


class TrainingJobNotTerminal(TrainServiceError):
    """A delete was refused because the job is still queued or running."""

    def __init__(self, job_id: int, state: JobState) -> None:
        self.job_id = job_id
        self.state = state
        super().__init__(
            f"Training job {job_id} is {state.value} and only a terminal job can be "
            "deleted."
        )


class TrainingJobAccessForbidden(TrainServiceError):
    """The caller can read this job but does not own it."""

    def __init__(self, job_id: int) -> None:
        self.job_id = job_id
        super().__init__(f"Training job {job_id} belongs to another user.")


def state_for_training_job(job: TrainingJob) -> JobState | None:
    """This slice's ``(row) -> JobState`` adapter: ``None`` when there is no state.

    The indirection *is* the adapter. :mod:`api_v4.jobs` rules that each slice writes its
    own mapping function and builds its own envelope; training writes no new vocabulary,
    because the status it reads is ``assessment.status`` and
    :func:`~api_v4.jobs.state_for_assessment_status` already owns that translation —
    shared precisely because assessments and training both read it. What training adds is
    the hop to another row, and the fact that the other row may not be there.

    Returning ``None`` rather than raising is deliberate and is the reason this function
    exists at all: the list has a place to report the fault (``state: null`` plus an
    ``error``) and would otherwise have to catch an exception per row to build a page.
    The two callers that *cannot* report it in a body — the single-job read and the
    delete — check for ``None`` and raise :class:`TrainingJobStateUnavailable`
    themselves.

    An assessment whose ``status`` is outside the four internal values still raises
    ``ValueError`` from the shared translator, reaching the #828 catch-all as a 500. That
    is :mod:`api_v4.jobs`' documented intent, left alone: a missing row is a nameable
    fault, while an unreadable status is a row the server does not understand.
    """
    if job.assessment is None:
        return None
    return state_for_assessment_status(job.assessment.status)


def session_state(states: Sequence[JobState | None]) -> JobState | None:
    """Aggregate a session's per-job states into the one a client branches on.

    ``FAILED`` if any job failed, else ``RUNNING`` if any is non-terminal, else
    ``SUCCEEDED`` — and ``None`` if any job has no readable state, since an aggregate
    computed over an unknown outcome would be a guess dressed as an answer.

    Note the rule never yields ``PENDING``: a session whose jobs are all queued reports
    ``RUNNING``. That is the stated definition (the distinction lives on the per-job
    states), and its visible consequence is that the session read never answers ``202``.
    """
    if any(state is None for state in states):
        return None
    if any(state is JobState.FAILED for state in states):
        return JobState.FAILED
    if any(not state.is_terminal for state in states):
        return JobState.RUNNING
    return JobState.SUCCEEDED


def _accessible_version_ids(user: UserDB):
    """Subquery yielding the ids of every version ``user``'s groups can reach.

    A subquery rather than a materialized list (which is what v3 builds, in two extra
    round trips) because it is used twice in the same statement — once per side of the
    pair — and an ``IN`` against it cannot multiply rows the way a join to
    ``bible_version_access`` would for a version reachable through two of the caller's
    groups.
    """
    return select(BibleVersionAccess.bible_version_id).where(
        BibleVersionAccess.group_id.in_(
            select(UserGroup.group_id).where(UserGroup.user_id == user.id)
        )
    )


def _visible_jobs_query(user: UserDB, *, include_deleted: bool = False):
    """Base ``SELECT TrainingJob`` scoped to what ``user`` may see.

    No ``limit``/``offset``/``order_by`` and no eager-load — callers add those, and the
    count query wraps this as a subquery, so the authorization logic lives in exactly one
    place for all five reads. The full rule is in the module docstring.

    The four joins are inner and unconditional: both branches need them, because even an
    admin gets the soft-delete filters, and both revision columns are ``NOT NULL`` so an
    inner join drops nothing.
    """
    source_revision = aliased(BibleRevision, name="source_revision")
    target_revision = aliased(BibleRevision, name="target_revision")
    source_version = aliased(BibleVersion, name="source_version")
    target_version = aliased(BibleVersion, name="target_version")

    stmt = (
        select(TrainingJob)
        .join(source_revision, source_revision.id == TrainingJob.source_revision_id)
        .join(target_revision, target_revision.id == TrainingJob.target_revision_id)
        .join(source_version, source_version.id == source_revision.bible_version_id)
        .join(target_version, target_version.id == target_revision.bible_version_id)
    )

    if not include_deleted:
        stmt = stmt.where(
            TrainingJob.deleted.is_not(True),
            source_revision.deleted.is_not(True),
            target_revision.deleted.is_not(True),
            source_version.deleted.is_not(True),
            target_version.deleted.is_not(True),
        )

    if not user.is_admin:
        accessible = _accessible_version_ids(user)
        stmt = stmt.where(
            or_(
                TrainingJob.owner_id == user.id,
                and_(
                    source_version.id.in_(accessible),
                    target_version.id.in_(accessible),
                ),
            )
        )
    return stmt


async def _latest_revision(db: AsyncSession, version_id: int) -> BibleRevision | None:
    """The most recent non-deleted revision of a version, or ``None``.

    Ordered by ``id`` descending, which is v3's own tiebreak and the right one: ``date``
    is user-supplied and nullable, so it cannot order this deterministically.
    """
    stmt = (
        select(BibleRevision)
        .where(
            BibleRevision.bible_version_id == version_id,
            BibleRevision.deleted.is_not(True),
        )
        .order_by(BibleRevision.id.desc())
        .limit(1)
    )
    return (await db.execute(stmt)).scalars().first()


async def _resolve_side(
    db: AsyncSession,
    user: UserDB,
    side: str,
    *,
    version_id: int | None,
    revision_id: int | None,
) -> BibleRevision:
    """Resolve one side of the pair to a revision the caller can see.

    Authorized through the Revisions and Versions slices' own predicates rather than
    ``security_routes/utilities.py``'s helpers, for the reason the predict slice states:
    they answer a stricter question (v4's predicates also exclude soft-deleted rows and
    their soft-deleted parents), and reusing them means training cannot come to disagree
    with ``GET /v4/revisions`` about which revisions exist.

    **Every refusal is a 404, where v3 answers 403 on the version branch.** These are
    reachability checks — can this caller's groups see the row — and #842 answers those
    with a 404 everywhere on this surface so that ids cannot be probed by watching a
    status code change.
    """
    if revision_id is not None:
        try:
            return await revision_service.get_revision(db, user, revision_id)
        except revision_service.RevisionNotFound as exc:
            raise SelectorNotVisible(f"{side}_revision_id", revision_id) from exc

    field = f"{side}_version_id"
    try:
        await version_service.get_version(db, user, version_id)
    except version_service.VersionNotFound as exc:
        raise SelectorNotVisible(field, version_id) from exc

    revision = await _latest_revision(db, version_id)
    if revision is None:
        raise VersionHasNoRevisions(field, version_id)
    return revision


async def _active_duplicate(
    db: AsyncSession,
    *,
    source_revision_id: int,
    target_revision_id: int,
    training_type: str,
    options: dict | None,
) -> TrainingJob | None:
    """An existing live job for the same triple and the same options, if there is one.

    "Active" is v3's definition, unchanged: the job is not soft-deleted, its linked
    assessment is not soft-deleted, and that assessment has not reached a terminal state.
    A finished run is therefore *not* a duplicate — retraining a pair is the normal way
    to pick up new verse text — which is the deliberate difference from
    ``POST /v4/assessments``, where a completed run does block a resubmit unless
    ``force`` is passed.

    Options are compared after both sides go through
    :func:`~train_routes.v3.train_routes._training_options_for_type`, so a v4 submit
    recognises a v3 job created with the same effective options as its duplicate.
    """
    stmt = (
        select(TrainingJob)
        .join(Assessment, Assessment.id == TrainingJob.assessment_id)
        .where(
            TrainingJob.source_revision_id == source_revision_id,
            TrainingJob.target_revision_id == target_revision_id,
            TrainingJob.type == training_type,
            TrainingJob.deleted.is_not(True),
            Assessment.deleted.is_not(True),
            Assessment.status.notin_(list(ASSESSMENT_TERMINAL_VALUES)),
        )
    )
    for existing in (await db.execute(stmt)).scalars().all():
        if _training_options_for_type(training_type, existing.options) == options:
            return existing
    return None


async def create_session(
    db: AsyncSession, user: UserDB, data
) -> tuple[str, list[TrainingJob]]:
    """Create a training session and dispatch its jobs. ``data`` is a
    ``TrainingSessionCreate``.

    Returns the new session key and the committed jobs. Raises, in the order they can
    occur: :class:`SelectorNotVisible`, :class:`VersionHasNoRevisions`,
    :class:`TrainingJobsAlreadyActive`.

    The per-type loop is v3's, and the order of its three steps is load-bearing: take the
    advisory lock for the triple, *then* run the duplicate check, *then* insert. The lock
    is transaction-scoped, so it is held until the commit at the bottom and the
    check-plus-insert is atomic against any other concurrent submit on the same triple —
    including one arriving on v3, since both surfaces derive the key from the same helper
    (#722). Different types take different keys, so the apps in one submit do not
    serialize against each other.

    Iterating ``TrainingType`` rather than the caller's ``apps`` list fixes the order in
    which locks are taken, so two submits selecting overlapping app sets cannot deadlock
    by acquiring the same two locks in opposite orders.

    Dispatch happens **after** the commit and is deliberately not part of the
    transaction: a spawn that fails leaves the row marked ``failed`` (with the reason in
    ``status_detail``) rather than rolling back the whole session, so the caller gets a
    session whose other jobs are running and one job that says why it did not start. That
    is v3's behaviour and the reason the submit is not a ``503`` when a spawn fails.
    """
    source_revision = await _resolve_side(
        db,
        user,
        "source",
        version_id=data.source_version_id,
        revision_id=data.source_revision_id,
    )
    target_revision = await _resolve_side(
        db,
        user,
        "target",
        version_id=data.target_version_id,
        revision_id=data.target_revision_id,
    )

    selected = set(data.apps) if data.apps else set(TrainingType)
    session_id = str(uuid.uuid4())
    jobs: list[TrainingJob] = []
    skipped: list[int] = []

    try:
        for training_type in TrainingType:
            if training_type not in selected:
                continue
            options = _training_options_for_type(training_type.value, data.options)

            await _acquire_training_job_dup_lock(
                db, source_revision.id, target_revision.id, training_type.value
            )
            duplicate = await _active_duplicate(
                db,
                source_revision_id=source_revision.id,
                target_revision_id=target_revision.id,
                training_type=training_type.value,
                options=options,
            )
            if duplicate is not None:
                logger.info(
                    f"skipping {training_type.value}: active training job "
                    f"{duplicate.id} already exists"
                )
                skipped.append(duplicate.id)
                continue

            # The paired Assessment row is what carries this job's status, timing and
            # results: the runner reports progress by PATCHing the assessment, and
            # pushes artifacts under its id. `revision_id` is the side being trained
            # (target) and `reference_id` what it is trained against (source) — see
            # v3's `_build_runner_train_config` for why that mapping is the way round
            # it is.
            assessment = Assessment(
                revision_id=target_revision.id,
                reference_id=source_revision.id,
                type=training_type.value,
                status=AssessmentStatus.queued.value,
                requested_time=datetime.utcnow(),
                owner_id=user.id,
                kwargs=options,
                is_training=True,
            )
            db.add(assessment)
            await db.flush()

            job = TrainingJob(
                type=training_type.value,
                source_revision_id=source_revision.id,
                target_revision_id=target_revision.id,
                source_version_id=source_revision.bible_version_id,
                target_version_id=target_revision.bible_version_id,
                options=options,
                requested_time=datetime.utcnow(),
                owner_id=user.id,
                session_id=session_id,
                assessment_id=assessment.id,
            )
            db.add(job)
            jobs.append(job)

        if not jobs:
            raise TrainingJobsAlreadyActive(
                sorted(skipped),
                sorted(t.value for t in TrainingType if t in selected),
            )

        await db.commit()
        for job in jobs:
            await db.refresh(job)
    except Exception:
        # Never leave the shared session in an aborted-transaction state — the same
        # guard every v4 write path uses. It also releases the advisory locks taken
        # above, which matters on the 409 path: those are held until the transaction
        # ends, and a submit that refused every app must not keep other submits waiting.
        await db.rollback()
        raise

    await _dispatch_all(db, jobs, source_revision, target_revision)
    return session_id, jobs


async def _dispatch_all(
    db: AsyncSession,
    jobs: list[TrainingJob],
    source_revision: BibleRevision,
    target_revision: BibleRevision,
) -> None:
    """Spawn every job's runner call in parallel and record the ones that could not be.

    Per-job isolation, as v3 does: one failing spawn marks its own assessment ``failed``
    and never prevents the others from starting. The config is built by v3's
    :func:`~train_routes.v3.train_routes._build_runner_train_config` so the payload has
    one definition shared with the surface the runner was written against.
    """

    async def dispatch(job: TrainingJob) -> tuple[TrainingJob, Exception | None]:
        try:
            if job.type not in TRAINABLE_ASSESSMENT_TYPES:
                raise RuntimeError(
                    f"No dispatch configured for training type {job.type}"
                )
            fn = modal.Function.from_name(
                RUNNER_APP, RUNNER_ENTRYPOINT, environment_name=settings.modal_env
            )
            config = _build_runner_train_config(
                job,
                source_revision.id,
                target_revision.id,
                source_revision.bible_version_id,
                target_revision.bible_version_id,
                job.options,
            )
            await fn.spawn.aio(config, settings.aqua_db)
            return job, None
        except Exception as exc:
            logger.error(
                f"error dispatching training job {job.id} ({job.type}): "
                f"{type(exc).__name__}: {exc}",
                exc_info=True,
            )
            return job, exc

    results = await asyncio.gather(*(dispatch(job) for job in jobs))
    failures = {
        job.assessment_id: f"dispatch_failed: {type(exc).__name__}: {exc}"
        for job, exc in results
        if exc is not None and job.assessment_id is not None
    }
    if not failures:
        return

    # Written directly rather than through the runner's status callback: the runner never
    # got the job, so nothing else will ever move these rows off `queued`.
    try:
        now = datetime.utcnow()
        stmt = select(Assessment).where(
            Assessment.id.in_(list(failures)), Assessment.deleted.is_not(True)
        )
        for assessment in (await db.execute(stmt)).scalars().all():
            assessment.status = AssessmentStatus.failed.value
            assessment.status_detail = failures[assessment.id]
            assessment.start_time = assessment.start_time or now
            assessment.end_time = now
        await db.commit()
    except Exception:
        await db.rollback()
        raise


async def list_jobs(
    db: AsyncSession,
    user: UserDB,
    *,
    limit: int,
    offset: int,
    state: JobState | None = None,
    training_type: TrainingType | None = None,
    source_version_id: int | None = None,
    target_version_id: int | None = None,
) -> tuple[list[TrainingJob], int]:
    """Return one page of training jobs the user may see, and the total match count.

    The four filters are v3's. ``state`` is the one that changed shape: v3 filters on the
    raw internal ``Assessment.status`` string, so a caller has to know v4's public
    vocabulary translates to it; here the parameter takes the public
    :class:`~api_v4.jobs.JobState` and is translated back through
    :data:`INTERNAL_STATUS_FOR_STATE` before it reaches the column. Note the filter
    joins the assessment, so filtering by any state excludes the jobs that have none —
    which is the honest answer, since such a job has no state to match.

    The version filters are plain equality on the denormalized columns, applied *after*
    the visibility predicate, so they can only narrow what the caller could already see:
    a version id outside their groups yields an empty page rather than a 404.

    ``total`` counts all matching rows ignoring ``limit``/``offset``. It is a second
    statement, so a concurrent write between the two can cause the usual rare
    offset-pagination drift between ``total`` and ``len(items)``.

    Ordered by ``id`` ascending, like every other v4 list and unlike v3's unordered
    (and unbounded) result: offset pagination needs a total order on a column that
    cannot tie or move, and ``requested_time`` is nullable.
    """
    stmt = _visible_jobs_query(user)
    if state is not None:
        stmt = stmt.join(Assessment, Assessment.id == TrainingJob.assessment_id).where(
            Assessment.status == INTERNAL_STATUS_FOR_STATE[state].value
        )
    if training_type is not None:
        stmt = stmt.where(TrainingJob.type == training_type.value)
    if source_version_id is not None:
        stmt = stmt.where(TrainingJob.source_version_id == source_version_id)
    if target_version_id is not None:
        stmt = stmt.where(TrainingJob.target_version_id == target_version_id)

    total = (
        await db.execute(select(func.count()).select_from(stmt.subquery()))
    ).scalar_one()
    rows = await db.execute(
        stmt.options(selectinload(TrainingJob.assessment))
        .order_by(TrainingJob.id)
        .limit(limit)
        .offset(offset)
    )
    return list(rows.scalars().unique().all()), total


async def get_job(db: AsyncSession, user: UserDB, job_id: int) -> TrainingJob:
    """Return one training job the user may see, or raise :class:`TrainingJobNotFound`."""
    stmt = (
        _visible_jobs_query(user)
        .options(selectinload(TrainingJob.assessment))
        .where(TrainingJob.id == job_id)
    )
    job = (await db.execute(stmt)).scalars().first()
    if job is None:
        raise TrainingJobNotFound(job_id)
    return job


async def get_session_jobs(
    db: AsyncSession, user: UserDB, session_id: str
) -> list[TrainingJob]:
    """Return the visible jobs of one session, or raise
    :class:`TrainingSessionNotFound`.

    An empty result is a 404 rather than an empty session, because a session is a column
    value: there is no row whose existence could be confirmed independently of its jobs.
    A caller who can see none of a session's jobs therefore gets the same answer as one
    naming a key that was never issued, which is also the behaviour that keeps session
    keys from being probed.
    """
    stmt = (
        _visible_jobs_query(user)
        .options(selectinload(TrainingJob.assessment))
        .where(TrainingJob.session_id == session_id)
        .order_by(TrainingJob.id)
    )
    jobs = list((await db.execute(stmt)).scalars().unique().all())
    if not jobs:
        raise TrainingSessionNotFound(session_id)
    return jobs


async def soft_delete_job(db: AsyncSession, user: UserDB, job_id: int) -> TrainingJob:
    """Soft-delete a terminal training job (its owner, or an admin).

    Four refusals, in this order, and the order is the security property: the row is
    resolved through the group-scoped predicate first, so
    :class:`TrainingJobAccessForbidden` is reachable only for a job whose existence the
    caller has already established. v3 looks the row up with no permission filter and
    answers 403, which makes its status code an existence oracle.

    Idempotent: re-deleting an already-deleted job is a no-op rather than v3's 404. That
    is why the lookup passes ``include_deleted=True`` — and it is also what keeps a job
    deletable after its revision was soft-deleted, which the read predicate hides.

    The terminal check reads the linked assessment, so a job with none cannot be verified
    terminal and is refused with :class:`TrainingJobStateUnavailable` — v3's own answer
    on this path, kept because the alternative is deleting a row that may still have a
    GPU job running behind it.
    """
    stmt = (
        _visible_jobs_query(user, include_deleted=True)
        .options(selectinload(TrainingJob.assessment))
        .where(TrainingJob.id == job_id)
    )
    job = (await db.execute(stmt)).scalars().first()
    if job is None:
        raise TrainingJobNotFound(job_id)
    if not user.is_admin and job.owner_id != user.id:
        raise TrainingJobAccessForbidden(job_id)
    if job.deleted:
        return job

    state = state_for_training_job(job)
    if state is None:
        raise TrainingJobStateUnavailable(job_id)
    if not state.is_terminal:
        raise TrainingJobNotTerminal(job_id, state)

    try:
        job.deleted = True
        job.deleted_at = datetime.utcnow()
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return job


async def inference_readiness(
    db: AsyncSession, source_revision_id: int, target_revision_id: int
) -> dict[TrainingType, InferenceReadinessOut]:
    """Which analyses can be run against this revision pair now.

    Computed over **every** finished training job for the pair, not only one session's:
    readiness is a property of the pair, so a type trained by an earlier session counts
    and a type this session never selected can be ready. "Finished" is read off the
    linked assessment, since that is where a training run's status lives.
    """
    stmt = (
        select(TrainingJob.type)
        .join(Assessment, Assessment.id == TrainingJob.assessment_id)
        .where(
            TrainingJob.source_revision_id == source_revision_id,
            TrainingJob.target_revision_id == target_revision_id,
            TrainingJob.deleted.is_not(True),
            Assessment.status == ASSESSMENT_FINISHED_VALUE,
        )
    )
    completed = {row[0] for row in (await db.execute(stmt)).all()}

    readiness: dict[TrainingType, InferenceReadinessOut] = {}
    for app, required in INFERENCE_DEPENDENCIES.items():
        pending = [TrainingType(t) for t in required if t not in completed]
        readiness[TrainingType(app)] = InferenceReadinessOut(
            ready=not pending, pending_training=pending
        )
    return readiness


# ---------------------------------------------------------------------------
# GET /v4/training-sessions/{session_id}/results — the interleaved per-verse read.
# ---------------------------------------------------------------------------


async def _source_side_assessment_id(
    db: AsyncSession, source_revision_id: int, training_type: str
) -> int | None:
    """The latest finished assessment of ``training_type`` trained *on* the source side.

    Source-side corpora come from a separate session that swapped the pair round, so
    there is no link from this session to them; they are found by looking for a finished
    assessment whose ``revision_id`` is this session's source revision.

    Authorization note: the caller has already passed the session's own visibility
    predicate, which gates on access to both revisions — so any finished assessment on
    ``source_revision_id`` is on a revision they can already see, and its corpus
    statistics are derivable by anyone with that access. This therefore does not
    re-filter on ownership, matching v3.

    Ties break on ``id`` with ``NULLS LAST`` on ``end_time``, so a stale half-finished
    row cannot preempt a real one and the pick is deterministic. Two rows are fetched
    rather than one purely so an unusual workflow state can be logged without
    materializing every historical assessment on a hot path.
    """
    stmt = (
        select(Assessment.id)
        .where(
            Assessment.revision_id == source_revision_id,
            Assessment.type == training_type,
            Assessment.status == ASSESSMENT_FINISHED_VALUE,
            Assessment.deleted.is_not(True),
        )
        .order_by(Assessment.end_time.desc().nullslast(), Assessment.id.desc())
        .limit(2)
    )
    rows = (await db.execute(stmt)).all()
    if not rows:
        return None
    if len(rows) > 1:
        logger.warning(
            "multiple finished source-side assessments matched; picking latest",
            extra={
                "source_revision_id": source_revision_id,
                "assessment_type": training_type,
                "picked_assessment_id": rows[0][0],
            },
        )
    return rows[0][0]


def _scope_columns(query, model, scope):
    """Narrow a subquery that has its own ``book``/``chapter``/``verse`` columns."""
    if scope.book is not None:
        query = query.where(model.book == scope.book)
    if scope.chapter is not None:
        query = query.where(model.chapter == scope.chapter)
    if scope.verse is not None:
        query = query.where(model.verse == scope.verse)
    return query


def _scope_via_reference(query, scope):
    """Narrow a vref-only subquery through its join to ``verse_reference``.

    ``tfidf_pca_vector`` and ``ngram_vref_table`` store a vref and nothing else, so the
    location triple has to be derived: the chapter number is the second space-separated
    field of ``verse_reference.chapter`` (``"GEN 1"`` -> ``1``).
    """
    if scope.book is not None:
        query = query.where(VerseReference.book_reference == scope.book)
    if scope.chapter is not None:
        query = query.where(
            func.split_part(VerseReference.chapter, " ", 2).cast(Integer)
            == scope.chapter
        )
    if scope.verse is not None:
        query = query.where(VerseReference.number == scope.verse)
    return query


def _derived_location_columns(vref_column):
    """The union's four columns for a table that stores a vref and nothing else."""
    return (
        vref_column.label("vref"),
        VerseReference.book_reference.label("book"),
        func.split_part(VerseReference.chapter, " ", 2).cast(Integer).label("chapter"),
        VerseReference.number.label("verse"),
    )


def _tfidf_vref_subquery(assessment_id: int, scope):
    """One member of the vref union: the verses one TF-IDF corpus has vectors for."""
    return _scope_via_reference(
        select(*_derived_location_columns(TfidfPcaVector.vref))
        .join(VerseReference, VerseReference.full_verse_id == TfidfPcaVector.vref)
        .where(TfidfPcaVector.assessment_id == assessment_id),
        scope,
    )


def _ngram_vref_subquery(assessment_id: int, scope):
    """One member of the vref union: the verses one n-gram corpus fires on.

    Emits a row per ``(ngram, vref)`` pair, which is why the union must deduplicate —
    see :func:`_page_vrefs`.
    """
    return _scope_via_reference(
        select(*_derived_location_columns(NgramVrefTable.vref))
        .join(NgramsTable, NgramsTable.id == NgramVrefTable.ngram_id)
        .join(VerseReference, VerseReference.full_verse_id == NgramVrefTable.vref)
        .where(NgramsTable.assessment_id == assessment_id),
        scope,
    )


def _located_subquery(model, assessment_id: int, scope):
    """One member of the vref union for a table carrying its own location triple."""
    return _scope_columns(
        select(model.vref, model.book, model.chapter, model.verse).where(
            model.assessment_id == assessment_id
        ),
        model,
        scope,
    )


async def _page_vrefs(
    db: AsyncSession, subqueries: list, *, limit: int, offset: int
) -> tuple[list[str], int]:
    """The page's vrefs in canonical order, and how many there are in total.

    ``union`` (not ``union_all``) is load-bearing: semantic-similarity and
    word-alignment both write to ``assessment_result``, and one n-gram subquery emits a
    row per ``(ngram, vref)`` pair, so without it a verse would paginate several times.
    The ``distinct`` on the joined subquery covers the single-member case, where
    SQLAlchemy emits no ``UNION`` wrapper at all and those internal duplicates would
    otherwise reach both ``total`` and the page.

    The join to ``book_reference`` is applied before the count so that ``total`` and the
    page see the same row set — a vref whose book matches no reference row would
    otherwise be counted and never returned.
    """
    if not subqueries:
        return [], 0

    union = subqueries[0].union(*subqueries[1:]).subquery()
    joined = (
        select(
            union.c.vref,
            BookReference.number.label("book_number"),
            union.c.chapter,
            union.c.verse,
        )
        .join(BookReference, BookReference.abbreviation == union.c.book)
        .distinct()
        .subquery()
    )
    total = (
        await db.execute(select(func.count()).select_from(joined))
    ).scalar_one() or 0
    rows = await db.execute(
        select(joined.c.vref)
        .order_by(joined.c.book_number, joined.c.chapter, joined.c.verse)
        .limit(limit)
        .offset(offset)
    )
    return [row[0] for row in rows.all()], total


async def _verse_text(
    db: AsyncSession, revision_id: int, vrefs: list[str]
) -> dict[str, str]:
    """Bulk-fetch one revision's text for a set of vrefs, skipping rows with no text."""
    if not vrefs:
        return {}
    rows = await db.execute(
        select(VerseText.verse_reference, VerseText.text).where(
            VerseText.revision_id == revision_id,
            VerseText.verse_reference.in_(vrefs),
        )
    )
    return {row[0]: row[1] for row in rows.all() if row[1] is not None}


#: Nearest-neighbour search over one corpus, for every vref on the page at once. Raw SQL
#: because the ``LATERAL`` join is what keeps this one query rather than one per verse,
#: and SQLAlchemy's ORM layer cannot express the correlated per-row ``LIMIT``.
_NEIGHBOURS_SQL = text(
    """
    SELECT q.vref AS query_vref,
           nn.vref AS neighbour_vref,
           nn.cosine_similarity AS similarity
    FROM tfidf_pca_vector AS q
    JOIN LATERAL (
        SELECT c.vref,
               inner_product(c.vector, q.vector) AS cosine_similarity
        FROM tfidf_pca_vector AS c
        WHERE c.assessment_id = :assessment_id
          AND c.vref != q.vref
        ORDER BY cosine_similarity DESC
        LIMIT :limit
    ) AS nn ON true
    WHERE q.assessment_id = :assessment_id
      AND q.vref IN :page_vrefs
    ORDER BY q.vref, nn.cosine_similarity DESC
    """
).bindparams(bindparam("page_vrefs", expanding=True))


async def _neighbours(
    db: AsyncSession, assessment_id: int, page_vrefs: list[str], top_k: int
) -> dict[str, list[tuple[str, float]]]:
    """Nearest neighbours per page vref within one corpus, most similar first.

    When run against the *source* corpus this is still keyed on the page's vrefs: it
    asks "for each of these verses, what is nearest to it inside the source-side
    corpus", not for a separate source-side pagination. A verse the source corpus has no
    vector for simply comes back with no neighbours.
    """
    rows = await db.execute(
        _NEIGHBOURS_SQL,
        {"assessment_id": assessment_id, "page_vrefs": page_vrefs, "limit": top_k},
    )
    buckets: dict[str, list[tuple[str, float]]] = {}
    for row in rows.all():
        similarity = float(row.similarity) if row.similarity is not None else 0.0
        buckets.setdefault(row.query_vref, []).append((row.neighbour_vref, similarity))
    return buckets


async def _ngram_buckets(
    db: AsyncSession, assessment_id: int, page_vrefs: list[str]
) -> dict[int, dict[str, Any]]:
    """``{ngram_id: {ngram, ngram_size, vrefs}}`` for n-grams firing on the page.

    Each bucket carries the n-gram's **full** occurrence list rather than the
    intersection with the page, which is what makes one n-gram's entry identical
    wherever it appears — the same shape ``POST /v4/predictions`` returns.
    """
    if not page_vrefs:
        return {}
    matching = (
        select(NgramsTable.id)
        .join(NgramVrefTable, NgramVrefTable.ngram_id == NgramsTable.id)
        .where(
            NgramsTable.assessment_id == assessment_id,
            NgramVrefTable.vref.in_(page_vrefs),
        )
        .distinct()
        .subquery()
    )
    rows = await db.execute(
        select(
            NgramsTable.id,
            NgramsTable.ngram,
            NgramsTable.ngram_size,
            NgramVrefTable.vref,
        )
        .join(NgramVrefTable, NgramVrefTable.ngram_id == NgramsTable.id)
        .join(matching, matching.c.id == NgramsTable.id)
    )
    buckets: dict[int, dict[str, Any]] = {}
    for ngram_id, ngram, size, vref in rows.all():
        bucket = buckets.setdefault(
            ngram_id, {"ngram": ngram, "ngram_size": size, "vrefs": []}
        )
        bucket["vrefs"].append(vref)
    return buckets


def _verse_score(row) -> TrainingVerseScore:
    return TrainingVerseScore(
        score=float(row.score) if row.score is not None else None,
        flag=bool(row.flag),
        hide=bool(row.hide),
        note=row.note,
    )


async def session_results(
    db: AsyncSession,
    jobs: list[TrainingJob],
    *,
    scope,
    limit: int,
    offset: int,
    tfidf_top_k: int,
) -> tuple[list[TrainingResultRow], int]:
    """One page of interleaved per-verse results for a session's finished jobs.

    ``jobs`` are the session's jobs as :func:`get_session_jobs` returned them — already
    authorized, with their assessments loaded — so this function performs no
    authorization of its own and must not be called with an unvetted list.

    **Only finished jobs contribute.** A queued, running or failed job's type is simply
    absent from every row; its state is visible on the session read instead. The vref
    universe is the union of the per-verse tables of every finished type, which is what
    makes one ordered, offset-paginated sequence out of four differently-keyed tables.

    Both sides of each corpus-shaped type are looked up: the session's own assessment for
    the target side, and (only when that type finished here) the latest finished
    same-type assessment on the session's source revision for the source side. The source
    side contributes to the vref universe too, so a verse covered only by source-side
    hits still paginates.
    """
    session_source_revision_id = jobs[0].source_revision_id
    session_target_revision_id = jobs[0].target_revision_id

    finished: dict[str, int] = {
        job.type: job.assessment_id
        for job in jobs
        if job.assessment is not None
        and job.assessment.status == ASSESSMENT_FINISHED_VALUE
        and job.assessment_id is not None
    }
    sem_sim_id = finished.get(TrainingType.semantic_similarity.value)
    word_align_id = finished.get(TrainingType.word_alignment.value)
    ngrams_id = finished.get(TrainingType.ngrams.value)
    tfidf_id = finished.get(TrainingType.tfidf.value)

    source_ngrams_id = (
        await _source_side_assessment_id(
            db, session_source_revision_id, TrainingType.ngrams.value
        )
        if ngrams_id is not None
        else None
    )
    source_tfidf_id = (
        await _source_side_assessment_id(
            db, session_source_revision_id, TrainingType.tfidf.value
        )
        if tfidf_id is not None
        else None
    )

    subqueries = []
    if sem_sim_id is not None:
        subqueries.append(_located_subquery(AssessmentResult, sem_sim_id, scope))
    if word_align_id is not None:
        subqueries.append(
            _located_subquery(AlignmentTopSourceScores, word_align_id, scope)
        )
        # The verse-level word-alignment score lives in `assessment_result` beside the
        # per-word rows, so a verse scored but not aligned still paginates.
        subqueries.append(_located_subquery(AssessmentResult, word_align_id, scope))
    for assessment_id in (tfidf_id, source_tfidf_id):
        if assessment_id is not None:
            subqueries.append(_tfidf_vref_subquery(assessment_id, scope))
    for assessment_id in (ngrams_id, source_ngrams_id):
        if assessment_id is not None:
            subqueries.append(_ngram_vref_subquery(assessment_id, scope))

    page_vrefs, total = await _page_vrefs(db, subqueries, limit=limit, offset=offset)
    if not page_vrefs:
        return [], total

    sem_sim_by_vref: dict[str, TrainingVerseScore] = {}
    if sem_sim_id is not None:
        rows = await db.execute(
            select(AssessmentResult)
            .where(
                AssessmentResult.assessment_id == sem_sim_id,
                AssessmentResult.vref.in_(page_vrefs),
            )
            .order_by(AssessmentResult.id.asc())
        )
        for row in rows.scalars().all():
            # First write wins: `(assessment_id, vref)` has no uniqueness constraint, so
            # duplicates are possible, and `ORDER BY id` makes which one wins
            # deterministic rather than whichever the planner returned last.
            sem_sim_by_vref.setdefault(row.vref, _verse_score(row))

    alignments_by_vref: dict[str, list[TrainingWordAlignment]] = {}
    alignment_score_by_vref: dict[str, TrainingVerseScore] = {}
    if word_align_id is not None:
        rows = await db.execute(
            select(AlignmentTopSourceScores).where(
                AlignmentTopSourceScores.assessment_id == word_align_id,
                AlignmentTopSourceScores.vref.in_(page_vrefs),
            )
        )
        for row in rows.scalars().all():
            alignments_by_vref.setdefault(row.vref, []).append(
                TrainingWordAlignment(
                    source=row.source,
                    target=row.target,
                    score=float(row.score) if row.score is not None else None,
                    flag=bool(row.flag),
                    hide=bool(row.hide),
                    note=row.note,
                )
            )
        rows = await db.execute(
            select(AssessmentResult)
            .where(
                AssessmentResult.assessment_id == word_align_id,
                AssessmentResult.vref.in_(page_vrefs),
            )
            .order_by(AssessmentResult.id.asc())
        )
        for row in rows.scalars().all():
            alignment_score_by_vref.setdefault(row.vref, _verse_score(row))

    target_neighbours: dict[str, list[tuple[str, float]]] = {}
    source_neighbours: dict[str, list[tuple[str, float]]] = {}
    if tfidf_id is not None:
        target_neighbours = await _neighbours(db, tfidf_id, page_vrefs, tfidf_top_k)
        if source_tfidf_id is not None:
            source_neighbours = await _neighbours(
                db, source_tfidf_id, page_vrefs, tfidf_top_k
            )

    target_ngrams: dict[int, dict[str, Any]] = {}
    source_ngrams: dict[int, dict[str, Any]] = {}
    if ngrams_id is not None:
        target_ngrams = await _ngram_buckets(db, ngrams_id, page_vrefs)
    if source_ngrams_id is not None:
        source_ngrams = await _ngram_buckets(db, source_ngrams_id, page_vrefs)

    # One text fetch per revision for every vref the response mentions — the page's own
    # verses, every neighbour, and every verse in any n-gram's occurrence list.
    text_vrefs: set[str] = set(page_vrefs)
    for buckets in (target_neighbours, source_neighbours):
        for neighbours in buckets.values():
            text_vrefs.update(vref for vref, _ in neighbours)
    for buckets in (target_ngrams, source_ngrams):
        for bucket in buckets.values():
            text_vrefs.update(bucket["vrefs"])
    all_vrefs = sorted(text_vrefs)
    target_text = await _verse_text(db, session_target_revision_id, all_vrefs)
    source_text = await _verse_text(db, session_source_revision_id, all_vrefs)

    def build_neighbours(raw: list[tuple[str, float]]) -> list[TrainingNeighbour]:
        return [
            TrainingNeighbour(
                vref=vref,
                similarity=similarity,
                target_text=target_text.get(vref),
                source_text=source_text.get(vref),
            )
            for vref, similarity in raw
        ]

    def build_matches(buckets: dict[int, dict[str, Any]]) -> dict[str, list]:
        """One match object per n-gram, attached to each page vref it fires on."""
        page = set(page_vrefs)
        per_vref: dict[str, list[TrainingNgramMatch]] = {v: [] for v in page_vrefs}
        for ngram_id, bucket in buckets.items():
            match = TrainingNgramMatch(
                id=ngram_id,
                ngram=bucket["ngram"],
                ngram_size=bucket["ngram_size"],
                occurrences=[
                    TrainingNgramOccurrence(
                        vref=vref,
                        target_text=target_text.get(vref),
                        source_text=source_text.get(vref),
                    )
                    for vref in bucket["vrefs"]
                ],
            )
            for vref in bucket["vrefs"]:
                if vref in page:
                    per_vref[vref].append(match)
        return per_vref

    target_matches = build_matches(target_ngrams)
    source_matches = build_matches(source_ngrams)

    rows = []
    for vref in page_vrefs:
        tfidf_block = None
        if tfidf_id is not None:
            tfidf_block = TrainingTfidfNeighbours(
                target_neighbours=build_neighbours(target_neighbours.get(vref, [])),
                source_neighbours=(
                    build_neighbours(source_neighbours.get(vref, []))
                    if source_tfidf_id is not None
                    else None
                ),
            )
        ngrams_block = None
        if ngrams_id is not None:
            ngrams_block = TrainingNgrams(
                target_corpus=target_matches.get(vref, []),
                source_corpus=(
                    source_matches.get(vref, [])
                    if source_ngrams_id is not None
                    else None
                ),
            )
        rows.append(
            TrainingResultRow(
                vref=vref,
                semantic_similarity=sem_sim_by_vref.get(vref),
                word_alignment=alignments_by_vref.get(vref, []),
                word_alignment_score=alignment_score_by_vref.get(vref),
                tfidf=tfidf_block,
                ngrams=ngrams_block,
            )
        )
    return rows, total

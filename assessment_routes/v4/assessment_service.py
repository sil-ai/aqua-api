"""Assessment create service for the v4 surface (issues #865/#893, epic #842).

HTTP-agnostic data access and authorization behind ``POST /v4/assessments``,
following the pattern :mod:`bible_routes.v4.version_service` established: functions
take an :class:`~sqlalchemy.ext.asyncio.AsyncSession`, the current
:class:`~database.models.UserDB` and plain data, return ORM rows, and raise the small
:class:`AssessmentServiceError` signals below. The router
(:mod:`assessment_routes.v4.assessment_routes`) owns the mapping onto the #828 error
envelope.

Scope is **create only**. List, poll, delete and the typed result sub-resources are
separate PRs on #893; the runner-facing surface (``results_push_*``, ``eflomal-*``,
``tfidf-artifacts/*``, the status ``PATCH``) stays on v3 permanently — it is our own
code talking to our own code and is not client contract (#842's 2026-08-25b decision
4).


What v4 changes, and what it deliberately does not
--------------------------------------------------

**The wire contract changes; the stored row does not.** ``Assessment`` is a table the
frozen v3 surface still reads and writes, and ``Assessment.kwargs`` is part of an
assessment's *identity* rather than just a runner payload: v3's create-time dedup
compares it for exact equality and the read endpoints tell eflomal from fastalign
purely by the flag stored inside it. So the typed options union
(:mod:`api_v4.schemas.assessment`) is a validation layer over v3's storage shape —
see :func:`_stored_kwargs`, and the v3/v4 parity test in
``test_assessment_routes_v4``.

**Authorization is added (#865).** v3 validated that the revision and reference rows
*exist* but never checked the caller against them, so any authenticated user could
spend GPU time assessing another group's revisions — and, through the results, read
across the group boundary. Both ids now go through
:func:`bible_routes.v4.revision_service.get_revision`, the same visibility predicate
the Revisions slice serves reads from, so "a revision this caller may see" has one
implementation on the v4 surface.

  That helper cannot distinguish "no such revision" from "not yours", by design — the
  read predicate is a single query and reporting the two separately would let a
  caller probe which revision ids exist. So denial is a 404, not a 403. The router
  documents the choice where the status code is actually chosen.

**Dedup and its advisory lock are reused verbatim, not reimplemented.** Both surfaces
are live at once, so a v4 POST can race a v3 POST on the same work. See
:func:`_acquire_dup_lock` below for why the *key* has to come from v3's own helpers.

**``modal_env`` is resolved server-side.** v3 lets an admin caller choose which Modal
environment executes the job; v4 takes ``settings.modal_env`` and does not read the
request. The runner still receives a value — it just is not caller-controlled.

**``source_version_id`` / ``target_version_id`` are derived, never accepted.**
Verified against v3 rather than assumed: v3's own locals are derived from the two
revision rows (``assessment_routes.py:545-552``, ``target ← revision.bible_version_id``
and ``source ← reference.bible_version_id``) and are *not* read from any query
parameter, even though the only client computes and sends them. v4 derives them the
same way from the rows it has already loaded for authorization.


The create sequence, and why it is ordered this way
---------------------------------------------------

1. Authorize ``revision_id``, then ``reference_id`` — before anything is written or
   dispatched, which is the #865 fix.
2. Derive the version ids, and resolve ``transcribed_audio`` (which may need the
   draft version's own default).
3. Build the stored ``kwargs``.
4. Unless ``force``, refuse if an equivalent assessment already **finished**.
5. Take the per-quadruple advisory lock.
6. Unless the caller is an admin, refuse if an equivalent assessment is still
   **running**.
7. ``INSERT`` the queued row and commit — which also releases the lock.
8. Dispatch to Modal, transitioning ``queued -> running`` under ``FOR UPDATE``.

Steps 4-7 reproduce v3's semantics exactly, including two asymmetries that look like
bugs and are not:

* ``force`` bypasses the *finished* check only. An equivalent assessment that is
  still in flight is a conflict either way, because rerunning it would double-dispatch
  the same GPU work rather than replace a stale answer.
* The **finished** check discriminates on ``first_vref`` / ``last_vref`` /
  ``transcribed_audio`` / the eflomal runner but ignores ``finetune`` and
  ``response_language``, while the **in-progress** check compares the whole ``kwargs``
  for exact equality. Preserved rather than harmonized: v3 is frozen and live, so a
  v4 POST that deduped differently from a v3 POST against the same rows would be the
  duplicate-run bug this module exists to avoid.
"""

from datetime import datetime, timedelta

from fastapi import HTTPException, status
from sqlalchemy import JSON, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.schemas.assessment import (
    AgentCritiqueOptions,
    ReferencedAssessmentOptions,
    WordAlignmentOptions,
)
from assessment_routes.v3.alignment_filters import eflomal_method_clause

# Imported, never modified — v3 is frozen (epic #842). Three of these four are
# private names, taken deliberately: see _acquire_dup_lock and _dispatch below for
# why re-deriving either the lock key or the runner payload in v4 would be a bug
# rather than a duplication.
from assessment_routes.v3.assessment_routes import (
    STALE_ASSESSMENT_HOURS,
    _acquire_assess_dup_lock,
    _canonicalize_kwargs,
    call_assessment_runner,
)
from bible_routes.v4 import revision_service
from config import settings
from database.models import Assessment, BibleVersion, UserDB
from schemas.assessment import (
    ASSESSMENT_TERMINAL_STATUSES,
    AssessmentIn,
    AssessmentStatus,
)
from utils.logging_config import setup_logger

logger = setup_logger(__name__)

#: Sent to the runner on every dispatch. ``return_all_results`` is off the v4 client
#: contract (#512 wants it gone from the API) but the runner — a separate repo — still
#: reads it out of the config, so dropping it from the *body* must not drop it from
#: the *payload*. This is v3's own route default.
RETURN_ALL_RESULTS = False


class AssessmentServiceError(Exception):
    """Base for assessment-service domain signals the router maps to V4APIError."""


class RevisionNotVisible(AssessmentServiceError):
    """``revision_id`` does not exist, or the caller may not see it (#865).

    One signal for both cases because :func:`revision_service.get_revision` cannot
    tell them apart and must not — see the module docstring.
    """

    def __init__(self, revision_id: int) -> None:
        self.revision_id = revision_id
        super().__init__(f"Revision {revision_id} does not exist.")


class ReferenceNotVisible(AssessmentServiceError):
    """``reference_id`` does not exist, or the caller may not see it (#865).

    Distinct from :class:`RevisionNotVisible` so the client learns *which* of the two
    ids was rejected; both are otherwise identical.
    """

    def __init__(self, reference_id: int) -> None:
        self.reference_id = reference_id
        super().__init__(f"Reference revision {reference_id} does not exist.")


class AssessmentAlreadyCompleted(AssessmentServiceError):
    """An equivalent assessment already finished; ``force`` would rerun it."""

    def __init__(self, existing_id: int) -> None:
        self.existing_id = existing_id
        super().__init__(
            f"Assessment already completed (id={existing_id}). "
            "Set force to true to rerun it."
        )


class AssessmentAlreadyInProgress(AssessmentServiceError):
    """An equivalent assessment is queued or running and has not gone stale.

    Not bypassable with ``force`` (v3 parity, module docstring): the existing run is
    about to produce exactly this answer.
    """

    def __init__(self, existing_id: int) -> None:
        self.existing_id = existing_id
        super().__init__(f"Assessment already in progress (id={existing_id}).")


class AssessmentAlreadyDispatched(AssessmentServiceError):
    """The row left ``queued`` before this request could dispatch it (#780).

    Raised when :func:`call_assessment_runner` refuses to re-spawn under its
    ``FOR UPDATE`` guard. Rare — it means something else advanced the row between this
    request's commit and its dispatch.
    """

    def __init__(self, assessment_id: int, current_status: str | None) -> None:
        self.assessment_id = assessment_id
        self.current_status = current_status
        super().__init__(
            f"Assessment {assessment_id} is no longer queued "
            f"(status={current_status!r}) and was not dispatched again."
        )


class AssessmentDispatchFailed(AssessmentServiceError):
    """The Modal runner could not be reached; the row was marked ``failed``."""

    def __init__(self, assessment_id: int) -> None:
        self.assessment_id = assessment_id
        super().__init__("The assessment runner is unavailable or failed.")


async def _authorized_revisions(
    db: AsyncSession, user: UserDB, revision_id: int, reference_id: int | None
):
    """Load the revision and (optional) reference the caller is allowed to use.

    The #865 fix. Both go through :func:`revision_service.get_revision`, which is
    visibility-scoped and already excludes soft-deleted revisions *and* revisions of
    soft-deleted versions — a strictly wider check than v3's ``revision.deleted``
    test, and one this module gets for free by not writing its own predicate.

    Ordered revision-then-reference so an unauthorized caller is told about the field
    they are most likely to have got wrong first, and so a single request never
    reports two denials at once.
    """
    try:
        revision = await revision_service.get_revision(db, user, revision_id)
    except revision_service.RevisionNotFound as exc:
        raise RevisionNotVisible(revision_id) from exc

    reference = None
    if reference_id is not None:
        try:
            reference = await revision_service.get_revision(db, user, reference_id)
        except revision_service.RevisionNotFound as exc:
            raise ReferenceNotVisible(reference_id) from exc
    return revision, reference


async def _resolve_transcribed_audio(
    db: AsyncSession, options, target_version_id: int | None
) -> bool:
    """Resolve the effective ``transcribed_audio`` flag for an ``agent-critique`` run.

    Precedence, matching v3 (``assessment_routes.py:554-567``):

    1. an explicit value in the request wins;
    2. otherwise the draft version's own ``transcribed_audio`` column supplies the
       default (#815) — a version uploaded as ASR output critiques as ASR output
       without every caller having to remember to say so.

    Always ``False`` for the other six types, which is what lets :func:`_stored_kwargs`
    overlay the key unconditionally.

    The version-inherited default is why
    :attr:`~api_v4.schemas.assessment.AgentCritiqueOptions.transcribed_audio` is
    ``bool | None`` rather than a plain boolean defaulting to false. Collapsing it
    would be a silent behaviour change with no error attached: an ASR draft would be
    critiqued as if it were clean text, and the resulting row would also dedup
    separately from the v3-created equivalent.
    """
    if not isinstance(options, AgentCritiqueOptions):
        return False
    if options.transcribed_audio is not None:
        return options.transcribed_audio
    version = (
        await db.get(BibleVersion, target_version_id)
        if target_version_id is not None
        else None
    )
    # bool(): the column is nullable, and a NULL must read as "not transcribed"
    # rather than reach the stored kwargs as a JSON null.
    return bool(version.transcribed_audio) if version is not None else False


def _stored_kwargs(options, *, is_transcribed: bool) -> dict | None:
    """Build the ``Assessment.kwargs`` JSONB payload for a request.

    Each union member states what it stores (see
    :mod:`api_v4.schemas.assessment`); this adds the one key that could not be
    resolved from the request alone, and applies v3's empty-is-null normalization.

    ``{}`` normalizes to ``None`` because v3 treats them as the same thing at the
    request layer (``assessment_routes.py:456-459``) and its in-progress dedup then
    has to match all three of SQL ``NULL``, JSON ``null`` and ``{}`` to catch the
    legacy rows that predate that rule. Emitting ``{}`` here would add a fourth
    spelling of "no options" to a table two API versions read.
    """
    stored = options.stored_options()
    if is_transcribed:
        # Present-or-absent, exactly as for use_eflomal: stored as
        # {"transcribed_audio": true} when on, and as nothing at all when off
        # (assessment_routes.py:582-593). An explicit false would read as off to the
        # containment probes but would not match v3's exact-equality dedup.
        stored["transcribed_audio"] = True
    return stored or None


def _completed_duplicate_query(
    *,
    revision_id: int,
    reference_id: int | None,
    assessment_type: str,
    kwargs: dict | None,
    is_eflomal: bool,
    is_transcribed: bool,
):
    """The "has an equivalent assessment already finished?" query (v3 parity).

    Mirrors ``assessment_routes.py:599-657`` clause for clause, including which
    options it discriminates on and which it ignores — see the module docstring for
    why that asymmetry is preserved rather than tidied.

    The vref clauses use JSONB containment (``@>``) and key-presence (``?``) rather
    than equality on the whole column, so a run scoped to ``GEN 1:1`` does not dedup
    against a whole-chapter run of the same pair. Note both halves are needed: without
    the ``NOT (kwargs ? 'last_vref')`` arm, a request that omits ``last_vref`` would
    match a stored row that has one.
    """
    stmt = (
        select(Assessment)
        .where(
            Assessment.revision_id == revision_id,
            Assessment.type == assessment_type,
            Assessment.status == AssessmentStatus.finished.value,
            Assessment.deleted.is_not(True),
        )
        .order_by(Assessment.end_time.desc())
        .limit(1)
    )
    if reference_id is not None:
        stmt = stmt.where(Assessment.reference_id == reference_id)
    else:
        stmt = stmt.where(Assessment.reference_id.is_(None))

    if assessment_type == "word-alignment":
        # Shared with the read endpoints so create-dedup and reads stay in lock-step
        # on what counts as an eflomal run. v3 guards this with
        # `if is_eflomal or a.type == "word-alignment"` because its `use_eflomal` was a
        # free-floating query flag that could be set on any type (and was rejected
        # afterwards); in v4 the option exists only on this member, so `is_eflomal`
        # implies the type and the second arm is all that is left.
        stmt = stmt.where(eflomal_method_clause(is_eflomal))

    for key in ("first_vref", "last_vref"):
        value = kwargs.get(key) if kwargs else None
        if value:
            stmt = stmt.where(Assessment.kwargs.op("@>")({key: value}))
        else:
            stmt = stmt.where(
                or_(Assessment.kwargs.is_(None), ~Assessment.kwargs.has_key(key))
            )

    if assessment_type == "agent-critique":
        if is_transcribed:
            stmt = stmt.where(Assessment.kwargs.op("@>")({"transcribed_audio": True}))
        else:
            stmt = stmt.where(
                or_(
                    Assessment.kwargs.is_(None),
                    ~Assessment.kwargs.has_key("transcribed_audio"),
                )
            )
    return stmt


def _in_progress_duplicate_query(
    *,
    revision_id: int,
    reference_id: int | None,
    assessment_type: str,
    kwargs: dict | None,
):
    """The "is an equivalent assessment still running?" query (v3 parity).

    Mirrors ``assessment_routes.py:695-731``. Two details worth keeping in view:

    * The ``requested_time > stale_cutoff`` bound is what stops an assessment whose
      runner died without ever reporting a terminal status from blocking that pair
      forever. It is a liveness guard, not a correctness one.
    * The three-way null match reflects how "no options" has been spelled on this
      table over time: new rows persist Python ``None`` as the JSON ``null`` literal,
      legacy rows may hold SQL ``NULL``, and older ones may hold ``{}`` from an empty
      ``extra_kwargs``. All three mean the same thing and must dedup together.
    """
    stale_cutoff = datetime.now() - timedelta(hours=STALE_ASSESSMENT_HOURS)
    stmt = (
        select(Assessment.id)
        .where(
            Assessment.revision_id == revision_id,
            Assessment.type == assessment_type,
            Assessment.status.notin_([s.value for s in ASSESSMENT_TERMINAL_STATUSES]),
            Assessment.deleted.is_not(True),
            Assessment.requested_time > stale_cutoff,
        )
        .limit(1)
    )
    if reference_id is not None:
        stmt = stmt.where(Assessment.reference_id == reference_id)
    else:
        stmt = stmt.where(Assessment.reference_id.is_(None))

    if kwargs is not None:
        stmt = stmt.where(Assessment.kwargs == kwargs)
    else:
        stmt = stmt.where(
            or_(
                Assessment.kwargs.is_(None),
                Assessment.kwargs == JSON.NULL,
                Assessment.kwargs == {},
            )
        )
    return stmt


async def _acquire_dup_lock(
    db: AsyncSession,
    *,
    revision_id: int,
    reference_id: int | None,
    assessment_type: str,
    kwargs: dict | None,
) -> None:
    """Serialize concurrent submits on the same (revision, reference, type, kwargs).

    A thin pass-through to v3's own helpers, and the pass-through is the point. The
    lock is a transaction-scoped Postgres advisory lock keyed by a hash of a namespace
    plus the quadruple (#780, sibling of training-job #722); without it two concurrent
    submits both clear the duplicate SELECT and both INSERT, and two Modal runs start.

    v4 must derive the key with :func:`_canonicalize_kwargs` and
    :func:`_acquire_assess_dup_lock` rather than computing its own, because **both
    surfaces are live simultaneously**: a v4 submit racing a v3 submit on the same
    quadruple is a real path, not a theoretical one, and a v4-local key would leave
    that race completely unprotected while looking correct in every v4-only test.

    Taken for admins too. The admin bypass below applies to the duplicate *check*, so
    without the lock two parallel admin submits could still both insert.
    """
    await _acquire_assess_dup_lock(
        db,
        revision_id,
        reference_id,
        assessment_type,
        _canonicalize_kwargs(kwargs),
    )


async def _dispatch(
    db: AsyncSession,
    assessment: Assessment,
    *,
    kwargs: dict | None,
    source_version_id: int | None,
    target_version_id: int | None,
) -> None:
    """Hand the committed row to the Modal runner.

    Delegates to v3's :func:`call_assessment_runner` rather than re-implementing the
    spawn, for two reasons that both cost real money to get wrong:

    * **The runner is a separate repository** and reads a fixed set of config keys —
      the ``AssessmentIn`` dump plus ``first_vref`` / ``last_vref`` lifted from
      ``kwargs`` to the top level, ``source_version_id``, ``target_version_id`` and
      ``return_all_results``. Building that payload here would give it a second
      definition that can drift from the runner's expectations silently.
    * It carries the #780 per-row guard: a ``SELECT ... FOR UPDATE`` that refuses to
      re-spawn a row which has already left ``queued``, and the atomic
      ``queued -> running`` transition, which is what stopped a single assessment
      being dispatched twice.

    Its ``HTTPException`` signals are translated into this module's domain errors, so
    nothing v3-shaped reaches the v4 error envelope.
    """
    payload = AssessmentIn(
        id=assessment.id,
        revision_id=assessment.revision_id,
        reference_id=assessment.reference_id,
        type=assessment.type,
        kwargs=kwargs,
    )
    await call_assessment_runner(
        payload,
        RETURN_ALL_RESULTS,
        # Server-side, never caller-controlled (module docstring). The runner still
        # needs a value; the client just does not get to choose it.
        settings.modal_env,
        source_version_id=source_version_id,
        target_version_id=target_version_id,
        db=db,
    )


async def create_assessment(db: AsyncSession, user: UserDB, data) -> Assessment:
    """Create a queued assessment and dispatch it. ``data`` is an ``AssessmentCreate``.

    Returns the committed :class:`~database.models.Assessment` row, already
    transitioned to ``running`` by the dispatch. See the module docstring for the
    ordering of the eight steps and why each is where it is.

    Raises, in the order they can occur: :class:`RevisionNotVisible`,
    :class:`ReferenceNotVisible`, :class:`AssessmentAlreadyCompleted`,
    :class:`AssessmentAlreadyInProgress`, :class:`AssessmentAlreadyDispatched`,
    :class:`AssessmentDispatchFailed`.
    """
    options = data.options
    assessment_type = options.type
    reference_id = (
        options.reference_id
        if isinstance(options, ReferencedAssessmentOptions)
        else None
    )

    revision, reference = await _authorized_revisions(
        db, user, data.revision_id, reference_id
    )

    # Derived, never accepted from the client (module docstring). Both feed the
    # runner config, which is keyed by version rather than revision.
    target_version_id = revision.bible_version_id
    source_version_id = reference.bible_version_id if reference is not None else None

    is_transcribed = await _resolve_transcribed_audio(db, options, target_version_id)
    kwargs = _stored_kwargs(options, is_transcribed=is_transcribed)
    # isinstance rather than getattr-with-a-default: only word-alignment has a
    # runner choice, and a member that grew one without being wired in here should
    # fail visibly rather than silently dedup as fastalign.
    is_eflomal = (
        options.use_eflomal if isinstance(options, WordAlignmentOptions) else False
    )

    if not data.force:
        existing = (
            (
                await db.execute(
                    _completed_duplicate_query(
                        revision_id=data.revision_id,
                        reference_id=reference_id,
                        assessment_type=assessment_type,
                        kwargs=kwargs,
                        is_eflomal=is_eflomal,
                        is_transcribed=is_transcribed,
                    )
                )
            )
            .scalars()
            .first()
        )
        if existing is not None:
            logger.info(
                "Blocked duplicate of finished assessment",
                extra={
                    "existing_id": existing.id,
                    "user_id": user.id,
                    "revision_id": data.revision_id,
                    "type": assessment_type,
                },
            )
            raise AssessmentAlreadyCompleted(existing.id)

    await _acquire_dup_lock(
        db,
        revision_id=data.revision_id,
        reference_id=reference_id,
        assessment_type=assessment_type,
        kwargs=kwargs,
    )

    if not user.is_admin:
        existing_id = (
            (
                await db.execute(
                    _in_progress_duplicate_query(
                        revision_id=data.revision_id,
                        reference_id=reference_id,
                        assessment_type=assessment_type,
                        kwargs=kwargs,
                    )
                )
            )
            .scalars()
            .first()
        )
        if existing_id is not None:
            raise AssessmentAlreadyInProgress(existing_id)

    assessment = Assessment(
        revision_id=data.revision_id,
        reference_id=reference_id,
        type=assessment_type,
        status=AssessmentStatus.queued.value,
        requested_time=datetime.now(),
        owner_id=user.id,
        kwargs=kwargs,
    )
    try:
        db.add(assessment)
        # Commit the queued row *before* dispatching: the runner may PATCH its status
        # the moment it picks the job up, and it cannot see an uncommitted row. This
        # also releases the advisory lock, which is safe — the check-then-insert pair
        # it protects is complete, so a waiter on the same quadruple now sees this row
        # in its own duplicate check.
        await db.commit()
    except Exception:
        # Never leave the shared session in an aborted-transaction state (the v4
        # service convention).
        await db.rollback()
        raise
    await db.refresh(assessment)
    # Read the id out now, while the instance is live. Every failure path below rolls
    # the session back, and `rollback()` expires an instance's attributes
    # unconditionally — `expire_on_commit=False` (database/dependencies.py) suppresses
    # that on *commit* only. Reading `assessment.id` after a rollback therefore fires a
    # lazy refresh, and a lazy refresh is IO from a plain attribute access, which under
    # asyncpg raises MissingGreenlet. That would turn every one of these domain errors
    # into an opaque 500 with no error envelope, in exactly the paths that exist to
    # report a specific failure.
    assessment_id = assessment.id

    try:
        await _dispatch(
            db,
            assessment,
            kwargs=kwargs,
            source_version_id=source_version_id,
            target_version_id=target_version_id,
        )
    except HTTPException as exc:
        # The row left `queued` between this request's commit and its dispatch, so
        # call_assessment_runner refused to re-spawn. Roll back its FOR UPDATE
        # transaction and report the conflict; the row keeps whatever state the other
        # writer gave it.
        await db.rollback()
        if exc.status_code == status.HTTP_409_CONFLICT:
            detail = exc.detail if isinstance(exc.detail, dict) else {}
            raise AssessmentAlreadyDispatched(
                assessment_id, detail.get("status")
            ) from exc
        # Any other HTTPException is unexpected here. In practice that means
        # call_assessment_runner's 404 — the row this request committed moments ago is
        # no longer in the table — which nothing in the codebase can currently cause:
        # assessment deletion is soft (`deleted = True`) and the guard selects by
        # primary key without filtering on it, so only an out-of-band DELETE lands
        # here. Neither side logs it (v3 raises that 404 without a log of its own), so
        # log it here: the reported 503 is otherwise indistinguishable from an ordinary
        # runner outage, in the one case where it means rows are vanishing from under
        # live requests. Still reported as ASSESSMENT_DISPATCH_FAILED — a 503 invites
        # the retry that would recreate the row, and this module's contract is that no
        # v3-shaped exception reaches the v4 error envelope.
        logger.error(
            "Unexpected HTTPException from the assessment runner",
            exc_info=True,
            extra={
                "assessment_id": assessment_id,
                "status_code": exc.status_code,
                "modal_env": settings.modal_env,
            },
        )
        raise AssessmentDispatchFailed(assessment_id) from exc
    except Exception as exc:
        logger.error(
            "Modal runner dispatch failed",
            exc_info=True,
            extra={
                "assessment_id": assessment_id,
                "modal_env": settings.modal_env,
                "error_type": type(exc).__name__,
            },
        )
        # Mark the row failed in a fresh transaction so it reflects reality: the runner
        # never got the chance to advance it past whatever state we left it in. Mirrors
        # v3's dispatch-failure handling.
        try:
            await db.rollback()
            assessment.status = AssessmentStatus.failed.value
            assessment.status_detail = f"dispatch_failed: {type(exc).__name__}: {exc}"
            assessment.end_time = datetime.utcnow()
            await db.commit()
        except SQLAlchemyError as cleanup_err:
            await db.rollback()
            logger.error(
                f"Failed to mark assessment {assessment_id} as failed "
                f"after runner error: {cleanup_err}"
            )
        raise AssessmentDispatchFailed(assessment_id) from exc

    # Commit the queued -> running transition _dispatch performed under FOR UPDATE.
    await db.commit()
    await db.refresh(assessment)
    return assessment

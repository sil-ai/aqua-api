"""Assessment data-access service for the v4 surface (issues #865/#893, epic #842).

HTTP-agnostic data access and authorization behind the ``/v4/assessments`` endpoints,
following the pattern :mod:`bible_routes.v4.version_service` established: functions
take an :class:`~sqlalchemy.ext.asyncio.AsyncSession`, the current
:class:`~database.models.UserDB` and plain data, return ORM rows, and raise the small
:class:`AssessmentServiceError` signals below. The router
(:mod:`assessment_routes.v4.assessment_routes`) owns the mapping onto the #828 error
envelope.

Scope is **create, read and delete**. The typed result sub-resources are a separate PR
on #893; the runner-facing surface (``results_push_*``, ``eflomal-*``,
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


Who can see an assessment (:func:`_visible_assessments_query`)
--------------------------------------------------------------

One predicate serves the list, the single read and the delete gate, because
authorization that is defined per endpoint is how four of this slice's five security
issues happened (#858/#860/#862/#865). It says an assessment is visible when:

1. the caller's groups reach the **revision's** version, **and** the **reference's**
   version too where the row has a reference. v3's non-admin list query already works
   this way (``assessment_routes.py:238-260``) and submit enforces the same rule on
   both ids, so this is carried over rather than decided;
2. the row is not ``is_training``; and
3. nothing in the chain is soft-deleted — the assessment, its revision, its revision's
   version, and the reference's revision and version.

Admins skip (1) and keep (2) and (3).

**Training rows are hidden** (#893's 2026-08-26 decision 3). ``train_routes/v3`` writes
its jobs into the *same* ``assessment`` table with ``is_training=True``, and v3's
``GET /assessment`` does not filter them, so v3 returns training jobs mixed in with
assessments. Copying v3's filter set verbatim would inherit that leak silently. #895
will expose those rows as their own resource, and one database row addressable as two
different v4 resources with two different shapes is very hard to walk back once a
client depends on it. Consequence, stated on the endpoint too: a client comparing v4's
list against v3's sees **fewer** rows, by design.

**A soft-deleted revision hides its assessments** (decision 5, a deliberate divergence
from v3). v3's read visibility checks only that the caller's groups reach the revision's
version; it never checks whether the revision or version is deleted. v4's revisions
service filters both levels, and this follows that precedent so the three read surfaces
agree: if ``GET /v4/revisions/{id}`` 404s, so do assessments of it. The note for
delta-sync consumers is that an assessment can leave a mirror's scope *without being
deleted itself*, because its revision was — the same class of event the periodic full
reconcile already exists for, now with a second cause.

``updated_since`` switches the list to **delta mode**, mirroring
:func:`bible_routes.v4.revision_service._visible_revisions_query`: only rows written
after that instant come back, and it *replaces* the deleted filters rather than
combining with them, so a mirror learns about soft-deletes. Scoping and the
``is_training`` exclusion are untouched. Naive-UTC normalization happens in the query
builder because it exists for the timezone-naive ``TIMESTAMP`` column the comparison
targets (asyncpg refuses an aware datetime against a naive one).

The scoping is expressed as ``version_id IN (subquery)`` rather than a join to
``bible_version_access``, for two reasons: the reference half is a *disjunction*
("either there is no reference, or its version is reachable") which a join cannot
express, and a join would multiply rows for a version reachable through two of the
caller's groups, forcing a ``distinct()`` that then has to be threaded through the
count/watermark subquery. v3 materializes the same set into a Python list with two
extra round trips; keeping it as a subquery is one statement.


Delete, and the three things it fixes (decision 4)
--------------------------------------------------

v3 soft-deletes, allows owner-or-admin, and returns 403 otherwise. v4 keeps all three
and changes this much:

**(a) The existence leak is closed.** v3 looks the row up with *no* permission filter
and then answers 403, so 404-vs-403 tells an unauthorized caller whether an id exists —
exactly the probe this slice already ruled out on submit (#865). v4 answers **404 when
the caller cannot reach the assessment, and 403 only when they can reach it but do not
own it** (:func:`_get_assessment_for_write`).

  The gate scopes by group access but does **not** filter the ``deleted`` flags, so it
  is a superset of the read predicate. That is deliberate on both counts:
  :func:`soft_delete_assessment` documents itself as idempotent, which requires the
  gate to load an already-deleted row; and hiding a row from *writes* because its
  revision was deleted would leave an owner unable to clean up rows they still own —
  the same call :mod:`bible_routes.v4.revision_service` makes for the same reason. The
  two differ only for rows the caller *would* see but for a deleted flag, where a 403
  discloses nothing they could not already learn from having group access at all.
  ``is_training`` rows stay excluded here too, so deleting one is a 404 rather than a
  back door into #895's resource.

**(b) ``owner_id`` is nullable, so legacy rows are admin-only.** Rows created before
the column existed have no owner, ``is_owner`` is false for everyone, and only an admin
can delete them. Not a bug to fix — a fact the endpoint documents, since otherwise it
reads as an authorization failure.

**(c) Deleting a queued or running assessment does not stop the Modal run.** It keeps
going, keeps costing GPU time, and its results still push back into the soft-deleted
row. Allowed anyway, and said out loud on the endpoint: refusing with a 409 while in
flight would block the most likely legitimate use of delete — getting rid of an
expensive run started by mistake — while providing no actual protection, because v4 has
no Modal handle to cancel with either way.

Verified rather than assumed: **delete-then-resubmit works.** Both
:func:`_completed_duplicate_query` and :func:`_in_progress_duplicate_query` already
filter ``Assessment.deleted.is_not(True)``, matching v3, so a soft-deleted assessment
does not block an identical resubmit with a spurious 409.
"""

from datetime import date, datetime, timedelta

from fastapi import HTTPException, status
from sqlalchemy import JSON, and_, func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

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
from database.models import (
    Assessment,
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    UserDB,
    UserGroup,
)
from schemas.assessment import (
    ASSESSMENT_TERMINAL_STATUSES,
    AssessmentIn,
    AssessmentStatus,
)
from utils.datetime_utils import as_naive_utc
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


class AssessmentNotFound(AssessmentServiceError):
    """No assessment with this id is visible to (read) or reachable by the caller.

    One signal for "no such id", "outside your groups", "soft-deleted", "its revision
    was soft-deleted" and "it is a training row", because the read predicate resolves
    all five in a single scoped query and separating them would hand back the existence
    oracle the predicate exists to remove (see the module docstring).
    """

    def __init__(self, assessment_id: int) -> None:
        self.assessment_id = assessment_id
        super().__init__(f"Assessment {assessment_id} does not exist.")


class AssessmentAccessForbidden(AssessmentServiceError):
    """The caller can reach the assessment but neither owns it nor is an admin.

    Raised only *after* :class:`AssessmentNotFound` has been ruled out, which is the
    decision-4(a) fix: v3 reported 403 for rows the caller could not see at all, so its
    404-vs-403 answer leaked whether an id existed.
    """

    def __init__(self, assessment_id: int) -> None:
        self.assessment_id = assessment_id
        super().__init__(f"Not authorized to delete assessment {assessment_id}.")


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


# ---------------------------------------------------------------------------
# The read / delete half: GET /v4/assessments, GET /v4/assessments/{id},
# DELETE /v4/assessments/{id}. See "Who can see an assessment" and "Delete, and
# the three things it fixes" in the module docstring for the decisions.
# ---------------------------------------------------------------------------


def _accessible_version_ids(user: UserDB):
    """Subquery yielding the ids of every version ``user``'s groups can reach.

    Kept as a subquery rather than materialized into a Python list (v3 runs two extra
    round trips to build one) because it is used **twice** in the same statement — once
    for the revision's version and once for the reference's — and because an ``IN``
    against it cannot multiply rows the way a join to ``bible_version_access`` would for
    a version reachable through two of the caller's groups.
    """
    return select(BibleVersionAccess.bible_version_id).where(
        BibleVersionAccess.group_id.in_(
            select(UserGroup.group_id).where(UserGroup.user_id == user.id)
        )
    )


def _visible_assessments_query(
    user: UserDB, *, include_deleted: bool, updated_since: datetime | None = None
):
    """Base ``SELECT Assessment`` scoped to what ``user`` may see.

    No ``limit``/``offset``/``order_by`` — callers add those, and the count/watermark
    query wraps this as a subquery, so the authorization logic lives in exactly one
    place for all three endpoints. The full rule, and why each clause is there, is in
    the module docstring; what follows is only what is easy to misread in the code.

    The joins to the revision and its version are **inner** and unconditional: both
    branches need them, because even an admin gets the soft-delete filters. The
    reference joins are **outer**, because most types have no reference — which is also
    why every clause touching them is guarded by
    ``or_(Assessment.reference_id.is_(None), ...)``. Without that guard a reference-free
    row would be filtered out, since a comparison against the NULLs an outer join
    produces is never true.

    ``is_not(True)`` rather than ``is_(False)`` on every ``deleted`` column: they are
    nullable and legacy rows may hold NULL, which the response layer coerces to
    ``False`` — so a NULL row must stay *visible* rather than silently vanish (the same
    v4 refinement the versions and revisions services document). ``is_training`` is
    ``NOT NULL`` with a false default, but is spelled the same way for consistency and
    because nothing here should depend on that constraint holding.

    ``include_deleted`` lifts all five deleted filters at once; ``updated_since``
    replaces them entirely (delta mode). Neither touches the group scoping or the
    ``is_training`` exclusion.
    """
    reference_revision = aliased(BibleRevision, name="reference_revision")
    reference_version = aliased(BibleVersion, name="reference_version")

    stmt = (
        select(Assessment)
        .join(BibleRevision, BibleRevision.id == Assessment.revision_id)
        .join(BibleVersion, BibleVersion.id == BibleRevision.bible_version_id)
        .outerjoin(reference_revision, reference_revision.id == Assessment.reference_id)
        .outerjoin(
            reference_version,
            reference_version.id == reference_revision.bible_version_id,
        )
        # Decision 3: training rows belong to #895's resource, not this one. Applied
        # on every branch, include_deleted and delta mode included.
        .where(Assessment.is_training.is_not(True))
    )

    if not user.is_admin:
        accessible = _accessible_version_ids(user)
        stmt = stmt.where(
            BibleVersion.id.in_(accessible),
            or_(
                Assessment.reference_id.is_(None),
                reference_version.id.in_(accessible),
            ),
        )

    if updated_since is not None:
        # Delta mode replaces the deleted filters entirely — a soft-delete is how a
        # mirror learns to drop the row.
        return stmt.where(Assessment.updated_at > as_naive_utc(updated_since))

    if not include_deleted:
        stmt = stmt.where(
            Assessment.deleted.is_not(True),
            BibleRevision.deleted.is_not(True),
            BibleVersion.deleted.is_not(True),
            or_(
                Assessment.reference_id.is_(None),
                and_(
                    reference_revision.deleted.is_not(True),
                    reference_version.deleted.is_not(True),
                ),
            ),
        )
    return stmt


async def list_assessments(
    db: AsyncSession,
    user: UserDB,
    *,
    limit: int,
    offset: int,
    ids: list[int] | None = None,
    revision_id: int | None = None,
    reference_id: int | None = None,
    assessment_type: str | None = None,
    include_deleted: bool = False,
    updated_since: datetime | None = None,
) -> tuple[list[Assessment], int, datetime | None]:
    """Return one page of assessments the user may see, the total match count, and the
    maximum ``updated_at`` across every matching row.

    The four filters are v3's, minus its unfiltered training rows: ``ids`` (repeated
    ``id=``), ``revision_id``, ``reference_id`` and ``assessment_type``. They are plain
    equality filters applied *after* the visibility predicate, so a filter can only ever
    narrow what the caller could already see — a ``revision_id`` they cannot reach
    yields an empty page rather than a leak. That is the deliberate difference from
    ``GET /v4/revisions``, which validates its ``version_id`` filter and 404s on an
    unusable one: there, the filter names the collection's *parent*, so a typo looks
    like "this version has no revisions" and is worth reporting; here, three of the four
    filters are ordinary attribute filters and one bad id in a batch of five should not
    fail the request (v3 documents the same silent-omission behavior for ``id``).

    ``include_deleted`` is honored only for admins, as on the versions and revisions
    lists; a non-admin never receives soft-deleted rows regardless of the flag.

    ``total`` counts *all* matching rows ignoring ``limit``/``offset`` (for the
    pagination envelope), computed from the same scoped query as the page. They are
    still two statements, so a concurrent write between them can cause the usual (rare)
    offset-pagination drift between ``total`` and ``len(items)``.

    ``updated_since`` narrows the page to the delta window and takes precedence over
    ``include_deleted``. The third return value is the raw input to the delta watermark
    — ``max(updated_at)`` over the whole matching set, aggregated in the *same*
    statement as ``total``, never over the returned page: rows are ordered by ``id``, so
    a page's maximum is not the window's. The router laps it via
    :func:`api_v4.delta.next_watermark`, which owns the lap so the three delta feeds
    cannot drift apart. ``None`` when nothing matched.

    Ordered by ``id`` ascending, like every other v4 list, rather than v3's
    ``requested_time`` descending. Offset pagination needs a total order on a column
    that cannot tie or move, and ``requested_time`` is nullable on legacy rows; a client
    that wants newest-first sorts the page it received or walks from the last page.
    """
    stmt = _visible_assessments_query(
        user,
        include_deleted=include_deleted and user.is_admin,
        updated_since=updated_since,
    )
    # `if ids` rather than `is not None`: an explicitly empty list is treated as "no id
    # filter" instead of compiling to `IN ()`, which would silently return nothing.
    if ids:
        stmt = stmt.where(Assessment.id.in_(ids))
    if revision_id is not None:
        stmt = stmt.where(Assessment.revision_id == revision_id)
    if reference_id is not None:
        stmt = stmt.where(Assessment.reference_id == reference_id)
    if assessment_type is not None:
        stmt = stmt.where(Assessment.type == assessment_type)

    scoped = stmt.subquery()
    total, max_updated_at = (
        await db.execute(
            select(func.count(), func.max(scoped.c.updated_at)).select_from(scoped)
        )
    ).one()
    result = await db.execute(stmt.order_by(Assessment.id).limit(limit).offset(offset))
    return list(result.scalars().all()), total, max_updated_at


async def get_assessment(
    db: AsyncSession, user: UserDB, assessment_id: int
) -> Assessment:
    """Return a single assessment the user may see, or raise :class:`AssessmentNotFound`.

    Visibility-scoped, so a caller asking for an assessment they cannot reach gets the
    same signal as for a truly missing id. New in v4 — v3 had no single-assessment read
    at all; a client polled by listing.
    """
    stmt = _visible_assessments_query(user, include_deleted=False).where(
        Assessment.id == assessment_id
    )
    assessment = (await db.execute(stmt)).scalars().first()
    if assessment is None:
        raise AssessmentNotFound(assessment_id)
    return assessment


async def _get_assessment_for_write(
    db: AsyncSession, user: UserDB, assessment_id: int
) -> Assessment:
    """Load an assessment for a write, enforcing the owner-or-admin gate.

    Two steps in this order, which *is* the decision-4(a) fix: resolve the row through
    the group-scoped predicate first — a caller who cannot reach it gets
    :class:`AssessmentNotFound` — and only then check ownership, so
    :class:`AssessmentAccessForbidden` is reachable exclusively for rows whose existence
    the caller has already established. v3 looked the row up with no permission filter
    and answered 403, which made its status code an existence oracle.

    ``include_deleted=True`` is passed deliberately, and it is the one place this gate
    is *wider* than the read predicate: see decision 4(a) in the module docstring for
    why an already-deleted row and a row whose revision was deleted both have to stay
    writable, and why the disclosure that widening implies is nil.

    ``owner_id`` is nullable, so on a legacy row with no owner ``is_owner`` is false for
    every caller and only an admin passes — decision 4(b), documented on the endpoint
    because it otherwise reads as an authorization bug.
    """
    stmt = _visible_assessments_query(user, include_deleted=True).where(
        Assessment.id == assessment_id
    )
    assessment = (await db.execute(stmt)).scalars().first()
    if assessment is None:
        raise AssessmentNotFound(assessment_id)
    if not user.is_admin and assessment.owner_id != user.id:
        raise AssessmentAccessForbidden(assessment_id)
    return assessment


async def soft_delete_assessment(
    db: AsyncSession, user: UserDB, assessment_id: int
) -> Assessment:
    """Soft-delete an assessment (owner or admin only). Mirrors v3 ``DELETE /assessment``.

    Authorized by :func:`_get_assessment_for_write`. Idempotent: re-deleting an
    already-soft-deleted row is allowed and writes the flag again. The result rows are
    left in place, exactly as v3 leaves them; this flips a flag, it does not reclaim
    storage.

    A queued or running assessment can be deleted, and doing so does **not** stop the
    Modal run — decision 4(c), stated on the endpoint. Note the run's own callbacks keep
    writing to the soft-deleted row, which is harmless: the row is hidden from reads,
    and its ``updated_at`` moving again simply re-delivers a row a mirror has already
    dropped.
    """
    assessment = await _get_assessment_for_write(db, user, assessment_id)

    try:
        assessment.deleted = True
        # date.today() rather than a full timestamp: it is what v3's delete and both v4
        # sibling services write, and the column is not on the wire. Not worth an
        # unexplained difference between the three.
        assessment.deletedAt = date.today()
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return assessment

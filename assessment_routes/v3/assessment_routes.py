__version__ = "v3"
# Standard library imports
import hashlib
import json
import os
import socket
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import fastapi
import modal
from dotenv import load_dotenv

# Third party imports
from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import JSON, BigInteger, bindparam, or_, select, text, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from assessment_routes.v3.alignment_filters import eflomal_method_clause
from database.dependencies import get_db
from database.models import Assessment, BibleRevision, BibleVersionAccess
from database.models import UserDB as UserModel
from database.models import UserGroup

# Local application imports
from models import (
    ASSESSMENT_TERMINAL_STATUSES,
    ASSESSMENT_VALID_TRANSITIONS,
    AssessmentIn,
    AssessmentOut,
    AssessmentStatus,
    AssessmentStatusUpdate,
)
from security_routes.auth_routes import get_current_user
from utils.logging_config import setup_logger

load_dotenv()

STALE_ASSESSMENT_HOURS = 2

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)


router = fastapi.APIRouter()


# Namespace tag for the per-quadruple advisory lock keyspace so we don't
# collide with any future pg_advisory_*lock callers (in particular the
# training-job dup lock in train_routes.v3, which has its own namespace).
# Bumping the suffix only matters once every running instance has been
# redeployed onto the new suffix; during a rolling deploy two instances with
# different namespaces wouldn't serialize against each other, so don't change
# this casually.
_ASSESS_DUP_LOCK_NS = "assessment_dup_v1"


def _canonicalize_kwargs(kwargs: Optional[Dict[str, Any]]) -> str:
    """Canonical JSON representation of an Assessment.kwargs payload for use
    as part of the advisory-lock key. Empty-dict normalizes to None to match
    the dup-check semantics (we treat `{}` as "no kwargs" at the request
    layer). Keys are sorted so logically-equal kwargs hash to the same key
    regardless of insertion order."""
    if not kwargs:
        return "null"
    return json.dumps(kwargs, sort_keys=True, separators=(",", ":"))


def _assess_dup_lock_key(
    revision_id: int,
    reference_id: Optional[int],
    assessment_type: str,
    kwargs_canonical: str,
) -> int:
    """Stable signed 64-bit key for a Postgres transaction-scoped advisory
    lock that serializes concurrent add_assessment() calls on the same
    (revision_id, reference_id, type, kwargs) quadruple (#780).

    Mirrors the training-job dup-lock helper (#722 / PR #771) — copied
    rather than shared so this PR doesn't depend on that one landing
    first. Extracting a shared util is tracked as a follow-up.

    pg_advisory_xact_lock(bigint) wants a signed int8. We derive a
    deterministic 64-bit value from a SHA-1 of the namespace + quadruple
    and fold it into the signed-int8 range. SHA-1 (rather than Python's
    built-in hash()) so the key is stable across processes / interpreter
    restarts. Collisions are vanishingly unlikely for realistic
    cardinality; a collision would merely serialize two unrelated
    quadruples (a small perf hit, not a correctness bug).
    """
    payload = (
        f"{_ASSESS_DUP_LOCK_NS}|"
        f"{revision_id}|{reference_id if reference_id is not None else ''}|"
        f"{assessment_type}|{kwargs_canonical}"
    ).encode("utf-8")
    digest = hashlib.sha1(payload).digest()
    unsigned = int.from_bytes(digest[:8], byteorder="big", signed=False)
    return unsigned - (1 << 63)


async def _acquire_assess_dup_lock(
    db: AsyncSession,
    revision_id: int,
    reference_id: Optional[int],
    assessment_type: str,
    kwargs_canonical: str,
) -> None:
    """Take a transaction-scoped Postgres advisory lock that serializes
    concurrent add_assessment() requests on the same
    (revision_id, reference_id, type, kwargs) quadruple.

    Released automatically when the surrounding transaction commits or
    rolls back, so we don't have to manage its lifetime. Pairs with the
    duplicate-check SELECT below to make check-then-insert atomic (#780).
    The lock is taken even for admin callers — the per-quadruple lock is
    cheap and prevents the admin/admin race where two parallel admin
    POSTs could both INSERT (admin bypass applies only to the duplicate
    *check*, not to the lock).
    """
    key = _assess_dup_lock_key(
        revision_id, reference_id, assessment_type, kwargs_canonical
    )
    await db.execute(
        text("SELECT pg_advisory_xact_lock(:key)").bindparams(
            bindparam("key", value=key, type_=BigInteger())
        )
    )


def _apply_filters(stmt, ids, revision_id, reference_id, type_):
    if ids is not None:
        stmt = stmt.where(Assessment.id.in_(ids))
    if revision_id is not None:
        stmt = stmt.where(Assessment.revision_id == revision_id)
    if reference_id is not None:
        stmt = stmt.where(Assessment.reference_id == reference_id)
    if type_ is not None:
        stmt = stmt.where(Assessment.type == type_)
    return stmt


@router.get("/assessment", response_model=List[AssessmentOut])
async def get_assessments(
    ids: Optional[List[int]] = Query(None, alias="id"),
    revision_id: Optional[int] = None,
    reference_id: Optional[int] = None,
    type: Optional[str] = None,
    current_user: UserModel = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns a list of assessments the current user is authorized to access.

    Optional query parameters:
    - id: Filter by one or more assessment IDs (repeated param, e.g. ?id=1&id=2).
      IDs that do not exist or are not accessible to the current user are silently
      omitted; a partial result is not an error.
    - revision_id: Filter assessments by revision ID
    - reference_id: Filter assessments by reference ID
    - type: Filter assessments by assessment type

    Currently supported assessment types are:

    - semantic-similarity (requires reference)
    - sentence-length
    - word-alignment (requires reference)
    - ngrams
    - tfidf
    - text-lengths (requires reference)
    - agent-critique (requires reference)


    Returns:
    Fields(AssessmentOut):
    - id: int
    Description: The unique identifier for the assessment.
    - revision_id: int
    Description: The unique identifier for the revision.
    - reference_id: Optional[int] = None
    Description: The unique identifier for the reference revision.
    - type: AssessmentType
    Description: The type of assessment to be run.
    - status: str
    Description: The status of the assessment. (queued, failed, finished)
    - requested_time: datetime.datetime
    Description: The time the assessment was requested.
    - start_time: datetime.datetime
    Description: The time the assessment was started.
    - end_time: datetime.datetime
    Description: The time the assessment was completed.
    - owner_id: int
    Description: The unique identifier for the owner of the assessment.

    """

    if current_user.is_admin:
        # Admin users can access all assessments
        stmt = select(Assessment).where(Assessment.deleted.is_(False))

        stmt = _apply_filters(stmt, ids, revision_id, reference_id, type)

        result = await db.execute(stmt)
        assessments = result.scalars().all()
    else:
        # Fetch the groups the user belongs to
        stmt = select(UserGroup.group_id).where(UserGroup.user_id == current_user.id)
        result = await db.execute(stmt)
        user_group_ids = [group_id[0] for group_id in result.all()]

        # Get versions the user has access to through their access to groups
        stmt = select(BibleVersionAccess.bible_version_id).where(
            BibleVersionAccess.group_id.in_(user_group_ids)
        )
        result = await db.execute(stmt)
        version_ids = [version_id[0] for version_id in result.all()]
        # Get assessments that the user has access to through their access to revision and reference

        ReferenceRevision = aliased(BibleRevision)

        # Explanation query:
        # Select all assessments where the Bible version of the revision is accessible by the user
        # (The revision of the assessment will always exist)
        # Then we make an outer join with the reference revision, in case the assessment has a reference, it brings it, otherwise it brings None
        # Filtering:
        # - The Bible version of the revision is accessible by the user
        # AND
        # - Either the assessment has no reference, or it it has, the Bible version of the reference is accessible by the user
        stmt = (
            select(Assessment)
            .distinct(Assessment.id)
            .join(BibleRevision, BibleRevision.id == Assessment.revision_id)
            .outerjoin(
                ReferenceRevision, ReferenceRevision.id == Assessment.reference_id
            )
            .filter(
                Assessment.deleted.is_not(True),
                BibleRevision.bible_version_id.in_(version_ids),
                or_(
                    Assessment.reference_id.is_(None),
                    ReferenceRevision.bible_version_id.in_(version_ids),
                ),
            )
        )

        stmt = _apply_filters(stmt, ids, revision_id, reference_id, type)

        result = await db.execute(stmt)
        assessments = result.scalars().all()

    # Convert SQLAlchemy models to Pydantic models
    assessment_data = [
        AssessmentOut.model_validate(assessment) for assessment in assessments
    ]
    assessment_data = sorted(
        assessment_data,
        key=lambda x: x.requested_time or datetime.min,
        reverse=True,
    )

    return assessment_data


# Helper function to call assessment runner
async def call_assessment_runner(
    assessment: AssessmentIn,
    return_all_results: bool,
    modal_env: str,
    source_version_id: Optional[int] = None,
    target_version_id: Optional[int] = None,
    db: Optional[AsyncSession] = None,
):
    """Spawn the Modal assessment runner for an existing Assessment row.

    Per-row idempotency guard (#780): if a `db` session is provided, we
    take a `SELECT ... FOR UPDATE` on the assessment row and verify it's
    still in `queued` status before spawning the Modal worker.  If the
    row is already `running` / `finished` / `failed`, we refuse to
    re-spawn and raise HTTPException(409). On success we atomically
    transition the row to `running` in the same transaction, closing the
    window between "INSERT queued" and "worker picks up and sets running"
    that allowed assessment_id=21288 to be dispatched twice.

    `db` is optional for backwards compatibility with the existing API
    POST flow (which always passes it); other callers that have already
    transitioned the row themselves can omit it.
    """
    if db is not None and assessment.id is not None:
        row = await db.execute(
            select(Assessment).where(Assessment.id == assessment.id).with_for_update()
        )
        a_row = row.scalar_one_or_none()
        if a_row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Assessment {assessment.id} not found",
            )
        if a_row.status != AssessmentStatus.queued.value:
            requested = (
                a_row.requested_time.isoformat()
                if a_row.requested_time is not None
                else None
            )
            logger.warning(
                "Refusing to re-spawn assessment not in queued status",
                extra={
                    "assessment_id": a_row.id,
                    "status": a_row.status,
                    "requested_time": requested,
                },
            )
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "detail": "Assessment in progress",
                    "existing_id": a_row.id,
                    "status": a_row.status,
                    "requested_time": requested,
                },
            )
        # Transition queued → running inside the same transaction so the
        # row reflects "dispatched" before we hand off to Modal. If the
        # spawn raises below, the caller rolls back and the row reverts
        # to queued. Use datetime.utcnow() to match the existing pattern
        # in update_assessment_status.
        a_row.status = AssessmentStatus.running.value
        a_row.start_time = datetime.utcnow()
        await db.flush()

    logger.info(
        "Calling Modal runner",
        extra={
            "modal_env": modal_env,
            "assessment_id": assessment.id,
            "revision_id": assessment.revision_id,
            "reference_id": assessment.reference_id,
            "assessment_type": assessment.type,
            "return_all_results": return_all_results,
        },
    )

    f = modal.Function.from_name(
        "runner", "run_assessment_runner", environment_name=modal_env
    )
    config = assessment.model_dump()
    # Backward compat: copy vref range from kwargs to top-level so the
    # runner (separate repo) can read them at either location.
    if config.get("kwargs"):
        for key in ("first_vref", "last_vref"):
            if key in config["kwargs"]:
                config[key] = config["kwargs"][key]
    config["source_version_id"] = source_version_id
    config["target_version_id"] = target_version_id
    config["return_all_results"] = return_all_results
    await f.spawn.aio(config, os.getenv("AQUA_DB", ""))


@router.post("/assessment", response_model=List[AssessmentOut])
async def add_assessment(
    a: AssessmentIn = Depends(),
    extra_kwargs: Optional[str] = Query(
        None,
        description="JSON-encoded dict of extra keyword arguments to pass to the assessment function",
    ),
    use_eflomal: Optional[bool] = Query(
        None,
        description="Word-alignment runner selector. true runs eflomal, false runs fastalign. When omitted, eflomal runs by default unless use_eflomal was injected via extra_kwargs, in which case the injected value is honored — the typed query param always wins over an injected value. Source/target version IDs are derived from reference_id/revision_id.",
    ),
    transcribed_audio: Optional[bool] = Query(
        None,
        description="agent-critique only. true tells the agent the draft is a transcription of recorded audio (ASR), so the back-translation/critique prompts expect surface transcription noise while still flagging genuine content differences. Default off (unset). The typed query param wins over a transcribed_audio value injected via extra_kwargs.",
    ),
    force: bool = Query(
        False,
        description="Force rerun even if a completed assessment already exists",
    ),
    modal_env: Optional[str] = Query(
        None,
        description="Modal environment to run the assessment in (e.g. 'main' or 'dev'). Defaults to server MODAL_ENV.",
    ),
    return_all_results: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Requests an assessment to be run on a revision and (where required) a reference revision.

    Currently supported assessment types are:
    - semantic-similarity (requires reference)
    - sentence-length
    - word-alignment (requires reference; runs eflomal-based alignment by default.
      Pass `use_eflomal=false` to run fastalign instead. Source/target version IDs
      are derived from reference_id and revision_id respectively.)
    - ngrams
    - tfidf
    - text-lengths
    - agent-critique (requires reference; pass `transcribed_audio=true` when the
      draft is a transcription of recorded audio so the agent expects ASR
      surface noise. Default off.)

    For those assessments that require a reference, the reference_id should be the id of the revision with which the revision will be compared.

    Optional `extra_kwargs` query parameter accepts a JSON-encoded dict of extra keyword
    arguments to pass through to the assessment function (e.g., `{"top_k": 5}`). Values
    must be scalar types (str, int, float, bool, null). Max 20 keys.

    Add an assessment entry. For regular users, an entry is added for each group they are part of.
    For admin users, the entry is not linked to any specific group.
    """
    if modal_env is not None and not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admin users can specify modal_env.",
        )

    if (
        a.type in ["semantic-similarity", "word-alignment", "agent-critique"]
        and a.reference_id is None
    ):
        raise HTTPException(
            status_code=400, detail=f"Assessment type {a.type} requires a reference_id."
        )

    # Parse extra_kwargs JSON string into a validated dict
    parsed_kwargs = None
    if extra_kwargs is not None:
        try:
            parsed_kwargs = json.loads(extra_kwargs)
        except (json.JSONDecodeError, TypeError) as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid kwargs JSON: {e}"
            ) from e
        if not isinstance(parsed_kwargs, dict):
            raise HTTPException(status_code=400, detail="kwargs must be a JSON object")
        # Validate through the model's field_validator explicitly
        try:
            parsed_kwargs = AssessmentIn.validate_kwargs(parsed_kwargs)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        # Treat an empty kwargs object the same as no kwargs supplied
        if not parsed_kwargs:
            parsed_kwargs = None
        a.kwargs = parsed_kwargs

    # Strict-bool check first: a caller could route around the typed query
    # param by injecting `use_eflomal: 1` (or "true", etc.) via extra_kwargs.
    # The dedup SQL uses JSONB containment which is strictly typed, so a
    # truthy-but-non-bool value would silently bypass dedup. Reject it before
    # the eflomal default gets folded in below — otherwise the fold would
    # overwrite the bad value and mask this error.
    injected_eflomal = a.kwargs.get("use_eflomal") if a.kwargs else None
    if injected_eflomal is not None and not isinstance(injected_eflomal, bool):
        raise HTTPException(
            status_code=400,
            detail="use_eflomal must be a boolean.",
        )

    # Resolve the effective runner. Eflomal is the default for word-alignment;
    # callers opt out with use_eflomal=false. The typed query param wins over
    # any value injected via extra_kwargs. For non-word-alignment types eflomal
    # is never implied by default; an explicit eflomal request is rejected just
    # below.
    if use_eflomal is None:
        is_eflomal = (
            injected_eflomal
            if injected_eflomal is not None
            else a.type == "word-alignment"
        )
    else:
        is_eflomal = use_eflomal
    if is_eflomal and a.type != "word-alignment":
        raise HTTPException(
            status_code=400,
            detail="use_eflomal is only valid for word-alignment assessments.",
        )

    # Fold the resolved runner into kwargs so it reaches Modal, the create-time
    # dedup check, and the read endpoints. Eflomal is stored as
    # {"use_eflomal": true}; fastalign stores no flag. On an explicit opt-out we
    # strip any injected flag so the row reads as fastalign and dedup stays
    # correct.
    if is_eflomal:
        combined_kwargs = dict(a.kwargs or {})
        combined_kwargs["use_eflomal"] = True
        try:
            combined_kwargs = AssessmentIn.validate_kwargs(combined_kwargs)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        a.kwargs = combined_kwargs
        parsed_kwargs = combined_kwargs
    elif a.kwargs and "use_eflomal" in a.kwargs:
        stripped = {k: v for k, v in a.kwargs.items() if k != "use_eflomal"}
        a.kwargs = stripped or None
        parsed_kwargs = a.kwargs

    # Strict-bool check, mirroring use_eflomal: a caller could route around the
    # typed query param by injecting `transcribed_audio: 1` (or "true", etc.)
    # via extra_kwargs, which would silently bypass the JSONB-strict dedup
    # filter. Reject a truthy-but-non-bool value before the flag gets folded in.
    injected_transcribed = a.kwargs.get("transcribed_audio") if a.kwargs else None
    if injected_transcribed is not None and not isinstance(injected_transcribed, bool):
        raise HTTPException(
            status_code=400,
            detail="transcribed_audio must be a boolean.",
        )

    # Resolve the effective flag. Default is off; the typed query param wins
    # over any value injected via extra_kwargs. Only meaningful for
    # agent-critique — an explicit request on any other type is rejected.
    if transcribed_audio is None:
        is_transcribed = injected_transcribed
    else:
        is_transcribed = transcribed_audio
    if is_transcribed and a.type != "agent-critique":
        raise HTTPException(
            status_code=400,
            detail="transcribed_audio is only valid for agent-critique assessments.",
        )

    # Fold the resolved flag into kwargs so it reaches Modal, the create-time
    # dedup check, and the read endpoints. Stored as {"transcribed_audio": true}
    # when on; on an explicit opt-out we strip any injected flag so the row
    # reads as off and dedup stays correct.
    if is_transcribed:
        combined_kwargs = dict(a.kwargs or {})
        combined_kwargs["transcribed_audio"] = True
        try:
            combined_kwargs = AssessmentIn.validate_kwargs(combined_kwargs)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        a.kwargs = combined_kwargs
        parsed_kwargs = combined_kwargs
    elif a.kwargs and "transcribed_audio" in a.kwargs:
        stripped = {k: v for k, v in a.kwargs.items() if k != "transcribed_audio"}
        a.kwargs = stripped or None
        parsed_kwargs = a.kwargs

    # Derive source/target version IDs from the reference and revision rows
    # for every assessment type (not just eflomal). Downstream consumers
    # (agent runner, eflomal pipeline, version-id-keyed artifact stores) all
    # require these fields. Mapping:
    #   source_version_id ← bible_version_id of reference_id
    #   target_version_id ← bible_version_id of revision_id
    revision = await db.get(BibleRevision, a.revision_id)
    if revision is None or revision.deleted:
        raise HTTPException(status_code=404, detail="revision_id does not exist.")
    reference = (
        await db.get(BibleRevision, a.reference_id)
        if a.reference_id is not None
        else None
    )
    if a.reference_id is not None and (reference is None or reference.deleted):
        raise HTTPException(status_code=404, detail="reference_id does not exist.")
    target_version_id: Optional[int] = revision.bible_version_id
    source_version_id: Optional[int] = (
        reference.bible_version_id if reference is not None else None
    )

    # Check for already-completed assessment (force=true bypasses this)
    if not force:
        completed_stmt = (
            select(Assessment)
            .where(
                Assessment.revision_id == a.revision_id,
                Assessment.type == a.type,
                Assessment.status == "finished",
                Assessment.deleted.is_not(True),
            )
            .order_by(Assessment.end_time.desc())
            .limit(1)
        )
        if a.reference_id is not None:
            completed_stmt = completed_stmt.where(
                Assessment.reference_id == a.reference_id
            )
        else:
            completed_stmt = completed_stmt.where(Assessment.reference_id.is_(None))
        # Distinguish eflomal from regular word-alignment. Shared with the read
        # endpoints via eflomal_method_clause so create-dedup and reads stay in
        # lock-step. Only applies to word-alignment assessments (or explicit
        # eflomal requests); other types carry no runner distinction.
        if is_eflomal or a.type == "word-alignment":
            completed_stmt = completed_stmt.where(eflomal_method_clause(is_eflomal))
        # Distinguish by verse range
        if parsed_kwargs and parsed_kwargs.get("first_vref"):
            completed_stmt = completed_stmt.where(
                Assessment.kwargs.op("@>")({"first_vref": parsed_kwargs["first_vref"]})
            )
        else:
            completed_stmt = completed_stmt.where(
                or_(
                    Assessment.kwargs.is_(None),
                    ~Assessment.kwargs.has_key("first_vref"),
                )
            )
        if parsed_kwargs and parsed_kwargs.get("last_vref"):
            completed_stmt = completed_stmt.where(
                Assessment.kwargs.op("@>")({"last_vref": parsed_kwargs["last_vref"]})
            )
        else:
            completed_stmt = completed_stmt.where(
                or_(
                    Assessment.kwargs.is_(None),
                    ~Assessment.kwargs.has_key("last_vref"),
                )
            )
        result = await db.execute(completed_stmt)
        existing = result.scalars().first()
        if existing is not None:
            logger.info(
                "Blocked duplicate of finished assessment",
                extra={
                    "existing_id": existing.id,
                    "user_id": current_user.id,
                    "revision_id": a.revision_id,
                    "type": a.type,
                },
            )
            raise HTTPException(
                status_code=409,
                detail=f"Assessment already completed (id={existing.id}). Use force=true to rerun.",
            )

    # Serialize concurrent POSTs on the same (revision, reference, type,
    # kwargs) quadruple with a transaction-scoped Postgres advisory lock.
    # Without this, two concurrent requests both pass the duplicate-check
    # SELECT below and both INSERT, leaving two queued assessments that
    # each dispatch a Modal runner (#780, sibling of training-job #722).
    # The lock is held until commit/rollback at the bottom of this
    # function, so the dup-check + insert is atomic with respect to any
    # other concurrent request on the same quadruple. The lock is taken
    # even for admins — admin bypass is preserved for the *check*, but
    # the lock still serializes per-quadruple so two parallel admin
    # POSTs can't both INSERT in parallel.
    kwargs_canonical = _canonicalize_kwargs(parsed_kwargs)
    await _acquire_assess_dup_lock(
        db, a.revision_id, a.reference_id, a.type, kwargs_canonical
    )

    # Check for duplicate in-progress assessment (admins can bypass)
    if not current_user.is_admin:
        stale_cutoff = datetime.now() - timedelta(hours=STALE_ASSESSMENT_HOURS)
        stmt = (
            select(Assessment.id)
            .where(
                Assessment.revision_id == a.revision_id,
                Assessment.type == a.type,
                Assessment.status.notin_(
                    [s.value for s in ASSESSMENT_TERMINAL_STATUSES]
                ),
                Assessment.deleted.is_not(True),
                Assessment.requested_time > stale_cutoff,
            )
            .limit(1)
        )
        if a.reference_id is not None:
            stmt = stmt.where(Assessment.reference_id == a.reference_id)
        else:
            stmt = stmt.where(Assessment.reference_id.is_(None))
        if parsed_kwargs is not None:
            stmt = stmt.where(Assessment.kwargs == parsed_kwargs)
        else:
            # New rows persist Python None as the JSON null literal (matched
            # by `== JSON.NULL`). The `is_(None)` arm catches legacy rows
            # stored as SQL NULL, and `== {}` catches legacy rows where an
            # empty `extra_kwargs` was persisted as a JSONB empty object
            # (we now normalize that to None on the request side).
            stmt = stmt.where(
                or_(
                    Assessment.kwargs.is_(None),
                    Assessment.kwargs == JSON.NULL,
                    Assessment.kwargs == {},
                )
            )
        result = await db.execute(stmt)
        existing_id = result.scalars().first()
        if existing_id is not None:
            raise HTTPException(
                status_code=409,
                detail=f"Duplicate assessment already in progress (id={existing_id})",
            )

    assessment = Assessment(
        revision_id=a.revision_id,
        reference_id=a.reference_id,
        type=a.type,
        status="queued",
        requested_time=datetime.now(),
        owner_id=current_user.id,
        kwargs=parsed_kwargs,
    )

    db.add(assessment)
    # Commit the INSERT (in `queued` state) before spawning so the Modal
    # worker can PATCH /v3/assessment/{id}/status as soon as it picks
    # the job up — without this, a fast worker could try to mutate a
    # row that hasn't been committed yet. Releasing the advisory lock
    # here is safe: by this point the dup-check + INSERT pair is atomic
    # under the lock, so any concurrent waiter on the same quadruple
    # will see this row in its dup-check after the lock releases.
    # Mirrors the train_routes dispatch pattern (#722 / PR #771).
    await db.commit()
    await db.refresh(assessment)
    a.id = assessment.id

    # Resolve Modal environment once at the route level
    resolved_modal_env = modal_env or os.getenv("MODAL_ENV", "main")

    # Dispatch to Modal runner (fire-and-forget via spawn). Pass `db` so
    # the runner helper takes a SELECT ... FOR UPDATE on the row and
    # transitions queued→running atomically before the spawn — closes
    # the window that let assessment_id=21288 be dispatched twice (#780).
    # The FOR UPDATE serializes any concurrent call_assessment_runner
    # invocations on the same id: the first transitions to `running`
    # and spawns; the second sees `running` and 409s without spawning.
    try:
        await call_assessment_runner(
            a,
            return_all_results,
            resolved_modal_env,
            source_version_id=source_version_id,
            target_version_id=target_version_id,
            db=db,
        )
    except HTTPException:
        # Re-spawn refused (e.g., row is no longer queued). Roll back
        # the FOR UPDATE txn so we don't leave anything pending, and
        # propagate the HTTP error to the caller verbatim.
        await db.rollback()
        raise
    except Exception as e:
        logger.error(
            "Modal runner dispatch failed",
            exc_info=True,
            extra={
                "assessment_id": assessment.id,
                "modal_env": resolved_modal_env,
                "error_type": type(e).__name__,
            },
        )
        # Mark the row as failed in a fresh transaction so it reflects
        # reality (the runner never got a chance to advance it past
        # whatever state we left it in). Mirrors the train_routes
        # failure-handling pattern.
        try:
            await db.rollback()
            assessment.status = AssessmentStatus.failed.value
            assessment.status_detail = f"dispatch_failed: {type(e).__name__}: {e}"
            assessment.end_time = datetime.utcnow()
            await db.commit()
        except SQLAlchemyError as cleanup_err:
            await db.rollback()
            logger.error(
                f"Failed to mark assessment {assessment.id} as failed "
                f"after runner error: {cleanup_err}"
            )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Assessment runner service is unavailable or failed.",
        ) from e

    # Commit the queued → running transition that call_assessment_runner
    # performed under FOR UPDATE.
    await db.commit()
    await db.refresh(assessment)

    return [AssessmentOut.model_validate(assessment)]


@router.patch("/assessment/{assessment_id}/status", response_model=AssessmentOut)
async def update_assessment_status(
    assessment_id: int,
    update: AssessmentStatusUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Runner callback to update assessment status.

    Auth: admin, assessment owner, or any user with group access to the
    assessment's bible version.  This mirrors the training PATCH pattern.
    """
    result = await db.execute(select(Assessment).where(Assessment.id == assessment_id))
    assessment = result.scalars().first()
    if not assessment or assessment.deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Assessment not found",
        )

    if not current_user.is_admin and assessment.owner_id != current_user.id:
        revision = await db.get(BibleRevision, assessment.revision_id)
        if not revision:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Assessment revision not found",
            )
        version_access = await db.execute(
            select(BibleVersionAccess.bible_version_id).where(
                BibleVersionAccess.group_id.in_(
                    select(UserGroup.group_id).where(
                        UserGroup.user_id == current_user.id
                    )
                )
            )
        )
        accessible_version_ids = {row[0] for row in version_access.all()}
        if revision.bible_version_id not in accessible_version_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not authorized to update this assessment",
            )

    if assessment.status in ASSESSMENT_TERMINAL_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Assessment is already in terminal status '{assessment.status}'",
        )

    allowed_next = ASSESSMENT_VALID_TRANSITIONS.get(assessment.status, set())
    if update.status not in allowed_next:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid transition from '{assessment.status}' to '{update.status}'",
        )

    assessment.status = update.status
    if update.status_detail is not None:
        assessment.status_detail = update.status_detail
    if update.percent_complete is not None:
        assessment.percent_complete = update.percent_complete

    if assessment.start_time is None and update.status != "queued":
        assessment.start_time = datetime.utcnow()

    if update.status in ASSESSMENT_TERMINAL_STATUSES:
        assessment.end_time = datetime.utcnow()

    await db.commit()
    await db.refresh(assessment)
    return AssessmentOut.model_validate(assessment)


@router.post("/assessment/{assessment_id}/increment-attempts")
async def increment_assessment_attempts(
    assessment_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """Atomically increment ``attempt_count`` for an assessment and return
    the new value.

    Called by the Modal runner's lifecycle helper at the start of each
    attempt (after the initial `running` PATCH, so terminal-409 retries
    don't bump the counter for no-ops). The lifecycle uses the returned
    count to decide whether to PATCH ``failed`` on exception (only once
    ``attempt_count >= max_attempts``).

    The UPDATE ... RETURNING runs as a single atomic statement so two
    concurrent retries on the same row can't observe the same
    pre-increment value — Postgres serializes the writes and each call
    sees its own post-increment count.

    Auth: admin, assessment owner, or any user with group access to the
    assessment's bible version. Mirrors ``update_assessment_status``.
    """
    result = await db.execute(select(Assessment).where(Assessment.id == assessment_id))
    assessment = result.scalars().first()
    if not assessment or assessment.deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Assessment not found",
        )

    if not current_user.is_admin and assessment.owner_id != current_user.id:
        revision = await db.get(BibleRevision, assessment.revision_id)
        if not revision:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Assessment revision not found",
            )
        version_access = await db.execute(
            select(BibleVersionAccess.bible_version_id).where(
                BibleVersionAccess.group_id.in_(
                    select(UserGroup.group_id).where(
                        UserGroup.user_id == current_user.id
                    )
                )
            )
        )
        accessible_version_ids = {row[0] for row in version_access.all()}
        if revision.bible_version_id not in accessible_version_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not authorized to update this assessment",
            )

    # Filter on `deleted is not True` in the UPDATE so a race with a
    # concurrent DELETE between the SELECT above and this UPDATE doesn't
    # increment a freshly-deleted row. If the row was deleted in that
    # window, `scalar_one_or_none()` returns None and we 404.
    stmt = (
        update(Assessment)
        .where(Assessment.id == assessment_id, Assessment.deleted.is_not(True))
        .values(attempt_count=Assessment.attempt_count + 1)
        .returning(Assessment.attempt_count)
    )
    res = await db.execute(stmt)
    new_count = res.scalar_one_or_none()
    if new_count is None:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Assessment not found",
        )
    await db.commit()
    return {"attempt_count": new_count}


@router.delete("/assessment")
async def delete_assessment(
    assessment_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user),
):
    """
    Deletes an assessment if the user is authorized.

    Input:
    - assessment_id: int
    Description: The unique identifier for the assessment.
    """

    # Check if the assessment exists and fetch it asynchronously
    result = await db.execute(select(Assessment).filter(Assessment.id == assessment_id))
    assessment = result.scalars().first()
    if not assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Assessment not found."
        )

    # Check if the user is owner of the assesment or if it is admin
    is_owner = assessment.owner_id == current_user.id

    if is_owner or current_user.is_admin:
        # Mark the assessment as deleted instead of actually removing it
        assessment.deleted = True
        assessment.deletedAt = date.today()
        await db.commit()
        return {"detail": f"Assessment {assessment_id} deleted successfully"}

    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User not authorized to delete this assessment.",
        )

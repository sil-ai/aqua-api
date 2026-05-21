__version__ = "v3"

import socket
from datetime import datetime, timedelta, timezone
from typing import List

import fastapi
from fastapi import Depends, Query
from pydantic import BaseModel
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import Assessment
from database.models import UserDB as UserModel
from models import ASSESSMENT_TERMINAL_STATUSES, AssessmentStatus
from security_routes.admin_routes import get_current_admin
from utils.logging_config import setup_logger

DEFAULT_TIMEOUT_HOURS = 24
MIN_TIMEOUT_HOURS = 2
MAX_TIMEOUT_HOURS = 87600  # ten years; guard against timedelta overflow
MAX_RETURNED_SWEPT_IDS = 1000
TIMEOUT_STATUS_DETAIL = "Marked failed by upstream timeout sweep"
NON_TERMINAL_STATUS_VALUES = [
    s.value for s in AssessmentStatus if s not in ASSESSMENT_TERMINAL_STATUSES
]

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()


class TimeoutSweepResult(BaseModel):
    swept_count: int
    swept_ids: List[int]
    truncated: bool
    cutoff: datetime
    hours: int


async def sweep_stuck_assessments(
    db: AsyncSession, hours: int = DEFAULT_TIMEOUT_HOURS
) -> TimeoutSweepResult:
    # Use tz-aware UTC so the cutoff comparison stays correct regardless of
    # the server's local timezone. requested_time is also written as
    # tz-aware UTC (see assessment_routes.v3.assessment_routes); pre-existing
    # naive rows are interpreted by Postgres as the session's timezone when
    # compared against a tz-aware bound, which is harmless here.
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    stmt = (
        update(Assessment)
        .where(
            Assessment.status.in_(NON_TERMINAL_STATUS_VALUES),
            Assessment.deleted.is_not(True),
            Assessment.requested_time < cutoff,
        )
        .values(
            status=AssessmentStatus.failed.value,
            status_detail=TIMEOUT_STATUS_DETAIL,
            end_time=now,
        )
        .returning(Assessment.id)
        .execution_options(synchronize_session=False)
    )
    result = await db.execute(stmt)
    swept_ids = [row[0] for row in result.all()]
    await db.commit()

    truncated = len(swept_ids) > MAX_RETURNED_SWEPT_IDS
    returned_ids = swept_ids[:MAX_RETURNED_SWEPT_IDS]

    logger.info(
        "Timeout sweep completed",
        extra={
            "swept_count": len(swept_ids),
            "hours": hours,
            "cutoff": cutoff.isoformat(),
            "swept_ids_sample": swept_ids[:50],
        },
    )

    return TimeoutSweepResult(
        swept_count=len(swept_ids),
        swept_ids=returned_ids,
        truncated=truncated,
        cutoff=cutoff,
        hours=hours,
    )


@router.post("/assessment/timeout-sweep", response_model=TimeoutSweepResult)
async def timeout_sweep(
    hours: int = Query(
        DEFAULT_TIMEOUT_HOURS,
        ge=MIN_TIMEOUT_HOURS,
        le=MAX_TIMEOUT_HOURS,
        description=(
            "Mark assessments as failed if they are still queued or running and "
            "their requested_time is older than this many hours."
        ),
    ),
    db: AsyncSession = Depends(get_db),
    _: UserModel = Depends(get_current_admin),
) -> TimeoutSweepResult:
    """Admin-only sweep that marks stuck non-terminal assessments as failed.

    Assessments accumulate in the database in non-terminal states (queued,
    running) when an upstream runner is lost or never reports a final status.
    Calling this endpoint transitions any such assessment older than `hours`
    to `failed` with a traceable status_detail.
    """
    return await sweep_stuck_assessments(db, hours=hours)

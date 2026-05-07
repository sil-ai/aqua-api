__version__ = "v3"

import socket
from datetime import datetime, timedelta
from typing import List

import fastapi
from fastapi import Depends, Query
from pydantic import BaseModel
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from database.dependencies import get_db
from database.models import Assessment
from database.models import UserDB as UserModel
from security_routes.admin_routes import get_current_admin
from utils.logging_config import setup_logger

DEFAULT_TIMEOUT_HOURS = 24
TIMEOUT_STATUS_DETAIL = "Marked failed by upstream timeout sweep"
NON_TERMINAL_STATUSES = ["queued", "running"]

container_id = socket.gethostname()
logger = setup_logger(__name__, container_id=container_id)

router = fastapi.APIRouter()


class TimeoutSweepResult(BaseModel):
    swept_count: int
    swept_ids: List[int]
    cutoff: datetime
    hours: int


async def sweep_stuck_assessments(
    db: AsyncSession, hours: int = DEFAULT_TIMEOUT_HOURS
) -> TimeoutSweepResult:
    now = datetime.utcnow()
    cutoff = now - timedelta(hours=hours)
    stmt = (
        update(Assessment)
        .where(
            Assessment.status.in_(NON_TERMINAL_STATUSES),
            Assessment.deleted.is_not(True),
            Assessment.requested_time < cutoff,
        )
        .values(
            status="failed",
            status_detail=TIMEOUT_STATUS_DETAIL,
            end_time=now,
        )
        .returning(Assessment.id)
        .execution_options(synchronize_session=False)
    )
    result = await db.execute(stmt)
    swept_ids = [row[0] for row in result.all()]
    await db.commit()

    if swept_ids:
        logger.info(
            "Timeout sweep marked stuck assessments as failed",
            extra={
                "swept_count": len(swept_ids),
                "hours": hours,
                "cutoff": cutoff.isoformat(),
                "swept_ids_sample": swept_ids[:50],
            },
        )

    return TimeoutSweepResult(
        swept_count=len(swept_ids),
        swept_ids=swept_ids,
        cutoff=cutoff,
        hours=hours,
    )


@router.post("/assessment/timeout-sweep", response_model=TimeoutSweepResult)
async def timeout_sweep(
    hours: int = Query(
        DEFAULT_TIMEOUT_HOURS,
        ge=1,
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

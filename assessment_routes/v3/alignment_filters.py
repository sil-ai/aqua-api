"""Shared SQLAlchemy filters for distinguishing word-alignment runners.

Both fastalign and eflomal produce ``type="word-alignment"`` assessments; they
are told apart only by the ``use_eflomal`` flag stored in ``Assessment.kwargs``
(a JSONB column, see ``assessment_routes.py`` create logic). This module is the
single source of truth for that distinction so the create-time dedup logic and
the read endpoints stay in lock-step.
"""

from typing import Optional

from sqlalchemy import or_

from database.models import Assessment

# Containment payload used with the JSONB ``@>`` operator. An assessment is an
# eflomal run iff its kwargs contain ``{"use_eflomal": true}``.
_EFLOMAL_KWARG = {"use_eflomal": True}


def eflomal_method_clause(use_eflomal: Optional[bool]):
    """Return a clause selecting word-alignment assessments by runner.

    - ``use_eflomal is True``  -> eflomal only (``kwargs @> {"use_eflomal": true}``)
    - ``use_eflomal`` is ``False`` or ``None`` -> fastalign only
      (``kwargs IS NULL`` or kwargs not containing the flag)

    The ``None`` default maps to fastalign so read endpoints stay
    backward-compatible and symmetric with the assessment-create endpoint, where
    ``use_eflomal`` defaults to false. AND this into a select that already
    filters ``Assessment.type == "word-alignment"``.
    """
    if use_eflomal is True:
        return Assessment.kwargs.op("@>")(_EFLOMAL_KWARG)
    return or_(
        Assessment.kwargs.is_(None),
        ~Assessment.kwargs.op("@>")(_EFLOMAL_KWARG),
    )

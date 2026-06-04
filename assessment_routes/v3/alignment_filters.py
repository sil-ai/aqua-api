"""Shared SQLAlchemy filters for distinguishing word-alignment runners.

Both fastalign and eflomal produce ``type="word-alignment"`` assessments; they
are told apart only by the ``use_eflomal`` flag stored in ``Assessment.kwargs``
(a JSONB column, see ``assessment_routes.py`` create logic). This module is the
single source of truth for that distinction.

The clause is used in two contexts with intentionally different ``None``
handling:

- **Create / dedup** (``assessment_routes.py``) resolves the runner to a
  concrete ``bool`` *before* calling this clause (eflomal is the create-time
  default), so the create path never passes ``None`` here.
- **Read endpoints** pass the request's ``use_eflomal`` straight through. There,
  ``None`` means "no runner preference": the clause applies no runner filter and
  each calling query decides how "most recent" is resolved (most order by
  ``Assessment.end_time``; the compare/missingwords baseline selection uses
  ``func.max(Assessment.id)``).
"""

from typing import Optional

from sqlalchemy import or_, true

from database.models import Assessment

# Containment payload used with the JSONB ``@>`` operator. An assessment is an
# eflomal run iff its kwargs contain ``{"use_eflomal": true}``.
_EFLOMAL_KWARG = {"use_eflomal": True}


def eflomal_method_clause(use_eflomal: Optional[bool]):
    """Return a clause selecting word-alignment assessments by runner.

    - ``use_eflomal is None`` -> no runner filter (matches both runners). The
      calling query then selects an assessment regardless of method by its own
      "most recent" rule (``end_time`` order or ``max(id)``). This is the
      read-endpoint default.
    - ``use_eflomal is True`` -> eflomal only
      (``kwargs @> {"use_eflomal": true}``)
    - ``use_eflomal is False`` -> fastalign only
      (``kwargs IS NULL`` or kwargs not containing the flag)

    The ``None`` branch only ever affects read endpoints: the create/dedup path
    resolves ``None`` to a concrete ``bool`` before calling (eflomal stays the
    create-time default), so it never relies on this no-op. AND this into a
    select that already filters ``Assessment.type == "word-alignment"``.
    """
    if use_eflomal is None:
        return true()
    if use_eflomal is False:
        return or_(
            Assessment.kwargs.is_(None),
            ~Assessment.kwargs.op("@>")(_EFLOMAL_KWARG),
        )
    return Assessment.kwargs.op("@>")(_EFLOMAL_KWARG)

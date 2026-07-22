"""Per-domain Pydantic schema package (issue #729).

The former monolithic ``models.py`` (2,000+ lines, 75+ classes) has been split
into one module per domain. Import from the specific submodule in new code::

    from schemas.bible import VersionIn
    from schemas.assessment import AssessmentOut

The whole set is also re-exported here (and, in turn, from the legacy
``models`` shim) so the frozen v3 surface's historical ``from models import X``
imports keep working unchanged during the v3→v4 transition (epic #842).

Module dependency order is acyclic: ``validators`` has no deps; ``tfidf``
depends on ``assessment``; ``pivot`` on ``tokenizer``; ``training`` on
``validators``/``assessment``/``agent``.
"""

from . import (
    affix,
    agent,
    assessment,
    bible,
    eflomal,
    pivot,
    predict,
    security,
    tfidf,
    tokenizer,
    training,
    validators,
)
from .affix import *  # noqa: F401,F403
from .agent import *  # noqa: F401,F403
from .assessment import *  # noqa: F401,F403
from .bible import *  # noqa: F401,F403
from .eflomal import *  # noqa: F401,F403
from .pivot import *  # noqa: F401,F403
from .predict import *  # noqa: F401,F403
from .security import *  # noqa: F401,F403
from .tfidf import *  # noqa: F401,F403
from .tokenizer import *  # noqa: F401,F403
from .training import *  # noqa: F401,F403
from .validators import *  # noqa: F401,F403

# Aggregate every submodule's public surface so ``from schemas import *`` (and
# the ``models`` shim built on it) re-export the exact set the monolith did.
__all__ = [
    *affix.__all__,
    *agent.__all__,
    *assessment.__all__,
    *bible.__all__,
    *eflomal.__all__,
    *pivot.__all__,
    *predict.__all__,
    *security.__all__,
    *tfidf.__all__,
    *tokenizer.__all__,
    *training.__all__,
    *validators.__all__,
]

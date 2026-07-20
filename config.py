"""Centralized, typed application configuration.

Every environment-driven setting is declared here as a single ``Settings``
model and validated when this module is first imported, so a missing or
malformed *required* variable fails loudly at boot instead of surfacing as a
subtle runtime bug (cf. #712 boolean-truthiness, #716 empty ``SECRET_KEY``).
Import the module-level ``settings`` singleton wherever config is needed::

    from config import settings

    engine = create_async_engine(settings.aqua_db)

Environment variable NAMES are a deployment contract — App Runner, Modal, and
CI all set them by name. Field names are the lowercased versions of the same
names and are matched case-insensitively, so the wire names (``AQUA_DB``,
``SECRET_KEY``, ``LOKI_*``, ``MODAL_ENV`` …) are unchanged.

Note on ``.env`` loading: we call ``load_dotenv()`` here (populating
``os.environ``) rather than using pydantic-settings' ``env_file`` support.
This keeps a single, well-defined precedence (real env vars win over ``.env``,
which is python-dotenv's default) and, importantly, means constructing a fresh
``Settings()`` reflects only the current process environment — which the
fail-fast import checks in ``security_routes.utilities`` and the per-app CORS
wiring in ``app.configure_cors`` rely on.
"""

from typing import Optional

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load .env into os.environ before Settings reads it. Existing environment
# variables take precedence (python-dotenv's default override=False), matching
# the behavior the app relied on previously.
load_dotenv()


class Settings(BaseSettings):
    """Typed application configuration, sourced from the environment."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra="ignore",
    )

    # --- Database -------------------------------------------------------
    # Required: the async SQLAlchemy URL (postgresql+asyncpg://...). A missing
    # value fails validation here, at boot, rather than deep inside a request.
    aqua_db: str

    # "null" forces SQLAlchemy's NullPool (used by the test suite, whose
    # TestClient spawns a fresh event loop per request). Anything else uses the
    # pooled engine configured below.
    aqua_db_poolclass: Optional[str] = None
    aqua_db_pool_size: int = 2
    aqua_db_max_overflow: int = 3
    aqua_db_pool_timeout: int = 10
    aqua_db_pool_recycle: int = 1800

    # --- Auth -----------------------------------------------------------
    # Optional at this layer so that importing config never fails for consumers
    # that don't need JWT signing (notably Alembic, which imports
    # database.database for its metadata and only ever sets AQUA_DB). The
    # non-empty requirement is enforced where the key is actually used, in
    # security_routes.utilities.
    secret_key: Optional[str] = None

    # --- CORS -----------------------------------------------------------
    # Comma-separated list of extra allowed origins, layered on top of the
    # baked-in defaults in app.DEFAULT_ALLOWED_ORIGINS. Parsed by
    # app._parse_allowed_origins.
    allowed_origins: str = ""

    # --- Modal ----------------------------------------------------------
    modal_env: str = "main"

    # --- Predict / assessment thresholds --------------------------------
    predict_per_app_timeout_s: float = 60.0
    alignment_threshold: float = 0.2
    missing_words_missing_threshold: float = 0.15
    missing_words_match_threshold: float = 0.2

    # --- Observability / Loki -------------------------------------------
    # A real bool so pydantic parses "true"/"false"/"1"/"0" correctly, instead
    # of the bool(os.getenv(...)) footgun where any non-empty string is truthy.
    loki_enabled: bool = False
    loki_url: Optional[str] = None
    loki_auth_token: Optional[str] = None
    project_name: str = "aqua-api"
    environment_loki: str = "local"


# Instantiated once, at import; import this singleton everywhere config is read.
# Constructing it validates the environment, so a missing required variable
# (e.g. AQUA_DB) raises immediately at application boot.
settings = Settings()

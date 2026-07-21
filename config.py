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
from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Valid values for the Loki ``environment`` label. Mirrors
# observability_library.log_schema.LokiLoggerLabels.environment
# (Literal["release", "main", "development"]). Duplicated here on purpose:
# config must not import observability-library, whose import is optional and
# guarded in utils.logging_config so a missing/broken install can't crash boot.
_VALID_LOKI_ENVIRONMENTS = ("release", "main", "development")

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

    @field_validator("aqua_db")
    @classmethod
    def _aqua_db_nonempty(cls, v: str) -> str:
        # A required str is satisfied by an explicitly-empty ``AQUA_DB=``, which
        # would otherwise slip past boot validation and only fail later as an
        # opaque SQLAlchemy error. Reject empty/whitespace to keep the fail-fast
        # guarantee (mirrors the SECRET_KEY check in security_routes.utilities).
        if not v.strip():
            raise ValueError("AQUA_DB environment variable must not be empty")
        return v

    # "null" forces SQLAlchemy's NullPool (used by the test suite, whose
    # TestClient spawns a fresh event loop per request). Anything else uses the
    # pooled engine configured below.
    aqua_db_poolclass: Optional[str] = None
    aqua_db_pool_size: int = 2
    aqua_db_max_overflow: int = 3
    aqua_db_pool_timeout: int = 10
    aqua_db_pool_recycle: int = 1800

    @field_validator(
        "aqua_db_pool_size",
        "aqua_db_max_overflow",
        "aqua_db_pool_timeout",
        "aqua_db_pool_recycle",
        mode="before",
    )
    @classmethod
    def _blank_pool_var_to_default(cls, v, info):
        # docker-compose wires these as ``AQUA_DB_POOL_SIZE=${AQUA_DB_POOL_SIZE:-}``,
        # i.e. present-but-blank means "use the app default". The pre-#847
        # ``_env_int`` helper honored that (empty string => default); pydantic's
        # int parsing does not — a present ``""`` raises int_parsing at boot. Map a
        # blank/whitespace value back to the field's declared default to restore
        # that behavior. Everything else (valid ints, and non-blank garbage that
        # must still fail fast) falls through to pydantic's normal coercion.
        if isinstance(v, str) and v.strip() == "":
            return cls.model_fields[info.field_name].default
        return v

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
    environment_loki: str = "development"

    @model_validator(mode="after")
    def _valid_loki_environment(self):
        # Only meaningful when Loki shipping is on: an ``ENVIRONMENT_LOKI`` value
        # outside the library's allowed set makes LokiLoggerLabels(...) raise
        # inside setup_logger's broad except, which now logs only the exception
        # *type* (to avoid leaking the auth token/URL) — so the misconfiguration
        # would be undiagnosable. Fail fast at boot instead, matching the rest of
        # this module. Left unchecked when Loki is disabled so local runs keep
        # working regardless of the label value.
        if self.loki_enabled and self.environment_loki not in _VALID_LOKI_ENVIRONMENTS:
            raise ValueError(
                "ENVIRONMENT_LOKI must be one of "
                f"{_VALID_LOKI_ENVIRONMENTS} when LOKI_ENABLED is true, "
                f"got {self.environment_loki!r}"
            )
        return self


# Instantiated once, at import; import this singleton everywhere config is read.
# Constructing it validates the environment, so a missing required variable
# (e.g. AQUA_DB) raises immediately at application boot.
settings = Settings()

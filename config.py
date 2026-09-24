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
from pydantic import Field, field_validator
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
    # Lower bounds keep the module's fail-loud-at-boot contract: a negative
    # (typo'd) value would otherwise pass Settings() and only surface as an
    # opaque error when the engine/asyncpg first uses it. pool_recycle allows
    # SQLAlchemy's -1 sentinel ("disable recycling"); statement_timeout allows
    # 0 (Postgres' "no limit"). pool_size/pool_timeout must be strictly
    # positive — a 0-size pool or 0s checkout timeout is nonsensical here.
    aqua_db_pool_size: int = Field(default=5, gt=0)
    aqua_db_max_overflow: int = Field(default=10, ge=0)
    aqua_db_pool_timeout: int = Field(default=30, gt=0)
    aqua_db_pool_recycle: int = Field(default=1800, ge=-1)
    # Server-side statement_timeout (ms) passed to asyncpg. The real safety
    # net against pool exhaustion: caps how long any single query can pin a
    # pooled connection, so one runaway query fails its own request instead
    # of starving every other caller on the worker. 0 disables (Postgres
    # default — no limit). See PR #656 / issue behind the QueuePool 500s.
    aqua_db_statement_timeout_ms: int = Field(default=60_000, ge=0)

    @field_validator(
        "aqua_db_pool_size",
        "aqua_db_max_overflow",
        "aqua_db_pool_timeout",
        "aqua_db_pool_recycle",
        "aqua_db_statement_timeout_ms",
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

    # --- TF-IDF encoder cache -------------------------------------------
    # Byte budget for the per-worker cache of rehydrated TF-IDF encoders in
    # assessment_routes.v3.tfidf_artifact_routes. The previous count-based cap
    # bounded nothing useful: an entry is dominated by a 300 x n_features SVD
    # components matrix, so the same 32 entries are a few hundred MB for one
    # corpus and several GB for another. This is a *per-worker* budget —
    # multiply by WEB_CONCURRENCY for the container total, and size it against
    # the host's memory.
    #
    # It bounds the *retained* encoders, not peak RSS: the entry just stored is
    # never evicted (so a single oversized encoder can exceed the budget on its
    # own, pinning the cache at one entry), and a miss transiently holds the
    # decoded vocabularies and the raw .npy bytes alongside the loaded matrix.
    # Eviction and the over-budget case are both logged.
    #
    # The default is sized from measured Bible-scale encoders, not guessed. Fitting
    # aqua-assessments' production config (word 1-2gram max_df=0.12, char_wb 3-6gram
    # max_df=0.3, both min_df=2) over real Bibles and measuring with
    # _encoder_nbytes at the float32 wire dtype the push actually stores:
    #
    #     KJV (English), 36,694 verses  -> 173,585 features -> 236 MB
    #     swh-ONEN (Swahili), 31,098    -> 217,042 features -> 297 MB
    #
    # char_wb 3-6grams dominate the feature count (98k-142k of those). 768MB holds
    # two such encoders, so a worker alternating between two assessments stops
    # rebuilding on every request. Per-worker peak during a miss is roughly this
    # budget + ~550MB transient (raw .npy bytes, the loaded matrix, and the decoded
    # vocabularies) + ~190MB interpreter; the miss path is serialised per worker, so
    # only one transient is live at a time. At WEB_CONCURRENCY=4 on an 8GB host that
    # is ~3.8GB steady and ~6GB if every worker misses at once. Lower it if the host
    # is smaller — the test fixtures' 300-document corpus is nothing like this, so
    # the eviction logs are the only real sizing signal.
    tfidf_encoder_cache_max_bytes: int = Field(default=768 * 1024 * 1024, gt=0)

    # How many per-revision partial GiST indexes on verse_text may exist at once
    # (assessment_routes/v4/tfidf_retrieval.py). A cap rather than "one per assessed
    # revision", because every index on verse_text is one more the planner considers on
    # *every* query against that table, not just the shortlist — and seven v3 modules read
    # verse_text too, so most of that cost is charged to callers that get nothing back.
    # Measured locally on a 600k-row stand-in (2,000 revisions x 300 verses, pg16),
    # planning time for a plain indexed point read on verse_text:
    #
    #     1 index    1.68 ms        500 indexes   12.5 ms
    #     20         1.26 ms      1,000          39.7 ms
    #     100        3.99 ms      2,000          65.2 ms
    #
    # ~40 us of planning per index. 5,066 distinct revisions have a tfidf assessment in
    # production, so one index per assessed revision would put ~200 ms of planning on
    # reads that plan in under 2 ms today.
    #
    # 128 costs ~4.5 ms and covers the revisions assessed in the last ~6.6 days, against
    # ~1.3 days at 32 — and ~21 distinct revisions are assessed per day, so a smaller cap
    # ages out faster than the work does. A revision past the cap still reads correctly,
    # just on a sequential scan: ~950 ms against ~115 ms, measured on prod for a
    # 31,098-verse revision. Raise it only with the planning cost of the whole verse_text
    # surface in mind, not just this endpoint's; 500 is already 12.5 ms.
    tfidf_shortlist_index_max: int = Field(default=128, gt=0)

    # The v4 similar-verses recipe cache (assessment_routes/v4/tfidf_retrieval.py), which
    # is a SECOND cache, not a re-keying of the one above. v3's _ENCODER_CACHE bounds
    # itself to tfidf_encoder_cache_max_bytes and still serves the POST path; the v4 cache
    # bounds itself to this. They are independent dicts with independent eviction, so a
    # worker serving both holds the sum of the two — which is why this is its own number
    # rather than a second reader of the one above, where the sum would silently have been
    # 1.5 GB.
    #
    # Sized from the same measurements, minus the SVD this path never loads. The components
    # matrix is what dominated an encoder: of the 236 MB measured for KJV, ~208 MB is the
    # 300 x 173,585 float32 matrix, leaving ~28 MB of vectorizers; Swahili's 297 MB leaves
    # ~37 MB the same way. So 256 MB holds roughly seven Bible-scale recipes, against the
    # three full encoders 768 MB held — ample for a worker alternating between revisions,
    # and a lower worst-case total than sharing the larger budget would have given.
    tfidf_recipe_cache_max_bytes: int = Field(default=256 * 1024 * 1024, gt=0)

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

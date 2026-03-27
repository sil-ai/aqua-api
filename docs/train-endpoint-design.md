# Plan: Train Endpoint for aqua-api

## Context

The current assessment system combines training and inference into a single slow operation. We're building a new paradigm that separates these concerns:

- **Training** (this plan): Long-running, one-time-per-text jobs that produce models (e.g., Serval NMT → HuggingFace). Dispatched to Modal.
- **Inference** (future): Fast, realtime operations using pre-trained models. Will be its own route module later.

This plan covers the **aqua-api side**: TrainingJob DB model, train routes, data endpoint for the Modal runner, and the runner dispatch. The Modal training runner itself will be implemented separately.

## Files to Create

| File | Purpose |
|------|---------|
| `train_routes/__init__.py` | Empty init |
| `train_routes/v3/__init__.py` | Empty init |
| `train_routes/v3/train_routes.py` | Route handlers |
| `alembic/migrations/versions/<hex>_create_training_job.py` | Migration |
| `test/test_train_routes/__init__.py` | Empty init |
| `test/test_train_routes/test_train_routes.py` | Tests |

## Files to Modify

| File | Change |
|------|--------|
| `database/models.py` | Add `TrainingJob` model (~line 156) |
| `models.py` | Add Pydantic models for training |
| `app.py` | Register `train_router_v3` |

---

## Step 1: `database/models.py` — TrainingJob model

Add after Assessment class. Uses existing imports; add `JSONB`, `Float` if needed.

```python
class TrainingJob(Base):
    __tablename__ = "training_job"

    id = Column(Integer, primary_key=True)
    type = Column(Text, nullable=False)                     # "serval-nmt", future types
    source_revision_id = Column(Integer, ForeignKey("bible_revision.id"), nullable=False)
    target_revision_id = Column(Integer, ForeignKey("bible_revision.id"), nullable=False)
    source_language = Column(String(3), ForeignKey("iso_language.iso639"), nullable=False)
    target_language = Column(String(3), ForeignKey("iso_language.iso639"), nullable=False)

    status = Column(Text, nullable=False, default="queued")
    status_detail = Column(Text, nullable=True)
    percent_complete = Column(Float, nullable=True)         # 0.0 to 100.0

    external_ids = Column(JSONB, nullable=True)             # Populated by runner during "preparing" phase
                                                            # Serval: {"engine_id", "build_id", "corpus_id"}
    result_url = Column(Text, nullable=True)                # HF repo URL, e.g. "https://huggingface.co/sil-ai/nllb-finetuned-{src}-{tgt}"
    result_metadata = Column(JSONB, nullable=True)
    options = Column(JSONB, nullable=True)                   # Training hyperparameters

    requested_time = Column(TIMESTAMP, default=func.now())
    start_time = Column(TIMESTAMP, nullable=True)
    end_time = Column(TIMESTAMP, nullable=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    # Soft delete (consistent with Assessment, BibleRevision, BibleVersion)
    deleted = Column(Boolean, default=False)
    deleted_at = Column(TIMESTAMP, nullable=True)

    source_revision = relationship("BibleRevision", foreign_keys=[source_revision_id])
    target_revision = relationship("BibleRevision", foreign_keys=[target_revision_id])
    owner = relationship("UserDB")

    __table_args__ = (
        Index("ix_training_job_status", "status"),
        Index("ix_training_job_type_status", "type", "status"),
        Index("ix_training_job_lang_pair", "source_language", "target_language"),
        Index("ix_training_job_revisions_type_status", "source_revision_id", "target_revision_id", "type", "status"),
    )
```

JSONB fields keep the model extensible — different training types store different external IDs, options, and result metadata without schema changes.

## Step 2: `models.py` — Pydantic models

```python
class TrainingType(str, Enum):
    serval_nmt = "serval-nmt"

class TrainingStatus(str, Enum):
    queued = "queued"
    preparing = "preparing"
    training = "training"
    downloading = "downloading"
    uploading = "uploading"
    completed = "completed"
    completed_with_errors = "completed_with_errors"
    failed = "failed"

class TrainingJobIn(BaseModel):
    source_revision_id: int
    target_revision_id: int
    type: TrainingType
    options: Optional[Dict[str, Any]] = None   # e.g. {"max_steps": 3000, "learning_rate": 0.0004}
    model_config = {"use_enum_values": True}

class TrainingJobOut(BaseModel):
    id: int
    type: str
    source_revision_id: int
    target_revision_id: int
    source_language: str
    target_language: str
    status: str
    status_detail: Optional[str] = None
    percent_complete: Optional[float] = None
    result_url: Optional[str] = None
    result_metadata: Optional[Dict[str, Any]] = None
    options: Optional[Dict[str, Any]] = None
    requested_time: Optional[datetime.datetime] = None
    start_time: Optional[datetime.datetime] = None
    end_time: Optional[datetime.datetime] = None
    owner_id: Optional[int] = None
    model_config = {"from_attributes": True, "use_enum_values": True}

class TrainingJobStatusUpdate(BaseModel):
    status: TrainingStatus
    status_detail: Optional[str] = None
    percent_complete: Optional[float] = Field(None, ge=0.0, le=100.0)
    external_ids: Optional[Dict[str, Any]] = None
    result_url: Optional[str] = None
    result_metadata: Optional[Dict[str, Any]] = None
    model_config = {"use_enum_values": True}
```

## Step 3: Migration

Create a new Alembic migration based on the current head revision in the alembic history. Creates the `training_job` table with all columns and indexes from Step 1.

## Step 4: `train_routes/v3/train_routes.py`

### `POST /train` — Create and dispatch training job

1. Auth: `Depends(get_current_user)` (from `security_routes/auth_routes.py:52`)
2. Validate both revision IDs exist
3. Look up languages: `BibleRevision` → `BibleVersion.iso_language` (models.py lines 208, 236)
4. Duplicate check: query for existing job with same (source_revision_id, target_revision_id, type, options) and status not in ("completed", "completed_with_errors", "failed") → return 409 with existing job. Jobs with different options (e.g., different hyperparameters) for the same revision pair and type are treated as distinct.
5. Create `TrainingJob` record, status="queued", `requested_time` set to `datetime.utcnow()`
6. Dispatch to Modal runner (POST with `TrainingJobOut` JSON, `MODAL_WEBHOOK_TOKEN` auth)
7. On Modal error: set status to "failed" with `status_detail` explaining the dispatch failure (keep the record for observability rather than deleting it)
8. Return `TrainingJobOut`

### `GET /train` — List training jobs

Query params: `status`, `type`, `source_language`, `target_language`.
Admin sees all; regular users filtered by group access to the revisions' bible versions (same access pattern as assessment GET at `assessment_routes/v3/assessment_routes.py:87-151`).
Only returns non-deleted jobs by default.

### `GET /train/{job_id}` — Single job

Auth: admin, owner, or group access to both revisions.

### `PATCH /train/{job_id}/status` — Runner callback

Auth: `Authorization: Bearer {MODAL_WEBHOOK_TOKEN}` (same token used for Modal dispatch; not user JWT).

Accepts `TrainingJobStatusUpdate`. Validates:
- Job exists and is not soft-deleted
- Job is not in a terminal status (`completed`, `completed_with_errors`, `failed`)
- State transition is valid per the state machine:
  - `queued` → `preparing` → `training` → `downloading` → `uploading` → `completed`
  - Any non-terminal status → `failed`
  - `uploading` → `completed_with_errors` (training succeeded but post-processing failed)

Sets `start_time` on first non-queued status, `end_time` on terminal status.

### `GET /train/{job_id}/data` — Parallel text for runner

Authenticated via `Authorization: Bearer {MODAL_WEBHOOK_TOKEN}` (same token as status callback).

Query param: `range_handling` = `filter` (default), `merge`, or `empty`.

| Value | Behavior |
|-------|----------|
| `filter` | Drop verse pairs where either side is `<range>` |
| `merge` | Combine `<range>` verses into the preceding verse using `merge_verse_ranges()` from `utils/verse_range_utils.py` (matches current word alignment behavior — Cassie's preference for alignment tasks) |
| `empty` | Return `<range>` verses with empty strings |

The existing `merge_verse_ranges()` utility handles grouping consecutive `<range>` verses with their anchor verse and combining text fields. For the training data endpoint, call it with `combine_fields=["source", "target"]` and the default string concatenation combiner.

Returns JSON array of aligned verse pairs:
```json
[{"vref": "GEN 1:1", "source": "In the beginning...", "target": "Hapo mwanzo..."}]
```

When `merge` is used, merged verses return a combined vref: `"vref": "GEN 1:1-2"`.

Query: self-join `VerseText` on `verse_reference` for both revisions. Filter out rows where either text is NULL, empty, or whitespace-only. Apply range handling per query param. Uses index `ix_verse_text_verse_reference_revision`.

### `DELETE /train/{job_id}` — Soft delete

Auth: owner or admin only.
Only allowed for jobs in terminal status (`completed`, `completed_with_errors`, `failed`). Returns 409 for active jobs — cancellation of running jobs is out of scope for this iteration.

## Step 5: `app.py` — Register router

```python
from train_routes.v3.train_routes import router as train_router_v3
# In configure_routing (after line 100):
app.include_router(train_router_v3, prefix="/v3", tags=["Version 3"])
app.include_router(train_router_v3, prefix="/latest", tags=["Version 3 / Latest"])
```

## Step 6: Tests

Use existing fixtures from `test/conftest.py`: `test_revision_id`, `test_revision_id_2`, `regular_token1`, `admin_token`, `eng`/`swh` languages.

- POST creates job with status="queued" (mock Modal dispatch)
- GET list returns jobs, respects auth
- GET single returns job details
- PATCH status callback updates fields, enforces state machine
- PATCH rejects invalid state transitions
- GET data returns parallel verse text (both filter and merge modes)
- Duplicate detection → 409 (same options), allowed (different options)
- DELETE only allowed for terminal jobs
- Unauthenticated requests → 401

---

## Modal Runner Contract (separate implementation)

### Workflow

The runner receives `TrainingJobOut`, then:
1. PATCH status → "preparing"
2. GET /train/{job_id}/data → fetch parallel text
3. Serval API: auth → upload source/target files → create NMT engine → create corpus → start build
4. PATCH status → "training" (poll, update percent_complete periodically)
5. On completion: download model tar.gz, extract → PATCH status → "downloading"
6. PATCH status → "uploading"
7. Upload to HuggingFace: `sil-ai/nllb-finetuned-{src}-{tgt}`
8. PATCH status → "completed" with `result_url` (e.g. `https://huggingface.co/sil-ai/nllb-finetuned-{src}-{tgt}`) and `result_metadata`

### Error handling and retries

- **Transient failures** (network errors, HTTP 5xx from Serval/aqua-api/HuggingFace): Retry up to 3 times with exponential backoff and jitter. Cap total retry time at ~10 minutes per operation.
- **Permanent failures** (HTTP 4xx other than 429): Do not retry. Set status to "failed" immediately.
- **Rate limiting** (HTTP 429): Respect `Retry-After` header; otherwise back off at least 5s, up to 3 attempts.

### Partial failures

- If Serval training completes and a usable model exists, but HuggingFace upload fails: set status to `"completed_with_errors"` with `result_metadata` indicating what succeeded and what failed. Set `status_detail` to e.g. `"completed_with_errors: huggingface upload failed"`.
- If no usable model exists (Serval build failed, corrupted artifact): set status to `"failed"`.

### status_detail conventions

Use short, machine-parseable prefixes plus human-readable explanation:
- `"preparing_failed: aqua-api unavailable (timeout)"`
- `"training_failed: serval build aborted (400 bad_request)"`
- `"upload_failed: huggingface 500 internal_server_error"`
- `"completed_with_errors: huggingface upload failed (will not retry)"`

### Runner env vars

`SERVAL_CLIENT_ID`, `SERVAL_CLIENT_SECRET`, `HF_TOKEN`, `MODAL_WEBHOOK_TOKEN`, `AQUA_API_URL`

---

## Stale Job Monitoring (future)

Jobs stuck in `queued` status should not remain indefinitely. A future enhancement should add a periodic health check (cron or scheduled task) that:
- Finds jobs where `status = 'queued'` and `requested_time < now() - max_queued_duration` (e.g. 60 minutes)
- Marks them as `failed` with `status_detail = "timed out while queued; runner never started"`
- Similarly monitors for jobs stuck in non-terminal statuses beyond expected durations

This is out of scope for the initial implementation but should be added before production use.

---

## Verification

1. Migration: `cd alembic && AQUA_DB=... ../.venv/bin/alembic upgrade head`
2. Tests: `AQUA_DB=... .venv/bin/python -m pytest test/test_train_routes/ -v`
3. Manual: POST /v3/train with valid revision IDs — creates DB record (Modal dispatch will 503 in dev, expected)

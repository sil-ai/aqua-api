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
    percent_complete = Column(Float, nullable=True)

    external_ids = Column(JSONB, nullable=True)             # Serval: {"engine_id", "build_id", "corpus_id"}
    result_url = Column(Text, nullable=True)                # HF repo URL when complete
    result_metadata = Column(JSONB, nullable=True)
    options = Column(JSONB, nullable=True)                   # Training hyperparameters

    requested_time = Column(TIMESTAMP, default=func.now())
    start_time = Column(TIMESTAMP, nullable=True)
    end_time = Column(TIMESTAMP, nullable=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    source_revision = relationship("BibleRevision", foreign_keys=[source_revision_id])
    target_revision = relationship("BibleRevision", foreign_keys=[target_revision_id])
    owner = relationship("UserDB")

    __table_args__ = (
        Index("ix_training_job_status", "status"),
        Index("ix_training_job_type_status", "type", "status"),
        Index("ix_training_job_lang_pair", "source_language", "target_language"),
        Index("ix_training_job_revisions_type", "source_revision_id", "target_revision_id", "type"),
    )
```

JSONB fields keep the model extensible — different training types store different external IDs, options, and result metadata without schema changes.

## Step 2: `models.py` — Pydantic models

```python
class TrainingType(str, Enum):
    serval_nmt = "serval-nmt"

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
    status: str                                    # preparing/training/downloading/uploading/completed/failed
    status_detail: Optional[str] = None
    percent_complete: Optional[float] = None
    external_ids: Optional[Dict[str, Any]] = None
    result_url: Optional[str] = None
    result_metadata: Optional[Dict[str, Any]] = None
```

## Step 3: Migration

Current head: `f6a7b8c9d0e1`. New migration creates `training_job` table with all columns and indexes.

## Step 4: `train_routes/v3/train_routes.py`

### `POST /train` — Create and dispatch training job

1. Auth: `Depends(get_current_user)` (from `security_routes/auth_routes.py:52`)
2. Validate both revision IDs exist
3. Look up languages: `BibleRevision` → `BibleVersion.iso_language` (models.py lines 208, 236)
4. Duplicate check: query for existing job with same (source_revision_id, target_revision_id, type) and status not in ("completed", "failed") → return 409 with existing job
5. Create `TrainingJob` record, status="queued"
6. Dispatch to Modal runner (POST with `TrainingJobOut` JSON, `MODAL_WEBHOOK_TOKEN` auth)
7. On Modal error: delete record, raise 503
8. Return `TrainingJobOut`

### `GET /train` — List training jobs

Query params: `status`, `type`, `source_language`, `target_language`.
Admin sees all; regular users filtered by group access to the revisions' bible versions (same access pattern as assessment GET at `assessment_routes/v3/assessment_routes.py:87-151`).

### `GET /train/{job_id}` — Single job

Auth: admin, owner, or group access to both revisions.

### `PATCH /train/{job_id}/status` — Runner callback

Authenticated via `Authorization: Bearer {MODAL_WEBHOOK_TOKEN}` (not user JWT).
Accepts `TrainingJobStatusUpdate`. Sets `start_time` on first non-queued status, `end_time` on completed/failed.

### `GET /train/{job_id}/data` — Parallel text for runner

Authenticated via runner token. Returns JSON array of aligned verse pairs:
```json
[{"vref": "GEN 1:1", "source": "In the beginning...", "target": "Hapo mwanzo..."}]
```

Query: self-join `VerseText` (models.py:319-340) on `verse_reference` for both revisions. Filter out nulls, blanks, and `<range>` markers. Uses index `ix_verse_text_verse_reference_revision`.

### `DELETE /train/{job_id}` — Soft delete (owner or admin)

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
- PATCH status callback updates fields
- GET data returns parallel verse text
- Duplicate detection → 409
- Unauthenticated requests → 401

## Modal Runner Contract (separate implementation)

The runner receives `TrainingJobOut`, then:
1. PATCH status → "preparing"
2. GET /train/{job_id}/data → fetch parallel text
3. Serval API: auth → upload source/target files → create NMT engine → create corpus → start build
4. PATCH status → "training" (poll, update percent_complete periodically)
5. On completion: download model tar.gz, extract
6. PATCH status → "uploading"
7. Upload to HuggingFace: `sil-ai/nllb-finetuned-{src}-{tgt}`
8. PATCH status → "completed" with `result_url` and `result_metadata`

Runner env vars: `SERVAL_CLIENT_ID`, `SERVAL_CLIENT_SECRET`, `HF_TOKEN`, `MODAL_WEBHOOK_TOKEN`, `AQUA_API_URL`

## Verification

1. Migration: `cd alembic && AQUA_DB=... ../.venv/bin/alembic upgrade head`
2. Tests: `AQUA_DB=... .venv/bin/python -m pytest test/test_train_routes/ -v`
3. Manual: POST /v3/train with valid revision IDs — creates DB record (Modal dispatch will 503 in dev, expected)

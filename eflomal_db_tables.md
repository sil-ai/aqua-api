# Eflomal DB Migration Plan — Table Creation Only

## Context

The eflomal word alignment assessment stores training artifacts (dictionary, co-occurrence stats, target word counts) that need to persist in PostgreSQL instead of a Modal volume. This PR is ONLY for creating the SQLAlchemy models and running the Alembic migration. No endpoints, no inference logic.

---

## SQLAlchemy Models

### `eflomal_model` — one row per training run

```python
class EflomalModel(Base):
    __tablename__ = "eflomal_model"

    id = Column(Integer, primary_key=True, autoincrement=True)
    assessment_id = Column(Integer, ForeignKey("assessment.id"), nullable=False, unique=True)
    artifact_version = Column(Integer, nullable=False, default=2)
    num_verse_pairs = Column(Integer)
    num_alignment_links = Column(Integer)
    num_dictionary_entries = Column(Integer)
    num_missing_words = Column(Integer)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
```

### `eflomal_dictionary` — word alignment pairs from training

Words stored in **original (un-normalized)** form.

```python
class EflomalDictionary(Base):
    __tablename__ = "eflomal_dictionary"

    id = Column(Integer, primary_key=True, autoincrement=True)
    model_id = Column(Integer, ForeignKey("eflomal_model.id", ondelete="CASCADE"), nullable=False)
    source_word = Column(String, nullable=False)
    target_word = Column(String, nullable=False)
    count = Column(Integer, nullable=False)
    probability = Column(Float, nullable=False)

    __table_args__ = (
        Index("ix_eflomal_dictionary_model_source", "model_id", "source_word"),
    )
```

### `eflomal_cooccurrence` — verse-level co-occurrence stats

Words stored in **normalized** form (lowercase, alphanumeric only).

```python
class EflomalCooccurrence(Base):
    __tablename__ = "eflomal_cooccurrence"

    id = Column(Integer, primary_key=True, autoincrement=True)
    model_id = Column(Integer, ForeignKey("eflomal_model.id", ondelete="CASCADE"), nullable=False)
    source_word = Column(String, nullable=False)
    target_word = Column(String, nullable=False)
    co_occur_count = Column(Integer, nullable=False)
    aligned_count = Column(Integer, nullable=False)

    __table_args__ = (
        Index("ix_eflomal_cooccurrence_lookup", "model_id", "source_word", "target_word"),
    )
```

### `eflomal_target_word_count` — target language word frequencies

Words stored in **normalized** form.

```python
class EflomalTargetWordCount(Base):
    __tablename__ = "eflomal_target_word_count"

    id = Column(Integer, primary_key=True, autoincrement=True)
    model_id = Column(Integer, ForeignKey("eflomal_model.id", ondelete="CASCADE"), nullable=False)
    word = Column(String, nullable=False)
    count = Column(Integer, nullable=False)

    __table_args__ = (
        Index("ix_eflomal_target_word_count_lookup", "model_id", "word"),
    )
```

---

## Notes

- All child tables use `ON DELETE CASCADE` on `model_id` so deleting a model cleans up everything.
- Dictionary and cooccurrence tables can have 10K-100K+ rows per model. The indexes are designed for the inference read pattern (lookup by `model_id` + source/target word).
- `assessment_id` is `UNIQUE` — one eflomal model per assessment run. Multiple trainings for the same revision/reference pair are differentiated by their `assessment_id`.

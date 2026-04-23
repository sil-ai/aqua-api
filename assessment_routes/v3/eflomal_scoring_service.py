"""Server-side eflomal verse-scoring orchestration.

Reads the three eflomal artifact tables (dictionary, cooccurrence — we don't
need target_word_counts for per-verse scoring) plus the source/target verse
text, then scores every verse pair that has text on both sides and writes one
row per verse into assessment_result.

Kept separate from eflomal_routes.py so it stays independently testable.
"""

import re
from typing import Dict, List, Tuple

from fastapi import HTTPException
from sqlalchemy import delete, insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from database.models import (
    Assessment,
    AssessmentResult,
    EflomalAssessment,
    EflomalCooccurrence,
    EflomalDictionary,
    EflomalTargetWordCount,
    VerseText,
)
from utils.eflomal_scoring import (
    build_reverse_dictionary,
    build_src_to_translations,
    compute_link_score,
    detect_missing_words_for_verse,
    normalize_dictionary_list,
    normalize_word,
    score_verse_pair,
)

# Mirror results_push_routes.py's regex so vrefs stored here parse the same.
_VREF_RE = re.compile(r"^([A-Z0-9]+)\s+(\d+):(\d+)$")

# Mirror the batching strategy used by other push endpoints in this module:
# cap per-batch at 5,000 rows and respect asyncpg's 32,767-parameter limit.
_BATCH_SIZE = 5_000
_PG_MAX_PARAMS = 32_767


def _parse_vref(vref: str) -> Tuple[str, int, int]:
    m = _VREF_RE.match(vref)
    if not m:
        raise HTTPException(status_code=400, detail=f"Invalid vref format: {vref!r}")
    return m.group(1), int(m.group(2)), int(m.group(3))


async def _load_artifacts(
    db: AsyncSession, eflomal_assessment_id: int
) -> Tuple[Dict, Dict, Dict]:
    """Fetch dictionary + cooccurrence rows and build the in-memory lookup
    structures score_verse_pair expects.

    Returns (dictionary, src_to_translations, cooccurrence).
    """
    dict_rows = (
        (
            await db.execute(
                select(EflomalDictionary).where(
                    EflomalDictionary.assessment_id == eflomal_assessment_id
                )
            )
        )
        .scalars()
        .all()
    )
    if not dict_rows:
        raise HTTPException(
            status_code=422,
            detail=(
                "Eflomal dictionary is empty for this assessment; "
                "push dictionary entries before scoring"
            ),
        )
    cooc_rows = (
        (
            await db.execute(
                select(EflomalCooccurrence).where(
                    EflomalCooccurrence.assessment_id == eflomal_assessment_id
                )
            )
        )
        .scalars()
        .all()
    )

    raw_dict_list = [
        {
            "source": r.source_word,
            "target": r.target_word,
            "count": r.count,
            "probability": r.probability,
        }
        for r in dict_rows
    ]
    dictionary = normalize_dictionary_list(raw_dict_list)
    # No min_count cutoff — match the reference _realtime_dictionary's
    # alignment behavior (every stored dictionary pair is eligible).
    src_to_translations = build_src_to_translations(dictionary)

    # EflomalCooccurrence rows already store normalized words (per the
    # training pipeline's normalize_word() output), so no further
    # normalization is needed on key construction.
    cooccurrence: Dict[Tuple[str, str], Dict] = {}
    for r in cooc_rows:
        key = (r.source_word, r.target_word)
        cooccurrence[key] = {
            "co_occur": r.co_occur_count,
            "aligned": r.aligned_count,
        }

    return dictionary, src_to_translations, cooccurrence


async def _load_verse_pairs(
    db: AsyncSession, revision_id: int, reference_id: int
) -> List[Tuple[str, str, str]]:
    """Return (vref, source_text, target_text) for every vref where both
    revisions have non-empty text.

    source_text = reference (reference_id), target_text = revision (revision_id) —
    matches the eflomal training convention where the reference language is
    the source and the revision being assessed is the target.

    Executes a single self-join on verse_text so we avoid pulling the full
    revision into Python just to intersect with the reference's vrefs.
    """
    src = aliased(VerseText, name="src")
    tgt = aliased(VerseText, name="tgt")
    stmt = (
        select(src.verse_reference, src.text, tgt.text)
        .join(tgt, tgt.verse_reference == src.verse_reference)
        .where(
            src.revision_id == reference_id,
            tgt.revision_id == revision_id,
            src.text.isnot(None),
            tgt.text.isnot(None),
            src.text != "",
            tgt.text != "",
        )
    )
    rows = (await db.execute(stmt)).all()
    return [(r[0], r[1], r[2]) for r in rows]


async def score_verses_for_assessment(
    db: AsyncSession, assessment_id: int
) -> List[int]:
    """Score every valid verse pair for the assessment and bulk-insert the
    results into assessment_result.

    Returns the list of inserted assessment_result row IDs. Raises
    HTTPException on validation errors (unknown assessment, wrong type,
    missing artifacts) so callers can propagate the status code directly.

    Idempotency: any existing assessment_result rows for the assessment are
    deleted before insert, so re-running this function overwrites cleanly
    without leaving orphaned rows.
    """
    # Resolve Assessment and sanity-check the type.
    assessment = (
        (await db.execute(select(Assessment).where(Assessment.id == assessment_id)))
        .scalars()
        .first()
    )
    if assessment is None or assessment.deleted:
        raise HTTPException(status_code=404, detail="Assessment not found")
    if assessment.type != "word-alignment":
        raise HTTPException(
            status_code=400,
            detail=(
                f"Assessment type must be 'word-alignment' for eflomal scoring, "
                f"got {assessment.type!r}"
            ),
        )
    if assessment.revision_id is None or assessment.reference_id is None:
        raise HTTPException(
            status_code=400,
            detail="Eflomal scoring requires both revision_id and reference_id",
        )

    # Resolve the EflomalAssessment row; its id is the FK target for the
    # artifact tables (not assessment.id).
    eflomal = (
        (
            await db.execute(
                select(EflomalAssessment).where(
                    EflomalAssessment.assessment_id == assessment_id
                )
            )
        )
        .scalars()
        .first()
    )
    if eflomal is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "No eflomal artifacts found — push metadata, dictionary, "
                "and cooccurrences first"
            ),
        )

    dictionary, src_to_translations, cooccurrence = await _load_artifacts(
        db, eflomal.id
    )
    pairs = await _load_verse_pairs(db, assessment.revision_id, assessment.reference_id)

    # Build insert rows. Leave note/source/target null per plan.
    rows = []
    for vref, src_text, tgt_text in pairs:
        metrics = score_verse_pair(
            src_text,
            tgt_text,
            dictionary,
            src_to_translations,
            cooccurrence,
        )
        book, chapter, verse = _parse_vref(vref)
        rows.append(
            {
                "assessment_id": assessment_id,
                "vref": vref,
                "score": metrics["verse_score"],
                "flag": False,
                "source": None,
                "target": None,
                "note": None,
                "book": book,
                "chapter": chapter,
                "verse": verse,
            }
        )

    # Idempotency: clear any prior rows for this assessment before inserting.
    await db.execute(
        delete(AssessmentResult).where(AssessmentResult.assessment_id == assessment_id)
    )

    if not rows:
        return []

    cols_per_row = len(AssessmentResult.__table__.columns)
    batch_size = min(_BATCH_SIZE, _PG_MAX_PARAMS // cols_per_row)
    inserted_ids: List[int] = []
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        stmt = insert(AssessmentResult).values(batch).returning(AssessmentResult.id)
        result = await db.execute(stmt)
        inserted_ids.extend(r[0] for r in result.fetchall())
    return inserted_ids


async def get_missing_words_for_assessment(
    db: AsyncSession,
    assessment_id: int,
    min_alignment_count: int = 10,
    min_frequency: float = 0.5,
    min_word_len: int = 3,
) -> List[Dict]:
    """Return per-verse missing-word detections from stored eflomal artifacts.

    For every verse pair where both the revision and reference have text,
    runs detect_missing_words_for_verse using the stored dictionary and
    target-word-count artifacts.  Returns a flat list of dicts, one entry per
    flagged target word, each tagged with its vref.

    Raises HTTPException on the same validation errors as
    score_verses_for_assessment (404 / 400).
    """
    assessment = (
        (await db.execute(select(Assessment).where(Assessment.id == assessment_id)))
        .scalars()
        .first()
    )
    if assessment is None or assessment.deleted:
        raise HTTPException(status_code=404, detail="Assessment not found")
    if assessment.type != "word-alignment":
        raise HTTPException(
            status_code=400,
            detail=(
                f"Assessment type must be 'word-alignment' for eflomal scoring, "
                f"got {assessment.type!r}"
            ),
        )
    if assessment.revision_id is None or assessment.reference_id is None:
        raise HTTPException(
            status_code=400,
            detail="Eflomal scoring requires both revision_id and reference_id",
        )

    eflomal = (
        (
            await db.execute(
                select(EflomalAssessment).where(
                    EflomalAssessment.assessment_id == assessment_id
                )
            )
        )
        .scalars()
        .first()
    )
    if eflomal is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "No eflomal artifacts found — push metadata, dictionary, "
                "and cooccurrences first"
            ),
        )

    dictionary, _src_to_translations, _cooccurrence = await _load_artifacts(
        db, eflomal.id
    )
    if not dictionary:
        raise HTTPException(
            status_code=422,
            detail=(
                "Eflomal dictionary is empty for this assessment; "
                "push dictionary entries before requesting missing words"
            ),
        )

    # Build reverse dictionary (tgt -> known sources) with min_count=3 to
    # match the aqua-assessments reference implementation.
    reverse_dict = build_reverse_dictionary(dictionary, min_count=3)

    # Load target word counts from DB.
    twc_rows = (
        (
            await db.execute(
                select(EflomalTargetWordCount).where(
                    EflomalTargetWordCount.assessment_id == eflomal.id
                )
            )
        )
        .scalars()
        .all()
    )
    tgt_word_counts = {r.word: r.count for r in twc_rows}

    pairs = await _load_verse_pairs(db, assessment.revision_id, assessment.reference_id)

    results: List[Dict] = []
    for vref, src_text, tgt_text in pairs:
        verse_missing = detect_missing_words_for_verse(
            src_text,
            tgt_text,
            reverse_dict,
            tgt_word_counts,
            min_alignment_count=min_alignment_count,
            min_frequency=min_frequency,
            min_word_len=min_word_len,
        )
        for entry in verse_missing:
            results.append({"vref": vref, **entry})

    return results


__all__ = [
    "score_verses_for_assessment",
    "get_missing_words_for_assessment",
    # Exported for tests; not called directly by route handlers.
    "_load_artifacts",
    "_load_verse_pairs",
    "_parse_vref",
    # Re-exports for convenience in tests that want to build artifacts inline.
    "normalize_dictionary_list",
    "build_reverse_dictionary",
    "build_src_to_translations",
    "compute_link_score",
    "normalize_word",
    "score_verse_pair",
    "detect_missing_words_for_verse",
]

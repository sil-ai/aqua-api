"""Helpers for computing eflomal-style missing words on the API side.

Ported from aqua-assessments/assessments/word_alignment/eflomal_steps/scoring.py
and generalised to detect orphans in either direction (target or source).
"""

from collections import defaultdict
from typing import Dict, Iterable, List, Set, Tuple

from assessment_routes.v3.eflomal_routes import _normalize_word as normalize_word

DEFAULT_MIN_ALIGNMENT_COUNT = 10
DEFAULT_MIN_FREQUENCY = 0.5
DEFAULT_MIN_WORD_LEN = 3
DEFAULT_REVERSE_DICT_MIN_COUNT = 3
KNOWN_COUNTERPARTS_SHOWN = 5


def build_directional_dicts(
    dict_rows: Iterable,
    min_count: int = DEFAULT_REVERSE_DICT_MIN_COUNT,
) -> Tuple[Dict[str, List[Tuple[str, int]]], Dict[str, List[Tuple[str, int]]]]:
    """Build (forward, reverse) dicts from EflomalDictionary rows.

    forward: src_norm -> [(tgt_norm, count), ...] sorted desc
    reverse: tgt_norm -> [(src_norm, count), ...] sorted desc

    Each row must expose ``.source_word``, ``.target_word``, ``.count``.
    Raw rows that collide under normalization (e.g. "God" and "god") are
    aggregated *before* applying ``min_count``, matching the behavior of
    ``assessment_routes.v3.eflomal_routes._build_reverse_dict``.
    """
    forward_grouped: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    reverse_grouped: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in dict_rows:
        src = normalize_word(row.source_word)
        tgt = normalize_word(row.target_word)
        if not src or not tgt:
            continue
        forward_grouped[src][tgt] += row.count
        reverse_grouped[tgt][src] += row.count

    forward = _finalize_grouped(forward_grouped, min_count)
    reverse = _finalize_grouped(reverse_grouped, min_count)
    return forward, reverse


def _finalize_grouped(
    grouped: Dict[str, Dict[str, int]], min_count: int
) -> Dict[str, List[Tuple[str, int]]]:
    out: Dict[str, List[Tuple[str, int]]] = {}
    for key, counterparts in grouped.items():
        items = [(cp, c) for cp, c in counterparts.items() if c >= min_count]
        if not items:
            continue
        # Deterministic order: highest count first, alphabetical tiebreaker.
        items.sort(key=lambda x: (-x[1], x[0]))
        out[key] = items
    return out


def compute_word_counts(texts: Iterable[str]) -> Dict[str, int]:
    """Tally normalized-word frequencies across a set of verse texts.

    Used to build ``src_word_counts`` on the fly for the source-direction scan
    (we don't store an eflomal_source_word_count table).
    """
    counts: Dict[str, int] = defaultdict(int)
    for blob in texts:
        if not blob:
            continue
        for word in blob.split():
            norm = normalize_word(word)
            if norm:
                counts[norm] += 1
    return dict(counts)


def greedy_align(
    src_words: List[str],
    tgt_words: List[str],
    forward_dict: Dict[str, List[Tuple[str, int]]],
) -> Tuple[Set[int], Set[int]]:
    """Greedy per-verse dictionary match. Returns (aligned_src_idx, aligned_tgt_idx).

    Reproduces app.py:_realtime_dictionary lines 567-585 in the reference impl:
    for each source word, walk its candidate translations in count-desc order
    and consume the first still-unmatched target index that carries the same
    normalized form.
    """
    tgt_norm_to_idx: Dict[str, List[int]] = defaultdict(list)
    for j, tgt in enumerate(tgt_words):
        tgt_norm = normalize_word(tgt)
        if tgt_norm:
            tgt_norm_to_idx[tgt_norm].append(j)

    aligned_src: Set[int] = set()
    aligned_tgt: Set[int] = set()

    for i, src in enumerate(src_words):
        src_norm = normalize_word(src)
        if not src_norm:
            continue
        for tgt_norm, _count in forward_dict.get(src_norm, []):
            matched = False
            for j in tgt_norm_to_idx.get(tgt_norm, []):
                if j not in aligned_tgt:
                    aligned_src.add(i)
                    aligned_tgt.add(j)
                    matched = True
                    break
            if matched:
                break
    return aligned_src, aligned_tgt


def _detect_orphans_one_side(
    *,
    candidate_words: List[str],
    aligned_indices: Set[int],
    counterpart_words_norm: Set[str],
    counterpart_dict: Dict[str, List[Tuple[str, int]]],
    candidate_word_counts: Dict[str, int],
    min_alignment_count: int,
    min_frequency: float,
    min_word_len: int,
) -> List[dict]:
    orphans: List[dict] = []
    for j, word in enumerate(candidate_words):
        if j in aligned_indices:
            continue
        word_norm = normalize_word(word)
        if not word_norm or len(word_norm) < min_word_len:
            continue
        known = counterpart_dict.get(word_norm, [])
        if not known:
            continue
        if any(cp in counterpart_words_norm for cp, _ in known):
            continue
        total_alignment_count = sum(c for _, c in known)
        if total_alignment_count < min_alignment_count:
            continue
        appearances = candidate_word_counts.get(word_norm, 0)
        if not appearances:
            continue
        alignment_frequency = total_alignment_count / appearances
        if alignment_frequency < min_frequency:
            continue
        known_str = "; ".join(
            f"{cp}({c})" for cp, c in known[:KNOWN_COUNTERPARTS_SHOWN]
        )
        orphans.append(
            {
                "missing_word": word,
                "known_counterparts": known_str,
                "score": round(min(alignment_frequency, 1.0), 4),
            }
        )
    return orphans


def detect_orphans(
    *,
    src_words: List[str],
    tgt_words: List[str],
    aligned_src: Set[int],
    aligned_tgt: Set[int],
    direction: str,
    forward_dict: Dict[str, List[Tuple[str, int]]],
    reverse_dict: Dict[str, List[Tuple[str, int]]],
    src_word_counts: Dict[str, int],
    tgt_word_counts: Dict[str, int],
    min_alignment_count: int = DEFAULT_MIN_ALIGNMENT_COUNT,
    min_frequency: float = DEFAULT_MIN_FREQUENCY,
    min_word_len: int = DEFAULT_MIN_WORD_LEN,
) -> List[dict]:
    """Detect orphans on either side (or both) of one verse pair.

    direction:
      - "target": find unaligned target words whose known source translations
        do not appear on the source side of this verse.
      - "source": symmetric — unaligned source words whose known target
        translations do not appear on the target side of this verse.
      - "both": both kinds, distinguished in the output by ``missing_side``.

    Each output dict has: ``missing_word``, ``missing_side``,
    ``known_counterparts``, ``score``.
    """
    src_norms = {n for n in (normalize_word(w) for w in src_words) if n}
    tgt_norms = {n for n in (normalize_word(w) for w in tgt_words) if n}

    out: List[dict] = []
    if direction in ("target", "both"):
        for o in _detect_orphans_one_side(
            candidate_words=tgt_words,
            aligned_indices=aligned_tgt,
            counterpart_words_norm=src_norms,
            counterpart_dict=reverse_dict,
            candidate_word_counts=tgt_word_counts,
            min_alignment_count=min_alignment_count,
            min_frequency=min_frequency,
            min_word_len=min_word_len,
        ):
            o["missing_side"] = "target"
            out.append(o)
    if direction in ("source", "both"):
        for o in _detect_orphans_one_side(
            candidate_words=src_words,
            aligned_indices=aligned_src,
            counterpart_words_norm=tgt_norms,
            counterpart_dict=forward_dict,
            candidate_word_counts=src_word_counts,
            min_alignment_count=min_alignment_count,
            min_frequency=min_frequency,
            min_word_len=min_word_len,
        ):
            o["missing_side"] = "source"
            out.append(o)
    return out

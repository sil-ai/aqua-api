"""Pure-Python eflomal verse-scoring primitives.

Ported from aqua-assessments (eflomal_steps/scoring.py and _realtime_dictionary
in assessments/word_alignment/app.py) so aqua-api can score verse pairs
server-side from stored eflomal artifacts (dictionary + cooccurrence) without
running eflomal itself. Stdlib-only (math + collections).

The same primitives will drive real-time verse-level prediction when that
endpoint is added later.
"""

import math
from collections import defaultdict
from typing import Dict, List, Tuple


def normalize_word(word: str) -> str:
    """Lowercase + keep only alphanumeric characters.

    Punctuation-only tokens collapse to the empty string; callers must skip
    those (keying dictionaries by '' would collide).
    """
    return "".join(c for c in word.lower() if c.isalnum())


def normalize_dictionary_list(
    dict_list: List[Dict],
) -> Dict[Tuple[str, str], Dict]:
    """Fold raw dictionary rows into a lookup keyed by (src_norm, tgt_norm).

    Input rows look like {"source": str, "target": str, "count": int,
    "probability": float}. Different original casings that normalize to the
    same pair are merged: counts are summed and probability is averaged
    weighted by count. Rows whose normalized form is empty on either side
    are dropped.
    """
    dictionary: Dict[Tuple[str, str], Dict] = {}
    for entry in dict_list:
        src_norm = normalize_word(entry["source"])
        tgt_norm = normalize_word(entry["target"])
        if not src_norm or not tgt_norm:
            continue
        key = (src_norm, tgt_norm)
        entry_count = entry["count"]
        entry_prob = entry.get("probability")
        if key in dictionary:
            existing = dictionary[key]
            old_count = existing["count"]
            combined_count = old_count + entry_count
            if entry_prob is not None and "probability" in existing:
                existing["probability"] = (
                    existing["probability"] * old_count + entry_prob * entry_count
                ) / combined_count
            existing["count"] = combined_count
        else:
            dictionary[key] = {"count": entry_count}
            if entry_prob is not None:
                dictionary[key]["probability"] = entry_prob
    return dictionary


def build_src_to_translations(
    dictionary: Dict[Tuple[str, str], Dict],
    min_count: int = 3,
) -> Dict[str, List[Tuple[str, int]]]:
    """Index the dictionary by source word: src_norm -> [(tgt_norm, count), ...]
    sorted by count descending.

    Entries with count < min_count are dropped to suppress noise; this mirrors
    build_reverse_dictionary() in the aqua-assessments port (same cutoff).
    Used by the greedy dictionary-lookup alignment inside score_verse_pair.
    """
    result: Dict[str, List[Tuple[str, int]]] = defaultdict(list)
    for (src_norm, tgt_norm), info in dictionary.items():
        if info["count"] >= min_count:
            result[src_norm].append((tgt_norm, info["count"]))
    for src in result:
        result[src].sort(key=lambda pair: pair[1], reverse=True)
    return dict(result)


def compute_link_score(
    src_word: str,
    tgt_word: str,
    dictionary: Dict[Tuple[str, str], Dict],
    cooccurrence: Dict[Tuple[str, str], Dict],
) -> float:
    """Confidence in [0, 1] for a single alignment link.

    When co_occur_count >= 2 (enough signal for the ratio to be meaningful),
    combine the stored model probability with the aligned/co_occur ratio via
    a weighted geometric mean (probability weight 2, co-occurrence weight 1).
    Otherwise fall back to the stored probability alone.
    """
    src_norm = normalize_word(src_word)
    tgt_norm = normalize_word(tgt_word)
    key = (src_norm, tgt_norm)

    s_prob = dictionary.get(key, {}).get("probability", 0.0)

    cooc = cooccurrence.get(key, {"co_occur": 0, "aligned": 0})
    co_occur_count = cooc["co_occur"]
    aligned_count = cooc["aligned"]

    if co_occur_count >= 2:
        s_cooc = aligned_count / co_occur_count
        log_score = (
            2.0 * math.log(max(s_prob, 1e-10)) + math.log(max(s_cooc, 1e-10))
        ) / 3.0
        return math.exp(log_score)
    return s_prob


def score_verse_pair(
    src_text: str,
    tgt_text: str,
    dictionary: Dict[Tuple[str, str], Dict],
    src_to_translations: Dict[str, List[Tuple[str, int]]],
    cooccurrence: Dict[Tuple[str, str], Dict],
) -> Dict[str, float]:
    """Score one verse pair from artifacts only (no eflomal call).

    Algorithm (port of _realtime_dictionary in aqua-assessments app.py):
      1. Split src_text / tgt_text on whitespace.
      2. Build tgt_norm -> [tgt_idx, ...] for greedy matching.
      3. For each source word in order, normalize, and walk
         src_to_translations[src_norm] (sorted by count desc). For the first
         candidate tgt_norm, take the first unmatched tgt index whose
         normalized form matches and commit the link.
      4. Score each link via compute_link_score.
      5. verse_score = mean(link_scores) * min(src_coverage, tgt_coverage).

    Returns {"verse_score", "avg_link_score", "coverage", "num_links"}.
    All zero if either side is empty or no links are found. Values are
    rounded to 4 decimals to match the aqua-assessments convention and keep
    stored scores consistent across back-end runs.
    """
    src_words = src_text.strip().split() if src_text else []
    tgt_words = tgt_text.strip().split() if tgt_text else []

    if not src_words or not tgt_words:
        return {
            "verse_score": 0.0,
            "avg_link_score": 0.0,
            "coverage": 0.0,
            "num_links": 0,
        }

    tgt_norm_to_idx: Dict[str, List[int]] = defaultdict(list)
    for j, tgt in enumerate(tgt_words):
        tgt_norm = normalize_word(tgt)
        if tgt_norm:
            tgt_norm_to_idx[tgt_norm].append(j)

    links: List[Tuple[int, int, str, str]] = []
    used_tgt: set = set()

    for i, src in enumerate(src_words):
        src_norm = normalize_word(src)
        if not src_norm:
            continue
        for tgt_norm, _count in src_to_translations.get(src_norm, []):
            matched = False
            for j in tgt_norm_to_idx.get(tgt_norm, []):
                if j not in used_tgt:
                    links.append((i, j, src, tgt_words[j]))
                    used_tgt.add(j)
                    matched = True
                    break
            if matched:
                break

    if not links:
        return {
            "verse_score": 0.0,
            "avg_link_score": 0.0,
            "coverage": 0.0,
            "num_links": 0,
        }

    link_scores = [
        compute_link_score(sw, tw, dictionary, cooccurrence) for _, _, sw, tw in links
    ]
    avg_link_score = sum(link_scores) / len(link_scores)

    aligned_src = len({i for i, _, _, _ in links})
    aligned_tgt = len({j for _, j, _, _ in links})
    src_coverage = aligned_src / len(src_words)
    tgt_coverage = aligned_tgt / len(tgt_words)
    coverage = min(src_coverage, tgt_coverage)

    verse_score = avg_link_score * coverage

    return {
        "verse_score": round(verse_score, 4),
        "avg_link_score": round(avg_link_score, 4),
        "coverage": round(coverage, 4),
        "num_links": len(links),
    }

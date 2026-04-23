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

    Input rows must have {"source": str, "target": str, "count": int,
    "probability": float}. `probability` is required — the aqua-api
    EflomalDictionary column is NOT NULL and EflomalDictionaryItem requires
    it at push time — so this function does not handle rows without it.
    Different original casings that normalize to the same pair are merged:
    counts are summed and probability is the count-weighted average. Rows
    whose normalized form is empty on either side are dropped.
    """
    dictionary: Dict[Tuple[str, str], Dict] = {}
    for entry in dict_list:
        src_norm = normalize_word(entry["source"])
        tgt_norm = normalize_word(entry["target"])
        if not src_norm or not tgt_norm:
            continue
        key = (src_norm, tgt_norm)
        entry_count = entry["count"]
        entry_prob = entry["probability"]
        if key in dictionary:
            existing = dictionary[key]
            old_count = existing["count"]
            combined_count = old_count + entry_count
            existing["probability"] = (
                existing["probability"] * old_count + entry_prob * entry_count
            ) / combined_count
            existing["count"] = combined_count
        else:
            dictionary[key] = {"count": entry_count, "probability": entry_prob}
    return dictionary


def build_src_to_translations(
    dictionary: Dict[Tuple[str, str], Dict],
    min_count: int = 1,
) -> Dict[str, List[Tuple[str, int]]]:
    """Index the dictionary by source word: src_norm -> [(tgt_norm, count), ...]
    sorted by count descending.

    Default `min_count=1` keeps every pair; matches the inline index the
    aqua-assessments reference builds in `_realtime_dictionary`, which
    applies no cutoff. Callers that want denoising (e.g. missing-words
    detection) can pass a higher threshold explicitly.
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
) -> Dict[str, float | int]:
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

    Coverage denominators use the count of non-empty-normalized tokens
    (i.e. tokens that were actually candidates for alignment), not the raw
    whitespace-split length, so punctuation-only tokens don't silently
    deflate the score.
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
    tgt_alignable = 0
    for j, tgt in enumerate(tgt_words):
        tgt_norm = normalize_word(tgt)
        if tgt_norm:
            tgt_norm_to_idx[tgt_norm].append(j)
            tgt_alignable += 1

    links: List[Tuple[int, int, str, str]] = []
    used_tgt: set = set()
    src_alignable = 0

    for i, src in enumerate(src_words):
        src_norm = normalize_word(src)
        if not src_norm:
            continue
        src_alignable += 1
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
    src_coverage = aligned_src / src_alignable
    tgt_coverage = aligned_tgt / tgt_alignable
    coverage = min(src_coverage, tgt_coverage)

    verse_score = avg_link_score * coverage

    return {
        "verse_score": round(verse_score, 4),
        "avg_link_score": round(avg_link_score, 4),
        "coverage": round(coverage, 4),
        "num_links": len(links),
    }


def build_reverse_dictionary(
    dictionary: Dict[Tuple[str, str], Dict],
    min_count: int = 3,
) -> Dict[str, List[Tuple[str, int]]]:
    """Index the dictionary by target word: tgt_norm -> [(src_norm, count), ...]
    sorted by count descending.

    Only pairs with count >= min_count are kept to filter low-signal noise.
    Used by detect_missing_words_for_verse to find whether a target word has
    known source equivalents that are absent from the current verse.
    """
    result: Dict[str, List[Tuple[str, int]]] = defaultdict(list)
    for (src_norm, tgt_norm), info in dictionary.items():
        if info["count"] >= min_count:
            result[tgt_norm].append((src_norm, info["count"]))
    for tgt in result:
        result[tgt].sort(key=lambda pair: pair[1], reverse=True)
    return dict(result)


def detect_missing_words_for_verse(
    src_text: str,
    tgt_text: str,
    reverse_dict: Dict[str, List[Tuple[str, int]]],
    tgt_word_counts: Dict[str, int],
    min_alignment_count: int = 10,
    min_frequency: float = 0.5,
    min_word_len: int = 3,
    aligned_tgt_indices: set = None,
) -> List[Dict]:
    """Detect target words that are likely missing translations.

    A target word is flagged as potentially missing if:
      1. It was not aligned to any source word in this verse.
      2. The dictionary contains known source equivalents for it
         (reverse_dict lookup).
      3. None of those known source equivalents appear in the source verse —
         i.e. the source side of the "usual translation" is absent, so the
         target word is unexpectedly present without a counterpart.
      4. The word meets minimum corpus frequency and alignment-frequency
         thresholds to filter noise.

    Port of detect_missing_words_single from aqua-assessments scoring.py.

    Args:
        src_text: Source (reference) verse text.
        tgt_text: Target (revision) verse text.
        reverse_dict: tgt_norm -> [(src_norm, count), ...] (see
            build_reverse_dictionary).
        tgt_word_counts: Corpus-wide normalized target word counts from
            EflomalTargetWordCount (used to compute alignment_frequency).
        min_alignment_count: Minimum total alignment count across the corpus
            for the target word to be considered signal-rich enough to flag.
        min_frequency: Minimum alignment_frequency (aligned_count /
            total_appearances) required to flag a word.
        min_word_len: Minimum normalized word length to consider.
        aligned_tgt_indices: Set of target word indices that were already
            aligned by score_verse_pair. Defaults to empty set (treat
            all target words as unaligned).

    Returns:
        List of dicts with keys: target_word, known_sources (str),
        score (float, rounded to 4 dp).
    """
    if aligned_tgt_indices is None:
        aligned_tgt_indices = set()

    tgt_words = tgt_text.strip().split() if tgt_text else []
    src_words = src_text.strip().split() if src_text else []
    src_norms = {normalize_word(w) for w in src_words if normalize_word(w)}

    missing: List[Dict] = []
    for j, tgt_word in enumerate(tgt_words):
        if j in aligned_tgt_indices:
            continue

        tgt_norm = normalize_word(tgt_word)
        if not tgt_norm or len(tgt_norm) < min_word_len:
            continue

        known_sources = reverse_dict.get(tgt_norm, [])
        if not known_sources:
            continue

        # If any known source equivalent is present in the source verse,
        # the word is not "missing" — it just wasn't aligned.
        if any(s in src_norms for s, _ in known_sources):
            continue

        total_count = sum(c for _, c in known_sources)
        if total_count < min_alignment_count:
            continue

        total_appearances = tgt_word_counts.get(tgt_norm, 0)
        if total_appearances == 0:
            continue
        alignment_frequency = total_count / total_appearances
        if alignment_frequency < min_frequency:
            continue

        known_str = "; ".join(f"{s}({c})" for s, c in known_sources[:5])
        missing.append(
            {
                "target_word": tgt_word,
                "known_sources": known_str,
                "score": round(min(alignment_frequency, 1.0), 4),
            }
        )

    return missing

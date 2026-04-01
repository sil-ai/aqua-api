import math
from collections import defaultdict
from dataclasses import dataclass


def normalize_word(word: str) -> str:
    """Lowercase and strip all non-alphanumeric characters."""
    word = word.lower()
    word = "".join(c for c in word if c.isalnum())
    return word


@dataclass
class PreparedArtifacts:
    dictionary: dict  # {(src_norm, tgt_norm): {"count": int, "probability": float}}
    cooccurrences: dict  # {(src_norm, tgt_norm): {"co_occur": int, "aligned": int}}
    target_word_counts: dict  # {word_norm: count}
    reverse_dict: dict  # {tgt_norm: [(src_norm, count), ...]} sorted by count desc
    src_to_translations: (
        dict  # {src_norm: [(tgt_norm, count), ...]} sorted by count desc
    )


def prepare_artifacts(
    dictionary_rows, cooccurrence_rows, target_word_count_rows
) -> PreparedArtifacts:
    """Transform DB rows into PreparedArtifacts ready for scoring.

    EflomalDictionary rows store words in original (non-normalized) form.
    EflomalCooccurrence and EflomalTargetWordCount rows already store normalized forms.
    """
    # Normalize dictionary keys; merge duplicate normalized pairs via weighted avg
    dictionary: dict = {}
    for row in dictionary_rows:
        src_norm = normalize_word(row.source_word)
        tgt_norm = normalize_word(row.target_word)
        if not src_norm or not tgt_norm:
            continue
        key = (src_norm, tgt_norm)
        if key in dictionary:
            existing = dictionary[key]
            old_count = existing["count"]
            new_count = old_count + row.count
            existing["probability"] = (
                existing["probability"] * old_count + row.probability * row.count
            ) / new_count
            existing["count"] = new_count
        else:
            dictionary[key] = {"count": row.count, "probability": row.probability}

    # Cooccurrence words are already stored in normalized form in the DB
    cooccurrences = {
        (row.source_word, row.target_word): {
            "co_occur": row.co_occur_count,
            "aligned": row.aligned_count,
        }
        for row in cooccurrence_rows
    }

    target_word_counts = {row.word: row.count for row in target_word_count_rows}

    reverse_dict = _build_reverse_dictionary(dictionary, min_count=3)
    src_to_translations = _build_src_to_translations(dictionary)

    return PreparedArtifacts(
        dictionary=dictionary,
        cooccurrences=cooccurrences,
        target_word_counts=target_word_counts,
        reverse_dict=reverse_dict,
        src_to_translations=src_to_translations,
    )


def _build_reverse_dictionary(dictionary: dict, min_count: int = 3) -> dict:
    """Build reverse lookup: tgt_norm -> [(src_norm, count), ...] sorted desc by count."""
    reverse: dict = defaultdict(list)
    for (src, tgt), info in dictionary.items():
        if info["count"] >= min_count:
            reverse[tgt].append((src, info["count"]))
    for tgt in reverse:
        reverse[tgt].sort(key=lambda x: x[1], reverse=True)
    return dict(reverse)


def _build_src_to_translations(dictionary: dict) -> dict:
    """Build forward lookup: src_norm -> [(tgt_norm, count), ...] sorted desc by count."""
    src_to_translations: dict = defaultdict(list)
    for (src_norm, tgt_norm), info in dictionary.items():
        src_to_translations[src_norm].append((tgt_norm, info["count"]))
    for src in src_to_translations:
        src_to_translations[src].sort(key=lambda x: x[1], reverse=True)
    return dict(src_to_translations)


def compute_link_score(
    src_word: str,
    tgt_word: str,
    dictionary: dict,
    cooccurrences: dict,
) -> float:
    """Score an alignment link using model probability and co-occurrence consistency.

    When co-occurrence data is available (co_occur >= 2), combines both signals
    via weighted geometric mean: exp((2*log(prob) + log(aligned/co_occur)) / 3).
    Falls back to stored probability alone otherwise.
    """
    src_norm = normalize_word(src_word)
    tgt_norm = normalize_word(tgt_word)
    key = (src_norm, tgt_norm)

    dict_info = dictionary.get(key, {})
    s_prob = dict_info.get("probability", 0.0)

    cooccur_info = cooccurrences.get(key, {"co_occur": 0, "aligned": 0})
    co_occur_count = cooccur_info["co_occur"]
    aligned_count = cooccur_info["aligned"]

    if co_occur_count >= 2:
        s_cooc = aligned_count / co_occur_count
        log_score = (
            2.0 * math.log(max(s_prob, 1e-10)) + math.log(max(s_cooc, 1e-10))
        ) / 3.0
        return math.exp(log_score)
    else:
        return s_prob


def score_verse_pair(text1: str, text2: str, artifacts: PreparedArtifacts) -> dict:
    """Score a single verse pair using trained eflomal artifacts.

    Returns a dict with verse_score, avg_link_score, coverage, alignment_links,
    and missing_words.
    """
    src_words = text1.strip().split()
    tgt_words = text2.strip().split()

    if not src_words or not tgt_words:
        return {
            "verse_score": 0.0,
            "avg_link_score": 0.0,
            "coverage": 0.0,
            "alignment_links": [],
            "missing_words": [],
        }

    # Build target index: normalized form -> [position, ...]
    tgt_norm_to_idx: dict = defaultdict(list)
    for j, tgt in enumerate(tgt_words):
        tgt_norm = normalize_word(tgt)
        if tgt_norm:
            tgt_norm_to_idx[tgt_norm].append(j)

    # Greedy left-to-right matching
    links = []  # (src_idx, tgt_idx, src_word, tgt_word)
    used_tgt: set = set()

    for i, src in enumerate(src_words):
        src_norm = normalize_word(src)
        if not src_norm:
            continue
        candidates = artifacts.src_to_translations.get(src_norm, [])
        for tgt_norm, _count in candidates:
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
            "alignment_links": [],
            "missing_words": _detect_missing_words(
                tgt_words,
                set(),
                artifacts.reverse_dict,
                src_words,
                artifacts.target_word_counts,
            ),
        }

    # Score each matched link
    raw_scores = []
    scored_links = []
    for _i, _j, src_word, tgt_word in links:
        raw_score = compute_link_score(
            src_word=src_word,
            tgt_word=tgt_word,
            dictionary=artifacts.dictionary,
            cooccurrences=artifacts.cooccurrences,
        )
        raw_scores.append(raw_score)
        scored_links.append(
            {
                "source_word": src_word,
                "target_word": tgt_word,
                "score": round(raw_score, 4),
            }
        )

    avg_link_score = sum(raw_scores) / len(raw_scores)

    aligned_src_count = len({i for i, _, _, _ in links})
    aligned_tgt_count = len({j for _, j, _, _ in links})
    src_cov = aligned_src_count / len(src_words)
    tgt_cov = aligned_tgt_count / len(tgt_words)
    coverage = min(src_cov, tgt_cov)
    verse_score = avg_link_score * coverage

    aligned_tgt_set = {j for _, j, _, _ in links}
    missing = _detect_missing_words(
        tgt_words,
        aligned_tgt_set,
        artifacts.reverse_dict,
        src_words,
        artifacts.target_word_counts,
    )

    return {
        "verse_score": round(verse_score, 4),
        "avg_link_score": round(avg_link_score, 4),
        "coverage": round(coverage, 4),
        "alignment_links": scored_links,
        "missing_words": missing,
    }


def _detect_missing_words(
    tgt_words: list,
    aligned_tgt_set: set,
    reverse_dict: dict,
    src_words: list,
    target_word_counts: dict,
    min_alignment_count: int = 10,
    min_frequency: float = 0.5,
) -> list:
    """Detect unaligned target words that likely represent missing source content."""
    src_norms = {normalize_word(w) for w in src_words if normalize_word(w)}
    missing = []

    for j, tgt_word in enumerate(tgt_words):
        if j in aligned_tgt_set:
            continue

        tgt_norm = normalize_word(tgt_word)
        if not tgt_norm or len(tgt_norm) < 3:
            continue

        known_sources = reverse_dict.get(tgt_norm, [])
        if not known_sources:
            continue

        # If any known source translation is present in the source, it's not missing
        if any(s in src_norms for s, _ in known_sources):
            continue

        total_count = sum(c for _, c in known_sources)
        if total_count < min_alignment_count:
            continue

        total_appearances = target_word_counts.get(tgt_norm)
        if not total_appearances:
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

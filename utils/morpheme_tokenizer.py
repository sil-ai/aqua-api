"""Viterbi morpheme segmenter.

Pure-Python, no API or LLM dependency. Given a morpheme inventory for a
language, segments arbitrary text into the minimum-cost sequence of known
morphemes and "gap" chunks (any span the morpheme set doesn't cover).

Ported from aqua-assessments/shared/morpheme_tokenizer.py.
"""

from __future__ import annotations

import unicodedata


def strip_punct(word: str) -> str:
    """Strip leading/trailing Unicode punctuation from a word."""
    start = 0
    while start < len(word) and unicodedata.category(word[start]).startswith("P"):
        start += 1
    end = len(word)
    while end > start and unicodedata.category(word[end - 1]).startswith("P"):
        end -= 1
    return word[start:end]


MORPH_CHAR_COST = 0.5
GAP_CHAR_COST = 1.0
TOKEN_COST = 1.0


def viterbi_segment(
    word: str,
    morpheme_set: set[str],
    max_morph_len: int,
) -> list[tuple[str, str]]:
    """Minimum-cost segmentation of `word` into morphemes and gaps.

    Returns a list of (kind, text) where kind is 'morph' or 'gap'.
    """
    n = len(word)
    if n == 0:
        return []
    INF = float("inf")
    best = [INF] * (n + 1)
    back: list[tuple[int, str] | None] = [None] * (n + 1)
    best[0] = 0.0

    for i in range(1, n + 1):
        for length in range(1, min(max_morph_len, i) + 1):
            j = i - length
            candidate = word[j:i]
            if candidate in morpheme_set:
                cost = best[j] + MORPH_CHAR_COST * length + TOKEN_COST
                if cost < best[i]:
                    best[i] = cost
                    back[i] = (j, "morph")
        for gap_len in range(1, i + 1):
            j = i - gap_len
            cost = best[j] + GAP_CHAR_COST * gap_len + TOKEN_COST
            if cost < best[i]:
                best[i] = cost
                back[i] = (j, "gap")

    segments: list[tuple[str, str]] = []
    i = n
    while i > 0:
        entry = back[i]
        if entry is None:
            raise ValueError(
                f"Viterbi backtrack broken at position {i} in word '{word}'"
            )
        j, kind = entry
        segments.append((kind, word[j:i]))
        i = j
    return list(reversed(segments))

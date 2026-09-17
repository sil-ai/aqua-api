r"""Unicode-correct word tokenization for the TF-IDF word analyzer.

Ported from ``aqua-assessments/assessments/tfidf/tfidf_steps/tokenizer.py``
(sil-ai/aqua-assessments#472). **The two copies must stay byte-for-byte
equivalent in behaviour.** Only the fitted vocabulary and IDF cross between
the repos, as JSONB — never the tokenizer object — so the two definitions
agree by construction or not at all. ``test_tfidf_tokenizer.py`` pins the
shared fixtures that make a drift visible; the runner has the mirror of it.

scikit-learn's default word ``token_pattern`` (``(?u)\b\w\w+\b``) compiles
with the stdlib ``re`` module, whose ``\w`` does not match Unicode combining
marks (categories ``Mn``/``Mc``). Every combining mark therefore reads as a
word boundary, so words in Brahmic scripts fragment mid-word: Devanagari
"प्रथम" ("first") tokenizes as "रथम", losing its first syllable to the virama
(U+094D). Worse than fragmenting, a word whose every consonant carries a
vowel sign has no two adjacent ``\w`` characters at all and disappears
outright — Marathi "देव" ("God") matched nothing. See issue #969 here and
sil-ai/aqua-assessments#470 for the measurements.

``token_pattern`` is hardcoded to compile with ``re``, so a Unicode-correct
pattern cannot be swapped in that way; the ``tokenizer=`` callable is the
only hook. The third-party ``regex`` package's ``\w`` does match combining
marks.

This also drops sklearn's implicit two-character minimum, since ``\w+`` keeps
single-character tokens that ``\w\w+`` discards. That is deliberate:
single-character words are real and common in these scripts (Marathi "व",
"and"), and it is also what stops the default pattern gluing a dropped
one-letter token's neighbours into a bigram that never occurs in the text
("Aaron's head" was being recorded as the phrase "aaron head").

Only the *word* analyzer takes this. ``char_wb`` splits on whitespace and
never consults ``\w``, so it was never affected, and sklearn warns when
handed a tokenizer it will not use.
"""

import regex

# Precompiled: the query side tokenizes on every `by_text`/`by_texts` call,
# and the runner tokenizes every verse of a whole Bible at fit time.
# VERSION1 is not load-bearing for a bare `\w+` -- it pins the semantics so a
# later pattern change can't silently mean something different.
_WORD_RE = regex.compile(r"\w+", flags=regex.UNICODE | regex.VERSION1)


def unicode_word_tokenizer(text: str) -> list[str]:
    """Split `text` into word tokens, keeping combining marks attached."""
    return _WORD_RE.findall(text)

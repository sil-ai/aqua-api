"""Regression tests for the query-side Unicode word tokenizer — issue #969.

Two things are pinned here.

**The tokenizer itself**, against fixtures shared with the runner. Only the
fitted vocabulary and IDF cross between ``aqua-api`` and ``aqua-assessments``,
as JSONB — never the tokenizer object — so the two definitions agree by
construction or not at all, and a drift shows up as retrieval quietly getting
worse rather than as an error. ``_SHARED_FIXTURES`` is the same corpus and the
same expected tokens as
``aqua-assessments/assessments/tfidf/tfidf_steps/test_tokenizer.py``
(sil-ai/aqua-assessments#472); changing one side alone fails that side's copy.

**The rehydration path**, which is the half this repo owns:
``_rehydrate_encoder`` must hand the word analyzer the same tokenizer the fit
used, or a Devanagari query lands on none of the stored terms. Every assertion
below fails against scikit-learn's default ``token_pattern``.
"""

import io
import re

import numpy as np
import pytest
from sklearn.feature_extraction.text import TfidfVectorizer

from assessment_routes.v3.tfidf_artifact_routes import _rehydrate_encoder
from utils.tfidf_tokenizer import unicode_word_tokenizer

# sklearn's default word `token_pattern`, quoted so the tests assert what this
# fix actually changes rather than trusting the description.
_SKLEARN_DEFAULT_TOKEN_PATTERN = r"(?u)\b\w\w+\b"

_MARATHI_CORPUS = [
    "सुरुवातीला देवाने आकाश व पृथ्वी ही निर्माण केली",
    "देव म्हणाला प्रकाश होवो आणि प्रकाश झाला",
    "देवाने प्रकाशाला दिवस म्हटले आणि अंधाराला रात्र म्हटले",
    "पृथ्वीवर पाणी होते आणि अंधार होता",
    "प्रथम दिवस संपला आणि रात्र आली",
]

# Kept literal, not computed from the corpus: a computed expectation would
# follow the tokenizer wherever it drifted. These are the bytes both repos
# must agree on.
_SHARED_FIXTURES = [
    (
        "सुरुवातीला देवाने आकाश व पृथ्वी ही निर्माण केली",
        ["सुरुवातीला", "देवाने", "आकाश", "व", "पृथ्वी", "ही", "निर्माण", "केली"],
    ),
    (
        "देव म्हणाला प्रकाश होवो आणि प्रकाश झाला",
        ["देव", "म्हणाला", "प्रकाश", "होवो", "आणि", "प्रकाश", "झाला"],
    ),
    (
        "देवाने प्रकाशाला दिवस म्हटले आणि अंधाराला रात्र म्हटले",
        ["देवाने", "प्रकाशाला", "दिवस", "म्हटले", "आणि", "अंधाराला", "रात्र", "म्हटले"],
    ),
    (
        "पृथ्वीवर पाणी होते आणि अंधार होता",
        ["पृथ्वीवर", "पाणी", "होते", "आणि", "अंधार", "होता"],
    ),
    (
        "प्रथम दिवस संपला आणि रात्र आली",
        ["प्रथम", "दिवस", "संपला", "आणि", "रात्र", "आली"],
    ),
]

_QUERY = "देवाने प्रकाश निर्माण केली"

# The word vectorizer `create_pca_vectors` fits, minus `max_df` — the real
# 0.12 would drop every term from a five-verse corpus.
_WORD_PARAMS = {
    "analyzer": "word",
    "ngram_range": (1, 2),
    "max_df": 0.5,
    "min_df": 2,
    "lowercase": True,
}
_CHAR_PARAMS = {
    "analyzer": "char_wb",
    "ngram_range": (3, 6),
    "max_df": 0.5,
    "min_df": 2,
    "lowercase": True,
}


def _fit(params, **overrides):
    """Fit a vectorizer on the Marathi corpus the way the runner does."""
    kwargs = dict(params)
    if params["analyzer"] == "word":
        kwargs.update(tokenizer=unicode_word_tokenizer, token_pattern=None)
    kwargs.update(overrides)
    vectorizer = TfidfVectorizer(**kwargs)
    vectorizer.fit(_MARATHI_CORPUS * 4)
    return vectorizer


def _artifact(vectorizer):
    """The (vocabulary, idf, params) triple `_rehydrate_encoder` is handed.

    Mirrors `vectorizer_payload` in aqua-assessments: `params` records no
    tokenizer, which is why rehydration has to supply one of its own.
    """
    ngram_range = vectorizer.ngram_range
    return (
        dict(vectorizer.vocabulary_),
        vectorizer.idf_.tolist(),
        {
            "analyzer": vectorizer.analyzer,
            "ngram_range": [int(ngram_range[0]), int(ngram_range[1])],
            "lowercase": bool(vectorizer.lowercase),
            "max_df": vectorizer.max_df,
            "min_df": vectorizer.min_df,
        },
    )


def _svd_artifact(n_features, n_components=2):
    """A minimal (components_npy, n_components) pair — `_rehydrate_encoder`
    rebuilds the SVD too, and only the vectorizers are under test here."""
    buf = io.BytesIO()
    np.save(buf, np.zeros((n_components, n_features), dtype="float32"))
    return buf.getvalue(), n_components


@pytest.mark.parametrize("text, expected", _SHARED_FIXTURES)
def test_shared_fixtures_tokenize_identically_in_both_repos(text, expected):
    assert unicode_word_tokenizer(text) == expected


@pytest.mark.parametrize("text, expected", _SHARED_FIXTURES)
def test_shared_fixtures_would_not_survive_the_default_pattern(text, expected):
    """The other half of the drift guard: if the fixtures stopped exercising
    the bug, agreeing on them would prove nothing."""
    assert re.findall(_SKLEARN_DEFAULT_TOKEN_PATTERN, text) != expected


def test_devanagari_word_keeps_its_combining_marks():
    """प्रथम ("first") lost its first syllable to the virama (U+094D) under
    the default pattern — the exact symptom #969 quotes."""
    assert unicode_word_tokenizer("प्रथम") == ["प्रथम"]
    assert re.findall(_SKLEARN_DEFAULT_TOKEN_PATTERN, "प्रथम") == ["रथम"]


def test_devanagari_words_are_not_dropped_entirely():
    """Worse than fragmentation: a word whose every consonant carries a vowel
    sign has no two adjacent `\\w` characters at all, so `\\b\\w\\w+\\b` matched
    nothing and the word vanished."""
    for word in ("देव", "पृथ्वी", "निर्माण", "केली"):
        assert unicode_word_tokenizer(word) == [word]
        assert re.findall(_SKLEARN_DEFAULT_TOKEN_PATTERN, word) == []


def test_single_character_words_are_kept():
    """`\\w+` drops sklearn's implicit two-character minimum on purpose:
    Marathi "व" ("and") is a real word the default pattern discarded."""
    assert unicode_word_tokenizer("व") == ["व"]
    assert unicode_word_tokenizer("देव व पृथ्वी") == ["देव", "व", "पृथ्वी"]


def test_ascii_text_is_unaffected():
    assert unicode_word_tokenizer("In the beginning God created") == [
        "In",
        "the",
        "beginning",
        "God",
        "created",
    ]


def test_punctuation_and_whitespace_are_not_tokens():
    assert unicode_word_tokenizer("God said, 'Let there be light.'") == [
        "God",
        "said",
        "Let",
        "there",
        "be",
        "light",
    ]
    assert unicode_word_tokenizer("") == []
    assert unicode_word_tokenizer("   \n\t  ") == []


def test_rehydrated_word_vectorizer_matches_the_fitted_one():
    """The fix. A query encoded by the rehydrated encoder must land on the
    same features the fit produced, or `by_text`/`by_texts` retrieve nothing
    for these languages."""
    word, char = _fit(_WORD_PARAMS), _fit(_CHAR_PARAMS)
    word_vec, char_vec, _ = _rehydrate_encoder(
        _artifact(word),
        _artifact(char),
        _svd_artifact(len(word.vocabulary_) + len(char.vocabulary_)),
    )

    from_fit = word.transform([_QUERY])
    assert from_fit.nnz > 0, "query should hit the corpus vocabulary at all"
    assert (from_fit != word_vec.transform([_QUERY])).nnz == 0
    # `char_wb` splits on whitespace and never consults `\w`, so it was never
    # affected — and sklearn warns if handed a tokenizer it will not use.
    assert char_vec.tokenizer is None
    assert (char.transform([_QUERY]) != char_vec.transform([_QUERY])).nnz == 0


def test_without_the_fix_a_devanagari_query_matches_nothing():
    """Pins down *why* rehydration sets the tokenizer: the mismatch does not
    raise, it returns an empty vector. Without this fix, that is what every
    Devanagari query gets once the runner's #472 lands."""
    word = _fit(_WORD_PARAMS)
    vocabulary, idf, params = _artifact(word)
    mismatched = TfidfVectorizer(
        analyzer=params["analyzer"],
        ngram_range=tuple(params["ngram_range"]),
        lowercase=params["lowercase"],
        max_df=params["max_df"],
        min_df=params["min_df"],
        vocabulary=vocabulary,
    )
    mismatched.idf_ = np.asarray(idf, dtype=float)

    assert mismatched.transform([_QUERY]).nnz == 0
    assert word.transform([_QUERY]).nnz > 0

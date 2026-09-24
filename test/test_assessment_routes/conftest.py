import pytest

from assessment_routes.v4 import tfidf_retrieval


@pytest.fixture(autouse=True)
def _clear_similar_verses_caches():
    """Empty the v4 similar-verses caches around every test.

    Both are process-global: the recipe cache and the corpus index cache are keyed on the
    revision, and a test that leaves an entry behind decides whether the next test's
    request is cold or warm — which changes its statement count, and on the index path
    which code builds the answer. Cleared before and after so a test sees neither its
    predecessor's entries nor leaves its own.
    """
    tfidf_retrieval._RECIPE_CACHE.clear()
    tfidf_retrieval.clear_corpus_indexes()
    yield
    tfidf_retrieval._RECIPE_CACHE.clear()
    tfidf_retrieval.clear_corpus_indexes()

import pytest

from assessment_routes.v4 import tfidf_retrieval


@pytest.fixture(autouse=True)
def _clear_tfidf_caches():
    """Empty the process-global TF-IDF recipe and corpus index caches around every test.

    The training-session results read ranks neighbours through the same index as the
    similar-verses POST (#978), keyed on the revision. Revision ids are fresh per test
    here, but a build still in flight when a test ends could otherwise land in the next
    one, so this clears before and after, as ``test_assessment_routes/conftest.py`` does.
    """
    tfidf_retrieval._RECIPE_CACHE.clear()
    tfidf_retrieval.clear_corpus_indexes()
    yield
    tfidf_retrieval._RECIPE_CACHE.clear()
    tfidf_retrieval.clear_corpus_indexes()

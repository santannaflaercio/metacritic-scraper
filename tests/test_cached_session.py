import pytest
import requests_cache
from requests.exceptions import ConnectionError


@pytest.fixture
def cached_session():
    """Fixture to create a CachedSession."""
    session = requests_cache.CachedSession('cache/movie_cache', expire_after=7200)
    yield session
    session.cache.clear()


def test_cached_session_creation(cached_session):
    """Test the creation of a CachedSession object."""
    assert isinstance(cached_session, requests_cache.CachedSession)


def test_cached_session_caching(cached_session):
    """Test if the CachedSession object caches requests."""
    url = 'http://example.com'
    # Making the first request and caching it
    try:
        response_1 = cached_session.get(url)
        assert response_1.from_cache is False
    except ConnectionError:
        pytest.skip("Internet connection required for this test.")

    # Making the second request, should be from cache
    response_2 = cached_session.get(url)
    assert response_2.from_cache

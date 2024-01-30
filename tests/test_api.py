# test_api.py
import pytest
from api.app import app


@pytest.fixture
def client():
    with app.test_client() as client:
        yield client  # This allows the client object to be used in the tests


def test_movies_returns_all_movies(client, monkeypatch):
    """Test if the /api/movies endpoint returns all movies."""

    def mock_execute_query(query):
        return [{"_id": "1", "title": "Movie 1"}, {"_id": "2", "title": "Movie 2"}]

    monkeypatch.setattr("api.app.execute_query", mock_execute_query)

    response = client.get("/api/movies")
    assert response.status_code == 200
    assert response.json == [{"_id": "1", "title": "Movie 1"}, {"_id": "2", "title": "Movie 2"}]


def test_movie_by_id_returns_correct_movie(client, monkeypatch):
    """Test if the /api/movies/<_id> endpoint returns the correct movie."""

    def mock_execute_query(query):
        return [{"_id": "1", "title": "Movie 1"}]

    monkeypatch.setattr("api.app.execute_query", mock_execute_query)

    response = client.get("/api/movies/1")
    assert response.status_code == 200
    assert response.json == [{"_id": "1", "title": "Movie 1"}]


def test_movie_by_id_returns_404_for_nonexistent_id(client, monkeypatch):
    """Test if the /api/movies/<_id> endpoint returns 404 for a nonexistent _id."""

    def mock_execute_query(query):
        return None

    monkeypatch.setattr("api.app.execute_query", mock_execute_query)

    response = client.get("/api/movies/999")
    assert response.status_code == 404


def test_movie_by_title_returns_correct_movie(client, monkeypatch):
    """Test if the /api/movies/title/<title> endpoint returns the correct movie."""

    def mock_execute_query(query):
        return [{"_id": "1", "title": "Movie 1"}]

    monkeypatch.setattr("api.app.execute_query", mock_execute_query)

    response = client.get("/api/movies/title/Movie%201")
    assert response.status_code == 200
    assert response.json == [{"_id": "1", "title": "Movie 1"}]


def test_movie_by_title_returns_404_for_nonexistent_title(client, monkeypatch):
    """Test if the /api/movies/title/<title> endpoint returns 404 for a nonexistent title."""

    def mock_execute_query(query):
        return None

    monkeypatch.setattr("api.app.execute_query", mock_execute_query)

    response = client.get("/api/movies/title/Nonexistent%20Movie")
    assert response.status_code == 404

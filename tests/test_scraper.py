from concurrent.futures import Future
from unittest.mock import patch, Mock, MagicMock

import pandas as pd
import pytest
from bs4 import BeautifulSoup

import scraper

# Constants for test data
TEST_URL = "https://example.com"


def setup_mock_session(mock_session):
    """
    Configure the mock session object, setting up methods and return values.
    """
    mock_response = mock_session.get.return_value
    mock_response.raise_for_status.return_value = None
    mock_response.content = "<html><head><title>Hello, World!</title></head><body><h1>Hello, World!</h1></body></html>"
    mock_response.encoding = "utf-8"
    return mock_response


@patch("scraper.session")
def test_get_page_content_success(mock_session):
    """
    Test the `get_page_content` function by making sure it retrieves the content
    correctly from a mock session and that the session's methods are called as expected.
    """
    mock_response = setup_mock_session(mock_session)

    soup = scraper.get_page_content(TEST_URL)

    mock_session.get.assert_called_once_with(TEST_URL)
    mock_response.raise_for_status.assert_called_once()
    assert soup.title.string == "Hello, World!"


@pytest.fixture
def movie_html():
    html_content = """
            <div class="c-finderProductCard">
                <h3 class="c-finderProductCard_titleHeading">Movie Title</h3>
                <div class="c-finderProductCard_meta">Release Date: 2022-01-01</div>
                <span class="c-finderProductCard_score">85</span>
            </div>
    """
    return BeautifulSoup(html_content, "html.parser")


def test_scraper_returns_correct_movie_data(movie_html):
    result = scraper.get_movie_data(movie_html)
    expected_movie_data = ["MOVIE TITLE", 2022, 85]
    assert result == expected_movie_data


def test_scrape_page():
    mock_movie_cards = [Mock(), Mock()]
    soup = Mock()
    soup.find_all.return_value = mock_movie_cards
    mock_movie_data = {"title": "test movie", "rating": "50"}

    with patch('scraper.get_page_content', return_value=soup) as mock_get_page_content:
        with patch('scraper.get_movie_data', return_value=mock_movie_data) as mock_get_movie_data:
            page_number = 1
            result = scraper.scrape_page(page_number)
            mock_get_page_content.assert_called_once_with(
                f"{scraper.BASE_URL}?page={page_number}")
            assert mock_get_movie_data.call_count == len(mock_movie_cards)
            assert result == [mock_movie_data for _ in mock_movie_cards]


def test_write_to_csv(tmp_path):
    def write_to_csv(m):
        try:
            existing_df = pd.read_csv(tmpfile)
        except FileNotFoundError:
            existing_df = pd.DataFrame(columns=["name", "year", "metascore"])

        new_df = pd.DataFrame(m, columns=["name", "year", "metascore"])

        combined_df = pd.concat([existing_df, new_df])

        final_df = combined_df.drop_duplicates(
            subset=["name", "year"], keep="first")
        final_df = final_df.sort_values(by=["metascore"], ascending=False)
        final_df.to_csv(tmpfile, index=False)

    tmpfile = tmp_path / "movies.csv"

    new_movies = [
        {"name": "Movie1", "year": 2001, "metascore": 75},
        {"name": "Movie2", "year": 2002, "metascore": 85}
    ]

    write_to_csv(new_movies)

    # Make assertions
    df = pd.read_csv(tmpfile)

    assert len(df) == 2
    assert df.loc[0]["name"] == "Movie2"
    assert df.loc[1]["name"] == "Movie1"
    assert df.loc[0]["year"] == 2002
    assert df.loc[1]["year"] == 2001
    assert df.loc[0]["metascore"] == 85
    assert df.loc[1]["metascore"] == 75


def test_get_total_pages():
    mock_soup = Mock()
    mock_pagination = [Mock(), Mock()]
    mock_soup.find_all.return_value = mock_pagination
    mock_pagination[-1].text = "10"

    with patch('scraper.get_page_content', return_value=mock_soup) as mock_get_page_content:
        result = scraper.get_total_pages()
        mock_get_page_content.assert_called_once_with(scraper.BASE_URL)
        assert result == 11


def test_average_score_per_year():
    df = pd.DataFrame([
        {"name": "Movie1", "year": 2001, "metascore": 75},
        {"name": "Movie2", "year": 2002, "metascore": 85},
        {"name": "Movie3", "year": 2001, "metascore": 95},
    ])

    result = scraper.average_score_per_year(df)
    assert result.loc[2001] == 85
    assert result.loc[2002] == 85


def test_count_movies_per_year():
    df = pd.DataFrame([
        {"name": "Movie1", "year": 2001, "metascore": 75},
        {"name": "Movie2", "year": 2002, "metascore": 85},
        {"name": "Movie3", "year": 2001, "metascore": 95},
    ])

    result = scraper.top_rated_per_year(df)
    assert result.loc[2001] == 2
    assert result.loc[2002] == 1

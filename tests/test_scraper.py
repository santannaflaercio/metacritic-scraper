import unittest
from unittest.mock import patch, MagicMock

from requests.exceptions import HTTPError

from src.scraper import Scraper, MovieScraper


class TestScraper(unittest.TestCase):
    @patch('src.scraper.requests_cache.CachedSession')
    def test_get_page_content_successful_request(self, mock_session):
        scraper = Scraper()
        mock_session.return_value.get.return_value.raise_for_status.return_value = None
        mock_session.return_value.get.return_value.content = b'<html></html>'
        result = scraper.get_page_content('https://www.metacritic.com/browse/movie/')
        self.assertIsNotNone(result)

    @patch('src.scraper.requests_cache.CachedSession')
    def test_get_page_content_failed_request(self, mock_session):
        scraper = Scraper()
        mock_session.return_value.get.return_value.raise_for_status.side_effect = HTTPError
        result = scraper.get_page_content('https://www.metacritic.com/browse/movie/')
        self.assertIsNone(result)

    def test_get_movie_data_with_valid_html(self):
        scraper = Scraper()
        movie_html = MagicMock()
        movie_html.find.side_effect = [MagicMock(text='1. Test Movie'), MagicMock(text='Meta: 2022'),
                                       MagicMock(text='Score: 90')]
        result = scraper.get_movie_data(movie_html)
        self.assertEqual(result, ['TEST MOVIE', 2022, 90])

    def test_get_movie_data_with_invalid_html(self):
        scraper = Scraper()
        movie_html = MagicMock()
        movie_html.find.side_effect = [None, None, None]
        result = scraper.get_movie_data(movie_html)
        self.assertListEqual(result, [None, None, None])

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import requests_cache
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from sqlalchemy import create_engine

import utils


class Scraper:
    def __init__(self):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        self.USER_AGENT = {"User-Agent": "Mozilla/5.0"}
        self.BASE_URL = "https://www.metacritic.com/browse/movie/"
        self.retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        self.adapter = HTTPAdapter(max_retries=self.retries, pool_connections=50, pool_maxsize=50)
        self.session = requests_cache.CachedSession("../cache/movie_cache", expire_after=7200)
        self.session.mount("http://", self.adapter)
        self.session.mount("https://", self.adapter)
        self.session.headers.update(self.USER_AGENT)

    def get_page_content(self, url):
        try:
            response = self.session.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            return soup
        except requests.RequestException as e:
            logging.error(f"Request error when fetching: {e}")
        return None

    @staticmethod
    def get_movie_data(movie_html):
        try:
            title_element = movie_html.find("h3", class_="c-finderProductCard_titleHeading")
            title = title_element.text.split(".", 1)[1].strip().upper() if title_element else None

            meta_element = movie_html.find("div", class_="c-finderProductCard_meta")
            year = int(re.search(r"\d{4}", meta_element.text).group()) if meta_element else None

            score_element = movie_html.find("span", class_="c-finderProductCard_score")
            cleaned_score_el = re.sub(r"\D", "", score_element.text) if score_element else None
            score = int(cleaned_score_el) if cleaned_score_el else None

            return [title, year, score]
        except (AttributeError, ValueError) as e:
            logging.error(f"Error parsing movie data: {e}")
            return None

    def scrape_page(self, page_number):
        logging.info(f"Scraping page {page_number}")
        page_url = f"{self.BASE_URL}?page={page_number}"
        soup = self.get_page_content(page_url)

        if soup:
            movie_cards = soup.find_all("div", class_="c-finderProductCard")
            movie_data = [self.get_movie_data(movie) for movie in movie_cards if movie]
            return [data for data in movie_data if data]
        logging.warning(f"No content found for page {page_number}")
        return []

    def get_total_pages(self):
        logging.info("Fetching total pages number")
        soup = self.get_page_content(self.BASE_URL)

        if soup:
            pagination = soup.find_all("span", "c-navigationPagination_item--page")
            last_page_number = int(pagination[-1].text) if pagination else 1
            return last_page_number + 1

        return 1


class DataWriter:
    def __init__(self):
        username = os.getenv("DB_USERNAME")
        password = os.getenv("DB_PASSWORD")
        self.engine = create_engine(f"postgresql://{username}:{password}@localhost:5432/postgres")

    def write_to_postgres(self, new_movies):
        new_df = pd.DataFrame(new_movies, columns=["title", "year", "metascore"])

        try:
            existing_df = pd.read_sql_table("movies", self.engine)
        except ValueError:
            existing_df = pd.DataFrame(columns=["title", "year", "metascore"])

        combined_df = pd.concat([existing_df, new_df])

        final_df = combined_df.drop_duplicates(subset=["title", "year"], keep="first")
        final_df = final_df.dropna(inplace=False)
        final_df = final_df.sort_values(by=["metascore"], ascending=False)

        final_df["_id"] = final_df.apply(utils.hash_record, axis=1)
        cols = list(final_df.columns)
        cols.remove("_id")
        cols = ['_id'] + cols
        final_df = final_df.reindex(columns=cols)
        final_df.to_sql("movies", self.engine, schema="projects", if_exists="replace", index=False)


class MovieScraper:
    def __init__(self):
        self.scraper = Scraper()
        self.data_writer = DataWriter()

    def scrape_movies(self):
        tasks = []
        total_pages = self.scraper.get_total_pages()
        num_workers = os.cpu_count() * 2

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            for page_num in range(total_pages-1):
                future = executor.submit(self.scraper.scrape_page, page_num)
                tasks.append(future)

        all_movies = []
        for future in as_completed(tasks):
            try:
                result = future.result()
                if result:
                    all_movies.extend(movie for movie in result if movie is not None)
            except Exception as e:
                logging.error(f"An error occurred with a task: {e}")

        self.data_writer.write_to_postgres(all_movies)
        logging.info("Scraping complete. Data stored into movies table in PostgreSQL")

    def main(self):
        self.scrape_movies()


if __name__ == "__main__":
    movie_scraper = MovieScraper()
    movie_scraper.main()

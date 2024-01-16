import re
import requests
import requests_cache
import pandas as pd
import logging

import seaborn as sns
import matplotlib.pyplot as plt
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

USER_AGENT = {"User-Agent": "Mozilla/5.0"}
BASE_URL = "https://www.metacritic.com/browse/movie/"

# Cache the requests for 2 hours
# CachedSession is thread-safe
session = requests_cache.CachedSession('movie_cache', expire_after=7200)
retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.headers.update(USER_AGENT)


def get_page_content(url):
    try:
        response = session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(
            response.content, "html.parser", from_encoding=response.encoding
        )
        return soup
    except requests.RequestException as e:
        logging.error(f"Request error when fetching: {e}")
    return None


def get_movie_data(movie):
    def _get_value(el, el_class):
        try:
            value = movie.find(el, class_=el_class)
            return value.text.strip() if value else None
        except AttributeError as e:
            logging.warning(f"Attribute error when fetching: {e}")
        return None

    try:
        raw_movie_name = _get_value("h3", "c-finderProductCard_titleHeading")
        movie_name = (
            re.sub(r"\d+,\d*\.|\d+\.", "", raw_movie_name).strip().upper()
            if raw_movie_name
            else ""
        )

        raw_movie_date = _get_value("div", "c-finderProductCard_meta")
        movie_year = (
            int(re.findall(r"\d{4}", raw_movie_date)
                [0]) if raw_movie_date else -1
        )

        raw_movie_score = _get_value("span", "c-finderProductCard_score")
        movie_score = (
            int(re.findall(r"\d{1,3}", raw_movie_score)
                [0]) if raw_movie_score else -1
        )

        if not all([movie_name, movie_year != -1, movie_score != -1]):
            raise ValueError("Incomplete data for movie")
    except ValueError as e:
        logging.error(f"Data validation error: {e}")
        return None

    return [movie_name, movie_year, movie_score]


def scrape_page(page_number):
    logging.info(f"Scraping page {page_number}")
    soup = get_page_content(f"{BASE_URL}?page={page_number}")

    if soup:
        movie_cards = soup.find_all("div", class_="c-finderProductCard")
        return [get_movie_data(movie) for movie in movie_cards]

    logging.warning(f"No content found for page {page_number}")
    return []


def write_to_csv(new_movies):
    try:
        existing_df = pd.read_csv("movies.csv")
    except FileNotFoundError:
        existing_df = pd.DataFrame(columns=["name", "year", "rating"])

    new_df = pd.DataFrame(new_movies, columns=["name", "year", "rating"])
    combined_df = pd.concat([existing_df, new_df])

    final_df = combined_df.drop_duplicates(
        subset=["name", "year"], keep="first")
    final_df = final_df.sort_values(by=["rating"], ascending=False)

    final_df.to_csv("movies.csv", index=False)


def get_total_pages():
    logging.info("Fetching total pages number")
    soup = get_page_content(BASE_URL)

    if soup:
        pagination = soup.find_all("span", "c-navigationPagination_item--page")
        last_page_number = int(pagination[-1].text) if pagination else 1
        return last_page_number + 1

    return 1


def average_score_per_year(df):
    return df.groupby("year")["rating"].mean().reset_index()


def top_rated_per_year(df):
    return df.loc[df.groupby('year')['rating'].idxmax()]


def main():
    # Scraping movies
    tasks = []
    total_pages = get_total_pages()
    with ThreadPoolExecutor(max_workers=10) as executor:
        for page_num in range(total_pages):
            future = executor.submit(scrape_page, page_num)
            tasks.append(future)

    all_movies = []
    for future in as_completed(tasks):
        try:
            result = future.result()
            all_movies.extend(movie for movie in result if movie is not None)
        except Exception as e:
            logging.error(f"An error occurred with a task: {e}")

    write_to_csv(all_movies)
    logging.info("Scraping complete. Data saved to movies.csv")

    # Loading data from CSV
    movies_df = pd.read_csv("movies.csv")

    # Plotting average scores per year
    plt.figure(figsize=(12, 6))
    avg_ratings = average_score_per_year(movies_df)
    sns.lineplot(x='year', y='rating', data=avg_ratings)
    plt.title("Average Movie Ratings Over Years")
    plt.xlabel("Year")
    plt.ylabel("Average Rating")
    plt.show()

    # Plotting distribution of movie ratings
    plt.figure(figsize=(10, 6))
    sns.histplot(movies_df['rating'], kde=True)
    plt.title('Distribution of Movie Ratings')
    plt.xlabel('Rating')
    plt.ylabel('Frequency')
    plt.show()

    # Plotting movie counts per year
    plt.figure(figsize=(14, 8))
    top_rated = top_rated_per_year(movies_df)
    sns.barplot(x='year', y='rating', data=top_rated)
    plt.xticks(rotation=45)
    plt.title('Top Rated Movie Each Year')
    plt.xlabel('Year')
    plt.ylabel('Rating')
    plt.show()


if __name__ == "__main__":
    main()

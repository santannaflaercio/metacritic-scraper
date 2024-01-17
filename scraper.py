import os
import re
import requests
import requests_cache
import pandas as pd
import logging

import plotly.express as px
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

USER_AGENT = {"User-Agent": "Mozilla/5.0"}
BASE_URL = "https://www.metacritic.com/browse/movie/"

retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)

# Cache the requests for 2 hours
# CachedSession is thread-safe
session = requests_cache.CachedSession("movie_cache", expire_after=7200)
session.mount("http://", adapter)
session.mount("https://", adapter)

session.headers.update(USER_AGENT)


def get_page_content(url):
    try:
        response = session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser", from_encoding=response.encoding)
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
        movie_name = re.sub(r"\d+,\d*\.|\d+\.", "", raw_movie_name).strip().upper() if raw_movie_name else ""

        raw_movie_date = _get_value("div", "c-finderProductCard_meta")
        movie_year = int(re.findall(r"\d{4}", raw_movie_date)[0]) if raw_movie_date else -1

        raw_movie_score = _get_value("span", "c-finderProductCard_score")
        movie_score = int(re.findall(r"\d{1,3}", raw_movie_score)[0]) if raw_movie_score else -1

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

    final_df = combined_df.drop_duplicates(subset=["name", "year"], keep="first")
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


def create_distribution_ratings_histogram_chart(df):
    return px.histogram(df, x="rating", nbins=30, title="Distribution of Movie Ratings")


def create_most_popular_per_decade_bar_chart(df: pd.DataFrame):
    df["decade"] = (df["year"] // 10) * 10
    most_popular_per_decade = df.sort_values(by=["decade", "rating"], ascending=[True, False]).groupby("decade").head(3)

    fig = px.bar(
        most_popular_per_decade,
        x="decade",
        y="rating",
        hover_data=["name", "year", "rating"],
        color="rating",
        labels={"decade": "Decade", "rating": "Rating", "name": "Movie Name"},
        title="Most Popular Movies per Decade",
    )

    fig.update_traces(
        hovertemplate="Name: %{customdata[0]}<br>Year: %{customdata[1]}",
        texttemplate="%{x}<br>%{y}",
        textposition="outside",
        textfont_size=10,
    )

    return fig


def create_perfect_classifications_line_chart(df: pd.DataFrame):
    max_rating = df["rating"].max()
    max_rating_movies = df[df["rating"] == max_rating]
    max_rating_movies["decade"] = (max_rating_movies["year"] // 10) * 10

    trends_by_decade = max_rating_movies["decade"].value_counts().sort_index()

    fig = px.scatter(trends_by_decade, x=trends_by_decade.index, y=trends_by_decade.values)
    fig.update_layout(
        xaxis_title="Decade",
        yaxis_title="Number of Movies",
        title="Number of Perfectly Rated Movies per Decade",
    )
    fig.update_traces(mode="lines+markers")
    return fig


def main():
    # Scraping movies
    tasks = []
    total_pages = get_total_pages()
    num_workers = os.cpu_count() * 2

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
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

    # Creating charts
    create_distribution_ratings_histogram_chart(movies_df).show()
    create_most_popular_per_decade_bar_chart(movies_df).show()
    create_perfect_classifications_line_chart(movies_df).show()


if __name__ == "__main__":
    main()

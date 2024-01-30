import os

import pandas as pd
from flask import Flask, jsonify
from sqlalchemy import create_engine

app = Flask(__name__)


def get_db_engine():
    username = os.environ.get("DB_USERNAME")
    password = os.environ.get("DB_PASSWORD")
    return create_engine(f"postgresql://{username}:{password}@localhost:5432/postgres")


def execute_query(query):
    engine = get_db_engine()
    df = pd.read_sql_query(query, engine)
    return df.to_dict("records")


@app.route("/api/movies", methods=["GET"])
def movies():
    result = execute_query("SELECT * FROM projects.movies")
    if not result:
        return jsonify({"message": "No records found"}), 404
    return jsonify(result)


@app.route("/api/movies/title/<title>", methods=["GET"])
def movie_by_title(title):
    result = execute_query(f"SELECT * FROM projects.movies WHERE title = '{title}'")
    if not result:
        return jsonify({"message": "No records found"}), 404
    return jsonify(result)


@app.route("/api/movies/<_id>", methods=["GET"])
def movie_by_id(_id):
    result = execute_query(f"SELECT * FROM projects.movies WHERE _id = '{_id}'")
    if not result:
        return jsonify({"message": "No records found"}), 404
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True)

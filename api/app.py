import os

from flask import Flask, jsonify
from sqlalchemy import create_engine
import pandas as pd

app = Flask(__name__)


def get_movies():
    # Crie uma conexão com o banco de dados
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/postgres')

    # Execute uma consulta SQL para obter todos os filmes
    query = "SELECT * FROM projects.movies"
    df = pd.read_sql_query(query, engine)

    # Converta o DataFrame em um dicionário e retorne como JSON
    movies = df.to_dict('records')
    return jsonify(movies)


@app.route('/api/movies', methods=['GET'])
def movies():
    return get_movies()


if __name__ == '__main__':
    app.run(debug=True)

from es import ElasticsearchDAO
from typing import List


class LoadService:

    def __init__(self, index: str):
        self._idx = index
        self._dao = ElasticsearchDAO(index)

    def load_data_to_es(self, movies_data: List[dict]):
        create_movies = list()
        update_movies = list()
        for movie in movies_data:
            if self._dao.exists(movie['id']):
                update_movies.append(movie)
            else:
                create_movies.append(movie)
        self._dao.parallel_create_bulk(create_movies)
        self._dao.parallel_update_bulk(update_movies)

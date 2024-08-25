import logging
from typing import List, Set

from sqlalchemy.engine.row import RowMapping


logger = logging.getLogger()


class TransformService:

    def pg_to_es_transform_data(self, movies_ids: Set[str], movies_data: List[RowMapping]) -> List[dict]:
        logger.info("Transforming data from Postgres to Elsaticsearch index format")
        # переменная для хранения массива словарей с полными данными о фильме
        # один словарь == один фильм
        merged_data = list()
        # для каждого фильма
        for id in movies_ids:
            # выбираем из результатов из БД те, что относятся к этому фильму
            movie_objects = self.sort_movies_data(id, movies_data)
            # если массив результатов не пустой
            if movie_objects:
                # объединяем все данные по одному фильму в один словарь
                movie_full_data: dict = self.merge_movie_data(movie_objects)
                # и добавляем этот словарь к массиву с полными данными о фильмах
                merged_data.append(movie_full_data)

        return merged_data

    def sort_movies_data(self, movie_id: str, all_movies_data: List[RowMapping]) -> List[RowMapping]:
        logger.info("Sorting movies data to get only that related to movie with id %s", movie_id)
        # переменная для хранения всех объектов, относящихся к конкретному фильму
        movie_objects = []
        # выбираем из результатов из БД те, что относятся к этому фильму
        for result in all_movies_data:
            if str(result.id) == movie_id:
                # добавляем результаты в массив результатов по конкретному фильму
                movie_objects.append(result)
        return movie_objects

    def merge_movie_data(self, movie_objects: List[RowMapping]) -> dict:
        logger.info("Merging movie data from different rows into one dictionary")
        movie_genres = set()
        movie_actors = set()
        movie_directors = set()
        movie_writers = set()
        # по каждому объекту в массиве фильмов
        for movie in movie_objects:
            # добавляем указанный в объекте жанр во множество жанров конкретного фильма
            movie_genres.add(movie.name)
            # в зависимости от роли персоны в объекте
            # добавляем персону во множество актеров, режиссеров или сценаристов фильма
            person_data = (movie.person_id, movie.person_name)
            if movie.role == 'actor':
                movie_actors.add(person_data)
            elif movie.role == 'director':
                movie_directors.add(person_data)
            elif movie.role == 'writer':
                movie_writers.add(person_data)
        # остальные данные по фильму во всех элементах массива одинаковые
        # поэтому выбираем первый элемент и работаем с ним
        movie_object = movie_objects[0]
        # и отдаем этот объект в функцию, которая вложит все агрегированные данные
        # и отдаст нам словарь фильма с полными данными и без лишних ключей
        full_movie = self.enclose_and_clean_movie_data(
            movie_object, movie_genres, movie_actors, movie_directors, movie_writers
        )
        return full_movie

    def enclose_and_clean_movie_data(self, movie: RowMapping,
                                     movie_genres: set,
                                     movie_actors: set,
                                     movie_directors: set,
                                     movie_writers: set) -> dict:
        logger.info("Enclosing keys/values needed for Elasticsearch index to dictionary and cleaning extra ones")
        # для того чтобы можно было изменять объект фильма и добавлять в него ключи,
        # меняем тип объекта с экземпляра sqlalchemy.engine.row.RowMapping на словарь
        movie = dict(movie)
        # удаляем ключи, которые не присутствуют в нашей схеме
        del movie['role']
        del movie['person_name']
        del movie['person_id']
        del movie['name']
        # вместо них добавляем ключи с агрегированными данными
        movie['genres'] = list(movie_genres)
        movie['actors'] = self.helper_make_person_dict(movie_actors)
        movie['actors_names'] = [actor['name'] for actor in movie['actors']]
        movie['directors'] = self.helper_make_person_dict(movie_directors)
        movie['directors_names'] = [director['name'] for director in movie['directors']]
        movie['writers'] = self.helper_make_person_dict(movie_writers)
        movie['writers_names'] = [writer['name'] for writer in movie['writers']]
        # конвертируем id фильма из uuid в строку
        movie['id'] = str(movie['id'])
        return movie

    def helper_make_person_dict(self, movie_persons: set) -> list:
        logger.info("Making a dictionary out of a person data")
        return list(
            {
                'id': str(person[0]),
                'name': person[1]
            } for person in movie_persons
        )

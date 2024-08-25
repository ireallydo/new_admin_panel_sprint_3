import logging
from typing import List

from es import ElasticsearchDAO


logger = logging.getLogger()


class LoadService:

    def __init__(self, index: str):
        self._idx = index
        self._dao = ElasticsearchDAO(index)

    def load_data_to_es(self, movies_data: List[dict]):
        logger.info("Prepating to load data to Elasticsearch")
        create_movies = list()
        update_movies = list()
        for movie in movies_data:
            logger.info("Sorting data, making creation and update buckets")
            if self._dao.exists(movie['id']):
                update_movies.append(movie)
            else:
                create_movies.append(movie)

        response = list()

        if create_movies:
            logger.info("There are documents to create in Elasticsearch")
            create_response = self._dao.parallel_create_bulk(create_movies)
            if create_response:
                logger.info("Managed to create all documents from creation bucket")
                response.append(create_response)
            else:
                logger.info("Did nor manage to create documents from creation bucket")
                return False
        if update_movies:
            logger.info("There are documents to update in Elasticsearch")
            update_response = self._dao.parallel_update_bulk(update_movies)
            if update_response:
                logger.info("Managed to create all documents from update bucket")
                response.append(update_response)
            else:
                logger.info("Did nor manage to create documents from update bucket")
                return False

        return response

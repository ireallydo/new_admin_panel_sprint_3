import logging
from time import sleep
from typing import List

from fault_tolerance_sys.state_monitoring import RedisStorage, State
from postgres_db.dto import ExtractResponseSchema
from services import ExtractService, LoadService, TransformService
from settings.configs import Config
from settings.constants import (DEFAULT_ENRICHER_STATE, ENRICHER_LOAD_LIMIT,
                                MOVIES_IDX, R_ENRICHER_STATE)
from settings.dto import EnrichConfigDTO, ExtractConfigDTO


logger = logging.getLogger()


class ETLProcess:

    def __init__(self):
        self._extractor = ExtractService()
        self._transformer = TransformService()
        self._loader = LoadService(MOVIES_IDX)
        self._config = Config()
        self._monitor = State(RedisStorage())

    def start_process(self,
                      data_modified: List[ExtractResponseSchema],
                      enrich_settings: EnrichConfigDTO = None):
        logger.info('Start ETL process')

        modified_entities_ids = {str(entity.id) for entity in data_modified}

        self._monitor.set_state(R_ENRICHER_STATE, DEFAULT_ENRICHER_STATE)
        logger.info('Set enricher state to default value before iterating: %s', R_ENRICHER_STATE)

        while True:
            enricher_state = int(self._monitor.get_state(R_ENRICHER_STATE))
            logger.info('Inside enricher loop. Current enricher state: %s', enricher_state)
            new_enricher_state = enricher_state + ENRICHER_LOAD_LIMIT

            # логика, которая позволяет пропустить этап "обогащения" для процесса
            # с использованием основной таблицы (фильмы), поскольку тут нам не нужно
            # предварительно обращаться к связанным сущностям, чтобы получить id фильмов

            if enrich_settings is not None:
                enrich_settings.base_entity_ids = modified_entities_ids
                enrich_settings.offset = enricher_state

                enrich_config = self._config.create_enrich_config(enrich_settings)

                movies = self._extractor.enrich_modified_data(enrich_config)

                logger.info("Got non-empty response from db with related movies data")
                logger.debug('Movies: %s', movies)
            else:
                movies = data_modified

            if movies:

                movies_ids = {str(movie.id) for movie in movies}

                merge_config = self._config.create_merge_config(movies_ids)
                pg_movies_data = self._extractor.merge_modified_data(merge_config)

                logger.debug('Got response from db with movies data: %s', pg_movies_data)
                transformed_data = self._transformer.pg_to_es_transform_data(
                    movies_ids=movies_ids,
                    movies_data=pg_movies_data
                )
                logger.debug('Got transformed data: %s', transformed_data)

                es_response = self._loader.load_data_to_es(transformed_data)
                logger.info(f"Got response from elasticsearch loader")
                logger.debug("Response from from elasticsearch loader: %s", es_response)

                # переход к следующему батчу оффсета только если запись в эластик прошла успешно
                if es_response:
                    logger.info("Positive response from elasticsearch loader")
                    self._monitor.set_state(R_ENRICHER_STATE,
                                            new_enricher_state)
                    logger.info(f"Set enricher state to new value: %s", new_enricher_state)

                logger.info("Negative response from elasticsearch loader. Will reiterate over same data")

                logger.info("Timeout for enricher for 1 sec")
                sleep(1)

                if enrich_settings is None:
                    break

            else:
                logger.info('All db data available re extracted entities was loaded to Elasticsearch')
                break


etl_process = ETLProcess()

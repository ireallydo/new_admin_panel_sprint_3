from functools import partial
import logging
from time import sleep

from es.es_connector import init_index
from fault_tolerance_sys.state_monitoring import RedisStorage, State
from processes import etl_process, monitoring_process
from settings.constants import (DEFAULT_EXTRACTOR_STATE, MOVIES_IDX,
                                R_GENRE_EXTRACTOR_STATE,
                                R_MOVIE_EXTRACTOR_STATE,
                                R_PERSON_EXTRACTOR_STATE)
from settings.dto import EnrichConfigDTO, ExtractConfigDTO
from utils.get_state_helper import get_state_helper
from utils.logger import setup_logger


def main():

    setup_logger()
    logger = logging.getLogger()

    init_index(MOVIES_IDX)

    redis_db = RedisStorage()
    monitor = State(redis_db)

    while True:

        # проверка по персоналиям

        person_monitoring_state = get_state_helper(R_PERSON_EXTRACTOR_STATE, DEFAULT_EXTRACTOR_STATE)
        person_extract_settings = ExtractConfigDTO(
            table_name='person',
            modified=person_monitoring_state
        )

        person_data_modified = monitoring_process.start_process(person_extract_settings)

        if person_data_modified:
            logger.info(f"Got non-empty response from db with modified data: persons")
            logger.debug('Modified data: %s', person_data_modified)
            person_modified_stamps = {entity.modified for entity in person_data_modified}
            new_person_extractor_state = str(max(person_modified_stamps))

            person_enrich_settings = EnrichConfigDTO(
                m2m_tb_name='person_film_work',
                m2m_tb_join_on_col='film_work_id',
                m2m_tb_filter_col='person_id',
            )

            etl_process.start_process(person_data_modified, person_enrich_settings)
            monitor.set_state(R_PERSON_EXTRACTOR_STATE, new_person_extractor_state)
            logger.info("Set person extractor state to new value: %s", new_person_extractor_state)

            logger.info("Timeout for extractor for 2 sec")
            sleep(2)

        # проверка по жанрам

        genre_monitoring_state = get_state_helper(R_GENRE_EXTRACTOR_STATE, DEFAULT_EXTRACTOR_STATE)
        genre_extract_settings = ExtractConfigDTO(
            table_name='genre',
            modified=genre_monitoring_state
        )

        genre_data_modified = monitoring_process.start_process(genre_extract_settings)

        if genre_data_modified:
            logger.info(f"Got non-empty response from db with modified data: genres")
            logger.debug('Modified data: %s', genre_data_modified)
            modified_stamps = {entity.modified for entity in genre_data_modified}
            new_genre_extractor_state = str(max(modified_stamps))

            genre_enrich_settings = EnrichConfigDTO(
                m2m_tb_name='genre_film_work',
                m2m_tb_join_on_col='film_work_id',
                m2m_tb_filter_col='genre_id',
            )

            etl_process.start_process(genre_data_modified, genre_enrich_settings)
            monitor.set_state(R_GENRE_EXTRACTOR_STATE, new_genre_extractor_state)
            logger.info("Set genre extractor state to new value: %s", new_genre_extractor_state)

            logger.info("Timeout for extractor for 2 sec")
            sleep(2)

        # проверка по фильмам

        movie_monitoring_state = get_state_helper(R_MOVIE_EXTRACTOR_STATE, DEFAULT_EXTRACTOR_STATE)
        movie_extract_settings = ExtractConfigDTO(
            table_name='film_work',
            modified=movie_monitoring_state,
            limit=2
        )

        movie_data_modified = monitoring_process.start_process(movie_extract_settings)

        if movie_data_modified:
            logger.info(f"Got non-empty response from db with modified data: movies")
            logger.debug('Modified data: %s', movie_data_modified)
            movie_modified_stamps = {entity.modified for entity in movie_data_modified}
            new_movie_extractor_state = str(max(movie_modified_stamps))

            # здесь не передаем конфигу для этапа "обогащения" данных,
            # поскольку на первом этапе уже достали данные по обновленным фильмам
            # и для получения их айдишников нам не нужно обращаться к связанным таблицам

            etl_process.start_process(movie_data_modified)
            monitor.set_state(R_MOVIE_EXTRACTOR_STATE, new_movie_extractor_state)
            logger.info("Set movie extractor state to new value: %s", new_movie_extractor_state)

            logger.info("Timeout for extractor for 2 sec")
            sleep(2)

        sleep(3)

        if not person_data_modified and not genre_data_modified and not movie_data_modified:
            logger.info("All modified data from db for now was loaded to Elasticsearch. "
                        "Timeout for 60 sec before retry request for fresh updates")
            sleep(60)


if __name__ == '__main__':
    main()

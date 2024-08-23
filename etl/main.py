from settings.constants import MOVIES_IDX, DEFAULT_EXTRACTOR_STATE, \
    DEFAULT_ENRICHER_STATE, R_EXTRACTOR_STATE, R_ENRICHER_STATE, ENRICHER_LOAD_LIMIT
from fault_tolerance_sys.state_monitoring import RedisStorage, State
from services import ExtractService, TransformService, LoadService
from es.es_connector import init_index
from utils.logger import setup_logger
from settings.configs import Config
from time import sleep
import logging


def main():

    setup_logger()
    logger = logging.getLogger()

    init_index(MOVIES_IDX)
    config = Config()

    extractor = ExtractService()
    transformer = TransformService()
    loader = LoadService(MOVIES_IDX)

    redis_db = RedisStorage()
    monitor = State(redis_db)


    while True:

        extractor_state = monitor.get_state(R_EXTRACTOR_STATE)
        if extractor_state  is None:
            logger.info('State for extractor is not set')
            monitor.set_state(R_EXTRACTOR_STATE, DEFAULT_EXTRACTOR_STATE)
            logger.info(f'Set extractor state to default value: {DEFAULT_EXTRACTOR_STATE}')
            extractor_state  = monitor.get_state(R_EXTRACTOR_STATE)

        extract_config = config.create_extract_config(extractor_state)
        persons_data_modified = extractor.extract_modified_data(extract_config)

        if persons_data_modified:
            logger.info(f"Got non-empty response from db with persons data")
            logger.debug(f'Persons data: {persons_data_modified}')
            person_modified_stamps = {person[1] for person in persons_data_modified}
            new_extractor_state = str(max(person_modified_stamps))

            persons_ids = {str(person[0]) for person in persons_data_modified }

            monitor.set_state(R_ENRICHER_STATE, DEFAULT_ENRICHER_STATE)
            logger.info(f'Set enricher state to default value before iterating: {R_ENRICHER_STATE}')

            while True:
                enricher_state = int(monitor.get_state(R_ENRICHER_STATE))
                logger.info(f'Inside enricher loop. Current enricher state: {enricher_state}')
                enrich_config = config.create_enrich_config(persons_ids, enricher_state)
                new_enricher_state = enricher_state + ENRICHER_LOAD_LIMIT

                movies_of_persons = extractor.enrich_modified_data(enrich_config)

                if movies_of_persons:
                    logger.info(f"Got non-empty response from db with movies of persons data")
                    logger.debug(f'Movies of persons data: {movies_of_persons}')
                    movies_ids = {str(movie[0]) for movie in movies_of_persons}

                    merge_config = config.create_merge_config(movies_ids)
                    pg_movies_data = extractor.merge_modified_data(merge_config)

                    logger.debug(f'Got response from db with movies data: {pg_movies_data}')
                    transformed_data = transformer.pg_to_es_transform_data(
                        movies_ids=movies_ids,
                        movies_data=pg_movies_data
                    )
                    logger.debug(f'Got transformed data: {transformed_data}')

                    es_response = loader.load_data_to_es(transformed_data)
                    logger.info(f"Got response from elasticsearch loader")
                    logger.debug(f"Response from from elasticsearch loader: {es_response}")

                    # переход к следующему батчу оффсета только если запись в эластик прошла успешно
                    if es_response:
                        logger.info("Positive response from elasticsearch loader")
                        monitor.set_state(R_ENRICHER_STATE,
                                          new_enricher_state)
                        logger.info(f"Set enricher state to new value: {new_enricher_state}")

                    logger.info("Negative response from elasticsearch loader. Will reiterate over same data")

                    logger.info("Timeout for enricher for 1 sec")
                    sleep(1)

                else:
                    logger.info('All db data available re extracted persons was loaded to Elasticsearch')
                    monitor.set_state(R_EXTRACTOR_STATE,
                                      new_extractor_state)
                    logger.info(f"Set extractor state to new value: {new_extractor_state}")

                    logger.info("Timeout for extractor for 2 sec")
                    sleep(2)
                    break
        else:
            logger.info("All modified data from db for now was loaded to Elasticsearch. "
                        "Timeout for extractor for 60 sec before retry request for fresh updates")
            sleep(60)


if __name__ == '__main__':
    main()

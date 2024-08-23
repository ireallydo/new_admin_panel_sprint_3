from time import sleep
from services import ExtractService, TransformService, LoadService
from settings.constants import MOVIES_IDX, R_EXTRACTOR_STATE, R_ENRICHER_STATE, ENRICHER_LOAD_LIMIT
from settings.configs import Config
from es.es_connector import init_index
from fault_tolerance_sys.state_monitoring import RedisStorage, State


def main():

    init_index(MOVIES_IDX)
    config = Config()

    extractor = ExtractService()
    transformer = TransformService()
    loader = LoadService(MOVIES_IDX)

    redis_db = RedisStorage()
    monitor = State(redis_db)

    monitor.set_state(R_EXTRACTOR_STATE,
                      '1000-01-01 00:00:00.000000')


    while True:

        extractor_state = monitor.get_state(R_EXTRACTOR_STATE)
        if extractor_state  is None:
            print('State is not set, will proceed to default')
            monitor.set_state(R_EXTRACTOR_STATE,
                              '1000-01-01 00:00:00.000000')
            extractor_state  = monitor.get_state('state')

        extract_config = config.create_extract_config(extractor_state)
        persons_data_modified = extractor.extract_modified_data(extract_config)

        if persons_data_modified:

            person_modified_stamps = {person[1] for person in persons_data_modified}
            new_extractor_state = str(max(person_modified_stamps))

            persons_ids = {str(person[0]) for person in persons_data_modified }


            monitor.set_state(R_ENRICHER_STATE,
                              0)

            while True:
                enricher_state = int(monitor.get_state(R_ENRICHER_STATE))
                enrich_config = config.create_enrich_config(persons_ids, enricher_state)
                new_enricher_state = enricher_state + ENRICHER_LOAD_LIMIT

                movies_of_persons = extractor.enrich_modified_data(enrich_config)

                if movies_of_persons:
                    movies_ids = {str(movie[0]) for movie in movies_of_persons}

                    merge_config = config.create_merge_config(movies_ids)
                    pg_movies_data = extractor.merge_modified_data(merge_config)


                    transformed_data = transformer.pg_to_es_transform_data(
                        movies_ids=movies_ids,
                        movies_data=pg_movies_data
                    )

                    resp = loader.load_data_to_es(transformed_data)

                    # переход к следующему батчу оффсета только если запись в эластик прошла успешно
                    if resp:
                        monitor.set_state(R_ENRICHER_STATE,
                                          new_enricher_state)

                    sleep(2)
                else:
                    # все записи, которые были доступны по выбранным персонам, записаны
                    # устанавливаем новое состояние для экстрактора и переходим в начало петли
                    monitor.set_state(R_EXTRACTOR_STATE,
                                      new_extractor_state)
                    sleep(10)
                    break
        else:
            print("That's all, folks!")
            break


if __name__ == '__main__':
    main()

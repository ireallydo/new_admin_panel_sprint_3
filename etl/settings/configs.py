import logging
from typing import Set

from postgres_db.dto import EnrichSchema, ExtractSchema, MergeSchema

from .constants import ENRICHER_LOAD_LIMIT, SCHEMA


logger = logging.getLogger()


class Config:

    @classmethod
    def create_extract_config(cls, modified: str):
        logger.info(f"Create extractor config with modified value: {modified}")
        extract_config = ExtractSchema(
            table=f'{SCHEMA}.person',
            modified=modified,
            limit=100
        )
        return extract_config

    @classmethod
    def create_enrich_config(cls, persons_ids: Set[str], offset: int):
        logger.info(f"Create enricher config with offset: {offset}")
        logger.debug(f"Persons ids passed to enricher config: {persons_ids}")
        enrich_config = EnrichSchema(
            table=f'{SCHEMA}.film_work',
            join_tb_cols_value={
                f'{SCHEMA}.person_film_work': {
                    'film_work_id': f'{SCHEMA}.film_work.id'
                }
            },
            filter_tb_col=(
                f'{SCHEMA}.person_film_work',
                'person_id'
            ),
            filter_values=persons_ids,
            limit=ENRICHER_LOAD_LIMIT,
            offset=offset
        )
        return enrich_config

    @classmethod
    def create_merge_config(cls, movies_ids: Set[str]):
        logger.info("Create merger config")
        logger.debug(f"Movies ids passed to enricher config: {movies_ids}")
        merge_config = MergeSchema(
            table=f'{SCHEMA}.film_work',
            select_tb_cols={
                f'{SCHEMA}.film_work': {
                    'id',
                    'title',
                    'description',
                    'rating as imdb_rating',
                },
                f'{SCHEMA}.person': {
                    'id as person_id',
                    'full_name as person_name'
                },
                f'{SCHEMA}.person_film_work': {
                    'role'
                },
                f'{SCHEMA}.genre': {
                    'name'
                }
            },
            join_tb_cols_value={
                f'{SCHEMA}.person_film_work': {
                    'film_work_id': f'{SCHEMA}.film_work.id'
                },
                f'{SCHEMA}.person': {
                    'id': f'{SCHEMA}.person_film_work.person_id'
                },
                f'{SCHEMA}.genre_film_work': {
                    'film_work_id': f'{SCHEMA}.film_work.id'
                },
                f'{SCHEMA}.genre': {
                    'id': f'{SCHEMA}.genre_film_work.genre_id'
                }
            },
            filter_tb_col=(
                f'{SCHEMA}.film_work',
                'id'
            ),
            filter_values=movies_ids
        )
        return merge_config
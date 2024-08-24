import logging
from typing import Type

from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from fault_tolerance_sys.backoff import PGBackoff
from postgres_db.database import SessionLocal
from postgres_db.dto import EnrichSchema


logger = logging.getLogger()


class EnricherDAO:

    def __init__(self, session_generator: Type[Session] = SessionLocal):
        self._session_generator = session_generator

    @PGBackoff.server_backoff
    @PGBackoff.timeout_backoff
    def get_related_data(self, config: EnrichSchema):
        logger.info("Making request to database")
        with self._session_generator() as session:
            filter_values = "'"+"', '".join(config.filter_values)+"'"

            join_list = list()

            for table in config.join_tb_cols_value.keys():
                col_value = config.join_tb_cols_value[table]
                for col in col_value.keys():
                    join =\
                    f"""
                        LEFT JOIN {table}
                        ON {table}.{col} = {col_value[col]}
                    """
                    join_list.append(join)
            join_clause = ' '.join(join_list)

            query = text(f"""
                    SELECT {config.table}.id, {config.table}.modified
                    FROM {config.table} 
                    {join_clause}
                    WHERE {config.filter_tb_col[0]}.{config.filter_tb_col[1]}
                    IN ({filter_values})
                    ORDER BY {config.table}.modified
                    LIMIT {config.limit}
                    OFFSET {config.offset};
                """
            )
            logger.debug(f"Request query string: {query}")

            result = session.execute(query)
            resp = [raw for raw in result]
            logger.debug(f"Got response from database: {resp}")
            return resp

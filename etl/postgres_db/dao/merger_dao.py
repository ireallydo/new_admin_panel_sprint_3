import logging
from typing import Type

from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from fault_tolerance_sys.backoff import PGBackoff
from postgres_db.database import SessionLocal
from postgres_db.dto import MergeSchema


logger = logging.getLogger()


class MergerDAO:

    def __init__(self, session_generator: Type[Session] = SessionLocal):
        self._session_generator = session_generator

    @PGBackoff.server_backoff
    @PGBackoff.timeout_backoff
    def get_merged_data(self, config: MergeSchema):
        logger.info("Making request to database")
        with self._session_generator() as session:
            filter_values = "'"+"', '".join(config.filter_values)+"'"

            select_list = list()

            for table in config.select_tb_cols.keys():
                for column in config.select_tb_cols[table]:
                    select_list.append(f"{table}.{column}")

            select_var_string = ", ".join(select_list)

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
                    SELECT {select_var_string}
                    FROM {config.table} 
                    {join_clause}
                    WHERE {config.filter_tb_col[0]}.{config.filter_tb_col[1]}
                    IN ({filter_values});
                """
            )
            logger.debug("Request query string: %s", query)

            result = session.execute(query)
            results_as_dict = result.mappings().all()
            logger.debug("Got response from database: %s", results_as_dict)
            return results_as_dict

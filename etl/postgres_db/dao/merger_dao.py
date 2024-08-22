from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from typing import Type
from postgres_db.database import SessionLocal
from services.dto import MergeSchema


class MergerDAO:

    def __init__(self, session_generator: Type[Session] = SessionLocal):
        self._session_generator = session_generator

    def get_merged_data(self, config: MergeSchema):
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

            result = session.execute(query)
            results_as_dict = result.mappings().all()
            return results_as_dict

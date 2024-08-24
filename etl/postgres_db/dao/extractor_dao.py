import logging
from typing import Type

from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from fault_tolerance_sys.backoff import PGBackoff
from postgres_db.database import SessionLocal
from postgres_db.dto import ExtractSchema


logger = logging.getLogger()


class ExtractorDAO:

    def __init__(self, session_generator: Type[Session] = SessionLocal):
        self._session_generator = session_generator

    @PGBackoff.server_backoff
    @PGBackoff.timeout_backoff
    def get_modified_data(self, config: ExtractSchema):
        logger.info("Making request to database")
        with self._session_generator() as session:
            query = text(f"""
                SELECT id, modified
                FROM {config.table}
                WHERE modified > '{config.modified}'
                ORDER BY modified
                LIMIT {config.limit};
                """)
            logger.debug(f"Request query string: {query}")

            result = session.execute(query)
            resp = [raw for raw in result]
            logger.debug(f"Got response from database: {resp}")
            return resp

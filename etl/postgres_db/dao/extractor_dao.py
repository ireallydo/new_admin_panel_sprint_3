from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from typing import Type
from postgres_db.database import SessionLocal
from services.dto import ExtractSchema


class ExtractorDAO:

    def __init__(self, session_generator: Type[Session] = SessionLocal):
        self._session_generator = session_generator

    def get_modified_data(self, config: ExtractSchema):
        with self._session_generator() as session:
            query = text(f"""
                SELECT id, modified
                FROM {config.table}
                WHERE modified > '{config.modified}'
                ORDER BY modified
                LIMIT {config.limit};
                """)
            result = session.execute(query)
            resp = [raw for raw in result]
            return resp

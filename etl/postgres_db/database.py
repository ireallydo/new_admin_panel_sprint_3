from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, event
from settings import Settings
from time import sleep
import logging


logger = logging.getLogger()


settings = Settings()

db_con_str = (f"postgresql+psycopg2://"
              f"{settings.DB_USER}:{settings.DB_PASSWORD}"
              f"@{settings.DB_HOST}/{settings.DB_NAME}")
logger.debug(f"DB Connection string: {db_con_str }")
engine = create_engine(db_con_str, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, autocommit=False, autoflush=False, class_=Session)


# backoff система для коннекта с базой данных
@event.listens_for(engine, 'close')
def receive_close(dbapi_connection, connection_record):
    """listen for the 'close' event"""
    logger.critical("Database connection was closed unexpectedly")

    max_timeout = 377
    timeout = 1
    next_timeout = 2

    while True:
        try:
            logger.critical(f'Timeout before reconnection attempt: {timeout} seconds')
            sleep(timeout)
            if timeout < max_timeout:
                timeout, next_timeout = next_timeout, timeout + next_timeout
            logger.critical("Attempt reconnection to database")
            new_engine = create_engine(db_con_str, echo=False, pool_pre_ping=True)
            conn = new_engine.connect()
            conn.close()
            engine = new_engine
            logger.critical('Database reconnected successfully')
            break
        except OperationalError:
            logger.critical("Failed to reestate connection to database")

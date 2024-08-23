from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError
from settings import Settings
from time import sleep


settings = Settings()

db_con_str = (f"postgresql+psycopg2://"
              f"{settings.DB_USER}:{settings.DB_PASSWORD}"
              f"@{settings.DB_HOST}/{settings.DB_NAME}")
# logger.debug(f"DB Connection string: {db_con_str }")
engine = create_engine(db_con_str, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, autocommit=False, autoflush=False, class_=Session)


@event.listens_for(engine, 'close')
def receive_close(dbapi_connection, connection_record):
    "listen for the 'close' event"
    print(dbapi_connection)
    print(connection_record)
    print('Ooooops, db closed')

    max_timeout = 377
    timeout = 1
    next_timeout = 2

    while True:
        try:
            print(f'Timeout before reconnection attempt: {timeout} seconds')
            sleep(timeout)
            if timeout < max_timeout:
                timeout, next_timeout = next_timeout, timeout + next_timeout
            new_engine = create_engine(db_con_str, echo=False, pool_pre_ping=True)
            conn = new_engine.connect()
            conn.close()
            engine = new_engine
            print('Database reconnected successfully')
            break
        except OperationalError:
            print('Still not working')


    # ... (event handling logic) ...
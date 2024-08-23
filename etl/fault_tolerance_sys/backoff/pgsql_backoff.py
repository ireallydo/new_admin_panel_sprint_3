from sqlalchemy.exc import DBAPIError, TimeoutError
from time import sleep
import logging


logger = logging.getLogger()


# backoff для разрыва соединения прописан в etl/postgres_db/database.py
# поскольку в его логике используется подмена корневого соединения
# создаваемого в этом файле

class PGBackoff:

    @classmethod
    def server_backoff(cls, func):
        def inner_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except DBAPIError as e:
                logger.critical('Check DBAPIError error status code')
                if 500 <= int(e.code[1:]) <= 599:
                    logger.critical('Server problem with Postgres. Will attempt to reconnect')
                    max_timeout = 377
                    timeout = 1
                    next_timeout = 2
                    while True:
                        try:
                            logger.critical(f'Timeout before reconnection attempt: {timeout} seconds')
                            sleep(timeout)
                            if timeout < max_timeout:
                                timeout, next_timeout = next_timeout, timeout + next_timeout
                            return func(*args, **kwargs)
                        except DBAPIError:
                            logger.critical('Failed to reconnect to database')
                else:
                    logger.critical('Client problem with Postgres. Raising exception')
                    raise e
        return inner_func

    @classmethod
    def timeout_backoff(cls, func):
        def inner_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except TimeoutError as e:
                logger.critical('Got timeout error from Postgres. Will attempt to reconnect')
                max_timeout = 377
                timeout = 1
                next_timeout = 2
                while True:
                    try:
                        logger.critical(f'Timeout before reconnection attempt: {timeout} seconds')
                        sleep(timeout)
                        if timeout < max_timeout:
                            timeout, next_timeout = next_timeout, timeout + next_timeout
                        return func(*args, **kwargs)
                    except TimeoutError:
                        logger.critical('Failed to reconnect to database')
        return inner_func

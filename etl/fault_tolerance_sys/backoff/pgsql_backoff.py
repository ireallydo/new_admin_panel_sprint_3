from sqlalchemy.exc import DBAPIError, TimeoutError
from time import sleep

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
                print('Check DBAPIError error status code')
                if 500 <= int(e.code[1:]) <= 599:
                    print('Server problem with Postgres. Will attempt to reconnect')
                    max_timeout = 377
                    timeout = 1
                    next_timeout = 2
                    while True:
                        try:
                            print(f'Timeout before reconnection attempt: {timeout} seconds')
                            sleep(timeout)
                            if timeout < max_timeout:
                                timeout, next_timeout = next_timeout, timeout + next_timeout
                            return func(*args, **kwargs)
                        except DBAPIError:
                            print('Still not working')
                else:
                    print('Client problem with Postgres. Raising exception')
                    raise e
        return inner_func

    @classmethod
    def timeout_backoff(cls, func):
        def inner_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except TimeoutError as e:
                print('Got timeout error from Postgres. Will attempt to reconnect')
                max_timeout = 377
                timeout = 1
                next_timeout = 2
                while True:
                    try:
                        print(f'Timeout before reconnection attempt: {timeout} seconds')
                        sleep(timeout)
                        if timeout < max_timeout:
                            timeout, next_timeout = next_timeout, timeout + next_timeout
                        return func(*args, **kwargs)
                    except TimeoutError:
                        print('Still not working')
        return inner_func

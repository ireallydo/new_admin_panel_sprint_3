from elastic_transport import ConnectionError, ConnectionTimeout
from elasticsearch import ApiError
from time import sleep


class ESBackoff:

    @classmethod
    def connection_backoff(cls, func):
        def inner_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ConnectionError or ConnectionTimeout:
                print('Lost connection to Elasticsearch. Will attempt to reconnect')
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
                    except ConnectionError or ConnectionTimeout:
                        print('Still not working')
        return inner_func


    @classmethod
    def server_backoff(cls, func):
        def inner_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ApiError as e:
                print('Check API error status code')
                if 500 <= int(e.status_code) <= 599:
                    print('Server problem with Elasticsearch. Will attempt to reconnect')
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
                        except ConnectionError:
                            print('Still not working')
                else:
                    print('Client problem with Elasticsearch. Raising exception')
                    raise e
        return inner_func
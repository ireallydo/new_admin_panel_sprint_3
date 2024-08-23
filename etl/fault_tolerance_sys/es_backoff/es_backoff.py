from elastic_transport import ConnectionError
from time import sleep

def es_connection_backoff(func):
    def inner_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ConnectionError:
            print('Lost connection to Elasticsearch')
            max_timeout = 377
            timeout = 1
            next_timeout = 2
            while True:
                try:
                    print(f'Timeout before reconnection attempt: {timeout} seconds')
                    sleep(timeout)
                    if timeout < max_timeout:
                        timeout, next_timeout = next_timeout, timeout + next_timeout
                    # es = Elasticsearch("http://127.0.0.1:9200")
                    return func(*args, **kwargs)
                except ConnectionError:
                    print('Still not working')
    return inner_func

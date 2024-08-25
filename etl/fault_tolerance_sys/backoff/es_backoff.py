import logging
from time import sleep

from elastic_transport import ConnectionError, ConnectionTimeout
from elasticsearch import ApiError


logger = logging.getLogger()


class ESBackoff:

    @classmethod
    def connection_backoff(cls, func):
        def inner_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ConnectionError or ConnectionTimeout:
                logger.critical('Lost connection to Elasticsearch. Will attempt to reconnect')
                max_timeout = 377
                timeout = 1
                next_timeout = 2
                while True:
                    try:
                        logger.critical('Timeout before reconnection attempt: %s seconds', timeout)
                        sleep(timeout)
                        if timeout < max_timeout:
                            timeout, next_timeout = next_timeout, timeout + next_timeout
                        return func(*args, **kwargs)
                    except ConnectionError or ConnectionTimeout:
                        logger.critical('Failed to reconnect to Elasticsearch')
        return inner_func


    @classmethod
    def server_backoff(cls, func):
        def inner_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ApiError as e:
                logger.critical('Check API error status code')
                if 500 <= int(e.status_code) <= 599:
                    logger.critical('Server problem with Elasticsearch. Will attempt to reconnect')
                    max_timeout = 377
                    timeout = 1
                    next_timeout = 2
                    while True:
                        try:
                            logger.critical('Timeout before reconnection attempt: %s seconds', timeout)
                            sleep(timeout)
                            if timeout < max_timeout:
                                timeout, next_timeout = next_timeout, timeout + next_timeout
                            return func(*args, **kwargs)
                        except ApiError:
                            logger.critical('Failed to reconnect to Elasticsearch')
                else:
                    logger.critical('Client problem with Elasticsearch. Raising exception')
                    raise e
        return inner_func
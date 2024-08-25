import logging
import logging.handlers
import os

from settings import Settings


def setup_logger():

    settings = Settings()
    file_path = "/".join(settings.LOG_FILEPATH.split("/")[:-1])
    try:
        os.makedirs(file_path, exist_ok=True)
    except FileNotFoundError:
        pass

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s [%(pathname)s:%(lineno)d]: %(message)s ')

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=settings.LOG_FILEPATH,
        when='D',
        interval=settings.LOG_ROTATION,
        backupCount=settings.LOG_RETENTION
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    root_logger.info('Logger is set up!')

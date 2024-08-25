import logging
from typing import Any

from fault_tolerance_sys.state_monitoring import RedisStorage, State


logger = logging.getLogger()


redis_db = RedisStorage()
monitor = State(redis_db)


def get_state_helper(state_monitor_name: str, default_state: Any) -> str:
    extractor_state = monitor.get_state(state_monitor_name)
    if extractor_state is None:
        logger.info('State for extractor is not set')
        monitor.set_state(state_monitor_name, default_state)
        logger.info(f'Set extractor state to default value: {default_state}')
        extractor_state = monitor.get_state(state_monitor_name)
    return extractor_state

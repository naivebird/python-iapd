import logging
import time
from functools import wraps

from requests import RequestException

logger = logging.getLogger('Utils')


def crawler_retry(max_retries=3, delay=5, back_off=1, default_value=None, retry_codes=(429, 503)):
    def wrapper(func):
        @wraps(func)
        def retry_func(*args, **kwargs):
            delay_time = delay
            count = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except RequestException as e:
                    count += 1
                    if count > max_retries:
                        logger.debug('Max retries exceeded.')
                        break
                    if hasattr(e.response, 'status_code'):
                        if e.response.status_code in retry_codes:
                            logger.debug("Error: {}, retrying in {} seconds.".format(e, delay_time))
                            time.sleep(delay_time)
                            delay_time *= back_off
                            continue
                    logger.exception(e)
                    break
                except Exception as e:
                    logger.exception(e)
                    break
            return default_value

        return retry_func

    return wrapper

import sys

import requests
import logging


class DataRetriever:

    def __init__(self, timeout=60):
        self.timeout = timeout

    def req_call(self, url, headers, params, max_retries=3):
        retry_count = 0
        while retry_count <= max_retries:
            try:
                logging.info(f"API call to %s with header %s and params %s.", url, headers, params)
                r = requests.get(url, headers=headers, timeout=self.timeout, params=params)
                r.raise_for_status()
                return r
            except Exception as e:
                logging.error("Error fetching data; error:")
                logging.error("%s", str(e))
                retry_count += 1
        logging.error("Max retries exceeded. Exiting.")
        return None

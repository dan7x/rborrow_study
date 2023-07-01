import logging
import sys
import os
import time

import pandas as pd
from historical_gen._dataretriever import DataRetriever
from historical_gen.pushshift import PushShift


class SubmissionRetriever(DataRetriever):

    def __init__(self, start_date: str, end_date: str, timeout: int = 60, delay: int = 2):
        """
        :param start_date: Start date as 'YYYY-MM-DD'
        :param end_date: End date as 'YYYY-MM-DD'
        :param timeout: Timeout before request error
        :param delay: Delay between each request
        """
        super().__init__(timeout)
        self.start_date = start_date
        self.end_date = end_date
        self.delay = delay

    def fetch_submissions(self, output_dir: str, freq='d') -> tuple[int, int]:
        """
        Fetch data by submissions.

        :return: Tuple(# days fetched, # total days in range)
        """

        day_ls = pd.date_range(self.start_date, self.end_date, freq=freq)
        day_ct = 0
        for date, next_date in zip(day_ls, day_ls[1:]):
            logging.debug("Fetching submission from %s to %s", date, next_date)
            pushshift = PushShift(start_date_unix=int(date.timestamp()),
                                  end_date_unix=int(next_date.timestamp()),
                                  timeout=self.timeout)

            cur_submission_raw = pushshift.submissions()
            if cur_submission_raw is None or cur_submission_raw.status_code != 200:
                logging.error("Error occurred or HTTP response was not 200.")
                sys.exit(1)
            submissions = cur_submission_raw.json()['data']
            submissions_df = pd.DataFrame(submissions)

            if len(submissions_df) > 0:
                date_str = date.strftime('%Y-%m-%d')
                date_next_str = next_date.strftime('%Y-%m-%d')

                output_file = f"submissions_{date_str}_to_{date_next_str}.csv"
                output_file_full_path = os.path.join(output_dir, output_file)
                logging.debug("Writing %d submissions to file %s", len(submissions_df), output_file_full_path)
                submissions_df.to_csv(output_file_full_path, escapechar='\\', index=False)
                day_ct += 1

            time.sleep(self.delay)

        return day_ct, max(len(day_ls) - 1, 0)


class CommentRetriever(DataRetriever):

    def __init__(self, start_date: str = None, end_date: str = None, timeout: int = 60, delay: int = 2):
        """
        :param start_date: Start date as 'YYYY-MM-DD'
        :param end_date: End date as 'YYYY-MM-DD'
        :param timeout: Timeout before request error
        :param delay: Delay between each request
        """
        super().__init__(timeout)
        self.start_date = start_date
        self.end_date = end_date
        self.delay = delay

    def fetch_comments(self, output_dir: str, freq='12H'):
        day_ls = pd.date_range(self.start_date, self.end_date, freq=freq)
        day_ct = 0
        for date, next_date in zip(day_ls, day_ls[1:]):
            logging.debug("Fetching comments from %s to %s", date, next_date)
            pushshift = PushShift(start_date_unix=int(date.timestamp()),
                                  end_date_unix=int(next_date.timestamp()),
                                  timeout=self.timeout)

            cur_comments_raw = pushshift.comment()
            if cur_comments_raw is None or cur_comments_raw.status_code != 200:
                logging.error("Error occurred or HTTP response was not 200.")
                sys.exit(1)
            comments = cur_comments_raw.json()['data']
            comments_df = pd.DataFrame(comments)

            if len(comments_df) > 0:
                date_str = date.strftime('%Y-%m-%d_%H-%M-%S')
                date_next_str = next_date.strftime('%Y-%m-%d_%H-%M-%S')

                output_file = f"comments_{date_str}_to_{date_next_str}.csv"
                output_file_full_path = os.path.join(output_dir, output_file)
                logging.debug("Writing %d comments to file %s", len(comments_df), output_file_full_path)
                comments_df.to_csv(output_file_full_path, escapechar='\\', index=False)
                day_ct += 1

            time.sleep(self.delay)

        return day_ct, max(len(day_ls) - 1, 0)

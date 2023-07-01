import datetime
import os
import sys

import requests
import logging
import time
import pandas as pd


class LoansRetriever:

    #  Da rules:
    #  https://github.com/LoansBot/web-backend/blob/master/API.md

    def __init__(self, chunk_size, auth=None, timeout=120, delay_major=30, delay_minor=1.2):
        """
        :param chunk_size: Size of each "batch" where applicable
        :param auth: A dict that provides the auth credentials for the API. If not supplied, then we will use the
        global tokens. If one is supplied, then we check for validity. Invalid login => auth set to None.
        :param timeout: How long to wait for a response before timing out.
        :param delay_major: How long to delay between processing each "batch" for any given process
        :param delay_minor: How long to delay between processing each element. Enabled through use_delay_minor.
        """
        assert chunk_size > 2  # Make sure that chunks will always have a first and last element
        self.chunk_size = chunk_size
        self.timeout = timeout
        self.delay_major = delay_major  # Set to 3 -- the min # secs before allowed to open another http req.
        self.delay_minor = delay_minor  # Delay between each individual req if we are in historical mode

        self.auth = auth
        self.auth_data = None

        self.headers = {
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "RBorrowResearch / 0.1, "
                          "bot description: (+https://github.com/dan7x/rborrow_study/blob/master/LOANS_API.md)"
        }
        self.BASE_URL = 'https://redditloans.com'

        self.authenticate(self.auth)

    def fetch_loan_ids(self, start_date, end_date, output_dir, endpoint='/api/loans', historical=False):
        if historical:
            end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            year_end_date = datetime.date(end_dt.year, 12, 31)
            day_ls = pd.date_range('2013-12-31', year_end_date, freq='Y')
        else:
            day_ls = pd.date_range(start_date, end_date, freq='D')
        loans_count = 0

        session = requests.Session()
        for date, next_date in zip(day_ls, day_ls[1:]):
            params = {
                'after_time': int(date.timestamp()),
                'before_time': int(next_date.timestamp()),
                'limit': 100000
            }
            r = self._req_call(session, endpoint, params)
            if r.status_code == 200:
                list_of_ids = r.json()

                if len(list_of_ids) > 0:
                    loans_count += len(list_of_ids)

                    df = pd.DataFrame({'id': list_of_ids})
                    date_str = date.strftime('%Y-%m-%d')
                    date_next_str = next_date.strftime('%Y-%m-%d')

                    output_file = f"loan_ids_{date_str}_to_{date_next_str}.csv"
                    output_file_full_path = os.path.join(output_dir, output_file)
                    logging.debug("Writing %d loan ids to file %s", len(df), output_file_full_path)
                    df.to_csv(output_file_full_path, escapechar='\\', index=False)
            else:
                logging.error("Failed to fetch data for %s to %s", start_date, end_date)
                sys.exit(1)

            time.sleep(self.delay_major)

        return loans_count

    def fetch_loans_by_id_list(self, ls, output_loan_basic_dir, output_loan_events_dir, use_delay_minor=False):
        """
        :param ls:
        :param output_loan_basic_dir:
        :param output_loan_events_dir:
        :param use_delay_minor: If true, then self.delay_minor will be used between each call.
        :return:
        """
        if len(ls) == 0:
            return 0, 0

        ls_chunks = self._divide_chunks(ls, self.chunk_size)
        for chunk in ls_chunks:
            session = requests.Session()

            session_basic_res: list[dict] = []
            session_events_res: list[dict] = []
            for elm in chunk:
                endpoint = f'/api/loans/{elm}/detailed'
                response = self._req_call(session, endpoint, dict(), max_retries=3)
                if response is not None and response.status_code == 200:
                    response_json = response.json()
                    assert 'basic' in response_json and 'events' in response_json
                    # Append
                    loan_basic = response_json['basic']
                    loan_basic['loan_id'] = elm
                    session_basic_res.append(loan_basic)

                    loan_events = response_json['events']
                    for event in loan_events:
                        event = self._clean_event(event, elm)
                        session_events_res.append(event)
                else:
                    sys.exit(1)
                    logging.error("404 Error fetching loan id %s", elm)
                if use_delay_minor:
                    logging.debug("Adding delay of %s seconds between id's to comply with API.", self.delay_minor)
                    time.sleep(self.delay_minor)

            chunk_start = chunk[0]
            chunk_end = chunk[-1]

            output_basic_file = f"loan_basic_{chunk_start}_to_{chunk_end}.csv"
            output_events_file = f"loan_events_{chunk_start}_to_{chunk_end}.csv"
            output_basic_file_full_path = os.path.join(output_loan_basic_dir, output_basic_file)
            output_events_file_full_path = os.path.join(output_loan_events_dir, output_events_file)

            df_basic = pd.DataFrame(session_basic_res)
            df_events = pd.DataFrame(session_events_res)

            df_basic.to_csv(output_basic_file_full_path, escapechar='\\', index=False)
            df_events.to_csv(output_events_file_full_path, escapechar='\\', index=False)

            time.sleep(self.delay_major)
        return len(result), len(ls)

    @staticmethod
    def _clean_event(event, loan_id):
        assert 'event_type' in event and 'occurred_at' in event
        event_type = event['event_type']
        assert event_type in ['creation', 'repayment', 'unpaid', 'admin']
        event['loan_id'] = loan_id

        event_columns = [
            'loan_id',
            'event_type',
            'occurred_at',
            'creation_type',
            'creation_permalink',
            'repayment_minor',
            'unpaid',
            'old_principal_minor',
            'new_principal_minor',
            'old_principal_repayment_minor',
            'new_principal_repayment_minor',
            'old_created_at',
            'new_created_at',
            'old_repaid_at',
            'new_repaid_at',
            'old_unpaid_at',
            'new_unpaid_at',
            'old_deleted_at',
            'new_deleted_at'
        ]
        for col in event_columns:
            if col not in event:  # Fill keys if not exist
                event[col] = None

        # Pop admin and reason keys if exists, since no perms (these fields also useless)
        event.pop('admin', None)
        event.pop('reason', None)
        return event

    def _req_call(self, session, endpoint, params, max_retries=3):
        """
        Helper function for calling loansbot API. Backoff-retry algo compliance is handled automatically
        but the caller must implement delay between HTTP connections/requests.

        :param session:
        :param endpoint:
        :param params:
        :param max_retries:
        :return:
        """
        retry_count = 0
        BACKOFF_DELAY = 62

        while retry_count <= max_retries:
            # Re-authenticate if required
            if self.auth_data is not None:
                auth_expiry = self.auth_data['expires_at_utc']
                if auth_expiry <= datetime.datetime.now().timestamp():
                    self.authenticate(self.auth)
                auth_token = self.auth_data['token']
                self.headers['Authorization'] = auth_token
            url = self.BASE_URL + endpoint
            logging.info(f"API call to %s with header %s and params %s.", url, self.headers, params)
            r = session.get(url, headers=self.headers, timeout=self.timeout, params=params)
            if r.status_code == 200 or r.status_code == 404:
                return r

            retry_count += 1
            logging.info("Request failed with status code %d. Delaying for %d seconds and retrying.",
                         r.status_code, BACKOFF_DELAY)
            time.sleep(BACKOFF_DELAY)  # Appease the backoff algo

        logging.error("Max retries exceeded for call to endpoint %s with headers %s and params %s. Exiting.",
                      endpoint, self.headers, params)
        sys.exit(1)

    def authenticate(self, auth: dict, endpoint='/api/users/login'):
        """
        Refresh the auth data for the user.

        :param auth: A dict with the login credentials
        :param endpoint: The login endpoint
        :return: The resulting dict from the request, or None if failed
        """
        if self.auth is None:
            return

        logging.info("Attempting to authenticate credentials %s", auth)

        # Assert that the auth dict is in the correct format
        assert 'username' in auth
        assert 'password' in auth
        assert 'password_authentication_id' in auth

        url = self.BASE_URL + endpoint
        response = requests.post(url, headers=self.headers, timeout=self.timeout, json=auth)
        if response.status_code != 200:
            logging.error("Authentication attempt failed! Supplied params: %s", auth)
            sys.exit(1)
        self.auth_data = response.json()
        return

    @staticmethod
    def _divide_chunks(ls, n):
        """
        Yield successive n-sized chunks from ls.

        :param ls: List to divide into chunks
        :param n: Size of each chunk
        :return: List of lists of size at most n.
        """
        for i in range(0, len(ls), n):
            yield ls[i:i + n]

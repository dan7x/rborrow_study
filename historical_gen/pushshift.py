from datetime import datetime

from historical_gen._dataretriever import DataRetriever


class PushShift(DataRetriever):
    BASE_URL = "https://api.pushshift.io"

    headers = {
        "Content-Type": "application/json; charset=utf-8"
    }

    def __init__(self, start_date_unix=None, end_date_unix=None, limit=500, timeout=60):
        super().__init__(timeout)
        if start_date_unix is not None:
            self.start_date_unix = start_date_unix
        if end_date_unix is not None:
            self.end_date_unix = end_date_unix
        self.limit = limit

    def submissions(self, subreddit='borrow', endpoint='/reddit/search/submission/'):
        assert self.start_date_unix is not None and self.end_date_unix is not None
        params = {
            'subreddit': subreddit,
            'sort': 'created_utc',
            'sort_type': 'desc',
            'since': self.start_date_unix,
            'until': self.end_date_unix,
            'size': self.limit
        }
        return self.req_call(self.BASE_URL + endpoint, self.headers, params)

    def comment(self, subreddit='borrow', endpoint='/reddit/search/comment/'):
        assert self.start_date_unix is not None and self.end_date_unix is not None
        params = {
            'q': 'loan',
            'subreddit': subreddit,
            'sort': 'created_utc',
            'sort_type': 'desc',
            'since': self.start_date_unix,
            'until': self.end_date_unix,
            'size': self.limit
        }
        return self.req_call(self.BASE_URL + endpoint, self.headers, params)

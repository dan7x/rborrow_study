import logging
import os
import pandas as pd
import numpy as np
from abc import ABC, abstractmethod


class Ingest(ABC):

    def __init__(self, folder, db):
        self.db = db
        self.folder = folder

    @abstractmethod
    def ingest(self):
        pass


class IngestSubmissions(Ingest):

    def __init__(self, folder, db):
        super().__init__(folder, db)

    def ingest(self):
        sql_create_staging_submissions_raw = """ 
        CREATE TABLE IF NOT EXISTS staging_submissions_raw (
            selftext TEXT,
            author_fullname TEXT,
            title TEXT,
            hidden INTEGER,
            author_flair_background_color TEXT,
            is_original_content INTEGER,
            is_reddit_media_domain INTEGER,
            is_meta INTEGER,
            link_flair_text TEXT,
            score INTEGER,
            author_premium INTEGER,
            upvote_ratio TEXT,
            total_awards_received INTEGER,
            edited INTEGER,
            is_self INTEGER,
            removed_by_category TEXT,
            author_flair_type TEXT,
            domain TEXT,
            archived INTEGER,
            no_follow INTEGER,
            is_crosspostable INTEGER,
            pinned INTEGER,
            over_18 INTEGER,
            media_only INTEGER,
            can_gild INTEGER,
            spoiler INTEGER,
            locked INTEGER,
            author_flair_text TEXT,
            removed_by TEXT,
            subreddit_id TEXT,
            id TEXT PRIMARY KEY,
            is_robot_indexable INTEGER,
            author TEXT,
            num_comments INTEGER,
            send_replies INTEGER,
            contest_mode INTEGER,
            author_patreon_flair INTEGER,
            author_flair_text_color TEXT,
            permalink TEXT,
            stickied INTEGER,
            url TEXT,
            subreddit_subscribers INTEGER,
            created_utc INTEGER,
            num_crossposts INTEGER,
            is_video INTEGER,
            retrieved_utc INTEGER,
            updated_utc INTEGER,
            utc_datetime_str TEXT 
        ); 
        """
        self.db.execute_query(sql_create_staging_submissions_raw)
        for filename in os.scandir(self.folder):
            if filename.is_file():
                self._ingest_file(filename.path)

    def _ingest_file(self, file):
        logging.info("Ingesting %s", file)
        df = pd.read_csv(file, escapechar='\\')

        cols = [
            "selftext",
            "author_fullname",
            "title",
            "hidden",
            "author_flair_background_color",
            "is_original_content",
            "is_reddit_media_domain",
            "is_meta",
            "link_flair_text",
            "score",
            "author_premium",
            "upvote_ratio",
            "total_awards_received",
            "edited",
            "is_self",
            "removed_by_category",
            "author_flair_type",
            "domain",
            "archived",
            "no_follow",
            "is_crosspostable",
            "pinned",
            "over_18",
            "media_only",
            "can_gild",
            "spoiler",
            "locked",
            "author_flair_text",
            "removed_by",
            "subreddit_id",
            "id",
            "is_robot_indexable",
            "author",
            "num_comments",
            "send_replies",
            "contest_mode",
            "author_patreon_flair",
            "author_flair_text_color",
            "permalink",
            "stickied",
            "url",
            "subreddit_subscribers",
            "created_utc",
            "num_crossposts",
            "is_video",
            "retrieved_utc",
            "updated_utc",
            "utc_datetime_str"
        ]

        for col in cols:
            if col not in df.columns:
                df[col] = np.nan
        df = df[cols]

        df.to_sql('staging_submissions_raw', self.db.conn, if_exists='append', index=False)


class IngestComments(Ingest):

    def __init__(self, folder, db):
        super().__init__(folder, db)

    def ingest(self):
        sql_create_staging_submissions_raw = """ 
        CREATE TABLE IF NOT EXISTS staging_comments_raw (
            id TEXT PRIMARY KEY,
            permalink TEXT,
            link_id TEXT,
            locked INTEGER,
            author TEXT,
            author_fullname TEXT,
            body TEXT,
            loan_id INTEGER,
            created_utc INTEGER,
            retrieved_utc INTEGER,
            updated_utc INTEGER,
            utc_datetime_str TEXT,
            nest_level INTEGER,
            is_submitter INTEGER,
            parent_id INTEGER
        ); 
        """
        self.db.execute_query(sql_create_staging_submissions_raw)
        for filename in os.scandir(self.folder):
            if filename.is_file():
                self._ingest_file(filename.path)

    def _ingest_file(self, file):
        logging.info("Ingesting %s", file)
        df = pd.read_csv(file, escapechar='\\')

        cols = [
            "id",
            "permalink",
            "link_id",
            "locked",
            "author",
            "author_fullname",
            "body",
            "loan_id",
            "created_utc",
            "retrieved_utc",
            "updated_utc",
            "utc_datetime_str",
            "nest_level",
            "is_submitter",
            "parent_id"
        ]

        for col in cols:
            if col not in df.columns:
                df[col] = np.nan

        df = df[cols]
        df = df[df['body'].str.contains("$loan", regex=False)]

        df.to_sql('staging_comments_raw', self.db.conn, if_exists='append', index=False)


class IngestLoanIDs(Ingest):

    def __init__(self, folder, db):
        super().__init__(folder, db)

    def ingest(self):
        sql_create_staging_submissions_raw = """ 
        CREATE TABLE IF NOT EXISTS staging_loan_ids (
            id INTEGER PRIMARY KEY
        ); 
        """
        self.db.execute_query(sql_create_staging_submissions_raw)
        for filename in os.scandir(self.folder):
            if filename.is_file():
                self._ingest_file(filename.path)

    def _ingest_file(self, file):
        logging.info("Ingesting %s", file)
        df = pd.read_csv(file, escapechar='\\')
        df.to_sql('staging_loan_ids', self.db.conn, if_exists='append', index=False)

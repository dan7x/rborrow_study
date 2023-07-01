import argparse
import datetime
import sys
import os.path
import logging
from pathlib import Path
from db import LoansDB

import yaml
from historical_gen.staging import SubmissionRetriever, CommentRetriever
from historical_gen.ingest import IngestSubmissions, IngestComments, IngestLoanIDs
from historical_gen.loansretriever import LoansRetriever


class App:

    def __init__(self, cfg="config.yaml"):
        # Get args
        self.args = self._parse_args()

        # Init config
        with open(cfg, "r") as stream:
            try:
                self.config = yaml.safe_load(stream)
            except yaml.YAMLError as e:
                print(e)
                sys.exit(1)

        # Init logging
        logging_cfg = self.config['LOGGING']
        logging.basicConfig(filename=logging_cfg['LOGFILE'],
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            level=logging.DEBUG
                            )

        # Init db
        self.db = LoansDB(self.config['DATA']['DB'])

        # LoansBot auth
        self.auth = {k.lower(): v for k, v in self.config['AUTH'].items()}

        # Everything else
        STAGING_ROOT = 'staging'
        STAGING_SUB = self.config['STAGING']
        self.staging_submissions_out = os.path.join(STAGING_ROOT, STAGING_SUB['SUBMISSIONS_FOLDER'])
        self.staging_comments_out = os.path.join(STAGING_ROOT, STAGING_SUB['COMMENTS_FOLDER'])
        self.staging_loan_ids_out = os.path.join(STAGING_ROOT, STAGING_SUB['LOAN_IDS_FOLDER'])
        self.staging_loan_basic = os.path.join(STAGING_ROOT, STAGING_SUB['LOAN_BASIC_FOLDER'])
        self.staging_loan_events = os.path.join(STAGING_ROOT, STAGING_SUB['LOAN_EVENTS_FOLDER'])

        Path(STAGING_ROOT).mkdir(parents=True, exist_ok=True)
        Path(self.staging_submissions_out).mkdir(parents=True, exist_ok=True)
        Path(self.staging_comments_out).mkdir(parents=True, exist_ok=True)
        Path(self.staging_loan_ids_out).mkdir(parents=True, exist_ok=True)
        Path(self.staging_loan_basic).mkdir(parents=True, exist_ok=True)
        Path(self.staging_loan_events).mkdir(parents=True, exist_ok=True)

        if self.config['DEBUG']:
            self._init_debug()

    @staticmethod
    def _init_debug():
        # Add handler for stdout
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        root.addHandler(handler)

    @staticmethod
    def _parse_args():
        parser = argparse.ArgumentParser(description='Scrape, ingest, and analyze r/borrow data.')
        parser.add_argument('--start-date',
                            help='Start date as "YYYY-MM-DD"',
                            default='2014-09-17')
        parser.add_argument('--end-date',
                            help='End date as "YYYY-MM-DD"',
                            default=datetime.date.today().strftime('%Y-%m-%d'))
        return parser.parse_args()

    def main(self):
        # Fetch raw submissions as CSV
        # submission_retriever = SubmissionRetriever(self.args.start_date, self.args.end_date, timeout=120, delay=1)
        # submissions = submission_retriever.fetch_submissions(self.staging_submissions_out)
        # logging.debug(str(submissions))

        # Fetch IDs of ingested submissions and fetch raw comments as CSV
        # comment_retriever = CommentRetriever(self.args.start_date, self.args.end_date, timeout=120, delay=1)
        # comments = comment_retriever.fetch_comments(self.staging_comments_out, freq='12H')
        # logging.debug(str(comments))

        # Ingest submission CSVs into db
        # submission_ingest = IngestSubmissions(self.staging_submissions_out, self.db)
        # submission_ingest.ingest()

        # Ingest comment CSVs into db
        # comment_ingest = IngestComments(self.staging_comments_out, self.db)
        # comment_ingest.ingest()

        # Fetch loan IDs
        loan_retriever = LoansRetriever(70, auth=self.auth)
        # loan_ids = loan_retriever.fetch_loan_ids(self.args.start_date, self.args.end_date, self.staging_loan_ids_out,
        #                                          historical=True)
        # logging.info("Identified %s loan ids", loan_ids)

        # Ingest Loan IDs
        # loan_ingest = IngestLoanIDs(self.staging_loan_ids_out, self.db)
        # loan_ingest.ingest()

        loan_id_query = """
        SELECT 
        id
        FROM staging_loan_ids
        WHERE id > 20813
        ORDER BY id ASC;
        """
        loan_ids = self.db.execute_query(loan_id_query)
        loan_ids = list(loan_ids)
        if len(loan_ids) > 0:
            loan_ids = [x['id'] for x in loan_ids]
            loan_retriever.fetch_loans_by_id_list(loan_ids, self.staging_loan_basic, self.staging_loan_events)
            breakpoint()

        # Clean and consolidate data using NLP


if __name__ == '__main__':
    App().main()

import sqlite3
from sqlite3 import Error
import sys
import logging


class LoansDB:

    def __init__(self, db_file: str):
        try:
            conn = sqlite3.connect(db_file)
            conn.row_factory = self.dict_factory
            self.conn = conn
        except Error as e:
            logging.error(e)
            sys.exit(1)

    def close(self):
        if self.conn is not None:
            self.conn.commit()
            self.conn.close()
            logging.info("Successfully closed db connection.")
            return
        logging.info("No db connection to close.")

    def execute_query(self, query, params=list()):
        try:
            c = self.conn.cursor()
            c.execute(query, params)
            return c
        except Error as e:
            logging.error(e)

    @staticmethod
    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

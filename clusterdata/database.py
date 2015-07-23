# -*- coding: utf-8 -*-

import psycopg2

from config import config


params = " ".join(["dbname='{}'",
                   "user='{}'",
                   "host='localhost'",
                   "password='{}'"])

user = config.get("database", "user")
pw = config.get("database", "pass")
db = config.get("database", "db")

params = params.format(db, user, pw)


class DatabaseConnectionError(Exception):
    pass


def get_connection():
    try:
        return psycopg2.connect(params)
    except:
        raise DatabaseConnectionError("cannot connect to database")

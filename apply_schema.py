import sys
import csv
from pprint import pprint

from itertools import groupby


disclaimer = """
Make sure that your input schema file is sanitized!
"""

class _TypeError(Exception):
    pass


def type_map(t):
    if t == "STRING_HASH":
        return "VARCHAR(512)"
    if t == "STRING_HASH_OR_INTEGER":
        raise _TypeError("Don't know how to interpret that")
    return t


def extract_table_name(row):
    path = row[0]
    name = path.split("/")[0]
    return name


def extract_index(row):
    return int(row[1])


def extract_type(row):
    return type_map(row[3])


def extract_mandatory(row):
    return row[4] == "YES"


def extract_col_name(row):
    repl = [(' ', '_'),
            ("user", "username"),
            ("/", "")]
    s = row[2]
    for a, b in repl:
        s = s.replace(a, b)
    return s


def describe_single_col(row):
    d = dict()
    d["name"] = extract_col_name(row)
    d["type"] = extract_type(row)

    d["extras"] = ""

    if extract_mandatory(row):
        d["extras"] += " NOT NULL"

    return "{name} {type}{extras}".format(**d)


def run(conn, f):
    reader = csv.reader(f)
    header = reader.next()

    grouped = groupby(reader, extract_table_name)

    with conn.cursor() as c:

        for name, g in grouped:
            try:
                d = dict(name=name, desc=", ".join(map(describe_single_col, g)))
            except _TypeError:
                print("skipping table '{}' with strange types".format(name))
            cmd = "DROP TABLE IF EXISTS {name};".format(**d)
            c.execute(cmd)
            cmd = "CREATE TABLE {name} ({desc});".format(**d)
            c.execute(cmd)


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("-y", "--disclaimer_accepted", action="store_true",
                        default=False,
                        help="I have read the disclaimer")
    parser.add_argument("-f", "--file", action="store", type=file,
                        default=sys.stdin,
                        help="location of schema.csv (default: stdin)")
    args = parser.parse_args()
    print(disclaimer)

    from database import connection as conn

    try:
        if not args.disclaimer_accepted:
            print("re-run with -y if your input is sanitized.")
        else:
            run(conn, args.file)
    except:
        raise
    else:
        conn.commit()
    finally:
        conn.close()
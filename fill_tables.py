
import logging
import csv
import glob
import gzip

from itertools import imap

from schema import get_tables


logger = logging.getLogger("fill_tables")
logging.basicConfig(level=logging.INFO)


def run(conn):
    for table in get_valid_tables():
        logger.info("*********************************************")
        logger.info("processing table {}".format(table.name))

        n = len(table)
        g = table.get_glob()

        cmd = "INSERT INTO {} VALUES ({});".format(
            table.name, ", ".join(["%s"]*n))

        for filename in glob.glob(g):
            logger.info("processing file {}".format(filename))

            with GzipFile(filename, 'r') as f:
                reader = csv.reader(f)
                parsed = imap(table.prepare, reader)

                with conn.cursor() as c:
                    c.executemany(cmd, parsed)
            conn.commit()


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("-y", "--disclaimer_accepted", action="store_true",
                        default=False,
                        help="I am running this on my own risk")
    args = parser.parse_args()

    from database import connection as conn

    try:
        if not args.disclaimer_accepted:
            print("re-run with -y if your input is sanitized.")
        else:
            run(conn)
    except:
        raise
    finally:
        conn.close()

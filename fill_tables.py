
import logging
import csv
import glob
import gzip

from clusterdata.schema import get_valid_tables
from clusterdata.database import get_connection


logger = logging.getLogger("fill_tables")
logging.basicConfig(level=logging.INFO)


def run(conn, debug):
    if not debug:
        logger.info("processing googleclusterdata, "
                    "this might take a *long* time...")
    for table in get_valid_tables():
        logger.info("")
        logger.info("*********************************************")
        logger.info("processing table '{}'".format(table.name))

        n = len(table)
        g = table.get_glob()

        cmd = "INSERT INTO {} VALUES ({});".format(
            table.name, ", ".join(["%s"]*n))

        for filename in sorted(glob.glob(g)):
            logger.info("processing file '{}'".format(filename))

            with gzip.GzipFile(filename, 'r') as f:
                reader = csv.reader(f)
                parsed = map(table.prepare, reader)

                with conn.cursor() as c:
                    c.executemany(cmd, parsed)
            if debug:
                logger.info("skipping remainder because "
                            "we're in debug mode")
                break
            conn.commit()


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("-y", "--disclaimer_accepted", action="store_true",
                        default=False,
                        help="I am running this on my own risk")
    parser.add_argument("-d", "--debug", action="store_true",
                        default=False,
                        help="run in debug mode (skips most data)")
    args = parser.parse_args()

    conn = get_connection()

    try:
        if not args.disclaimer_accepted:
            print("re-run with -y if your input is sanitized.")
        else:
            run(conn, args.debug)
    except:
        raise
    finally:
        conn.close()

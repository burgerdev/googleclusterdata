
import time
import logging
import glob
import gzip

from clusterdata.schema import get_valid_tables
from clusterdata.database import get_connection


logger = logging.getLogger("fill_tables")


def format_seconds(s):
    if s < 60:
        return "{:.1f}s".format(s)
    s /= 60.0
    if s < 60:
        return "{:.1f}m".format(s)
    s /= 60.0
    if s < 24:
        return "{:.1f}h".format(s)
    s /= 24.0
    return "{:.1f}d".format(s)


def run(conn, args):
    debug = args.debug

    already_processed = set()
    if args.import_file is not None:
        with open(args.import_file, 'r') as f:
            l = [line.strip() for line in f]
        already_processed = set(l)

    export_file = None
    if args.export_file is not None:
        export_file = open(args.export_file, 'a')

    try:
        if not debug:
            logger.info("processing googleclusterdata, "
                        "this might take a *long* time...")
        for table in get_valid_tables():
            logger.info("")
            logger.info("*********************************************")
            logger.info("processing table '{}'".format(table.name))

            start_time = time.time()

            g = table.get_glob()

            filenames = sorted(glob.glob(g))
            num_filenames = len(filenames)
            actually_processed = 0.0

            for i, filename in enumerate(filenames):
                if filename in already_processed:
                    logger.info("skipping file '{}'".format(filename))
                    continue
                logger.info("processing file '{}'".format(filename))

                with gzip.GzipFile(filename, 'r') as f:
                    with conn.cursor() as c:
                        c.copy_from(f, table.name, sep=',', null='')
                if debug:
                    logger.info("skipping remainder because "
                                "we're in debug mode")
                    break
                conn.commit()
                if export_file is not None:
                    export_file.write("{}\n".format(filename))

                actually_processed += 1

                total_elapsed_time = time.time() - start_time
                mean_elapsed_time = total_elapsed_time / actually_processed
                time_to_go = (num_filenames-i-1) * mean_elapsed_time
                logger.info("Estimated time remaining for this table: "
                            "{}".format(format_seconds(time_to_go)))

    finally:
        if export_file is not None:
            export_file.close()

if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("-y", "--disclaimer_accepted", action="store_true",
                        default=False,
                        help="I am running this on my own risk")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="print progress indicators")
    parser.add_argument("-d", "--debug", action="store_true", default=False,
                        help="run in debug mode (skips most data)")
    parser.add_argument("-e", "--export-file", action="store",
                        default=None,
                        help="save information to this file for a future run")
    parser.add_argument("-i", "--import-file", action="store",
                        default=None,
                        help="use this file to resume a former run")
    args = parser.parse_args()

    if args.export_file is None:
        args.export_file = args.import_file

    conn = get_connection()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARN)

    try:
        if not args.disclaimer_accepted:
            print("re-run with -y if your input is sanitized.")
        else:
            run(conn, args)
    except:
        raise
    finally:
        conn.close()

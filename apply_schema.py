
from schema import Table, get_table_names

import logging

logger = logging.getLogger("apply_schema")
logging.basicConfig(level=logging.INFO)

disclaimer = """
Make sure that your input schema file is sanitized!
"""


def run(conn):
    with conn.cursor() as c:
        for name in get_table_names():
            table = Table(name)
            try:
                desc = map(lambda col: col.describe(), table)
            except ValueError:
                logger.warn("skipping table '{}' with "
                            "strange types".format(name))
                continue

            logger.info("dropping table '{}', if it exists".format(name))
            cmd = "DROP TABLE IF EXISTS {};".format(name)
            c.execute(cmd)

            logger.info("creating table '{}', if it exists".format(name))
            cmd = "CREATE TABLE {} ({});".format(name, ", ".join(desc))
            c.execute(cmd)


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("-y", "--disclaimer_accepted", action="store_true",
                        default=False,
                        help="I have read the disclaimer")
    args = parser.parse_args()
    print(disclaimer)

    from database import connection as conn

    try:
        if not args.disclaimer_accepted:
            print("re-run with -y if your input is sanitized.")
        else:
            run(conn)
    except:
        raise
    else:
        conn.commit()
    finally:
        conn.close()
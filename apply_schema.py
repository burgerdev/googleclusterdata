
from schema import get_valid_tables

import logging

logger = logging.getLogger("apply_schema")
logging.basicConfig(level=logging.INFO)

disclaimer = """
Make sure that your input schema file is sanitized!
"""


def run(conn):
    with conn.cursor() as c:
        for table in get_valid_tables():
            name = table.name
            desc = map(lambda col: col.describe(), table)

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

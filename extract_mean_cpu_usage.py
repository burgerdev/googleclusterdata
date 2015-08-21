
import sys
import os

import numpy as np
import h5py

import logging

from clusterdata.database import get_connection
from clusterdata.log import update_log
from clusterdata.log import ConsoleOutputSuppression

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig()


_QUERY = """
SELECT cpu_rate, start_time, end_time FROM task_usage
ORDER BY start_time, end_time
LIMIT %(limit)s OFFSET %(offset)s
"""


class EndOfData(Exception):
    pass


def add_means_to_array(arr, out):
    """
    TODO doc
    """
    min_ind = int(np.floor(arr[0, 1]))
    max_ind = int(np.ceil(arr[-1, 2]))
    if max_ind == int(arr[-1, 2]):
        max_ind += 1
    n = max_ind-min_ind

    x = np.zeros((n,))

    for i in range(n):
        a = np.maximum(arr[:, 1], i)
        b = np.minimum(arr[:, 2], i+1)
        w = b - a
        w = np.minimum(w, 1)
        w = np.maximum(w, 0)
        x[i] = (arr[:, 0] * w).sum()

    out[min_ind:max_ind] += x


def get_data_for_interval(limit, offset, start, end, resolution, out):
    params = dict(limit=limit, offset=offset)

    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_QUERY, params)
            res = cursor.fetchall()

    x = np.asarray(res)
    if x.size == 0:
        raise EndOfData("no data in this range")
    # normalize to indices
    max_start_time = x[-1, 1]
    x[:, 1] = np.maximum(x[:, 1], start)
    x[:, 2] = np.minimum(x[:, 2], end)
    x[:, 1] -= start
    x[:, 2] -= start
    x[:, 1:] /= float(resolution)

    try:
        add_means_to_array(x, out)
    except ValueError:
        print(x[:, 1].min(), x[:, 2].min())
        print(x[:, 1].max(), x[:, 2].max())
        raise
    return max_start_time, len(x)


def run(args):
    h5f = h5py.File(args.output, 'w')
    times = np.arange(args.start, args.end, args.resolution)
    output = np.zeros((len(times), 2))
    output[:, 0] = times
    h5ds = h5f.require_dataset("cpu_usage",
                               shape=output.shape, dtype=np.float64)

    offset = 0
    limit = args.chunksize

    while True:
        try:
            t, n = get_data_for_interval(limit, offset, args.start, args.end,
                                         args.resolution, output[:, 1])
            offset += n
            if args.verbose:
                update_log(t, args.start, args.end)
            logger.info("Processed {} rows".format(offset))
        except EndOfData:
            break
        finally:
            h5ds[:] = output[:]

    if args.verbose:
        sys.stdout.write("\n")

    h5f.close()


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("output", help="write HDF5 output here")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="print progress indicators")
    parser.add_argument("-r", "--resolution", action="store", type=int,
                        default=int(1e6*60*5),
                        help="resolution of host load result in microseconds")
    parser.add_argument("-s", "--start", action="store", type=int,
                        default=600000000,
                        help="start time in microseconds")
    parser.add_argument("-e", "--end", action="store", type=int,
                        default=2506200000001,
                        help="end time in microseconds")
    parser.add_argument("-c", "--chunksize", action="store", type=int,
                        default=10000,
                        help="number of rows to fetch in each iteration")

    args = parser.parse_args()

    with ConsoleOutputSuppression():
        run(args)

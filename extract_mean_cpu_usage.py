
import sys

import numpy as np
import h5py

from clusterdata.database import get_connection


# good idea, but cpu usage is not normalized per machine but globally

#_MACHINE_SUBQUERY = """
#SELECT DISTINCT(machine_id) AS m_id, cpus FROM machine_events
#""".strip()
#
#_QUERY = """
#SELECT cpu_rate, cpus FROM task_usage JOIN ({}) AS machines
#ON task_usage.machine_id = machines.m_id
#WHERE task_usage.start_time <= %s AND task_usage.end_time > %s
#""".format(_MACHINE_SUBQUERY).strip()

_QUERY = """
SELECT cpu_rate, start_time, end_time FROM task_usage
WHERE start_time <= %(end)s AND end_time >= %(start)s
"""


def to_day(t):
    return t/(1e6*60*60*24)


def to_percentage(t, m):
    return float(t)/m*100


def update_log(current_time_us, min_time_us, max_time_us):
    current_time_us -= min_time_us
    max_time_us -= min_time_us
    current_time_d = to_day(current_time_us)
    max_time_d = to_day(max_time_us)
    percentage = to_percentage(current_time_d, max_time_d)
    sys.stdout.write("\rday {:.01f} of {:.01f} ({:.02f}%)"
                     "".format(current_time_d, max_time_d, percentage))
    sys.stdout.flush()


def get_cpu_usage_for_interval(start, end):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_QUERY, {'start': start, 'end': end})
            res = cursor.fetchall()
    x = np.asarray(res)
    if x.size == 0:
        return 0.0
    a = np.maximum(x[:, 1], start)
    b = np.minimum(x[:, 2], end)
    w = (b - a)/float(end - start)
    return (x[:, 0] * w).sum()


def run(args):
    h5f = h5py.File(args.output, 'w')
    times = np.arange(args.start, args.end, args.resolution)
    output = np.zeros((len(times), 2))
    output[:, 0] = times
    h5ds = h5f.require_dataset("cpu_usage",
                               shape=output.shape, dtype=np.float64)

    for i, t in enumerate(times):
        if args.verbose:
            update_log(t, args.start, args.end)
        output[i, 1] = get_cpu_usage_for_interval(t, t+args.resolution)

    if args.verbose:
        sys.stdout.write("\n")

    h5ds[:] = output[:]
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
                        
    args = parser.parse_args()

    run(args)
        

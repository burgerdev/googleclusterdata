
import time
import logging
import glob
import gzip
import csv

import h5py
import numpy as np
import pandas

from clusterdata.schema import get_valid_tables
from fill_tables import format_seconds


logger = logging.getLogger(__name__)


def add_means_to_array(arr, out):
    n = len(out)
    first_ind = int(np.floor(arr[:, 1].min()))
    first_ind = max(0, first_ind)
    last_ind = int(np.floor(arr[:, 2].max()))
    last_ind = min(n, last_ind)
    x = np.zeros((n,))

    for i in range(first_ind, last_ind):
        a = np.maximum(arr[:, 1], i)
        b = np.minimum(arr[:, 2], i+1)
        w = b - a
        w = np.minimum(w, 1)
        w = np.maximum(w, 0)
        x[i] = (arr[:, 0] * w).sum()

    out[:] += x


def process_csv(csv_file, start, end, resolution, out):
    df = pandas.read_csv(csv_file, header=None)
    x = df[[5, 0, 1]].as_matrix()
    x[:, 1] = np.maximum(x[:, 1], start)
    x[:, 2] = np.minimum(x[:, 2], end)
    x[:, 1] -= start
    x[:, 2] -= start
    x[:, 1:] /= float(resolution)
    add_means_to_array(x, out)


def run(args):
    times = np.arange(args.start, args.end, args.resolution)
    output = np.zeros((len(times), 2))
    output[:, 0] = times

    with h5py.File(args.output, 'w') as h5f:
        h5ds = h5f.require_dataset("cpu_usage",
                                   shape=output.shape, dtype=np.float64)
        h5ds[:] = output

    already_processed = set()
    if args.import_file is not None:
        with open(args.import_file, 'r') as f:
            l = [line.strip() for line in f]
        already_processed = set(l)

    export_file = None
    if args.export_file is not None:
        export_file = open(args.export_file, 'a')

    try:
        table = filter(lambda t: t.name == "task_usage", get_valid_tables())[0]

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

            with h5py.File(args.output, 'a') as h5f:
                h5ds = h5f.require_dataset("cpu_usage",
                                           shape=output.shape,
                                           dtype=np.float64)
                output[:] = h5ds[:]
                with gzip.GzipFile(filename, 'r') as f:
                    process_csv(f,
                                args.start, args.end, args.resolution,
                                output[:, 1])
                h5ds[:] = output[:]

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
    parser.add_argument("output", help="write HDF5 output here")
    parser.add_argument("-v", "--verbose", action="store_true", default=False,
                        help="print progress indicators")
    parser.add_argument("-r", "--resolution", action="store", type=int,
                        default=int(1e6*60),
                        help="resolution of host load result in microseconds")
    parser.add_argument("--start", action="store", type=int,
                        default=600000000,
                        help="start time in microseconds")
    parser.add_argument("--end", action="store", type=int,
                        default=2506200000001,
                        help="end time in microseconds")
    parser.add_argument("-e", "--export-file", action="store",
                        default=None,
                        help="save information to this file for a future run")
    parser.add_argument("-i", "--import-file", action="store",
                        default=None,
                        help="use this file to resume a former run")
    args = parser.parse_args()

    if args.export_file is None:
        args.export_file = args.import_file

    if args.verbose:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.WARN)
    logging.basicConfig()

    run(args)

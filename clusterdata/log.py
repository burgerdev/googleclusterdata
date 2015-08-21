
import sys
import os


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


class ConsoleOutputSuppression(object):
    def __enter__(self):
        os.system("stty -echo")

    def __exit__(self, *args):
        os.system("stty echo")

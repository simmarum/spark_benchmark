import time
import os


def create_log_file_path():
    tmp_name = int(time.time())
    log_path = os.path.join(
        os.path.dirname(__file__),
        'log',
        '{}.csv.log'.format(tmp_name)
    )
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "w") as myfile:
        tmp_row = [
            "tid",
            "s",
            "v",
            "w",
            "l_format",
            "n",
            "time",
            "cpu",
            "memory",
        ]
        myfile.write('\t'.join(tmp_row))
        myfile.write('\n')
    return log_path, tmp_name


def append_to_log_file(log_path, tid, s, v, w, l_format, n, ptimer):
    with open(log_path, "a") as myfile:
        tmp_row = [
            "{0:d}".format(tid),
            "{0}".format(s),
            "{0}".format(v),
            "{0}".format(w),
            "{0}".format(l_format),
            "{0:d}".format(n),
            "{0:.2f}".format(ptimer.t1 - ptimer.t0),
            "{0:.2f}".format(sum(ptimer.cpu_percent_list)/len(ptimer.cpu_percent_list)),
            "{0:d}".format(max(ptimer.rss_memory_list)),
            # ptimer.time_list
        ]
        myfile.write('\t'.join(tmp_row))
        myfile.write('\n')

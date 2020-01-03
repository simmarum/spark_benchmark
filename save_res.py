import time
import os


def create_res_file_path():
    tmp_name = int(time.time())
    res_path = os.path.join(
        os.path.dirname(__file__),
        'res',
        '{}.tsv'.format(tmp_name)
    )
    os.makedirs(os.path.dirname(res_path), exist_ok=True)
    with open(res_path, "w") as myfile:
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
            "read_bytes",
        ]
        myfile.write('\t'.join(tmp_row))
        myfile.write('\n')
    return res_path, tmp_name


def append_to_res_file(res_path, tid, s, v, w, l_format, n, ptimer, avg_st):
    ci = next(x[0] for x in enumerate(ptimer.time_list) if x[1] > avg_st)

    n_time = (ptimer.t1 - ptimer.t0) - avg_st

    tmp_cpu_list = ptimer.cpu_percent_list[ci:]
    n_cpu = sum(tmp_cpu_list)/len(tmp_cpu_list)

    tmp_mem_list = ptimer.rss_memory_list[ci:]
    n_mem = max(tmp_mem_list)

    with open(res_path, "a") as myfile:
        tmp_row = [
            "{0:d}".format(tid),
            "{0}".format(s),
            "{0}".format(v),
            "{0}".format(w),
            "{0}".format(l_format),
            "{0:d}".format(n),
            "{0:.2f}".format(n_time),
            "{0:.2f}".format(n_cpu),
            "{0:d}".format(n_mem),
            "{0:d}".format(ptimer.rb1 - ptimer.rb0),

            # ptimer.time_list
        ]
        myfile.write('\t'.join(tmp_row))
        myfile.write('\n')

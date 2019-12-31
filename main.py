from create_files import mapping
import subprocess
import psutil
import os
import time
from process_timer import ProcessTimer
from log_res import create_log_file_path, append_to_log_file


def get_benchmark_file_path():
    file_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            'benchmark.py',
        ))
    return file_path


def do_ben(avg_st):
    print("###### BENCHMARK ######")
    log_file_path, tid = create_log_file_path()
    b_file_path = get_benchmark_file_path()
    for ps in mapping['s']:
        for pv in mapping['v']:
            for pw in mapping['w']:
                for l_format in ['csv', 'csv.gz', 'snappy.parquet']:
                    for pn in range(1):
                        ptimer = ProcessTimer(['python3', b_file_path, ps, pv, pw, l_format])
                        tmp_data = []
                        try:
                            ptimer.execute()
                            # poll as often as possible; otherwise the subprocess might
                            # "sneak" in some extra memory usage while you aren't looking
                            while ptimer.poll():
                                time.sleep(0.2)
                        finally:
                                # make sure that we don't leave the process dangling?
                            ptimer.close()

                        append_to_log_file(log_file_path, tid, ps, pv, pw, l_format, pn, ptimer, avg_st)
                        print("@@", ptimer.cpu_percent_list)


def get_startup_time(n):
    b_file_path = get_benchmark_file_path()
    tmp_data = []
    for pn in range(n):
        ptimer = ProcessTimer(['python3', b_file_path, '0', '0', '0', '0'])
        try:
            ptimer.execute()
            while ptimer.poll():
                time.sleep(0.2)
        finally:
            ptimer.close()
        tmp_data.append(ptimer.t1-ptimer.t0)
    avg_time = sum(tmp_data)/len(tmp_data)
    return avg_time


def main():
    avg_st = get_startup_time(3)
    do_ben(avg_st)


if __name__ == '__main__':
    main()

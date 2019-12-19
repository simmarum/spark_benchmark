from create_files import mapping
import subprocess
import psutil
import os
import time
from process_timer import ProcessTimer
# # gives a single float value
# psutil.cpu_percent()
# # gives an object with many fields
# psutil.virtual_memory()
# # you can convert that object to a dictionary
# dict(psutil.virtual_memory()._asdict())


def get_benchmark_file_path():
    file_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            'benchmark.py',
        ))
    return file_path


def main():
    b_file_path = get_benchmark_file_path()
    for ps in mapping['s']:
        for pv in mapping['v']:
            for pw in mapping['w']:
                print(b_file_path)
                ptimer = ProcessTimer(['python3', b_file_path, ps, pv, pw])
                tmp_data = []
                try:
                    ptimer.execute()
                    # poll as often as possible; otherwise the subprocess might
                    # "sneak" in some extra memory usage while you aren't looking
                    while ptimer.poll():
                        time.sleep(0.9)
                finally:
                        # make sure that we don't leave the process dangling?
                    ptimer.close()
                print('\t', 'return code:', ptimer.p.returncode)
                print('\t', 'time:', ptimer.t1 - ptimer.t0)
                print('\t', 'cpu_percent:', len(ptimer.cpu_percent_list), ptimer.cpu_percent_list)
                print('\t', 'rss_memory:', len(ptimer.rss_memory_list), ptimer.rss_memory_list)
                print('\t', 'times:', len(ptimer.time_list), ptimer.time_list)


if __name__ == '__main__':
    main()

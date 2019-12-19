import base64
import os
import random
import string
import time
from datetime import datetime
from itertools import islice

import numpy as np
import pandas as pd

from more_itertools import unique_everseen

mapping = {
    's': {
        's': 500000,
        'l': 3000000,
    },
    'v': {
        'u': -1,
        'r': 5,
    },
    'w': {
        's': 3,  # (x3)
        'l': 9,  # (x3)
    }
}


def produce_amount_keys(amount_of_keys):
    def gen_keys(_urandom=os.urandom, _encode=base64.b32encode, _randint=np.random.randint):
        while True:
            _randint(20)
            yield _encode(_urandom(20))[:20].decode('ascii')
    return list(islice(unique_everseen(gen_keys()), amount_of_keys))


def produce_amount_keys_random(string_chars):
    return ''.join(random.choices(string_chars, k=20))


def create_output_path(s, v, w):
    save_path = os.path.join(
        os.path.dirname(__file__),
        'data',
        '{}_{}_{}.'.format(s, v, w)
    )
    return save_path


def create_file(s, v, w):

    if s not in mapping['s']:
        raise ValueError("s should be {}".format(list(mapping['s'])))
    if v not in mapping['v']:
        raise ValueError("v should be {}".format(list(mapping['v'])))
    if w not in mapping['w']:
        raise ValueError("w should be {}".format(list(mapping['w'])))

    string_chars = string.ascii_lowercase + string.ascii_uppercase
    date_format = "%Y-%m-%d %H:%M:%S"
    stime = time.mktime(time.strptime("2018-01-01 00:00:00", date_format))
    etime = time.mktime(time.strptime("2020-01-01 00:00:00", date_format))

    val_v = mapping['v'][v]
    val_s = mapping['s'][s]
    val_w = mapping['w'][w]
    if val_v < 1:
        eint = 10000000
        gen_p = string_chars
        gen_str = produce_amount_keys_random
    else:
        gen_p = produce_amount_keys(val_v*2)
        gen_str = random.choice
        etime = stime + val_v*2
        eint = val_v*2
    tmp_header = ['c{}'.format(i) for i in range(val_w*3)]
    tmp_data = []
    for i in range(val_s):
        tmp_row = []
        for j in range(val_w):
            tmp_row.extend(
                [
                    random.randint(1, eint),
                    gen_str(gen_p),
                    datetime.utcfromtimestamp(int(stime + random.random() * (etime - stime)))
                ]
            )
        tmp_data.append(tmp_row)

    df = pd.DataFrame(tmp_data, columns=tmp_header)
    print("Create dataset")
    save_path = create_output_path(s, v, w)
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    df.to_csv(save_path+"csv", index=False)
    print("Save file to: {}".format(save_path+"csv"))
    df.to_csv(save_path+"csv.gz", index=False)
    print("Save file to: {}".format(save_path+"csv.gz"))
    df.to_parquet(save_path+"snappy.parquet", index=False)
    print("Save file to: {}".format(save_path+"parquet"))

    return save_path


def main():
    for ps in mapping['s']:
        for pv in mapping['v']:
            for pw in mapping['w']:
                create_file(ps, pv, pw)


if __name__ == '__main__':
    main()

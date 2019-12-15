import base64
import math
import os
import random
import string
import time
from datetime import datetime
from itertools import islice

import numpy as np
import pandas as pd

from more_itertools import unique_everseen


def produce_amount_keys(amount_of_keys):
    def gen_keys(_urandom=os.urandom, _encode=base64.b32encode, _randint=np.random.randint):
        while True:
            _randint(20)
            yield _encode(_urandom(20))[:20].decode('ascii')
    return list(islice(unique_everseen(gen_keys()), amount_of_keys))


def produce_amount_keys_random(string_chars):
    return ''.join(random.choices(string_chars, k=20))

def create_file(size, variety, width):

    mapping = {
        'size': {
            'small': 2,
            'large': 4,
        },
        'variety': {
            'unique': -1,
            'random': 1,
        },
        'width': {
            'small': 1,  # (x3)
            'large': 2,  # (x3)
        }
    }

    if size not in mapping['size']:
        raise ValueError("Size should be {}".format(list(mapping['size'])))
    if variety not in mapping['variety']:
        raise ValueError("Variety should be {}".format(list(mapping['variety'])))
    if width not in mapping['width']:
        raise ValueError("Width should be {}".format(list(mapping['width'])))

    string_chars = string.ascii_lowercase + string.ascii_uppercase
    date_format = "%Y-%m-%d %H:%M:%S"
    stime = time.mktime(time.strptime("2018-01-01 00:00:00", date_format))
    etime = time.mktime(time.strptime("2020-01-01 00:00:00", date_format))

    v = mapping['variety'][variety]
    s = mapping['size'][size]
    w = mapping['width'][width]
    if v < 1:

        eint = 10000000
        gen_p = string_chars
        gen_str = produce_amount_keys_random
    else:
        gen_p = produce_amount_keys(v*2)
        gen_str = random.choice
        etime = stime + v*2
        eint = v*2
    tmp_header = ['c{}'.format(i)for i in range(w*3)]
    tmp_data = []
    for i in range(s):
        tmp_row = []
        for j in range(w):
            tmp_row.extend(
                [
                    random.randint(1, eint),
                    gen_str(gen_p),
                    datetime.utcfromtimestamp(int( stime + random.random() * (etime - stime)))
                ]
            )
        tmp_data.append(tmp_row)

    df = pd.DataFrame(tmp_data, columns = tmp_header)
    save_path = os.path.join(
        os.path.dirname(__file__),
        'data',
        '{}_{}_{}.csv'.format(size, variety, width)
    )
    os.makedirs(os.path.dirname(save_path),exist_ok=True)
    df.to_csv(save_path,index=False)

    print("Save file to: {}".format(save_path))
    return save_path


create_file('small', 'unique', 'small')
create_file('small', 'unique', 'large')
create_file('large', 'unique', 'small')
create_file('large', 'unique', 'large')
create_file('small', 'random', 'small')
create_file('small', 'random', 'large')
create_file('large', 'random', 'small')
create_file('large', 'random', 'large')

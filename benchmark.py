import time
import sys
from create_files import create_output_path


def fib(n):
    a, b = 0, 1
    for _ in range(n):
        yield a
        a, b = b, a + b


def do_benchmark(s, v, w):
    load_path = create_output_path(s, v, w)
    for i1 in range(100):
        for i2 in range(100):
            for i3 in range(100):
                for i4 in range(10):
                    x = i2*i1-i3-i4+i3*i2-i1-i4


def main():
    ps = sys.argv[1]
    pv = sys.argv[2]
    pw = sys.argv[3]
    do_benchmark(ps, pv, pw)


if __name__ == '__main__':
    main()
